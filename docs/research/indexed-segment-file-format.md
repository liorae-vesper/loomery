---

> **Research note for the Loomery project.** Informs decision D2 (storage).

# Indexed Segment File Format

A zero-extra-file index footer embedded in sealed segment files, enabling fast
recovery and selective lazy loading for the Loomery storage engine
(`RaftLogStorage` behind OpenRaft).

---

## 1. Motivation

Sequential recovery that reads every frame and CRC-checks each one is slow:
for 200,000 entries (~20 MB) it takes ~150–200 ms. SQLite and RocksDB recover
in ~0–120 ms because their index structures are already on disk.

The goal is to match that recovery speed **without adding extra files** to the
backup story. Solution: embed a small index block at the end of each sealed
segment file — analogous to RocksDB SST index blocks or SQLite B-tree pages.

---

## 2. Inspiration: the unified-log storage architecture

The segment-file design cribs the **WAL + memtables + segment writer +
snapshot writer** pattern from the unified-log architecture (the one `ra`
made famous, and that OpenRaft's storage examples follow).

### Storage flow (simplified)

```
All writes go through a single WAL:
  1. Entry appended to the live WAL file
  2. Entry also written to the per-group in-memory index
  3. Periodic fsync batches writes across all Raft groups
  4. On WAL rollover, the old file + index pass to the segment writer
  5. Segment writer flushes the index to per-group segment files on disk
  6. Old WAL file and index entries are dropped
```

### Key design points adopted

| Concept | Our adaptation |
|---|---|
| Single batched-fsync WAL | Single async flush task (`tokio::task::spawn_blocking`) |
| In-memory memtables for hot reads | `BTreeMap<u64, Offset>` index for log-index-ordered scans |
| Sealed segment files on rollover | Sealed `.seg` files with embedded footer |
| Recovery by replaying segments | Fast footer-driven recovery with optional skip-CRC |
| Per-server segments | Shared segments keyed by `(group_id, log_index)` |

### Key deviation

The classic WAL is **synchronous** — callers block on fsync. Our periodic
flush decouples the hot path from durability: the append returns after an
in-memory index insert (~µs), and a background task flushes pending entries to
the WAL on a count threshold or timer. That buys ~500k async appends/s at the
cost of a bounded durability window (the Raft durability guarantee still comes
from OpenRaft's commit path).

The indexed footer is an **independent enhancement** to the segment file
format that benefits both synchronous and periodic-flush designs.

---

## 3. Format Specification

A sealed segment file with an index footer has five sections:

```
┌────────────────────────────────────────────────┐
│ Frame 1                                         │
│   CRC32 (4 bytes) │ Length (4 bytes) │ Payload  │
├────────────────────────────────────────────────┤
│ Frame 2                                         │
├────────────────────────────────────────────────┤
│ ...                                             │
├────────────────────────────────────────────────┤
│ Frame N                                         │
├────────────────────────────────────────────────┤
│ Index block                                     │
│   bincode(map[(stream, idx) -> offset, ...])     │
├────────────────────────────────────────────────┤
│ Index length (4 bytes, big-endian u32)          │
├────────────────────────────────────────────────┤
│ Magic word (4 bytes: "SIDX")                     │
└────────────────────────────────────────────────┘
```

### 3.1 Frames

Each frame:

```
crc32(be32) || len(be32) || payload(len)
```

- `crc32` = CRC32 (`crc32fast`) of `len || payload`
- `payload` = `bincode` of `{ s: stream, i: index, d: data }` (or serde_json —
  see D3 in `design.md`; the format is serializer-agnostic)

### 3.2 Index block

A `bincode`-serialized map:

```rust
HashMap::from([
    (("stream-a".into(), 1), 0),     // offset of frame 1 in bytes
    (("stream-a".into(), 2), 108),   // offset of frame 2
    (("stream-b".into(), 1), 216),   // offset of frame 3
    // ...                           // 200,000 entries → ~4 MB
])
```

Key: `(String, u64)` — stream (group/tenant) + log index.
Value: `u64` — byte position of the frame's first byte in the file.

### 3.3 Footer structure

```
index_block || index_len(be32) || "SIDX"
```

- `index_block` — the serialized index map (variable length)
- `index_len` — byte size of `index_block` (big-endian u32)
- `"SIDX"` — 4-byte magic word identifying the format

### 3.4 Reading the footer

```rust
fn read_index(bytes: &[u8]) -> Option<HashMap<(String, u64), u64>> {
    let footer_start = bytes.len() - 8; // skip magic (4) + len (4)
    let index_len = u32::from_be_bytes(bytes[footer_start..footer_start + 4].try_into().ok()?) as usize;
    let index_start = footer_start - index_len;
    bincode::deserialize(&bytes[index_start..footer_start]).ok()
}
```

### 3.5 Integrity

The `"SIDX"` magic acts as a quick integrity check: if present, the segment was
cleanly sealed; if missing (old format or torn write), recovery falls back to
full CRC decode. CRC verification of individual frames is optional when the
footer is present (clean fsync on seal); for defense in depth, CRC can still be
checked on first lazy load.

---

## 4. Recovery strategies

### 4.1 Full load

Load all entries from all segments into the in-memory index on restart:

```rust
fn recover(bytes: &[u8]) -> Vec<Frame> {
    if has_footer(bytes) {
        Frame::decode_all_skip_crc(&bytes[..data_end(bytes)])
    } else {
        Frame::decode_all(bytes)
    }
}
```

**Benchmark (200,000 entries, ~20 MB):**

| Method | Decode time | Entries/s |
|---|---|---|
| Full CRC | ~190 ms | ~1.0M |
| Skip-CRC (footer) | ~150 ms | ~1.3M |
| **Speedup** | **~1.2×** | |

### 4.2 Lazy load (recommended for production)

On restart, read only the footer (~4 MB for 200k entries, ~5 ms), then load
individual frames on first access by seeking to their recorded offset:

```rust
// On restart: just read the footer (~5 ms)
let index = IndexedSegment::read_index(&bytes).unwrap();

// On first get(): seek and read exactly one frame
let mut frame = vec![0u8; 8 + len];
FileExt::read_exact_at(&file, &mut frame, offset)?; // pread
let (_crc, payload) = split_frame(frame);
Ok(bincode::deserialize::<FramePayload>(payload)?)
```

Recovery is then limited by: reading the footer (~5 ms), deserializing the
index (<1 ms), and random `pread` per frame afterward.

### 4.3 Hybrid (recommended for tenant workloads)

For streams queried immediately (active tenants), load eagerly via the offset
index; for idle streams, lazy-load on first access. The index can be
partitioned per stream (eager vs lazy lists).

---

## 5. Implementation notes

- **Encoding (write):** at seal time (WAL rollover), read all frames, build the
  index map, write `frames || index || index_len || "SIDX"` atomically (write
  tmp file + rename).
- **Skip-CRC decode:** locate the footer, strip it, sequential scan without CRC.
- **Lazy get:** keep the index in memory (`Arc<HashMap<...>>`); each access is
  one `pread`.
- **Format versioning:** `"SIDX"` is the format identifier; a format change
  ships a new magic (e.g. `"SID2"`) and recovery dispatches on it.

## 6. Benchmarks (targets on NVMe)

| Method | 10k | 50k | 200k | 1M |
|---|---|---|---|---|
| Full CRC decode | ~8 ms | ~43 ms | ~190 ms | ~1 s |
| Skip-CRC decode | ~6 ms | ~34 ms | ~150 ms | ~800 ms |
| Footer-only (no frames) | <1 ms | ~1 ms | ~5 ms | ~25 ms |

Footer overhead scales linearly with entry count: 0.5% at 10k entries, 20% at
200k, 50% at 1M. For the target workload (~10³–10⁵ entries per workspace) the
footer is 0.5%–20% of file size — acceptable for the recovery-speed gain.

### Comparison with databases

| Engine | Recovery (200k) | Index on disk? | Extra files? |
|---|---|---|---|
| SQLite (WAL mode) | ~0 ms | ✅ B-tree pages | No |
| RocksDB | ~120 ms | ✅ SST index blocks | No |
| Segment files (original) | ~150–200 ms | ❌ Rebuilt from raw frames | No |
| Segment files (footer, full load) | ~150 ms | ✅ Footer index | No |
| Segment files (footer, lazy load) | ~5 ms | ✅ Footer index | No |

## 7. Design decisions

1. **Why not separate index files?** A `.idx` file per segment doubles file
   count, complicates backup (two files per segment, or a re-index script), and
   risks cache coherency. Embedding keeps the backup story "copy the `.seg`
   files."
2. **Why `bincode` for the index?** Compact (~20 bytes/entry), deterministic
   (enables checksum comparison across nodes), and the same serializer as the
   payloads. A custom varint encoding would be smaller (~8 bytes/entry) but
   adds code complexity.
3. **Why not build the index incrementally?** The rollover-time scan is cheap
   (~1 ms per 1000 entries) and keeping index construction off the hot write
   path minimizes corruption risk.

## 8. Files

| File | Purpose |
|---|---|
| `storage/src/segment/indexed.rs` | `IndexedSegment` — encode, `read_index`, `recover`, lazy get |
| `storage/src/segment/frame.rs` | `Frame` — encode/decode (CRC and skip-CRC) |
| `storage/src/segment/writer.rs` | WAL rollover + footer sealing (flush task) |
| `storage/src/segment/engine.rs` | Recovery with footer detection (OpenRaft `RaftLogStorage` impl) |
| `storage/benches/` | Recovery / footer overhead benchmarks |

## 9. Related documents

| Document | Location |
|---|---|
| Storage Engine Alternatives | `storage-engine-alternatives.md` (this directory) |
| OpenRaft storage interfaces | `openraft-storage.md` (this directory) |
| Loomery design (D2/D3) | `../design.md` |
| Unified-log pattern (inspiration) | https://github.com/rabbitmq/ra/blob/main/docs/internals/INTERNALS.md |

*Compiled: 2026-08 (reshaped for the Rust/Tokio/OpenRaft stack).*