# Consensus failure injection

Run the fault experiment against the production OpenRaft, tonic and RocksDB
path, with three independent replica processes:

```sh
mise run test-consensus-failures
mise run test-consensus-failures -- \
  --batch-sizes 1 8 16 32 64 128 --trials 3 \
  --output benchmark-results/failure-matrix
```

Defaults test checkpoint and snapshot persistence, command batch limits 1 and
32, concurrency 128 and 256 commands per write phase. Use `--modes`,
`--batch-sizes`, `--concurrency`, `--operations`, `--trials` and `--scenarios`
to select the experiment. Operations must exceed each batch limit, concurrency
must cover it, and the full command set must fit the 4096-command dedup window.
Each case starts with fresh databases. Output directories must be new.

## Injected failures

| Scenario | Injection | Required result |
|---|---|---|
| `leader_crash` | SIGKILL the leader after observing the first successful reply from a streaming write phase | Elect a leader and retry on the surviving majority before restarting the old leader; preserve acknowledged commands |
| `follower_crash` | SIGKILL one follower at the same point | The remaining majority completes every write; the restarted follower catches up |
| `quorum_loss` | SIGKILL both followers during streaming writes | Fresh commands issued after both processes have exited receive no successful acknowledgement; retries work once the majority returns |
| `reply_loss` | Submit a complete write phase and deliberately discard its successful response | Every retry returns `Replayed` at its original index and produces no duplicate event |

The coordinator retains each observed streaming response before injecting the
crash. A reply received after followers die can still describe an entry committed
before the crash. The quorum test therefore issues eight *new* commands after
both followers have exited and requires zero successes for that phase.

Failure-phase proposals have a one-second deadline; recovery retries have a
ten-second deadline. Timing out does not cancel an admitted command. After
quorum returns, earlier queued commands may execute ahead of retries. Reports
retain the failure samples and the distinct recovery timeout.
Recovery also reselects the leader and retries transient routing/timeouts with
bounded backoff, retaining every attempt. Any command acknowledged by a recovery
attempt must keep its original index on subsequent retries.

Errors and missing replies are unknown outcomes. The runner retries the complete
original command set with identical causation keys and payloads, including
commands not yet admitted when the process died. It allows either an append or
a replay for unacknowledged commands. Previously acknowledged commands must
replay at their original Raft index. This is bounded-window dedup verification;
it does not claim unlimited exactly-once delivery.

Every case verifies the exact expected event identities, rejects missing or
duplicate events, and compares complete event contents/order across all three
replicas. It then SIGKILLs the entire cluster, reopens the same RocksDB databases
on the same membership ports, repeats the replay/index checks and verifies
replica convergence again. No final snapshot or graceful database close is
requested. Synchronized WAL writes, quorum requirements and persistence-mode
protection remain enabled.

## Storage callback failures

The Rust append tests add controlled injection at the storage API boundary:

```sh
mise exec -- cargo test -p loomery-shell raft::append_tests
```

- Hold a genuine OpenRaft flush callback after entries become readable: batched
  client replies must remain pending until the callback succeeds.
- Complete that callback with an injected WAL-sync error: both batched clients
  receive unknown-outcome errors, Raft stops, and neither command is applied.
- Attempt a write through a real read-only RocksDB handle: append and its flush
  callback both report failure, preserving the existing log.

The WAL-sync error is a test fixture, not an OS-level fsync failure. Fault control
lives in tests and benchmark tooling; deployments gain no runtime fault flag.

## Artifacts and limits

The runner saves a manifest, lockfile and per-case effective configuration,
replica identities, stderr logs, observed responses, retry outcomes, recovered
metrics and retained databases under ignored `benchmark-results/`. Errors save
an unsuccessful report and stop the experiment. It only signals child processes
it created; it never accepts external replica PIDs or database paths.

Streaming control-pipe overhead is included in these fault phases, so their
latencies are not throughput results. The normal [performance benchmark](README.md)
continues to use its original non-streaming measurement path.

These tests cover process crashes, majority availability, lost replies and the
flush-callback contract. SIGKILL exercises WAL recovery without orderly closure,
but does not model host power loss. Network partitions/delay, disk-full errors,
corrupt WALs and precisely interrupted snapshot installation/purge remain
separate experiments to add.

## Validation on 2026-10-01

The four-scenario matrix passed all 48 cases: two persistence modes × six
batch limits (1/8/16/32/64/128), with 256 commands and concurrency 128.
A further 12 leader-crash cases passed the stronger requirement to retry on
the surviving majority before restarting the old leader. Every replica
recovered 288 expected events, or 296 in the quorum-loss cases including the
eight isolated commands retried after recovery. No acknowledged command lost
its original dedup/index, and no fresh isolated command was acknowledged.

Raw artifacts are retained in `benchmark-results/failure-matrix-20261001-v3/`
and `benchmark-results/failure-leader-majority-20261001/`. These runs used
uncommitted source based on `0665b64`; their manifests record the dirty files.
The storage callback tests, full workspace/all-target test suite, strict Clippy
and Python fault-oracle tests also passed. This is one run per case, not a
long-duration fault campaign.
