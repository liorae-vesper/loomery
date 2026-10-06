# Third-party licenses

Loomery ships one bundled notice, `THIRDPARTY.yml` at the repository root,
generated from the workspace dependency graph by
[cargo-bundle-licenses](https://github.com/sstadick/cargo-bundle-licenses). It
carries the license text of every dependency — transitively, including optional
features and crates that only exist for other targets — which is what most
licenses require when a binary is redistributed.

## Tasks

| Task | What it does |
|---|---|
| `mise run licenses` | Regenerates `THIRDPARTY.yml` in place |
| `mise run licenses-check` | Regenerates into a temporary file and fails on any difference (the CI gate) |

Both run the same command:

```sh
cargo bundle-licenses --format yaml --output THIRDPARTY.yml \
  --features test-services --prefer Apache-2.0,MIT \
  --previous THIRDPARTY.yml
```

`RUST_LOG=error` keeps the tool's per-crate confidence warnings out of the task
output.

### Why those flags

- **`--features test-services`.** The Keycloak and NATS adapters are optional, and
  their tree (reqwest, rustls, async-nats, …) is the only place the
  `CDLA-Permissive-2.0` root certificates appear. Without the feature the tool
  sees a *strict subset* of the graph and the gate would pass silently, so the
  feature is part of both generation and checking.
- **`--prefer Apache-2.0,MIT`.** Many crates offer a choice (`MIT OR Apache-2.0`).
  The preference makes the tool emit one entry per package instead of one per
  alternative, which keeps the bundle small. Apache-2.0 comes first because its
  text is identical for every project, which lets entries be filled
  authoritatively even when a crate publishes no license file of its own.
- **`--previous THIRDPARTY.yml`.** Reapplies the hand-written entries below to a
  freshly generated file, so regeneration is a no-op when nothing changed.

The check is a strict text comparison, not just a "no new packages" test: a
dependency version bump also fails it, because the regenerated file records the
new version.

## Hand-filled entries

Eleven packages declare a license but publish no license file in the crate —
and `r-efi` publishes none anywhere in its repository. Their texts were taken
from upstream and filled in by hand; `--previous` preserves them across
regenerations.

| Packages | License | Text source |
|---|---|---|
| `async-nats`, `jni`, `jni-macros`, `jni-sys-macros`, `librocksdb-sys`, `openraft`, `openraft-macros`, `r-efi` (5.3.0, 6.0.0), `rustls-platform-verifier-android` | Apache-2.0 | [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0.txt) — identical for every project |
| `tonic-prost` | MIT | [`hyperium/tonic` LICENSE](https://github.com/hyperium/tonic/blob/master/LICENSE) |

`librocksdb-sys` declares `MIT/Apache-2.0/BSD-3-Clause`; the choice made by
`--prefer` is recorded in the bundle, so the text filled in always matches the
declared license of that entry.

A regeneration that introduces a new package with a missing text fails the gate
with `NOT FOUND` in the diff. Fill it from upstream the same way and commit —
the tool then carries it forward on every later run.

## Limits

- The bundle is built from `cargo metadata`, so it includes dependencies of
  targets Loomery never builds (wasm32, Android, Windows): `webpki-root-certs`,
  `jni*`, `r-efi`, `rustls-platform-verifier-android`. That is deliberate — the
  notice is a superset of what any single artifact contains, and `cargo-deny`
  evaluates the same all-target graph.
- It reproduces license *texts*, not Apache `NOTICE` files, and it is not legal
  advice.
- `--prefer` attributes a crate offering a choice under one alternative
  (Apache-2.0, else MIT). Both are on the `deny.toml` allowlist, so the
  attribution stays inside policy.

## CDLA-Permissive-2.0

`webpki-root-certs` — the root-certificate data used by rustls' platform
verifier for `wasm32`, pulled in through reqwest — is licensed under the
Community Data License Agreement – Permissive – Version 2.0. Section 2.1
requires the agreement text to travel with redistributed data; `THIRDPARTY.yml`
contains it verbatim, so a release that ships the bundle (together with
[`LICENSE`](../LICENSE)) satisfies the condition.

The license is permissive — use, modify and share, with no restrictions on
Results — and it is not OSI-approved because it is a *data* license rather than
a software one. Fedora's license data accepted it in 2025-05.
