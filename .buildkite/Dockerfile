# syntax=docker/dockerfile:1
#
# The CI image for Loomery.
#
# Every step of .buildkite/pipeline.yml runs inside this image, so no job
# installs anything at run time. The GitHub Actions workflow this replaced ran
# scripts/install-host-deps.sh on every runner for exactly the two packages
# that are baked in here instead.
#
# What is inside, and why:
#
#   * protobuf-compiler — loomery-shell's `build.rs` compiles proto/raft.proto
#     with tonic-prost-build, whose prost-build dependency shells out to protoc.
#   * clang + libclang-dev — librocksdb-sys runs bindgen at build time (the
#     `bindgen-runtime` feature), and clang-sys dlopens libclang.so.
#   * build-essential, pkg-config, zlib1g-dev — the RocksDB archive below is
#     compiled once here, and RocksDB builds zlib/lz4/bzip2 from the vendored
#     sources when no system library is found.
#   * the RocksDB archive at /opt/rocksdb that `librocksdb-sys` would otherwise
#     compile inside every step, with ROCKSDB_LIB_DIR set so the build script
#     links it instead of building it. See the prebuild section below.
#   * mise plus the toolchain that mise.toml/mise.lock pin — the Rust toolchain
#     (rust 1.98.1) and the cargo tools the mise tasks call: cargo-audit,
#     cargo-bundle-licenses, cargo-crap, cargo-deny, cargo-llvm-cov. The
#     lockfile is committed and `locked = true` is set, so this installs
#     exactly what a local `mise install` installs. `mr-boxington` comes from
#     there too; mise.toml turns it on with the Rust tool's `mr_boxington`
#     option, so the steps share one compiler-action store instead of
#     recompiling whatever the cache volume did not keep.
#   * docker-ce-cli and docker-compose-plugin — the `test services` step starts
#     the compose stack through the host daemon, over the socket the pipeline
#     mounts.
#
# Build it from the repository root (the pipeline's `build-ci-image` step runs
# this exact command):
#
#   docker build --file .buildkite/Dockerfile --tag loomery-ci .

FROM debian:bookworm-slim

# Pinned to the mise release this repository is developed against. Tool
# versions come from mise.lock, not from here.
ARG MISE_VERSION=v2026.9.9

# The mise shims go first on PATH because the plugin runs commands through a
# non-interactive shell, where `mise activate` never runs. mise's data directory
# is /usr/local/share/mise so the tools it installs are not scattered through a
# home directory; the Rust toolchain itself stays where rustup puts it, under
# /root, and the steps run as root (the plugin's default) because of that.
# MISE_TRUSTED_CONFIG_PATHS covers the plugin's default workdir: mise only loads
# `[tasks]`/`[env]` from a trusted config, and `mise run <task>` is how every CI
# step is invoked.
ENV MISE_DATA_DIR=/usr/local/share/mise \
    MISE_INSTALL_PATH=/usr/local/bin/mise \
    MISE_TRUSTED_CONFIG_PATHS=/workdir \
    MISE_YES=1 \
    PATH=/usr/local/share/mise/shims:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

# System build dependencies; see the header for why each one is here.
RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
        build-essential \
        ca-certificates \
        clang \
        curl \
        git \
        libclang-dev \
        pkg-config \
        protobuf-compiler \
        python3 \
        zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

# The Docker CLI and the `docker compose` plugin, from Docker's own Debian
# repository (Debian's own `docker.io` package ships no compose v2 plugin).
RUN install -m 0755 -d /etc/apt/keyrings \
    && curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc \
    && chmod a+r /etc/apt/keyrings/docker.asc \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/debian bookworm stable" \
        > /etc/apt/sources.list.d/docker.list \
    && apt-get update \
    && apt-get install --yes --no-install-recommends \
        docker-ce-cli \
        docker-compose-plugin \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://mise.run | MISE_VERSION="$MISE_VERSION" sh

# The toolchain, from the committed config + lockfile. `mise install` fails
# loudly on a missing or stale lock entry, which keeps the image in step with
# what contributors run locally.
WORKDIR /app
COPY mise.toml mise.lock ./
RUN mise install

# The RocksDB archive every step links against.
#
# `librocksdb-sys` compiles RocksDB's C++ sources in its build script, and that
# compile costs about five minutes on the CI agents. Build #25 spent 5m 47s in
# `cargo check -p loomery-shell`, some 313s of it with this single build script
# as the only unit still running, and build #24 spent 5m 20s the same way —
# while the same command finished in 3.5s when the build script was fresh. The
# only thing that could amortize it was a cache volume, and Buildkite attaches
# those best-effort and commits them only for a step that succeeds, so they
# cannot be relied on. Build the archive once here instead: when ROCKSDB_LIB_DIR
# is set the build script takes `try_to_find_and_link_lib` and returns before
# `build_rocksdb()`. bindgen still runs per configuration (measured 0.8s) and
# writes the bindings; no C++ is compiled, and the mtime-tracked
# `rerun-if-changed=rocksdb/` that made the build script re-run is never
# declared either.
#
# Only the manifests are copied, so this layer survives every source change and
# is invalidated only by a manifest, a lockfile or a dependency change. The
# member manifests are copied one by one — a glob would flatten the tree into a
# single `crates/` — and the count is checked against the lockfile's path
# packages below, so adding a workspace member fails this build loudly instead
# of silently narrowing what `-p librocksdb-sys` resolves. Cargo refuses a
# workspace member with no targets, which is why each member also gets an empty
# source file; nothing except `librocksdb-sys` is built from them.
#
# The feature set needs no hand-keeping: `-p librocksdb-sys` resolves the
# features the workspace asks of it (`static`, `bindgen-runtime`, `lz4` — the
# last from crates/shell/Cargo.toml), and cargo rejects `--features` for a
# package outside the workspace, so the manifests stay the only place that
# decides it. The check below pins `lz4`, the one compression backend the
# workspace asks for, so an archive built without it cannot ship quietly.
#
# The build runs in /app because `cargo` here is mise's shim and mise resolves
# the toolchain from the config beside the working directory: in a directory
# without one it exits with "No version is set for shim: cargo". /app already
# has mise.toml and mise.lock, and everything this step copies in is removed
# again below.
COPY Cargo.toml Cargo.lock ./
COPY crates/core/Cargo.toml crates/core/
COPY crates/genesis/Cargo.toml crates/genesis/
COPY crates/server/Cargo.toml crates/server/
COPY crates/shell/Cargo.toml crates/shell/
# CARGO_TARGET_DIR is set explicitly because mbx leaves a target directory the
# user chose alone, and a managed one would be a link into the store below. The
# registry, the target directory and the mbx store are all scratch: every step
# mounts its own, so none of it belongs in the image.
RUN set -eux; \
    test "$(python3 -c 'import tomllib; print(sum(1 for p in tomllib.load(open("Cargo.lock", "rb"))["package"] if "source" not in p))')" \
        = "$(set -- crates/*/Cargo.toml; echo $#)"; \
    for manifest in crates/*/Cargo.toml; do \
        dir="$(dirname "$manifest")"; \
        mkdir -p "$dir/src"; \
        : > "$dir/src/lib.rs"; \
    done; \
    CARGO_TARGET_DIR=/app/target cargo build --locked -p librocksdb-sys; \
    archive=""; \
    for candidate in /app/target/debug/build/librocksdb-sys-*/out/librocksdb.a; do \
        archive="$candidate"; \
        break; \
    done; \
    test -f "$archive"; \
    install -D -m 0644 "$archive" /opt/rocksdb/lib/librocksdb.a; \
    nm --undefined-only /opt/rocksdb/lib/librocksdb.a | grep -q LZ4_; \
    rm -rf /app/target /app/Cargo.toml /app/Cargo.lock /app/crates \
        /root/.cargo/registry /root/.cargo/git /root/.cache/mbx

# ROCKSDB_STATIC's value is not read — its presence selects the static link mode
# in `try_to_find_and_link_lib`. The stdc++ directive that the skipped
# `build_rocksdb()` would have emitted comes from
# `link_cxx_runtime_of_a_prebuilt_rocksdb` in crates/shell/build.rs.
ENV ROCKSDB_LIB_DIR=/opt/rocksdb/lib \
    ROCKSDB_STATIC=1

# The RustSec advisory database, cloned once here rather than fetched by the
# `audit` step at run time. Fetching it into the mounted cache volume failed on
# the hosted agent with `I/O operation failed: Device or resource busy (os error
# 16)` — libgit2's fetch is not happy on that mount — and the database is only as
# fresh as this image anyway, which is built once per pipeline run. The `.git`
# directory is dropped: `cargo audit --no-fetch` reads the files, and the history
# is dead weight in a layer.
RUN git clone --depth 1 https://github.com/RustSec/advisory-db.git /opt/rustsec-advisory-db \
    && rm -rf /opt/rustsec-advisory-db/.git

# rustup's default profile brings rustfmt and clippy, but cargo-llvm-cov (the
# `quality` step) needs the llvm-tools component, which is not part of it, and
# which cargo-llvm-cov otherwise prompts for and downloads mid-build. rustup
# lists it as `llvm-tools-<host>`, and installs it as `llvm-tools-preview`.
RUN rustup component add llvm-tools-preview

# Fail the build rather than ship an image with a half-installed toolchain.
# The cargo tools are checked through the mise shims, so this also covers mise
# resolving the shims and the pinned toolchain from the config above.
RUN set -eux; \
    protoc --version; \
    rustc --version; \
    cargo --version; \
    cargo fmt --version; \
    cargo clippy --version; \
    cargo llvm-cov --version; \
    cargo deny --version; \
    cargo audit --version; \
    cargo crap --version; \
    cargo bundle-licenses --version; \
    mise --version; \
    mbx --version; \
    docker --version; \
    docker compose version; \
    nm --version; \
    test -f /opt/rocksdb/lib/librocksdb.a; \
    llvm_bin="$(rustc --print sysroot)/lib/rustlib/$(rustc -vV | awk '/^host: /{print $2}')/bin"; \
    test -x "$llvm_bin/llvm-cov"; \
    test -x "$llvm_bin/llvm-profdata"
