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
#   * build-essential, pkg-config, zlib1g-dev — librocksdb-sys compiles
#     RocksDB's C++ sources, and builds zlib/lz4/bzip2 from the vendored
#     sources when no system library is found.
#   * mise plus the toolchain that mise.toml/mise.lock pin — the Rust toolchain
#     (rust 1.98.1) and the cargo tools the mise tasks call: cargo-audit,
#     cargo-bundle-licenses, cargo-crap, cargo-deny, cargo-llvm-cov. The
#     lockfile is committed and `locked = true` is set, so this installs
#     exactly what a local `mise install` installs.
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
    docker --version; \
    docker compose version; \
    llvm_bin="$(rustc --print sysroot)/lib/rustlib/$(rustc -vV | awk '/^host: /{print $2}')/bin"; \
    test -x "$llvm_bin/llvm-cov"; \
    test -x "$llvm_bin/llvm-profdata"
