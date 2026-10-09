#!/usr/bin/env bash
# SPDX-License-Identifier: MPL-2.0
#
# The system packages the Rust build needs that a GitHub-hosted runner does not
# already carry. `.github/workflows/ci.yml` runs this before `jdx/mise-action`,
# so no job has to repeat the list.
#
#   * clang, libclang-dev — librocksdb-sys runs bindgen at build time (the
#     `bindgen-runtime` feature), and clang-sys dlopens `libclang.so`.
#   * protobuf-compiler — `loomery-shell`'s `build.rs` compiles
#     `proto/raft.proto` with tonic-prost-build, whose prost-build dependency
#     shells out to `protoc`.
#   * zlib1g-dev — RocksDB links zlib for its compression code; without the
#     system headers it builds the vendored copy instead, which is slower for
#     no benefit.
#
# gcc/g++/make and pkg-config already ship in the runner image, which is why
# the Buildkite image this replaces only had to add the same handful of
# packages on top of Debian (see git history: .buildkite/Dockerfile).
#
# Idempotent: packages that are already installed are skipped, so a prebuilt
# runner image (or a second run in the same job) costs nothing.
set -euo pipefail

packages=(
    clang
    libclang-dev
    protobuf-compiler
    zlib1g-dev
)

missing=()
for package in "${packages[@]}"; do
    if ! dpkg-query -W -f='${Status}' "$package" 2>/dev/null | grep -q "install ok installed"; then
        missing+=("$package")
    fi
done

if [ "${#missing[@]}" -eq 0 ]; then
    echo "install-host-deps: every package is already installed"
    exit 0
fi

sudo=()
if [ "$(id -u)" -ne 0 ]; then
    if ! command -v sudo >/dev/null 2>&1; then
        echo "install-host-deps: ${missing[*]} missing, and no root or sudo to install them" >&2
        exit 1
    fi
    sudo=(sudo)
fi

echo "install-host-deps: installing ${missing[*]}"
"${sudo[@]}" apt-get update
"${sudo[@]}" apt-get install --yes --no-install-recommends "${missing[@]}"
