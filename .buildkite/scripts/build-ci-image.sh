#!/bin/sh
# Keep all image build layers (including mise's installed dependencies) in the
# hosted volume. A docker-container builder supports the local cache exporter
# independently of the host daemon's storage driver.
set -eu

: "${LOOMERY_CI_IMAGE:?LOOMERY_CI_IMAGE must be set}"
cache_dir=/ci-cache/buildkit
next_cache_dir=/ci-cache/buildkit-next

builder=$(docker buildx create --driver docker-container)
cleanup() {
    docker buildx rm "$builder" >/dev/null 2>&1 || true
}
trap cleanup EXIT

set --
if [ -f "$cache_dir/index.json" ]; then
    set -- --cache-from "type=local,src=$cache_dir"
fi

# Export to a new directory, then replace the previous cache only on success.
# This also discards obsolete layers instead of growing the cache indefinitely.
rm -rf "$next_cache_dir"
docker buildx build --builder "$builder" --load --progress plain \
    --file .buildkite/Dockerfile --tag "$LOOMERY_CI_IMAGE" \
    "$@" --cache-to "type=local,dest=$next_cache_dir,mode=max" .
rm -rf "$cache_dir"
mv "$next_cache_dir" "$cache_dir"

docker save --output loomery-ci.tar "$LOOMERY_CI_IMAGE"
gzip -1 -f loomery-ci.tar
