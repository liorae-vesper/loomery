#!/usr/bin/env bash
#
# The `test services` step: Keycloak + NATS JetStream integration suite, run
# from inside the CI image (see .buildkite/Dockerfile).
#
# The GitHub Actions job this replaces started the compose stack, waited for
# readiness, ran the tests, and tore the stack down again with an `if: always()`
# step. A trap does the same on every exit path here, including a readiness
# timeout.
#
# The step provides the two things this needs from its host:
#
#   * the Docker socket, so `docker compose` below drives the host daemon. The
#     compose file's relative bind mount therefore resolves on the host.
#   * host networking, so the ports the stack publishes —
#     4222/8222 for NATS, 8080 for Keycloak — are reachable at 127.0.0.1.
#
# The URLs are overridable, matching `mise run test-services`.
set -euo pipefail

export LOOMERY_TEST_NATS_URL="${LOOMERY_TEST_NATS_URL:-nats://127.0.0.1:4222}"
export LOOMERY_TEST_KEYCLOAK_URL="${LOOMERY_TEST_KEYCLOAK_URL:-http://127.0.0.1:8080}"

cleanup() {
    local status=$?
    # Leave the stack's own state and logs in the job log when anything failed —
    # the wait script dumps them for a readiness timeout, and this covers a failure
    # later in the step (the integration suite, or a container that died mid-run).
    if [ "$status" -ne 0 ]; then
        docker compose -f compose.test.yaml ps || true
        docker compose -f compose.test.yaml logs --no-color --tail 200 || true
    fi
    docker compose -f compose.test.yaml down -v || true
    exit "$status"
}
trap cleanup EXIT

docker compose -f compose.test.yaml up -d
bash scripts/test-services/wait-for-services.sh
cargo test -p loomery-shell --features test-services --test test_services
