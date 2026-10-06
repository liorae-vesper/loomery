#!/usr/bin/env bash
# Waits until the test services answer, so `mise run test-services` never races
# a still-booting Keycloak. Overridable to match the compose ports.
set -euo pipefail

nats_monitor="${LOOMERY_TEST_NATS_MONITOR:-http://127.0.0.1:8222}"
keycloak_url="${LOOMERY_TEST_KEYCLOAK_URL:-http://127.0.0.1:8080}"
realm="${LOOMERY_TEST_KEYCLOAK_REALM:-loomery}"

wait_for() {
    local name="$1" url="$2"
    for _ in $(seq 1 90); do
        if curl -fsS "$url" >/dev/null 2>&1; then
            echo "[test-services] $name is ready"
            return 0
        fi
        sleep 2
    done
    echo "[test-services] $name did not become ready: $url" >&2
    return 1
}

wait_for nats "$nats_monitor/healthz"
wait_for keycloak "$keycloak_url/realms/$realm"
