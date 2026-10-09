#!/usr/bin/env bash
# Waits until the test services answer, so `mise run test-services` never races
# a still-booting Keycloak. Ports are overridable to match the compose stack.
#
# Keycloak is waited for in two steps — the master realm, then ours — because the
# two failures mean different things: the first never answers if Keycloak did not
# boot at all, and the second never answers if it booted *without* importing the
# realm. The message says which, and the stack's own state and logs are printed
# before giving up, so a timeout in CI is diagnosable from the job log alone.
#
# `LOOMERY_TEST_WAIT_ATTEMPTS` (default 150, two seconds apart) is the budget. It
# is generous because a hosted agent can be slow to boot a JVM, and a timeout now
# names the half that failed rather than being ambiguous.
set -euo pipefail

nats_monitor="${LOOMERY_TEST_NATS_MONITOR:-http://127.0.0.1:8222}"
keycloak_url="${LOOMERY_TEST_KEYCLOAK_URL:-http://127.0.0.1:8080}"
realm="${LOOMERY_TEST_KEYCLOAK_REALM:-loomery}"
attempts="${LOOMERY_TEST_WAIT_ATTEMPTS:-150}"
interval="${LOOMERY_TEST_WAIT_INTERVAL:-2}"

# Prints what the stack is doing, so a timeout is not a dead end.
dump_stack() {
    local compose_file="${LOOMERY_TEST_COMPOSE_FILE:-compose.test.yaml}"
    [ -f "$compose_file" ] || return 0
    command -v docker >/dev/null 2>&1 || return 0
    echo "[test-services] stack state:" >&2
    docker compose -f "$compose_file" ps >&2 || true
    echo "[test-services] stack logs:" >&2
    docker compose -f "$compose_file" logs --no-color --tail 100 >&2 || true
}

wait_for() {
    local name="$1" url="$2"
    for _ in $(seq 1 "$attempts"); do
        if curl -fsS "$url" >/dev/null 2>&1; then
            echo "[test-services] $name is ready"
            return 0
        fi
        sleep "$interval"
    done
    echo "[test-services] $name did not become ready: $url" >&2
    dump_stack
    return 1
}

wait_for nats "$nats_monitor/healthz"
# The master realm exists as soon as Keycloak serves HTTP; ours exists only once
# the realm in the image has been imported.
wait_for keycloak "$keycloak_url/realms/master"
wait_for "the $realm realm" "$keycloak_url/realms/$realm"
