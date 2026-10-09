# SPDX-License-Identifier: MPL-2.0
#
# The test realm, baked into an image instead of bind-mounted from the checkout.
#
# `--import-realm` reads whatever is in /opt/keycloak/data/import on first boot, so
# the realm used to arrive through a bind mount of ./scripts/test-services/keycloak.
# That made the import depend on the daemon resolving a host path and on the
# container user being able to read it, and it failed *silently*: Keycloak booted,
# the realm never appeared, and the readiness probe timed out on a 404.
#
# In the image there is no host path to resolve, and a realm that cannot be copied
# fails the build instead of the suite. Change the realm by editing the JSON here
# and re-running `mise run test-services` (compose rebuilds it).
FROM quay.io/keycloak/keycloak:26.0

COPY loomery-realm.json /opt/keycloak/data/import/
