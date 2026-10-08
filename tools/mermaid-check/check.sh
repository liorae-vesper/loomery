#!/usr/bin/env sh
# SPDX-License-Identifier: MPL-2.0
#
# Runs the Mermaid diagram check over the documentation, installing the checker's
# dependencies on first use. Arguments are passed through to `check.mjs`:
#
#   sh tools/mermaid-check/check.sh docs README.md   # parse only
#   sh tools/mermaid-check/check.sh --render docs    # also render to out/
set -eu

cd "$(dirname "$0")/../.."

if [ ! -d tools/mermaid-check/node_modules ]; then
    npm ci --prefix tools/mermaid-check --no-audit --no-fund
fi

exec node tools/mermaid-check/check.mjs "$@"
