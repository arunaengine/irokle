#!/usr/bin/env bash
# Preserve the Python runner's status through shell launchers.
set -euo pipefail
exec python3 "$(dirname -- "$0")/checks.py" "$@"
