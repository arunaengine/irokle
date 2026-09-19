#!/usr/bin/env bash
# All children share the launcher's enforced cap; failure reaches its caller.
set -euo pipefail
repo=${1:?repository required}
output=${2:?new result directory required}
case_file=${3:?new manifest path required}
set -o noclobber
python3 -B "$(dirname -- "$0")/matrix.py" > "$case_file"
exec bash "$(dirname -- "$0")/checks.sh" "$repo" "$output" "$case_file"
