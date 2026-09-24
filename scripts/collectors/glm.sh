#!/bin/bash
# Read version-1 private context on stdin; emit quota JSON, never credentials.
# An optional first argument overrides the quota URL for an operator or fixture.
set -euo pipefail
directory=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
context=$(cat)
token=$(printf '%s' "$context" | jq -er 'select(.version == 1) | .auth.token | select(type == "string" and length > 0 and (test("[\u0000-\u001f\u007f]") | not))')
case "$token" in *$'\n'*|*$'\r'*) exit 1 ;; esac
metadata=$(printf '%s' "$context" | jq -ec '{models,now}')
printf 'Authorization: %s\n' "$token" |
  curl -q --fail --silent --show-error --connect-timeout 10 --max-time 25 --max-filesize 2097152 --header @- \
    "${1:-https://api.z.ai/api/monitor/usage/quota/limit}" |
  jq -e -L "$directory" --argjson ctx "$metadata" -f "$directory/glm.jq"
