#!/bin/bash
# Query native Codex account metadata in a temporary, MCP-free home; never run a model.
# The outer collector deadline and process owner also cover the app-server child.
set -euo pipefail
directory=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
context=$(cat)
binary=$(printf '%s' "$context" | jq -er 'select(.version == 1) | .harness.command | select(type == "string" and startswith("/") and (contains("\u0000") | not))')
login=$(printf '%s' "$context" | jq -er '.auth | select(.kind == "native_login") | .directory | select(type == "string" and startswith("/") and (contains("\u0000") | not))')
metadata=$(printf '%s' "$context" | jq -ec '{models,now}')
umask 077
temporary=$(mktemp -d "${TMPDIR:-/tmp}/agent-run-quota.XXXXXXXX")
server_pid=
# Close protocol pipes and remove the private home; Rust owns process termination.
cleanup() {
  rm -rf -- "$temporary"
}
trap cleanup EXIT
trap 'exit 1' TERM INT
ln -s "$login/auth.json" "$temporary/auth.json"
printf 'cli_auth_credentials_store = "file"\n' > "$temporary/config.toml"
mkfifo "$temporary/input" "$temporary/output"
exec 3<>"$temporary/input"
exec 4<>"$temporary/output"
CODEX_HOME="$temporary" "$binary" app-server 3>&- 4>&- < "$temporary/input" > "$temporary/output" &
server_pid=$!
# Read the response for one known request, ignoring notifications but refusing RPC errors.
response() {
  local expected=$1 line
  while IFS= read -r -t 25 line <&4; do
    if printf '%s' "$line" | jq -e --argjson id "$expected" '.id == $id' > /dev/null; then
      printf '%s' "$line" | jq -e 'if has("error") then error("metadata request failed") else .result end'
      return
    fi
  done
  return 1
}
printf '%s\n' '{"id":1,"method":"initialize","params":{"clientInfo":{"name":"agent-run-quota","version":"1"},"capabilities":{"experimentalApi":true}}}' >&3
response 1 > /dev/null
printf '%s\n' '{"method":"initialized"}' '{"id":2,"method":"account/rateLimits/read"}' >&3
response 2 | jq -e -L "$directory" --argjson ctx "$metadata" -f "$directory/codex.jq"

# EOF requests normal app-server shutdown; the Rust deadline bounds an unresponsive child.
exec 3>&-
wait "$server_pid"
