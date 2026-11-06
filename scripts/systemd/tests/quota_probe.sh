#!/usr/bin/env bash
# Exercise subscription probing with fake credentials and no network.
set -euo pipefail
umask 077
directory=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
temporary=$(mktemp -d "${TMPDIR:?}/quota-probe-test.XXXXXXXX")
trap 'rm -rf -- "$temporary"' EXIT
cp "$directory/agent-run-quota-probe.sh" "$temporary/probe.sh"
mkdir -p "$temporary/home/.agent-run/accounts/claude/fixture/claude-config"
printf '%s\n' '{"claudeAiOauth":{"accessToken":"synthetic-secret"}}' > \
    "$temporary/home/.agent-run/accounts/claude/fixture/claude-config/.credentials.json"
cat > "$temporary/claude.sh" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
input=$(cat)
printf '%s' "$input" | jq -e '.auth.kind == "token" and .auth.token == "synthetic-secret"' >/dev/null
case "${TEST_LEAK:-}" in
    value) printf '%s\n' '{"version":1,"windows":[{"pool":"synthetic-secret"}]}' ;;
    key) printf '%s\n' '{"version":1,"windows":[{"synthetic-secret":"value"}]}' ;;
    *) printf '%s\n' '{"version":1,"windows":[{"pool":"primary"}]}' ;;
esac
SH
cat > "$temporary/codex.sh" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
input=$(cat)
printf '%s' "$input" | jq -e '.auth.kind == "native_login" and (.auth.directory | endswith("/.codex"))' >/dev/null
printf '%s\n' '{"version":1,"windows":[]}'
[[ ${TEST_LATE_FAILURE:-} != 1 ]]
SH
context='{"version":1,"auth":{"kind":"claude_named","label":"fixture"},"fence":{"config":"a","account":"b"}}'
output=$(printf '%s' "$context" | HOME="$temporary/home" /bin/bash "$temporary/probe.sh")
printf '%s' "$output" | jq -e '.fence.config == "a" and .observation.windows[0].pool == "primary"' >/dev/null
for leak in value key; do
    if printf '%s' "$context" | HOME="$temporary/home" TEST_LEAK="$leak" /bin/bash "$temporary/probe.sh" >/dev/null 2>&1; then
        echo "credential echo was accepted" >&2
        exit 1
    fi
done
codex='{"version":1,"auth":{"kind":"codex_native"},"fence":{"config":"a","account":"b"}}'
output=$(printf '%s' "$codex" | HOME="$temporary/home" /bin/bash "$temporary/probe.sh")
printf '%s' "$output" | jq -e '.fence.account == "b" and .observation.version == 1' >/dev/null
if printf '%s' "$codex" | HOME="$temporary/home" TEST_LATE_FAILURE=1 /bin/bash "$temporary/probe.sh" >/dev/null 2>&1; then
    echo "late collector failure was accepted" >&2
    exit 1
fi
printf '%s\n' "quota probe checks passed"
