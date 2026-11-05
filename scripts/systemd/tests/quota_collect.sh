#!/usr/bin/env bash
# A late probe failure must never submit a quota observation.
set -euo pipefail
umask 077
directory=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
temporary=$(mktemp -d "${TMPDIR:?}/quota-collect-test.XXXXXXXX")
trap 'rm -rf -- "$temporary"' EXIT
source "$directory/agent-run-quota-collect.sh"
directory=$temporary
AR_BROKER_USER=fixture
AR_BROKER_HOME=$temporary
AR_BROKER_PATH=/usr/bin:/bin
AR_BROKER_TMP=$temporary
AR_STATE=$temporary
export LC_ALL=C
run_as() { shift; /usr/bin/env -i "$@"; }
report_failure() { :; }
cat > "$temporary/system_quota_ingest" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
directory=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
case "$1" in
    context) printf '%s\n' '{"fixture":true}' ;;
    ingest) cat >/dev/null; : > "$directory/ingested" ;;
    *) exit 1 ;;
esac
SH
cat > "$temporary/agent-run-quota-probe.sh" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
directory=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cat >/dev/null
printf '%s\n' '{"version":1,"windows":[]}'
printf '%s\n' "synthetic-private-diagnostic" >&2
[[ ! -e "$directory/fail-after-output" ]]
SH
chmod 700 "$temporary/system_quota_ingest" "$temporary/agent-run-quota-probe.sh"
: > "$temporary/fail-after-output"
if collect codex fixture fixture "$temporary" "$temporary" /usr/bin:/bin 2>"$temporary/stderr"; then
    echo "late probe failure was accepted" >&2
    exit 1
fi
[[ ! -e "$temporary/ingested" ]]
[[ ! -s "$temporary/stderr" ]]
rm "$temporary/fail-after-output"
collect codex fixture fixture "$temporary" "$temporary" /usr/bin:/bin
[[ -e "$temporary/ingested" ]]
printf '%s\n' "quota collection checks passed"
