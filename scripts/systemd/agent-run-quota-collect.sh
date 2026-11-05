#!/usr/bin/env bash
# Collect native quotas under their account owners and persist through the broker.
set -euo pipefail

directory=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)

# Drop to the account owning the operation before executing it.
run_as() {
    local user=$1
    shift
    /usr/sbin/runuser -u "$user" -- /usr/bin/env -i "$@"
}

# Emit a fixed diagnostic without retaining a collector's private output.
report_failure() {
    /usr/bin/logger -t agent-run-quota -- "$1"
}

collect() {
    local provider=$1 account=$2 user=$3 home=$4 temporary=$5 search_path=$6
    local observation
    if ! observation=$(
        {
        run_as "$AR_BROKER_USER" \
        HOME="$AR_BROKER_HOME" USER="$AR_BROKER_USER" LOGNAME="$AR_BROKER_USER" \
        PATH="$AR_BROKER_PATH" TMPDIR="$AR_BROKER_TMP" TMP="$AR_BROKER_TMP" TEMP="$AR_BROKER_TMP" \
        "$directory/system_quota_ingest" context "$AR_STATE" "$provider" "$account" |
        run_as "$user" \
        HOME="$home" USER="$user" LOGNAME="$user" PATH="$search_path" \
        TMPDIR="$temporary" TMP="$temporary" TEMP="$temporary" \
        /bin/bash "$directory/agent-run-quota-probe.sh" |
        /usr/bin/head -c 2097153 |
        /usr/bin/jq -ce '.'
        } 2>/dev/null
    ) || (( ${#observation} > 2097152 )); then
        report_failure "$provider probe failed"
        return 1
    fi
    if printf '%s' "$observation" | run_as "$AR_BROKER_USER" \
        HOME="$AR_BROKER_HOME" USER="$AR_BROKER_USER" LOGNAME="$AR_BROKER_USER" \
        PATH="$AR_BROKER_PATH" TMPDIR="$AR_BROKER_TMP" TMP="$AR_BROKER_TMP" TEMP="$AR_BROKER_TMP" \
        "$directory/system_quota_ingest" ingest "$AR_STATE" "$provider" "$account" >/dev/null 2>&1
    then
        return 0
    fi
    report_failure "$provider collection failed or outcome unknown"
    return 1
}

if [[ ${BASH_SOURCE[0]} == "$0" ]]; then
    # Both files are root-owned deployment settings, without credential values.
    source /etc/agent-run/service.env
    source /etc/agent-run/quota.env
    export LC_ALL=C
    cd /
    result=0
    if [[ -n ${AR_QUOTA_CODEX_ACCOUNT:-} ]]; then
        collect codex "$AR_QUOTA_CODEX_ACCOUNT" "$AR_CODEX_USER" "$AR_CODEX_HOME" "$AR_CODEX_TMP" "$AR_CODEX_PATH" || result=1
    fi
    if [[ -n ${AR_QUOTA_CLAUDE_ACCOUNT:-} ]]; then
        collect claude "$AR_QUOTA_CLAUDE_ACCOUNT" "$AR_CLAUDE_USER" "$AR_CLAUDE_HOME" "$AR_CLAUDE_TMP" "$AR_CLAUDE_PATH" || result=1
    fi
    exit "$result"
fi
