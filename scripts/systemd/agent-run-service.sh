#!/usr/bin/env bash
# Starts the shared broker and account-owned workers under one systemd unit.
set -Eeuo pipefail

: "${AR_BINARY:?}" "${AR_STATE:?}" "${AR_ROUTES:?}" "${AR_GROUP:?}"
: "${AR_BROKER_USER:?}" "${AR_BROKER_HOME:?}" "${AR_BROKER_TMP:?}" "${AR_BROKER_PATH:?}"
: "${AR_CODEX_USER:?}" "${AR_CODEX_GROUP:?}" "${AR_CODEX_HOME:?}" "${AR_CODEX_TMP:?}" "${AR_CODEX_PATH:?}"
: "${AR_CLAUDE_USER:?}" "${AR_CLAUDE_GROUP:?}" "${AR_CLAUDE_HOME:?}" "${AR_CLAUDE_TMP:?}" "${AR_CLAUDE_PATH:?}"
: "${AR_OPERATOR:?}" "${AR_WRITER:?}" "${AR_CODEX_WORKER:?}" "${AR_CLAUDE_WORKER:?}"
: "${AR_CODEX_CLIENT:?}" "${AR_CLAUDE_CLIENT:?}"

broker_dir=$(dirname -- "$AR_OPERATOR")
codex_dir=$(dirname -- "$AR_CODEX_WORKER")
claude_dir=$(dirname -- "$AR_CLAUDE_WORKER")
codex_client_dir=$(dirname -- "$AR_CODEX_CLIENT")
claude_client_dir=$(dirname -- "$AR_CLAUDE_CLIENT")
[[ $(dirname -- "$AR_WRITER") == "$broker_dir" ]]
for directory in "$broker_dir" "$codex_dir" "$claude_dir" "$codex_client_dir" "$claude_client_dir"; do
    case "$directory" in /run/agent-run/*) ;; *) echo "invalid runtime directory" >&2; exit 1 ;; esac
done

/usr/bin/install -d -o "$AR_BROKER_USER" -g "$AR_GROUP" -m 2750 -- "$broker_dir"
/usr/bin/install -d -o "$AR_CODEX_USER" -g "$AR_GROUP" -m 2750 -- "$codex_dir"
/usr/bin/install -d -o "$AR_CLAUDE_USER" -g "$AR_GROUP" -m 2750 -- "$claude_dir"
/usr/bin/install -d -o "$AR_BROKER_USER" -g "$AR_CODEX_GROUP" -m 2750 -- "$codex_client_dir"
/usr/bin/install -d -o "$AR_BROKER_USER" -g "$AR_CLAUDE_GROUP" -m 2750 -- "$claude_client_dir"

# Executes the fixed command after the OS establishes the account and groups.
run_as() {
    local kind=$1 user=$2 home=$3 tmp=$4 path=$5 pidfile=$6
    shift 6
    local -a environment=("HOME=$home" "USER=$user" "LOGNAME=$user" "PATH=$path"
        "TMPDIR=$tmp" "TMP=$tmp" "TEMP=$tmp")
    if [[ $kind == worker ]]; then
        environment+=("AGENT_RUN_SYSTEM_WRITER_SOCKET=$AR_WRITER"
            "AGENT_RUN_SYSTEM_OPERATOR_SOCKET=$AR_OPERATOR")
    fi
    exec /usr/sbin/runuser -u "$user" -- /usr/bin/env -i "${environment[@]}" \
        /bin/sh -c 'pidfile=$1; shift; printf "%s\n" "$$" > "$pidfile"; exec "$@"' \
        sh "$pidfile" "$AR_BINARY" --home "$AR_STATE" "$@"
}

# The runuser monitor retains this child until it exits; verify the exact child
# before signalling it, so a stale PID file cannot name an unrelated process.
child_of() {
    local pid=$1 monitor=$2 key value rest
    [[ $pid =~ ^[0-9]+$ && -r /proc/$pid/status ]] || return 1
    while read -r key value rest; do
        if [[ $key == PPid: ]]; then [[ $value == "$monitor" ]]; return; fi
    done < "/proc/$pid/status"
    return 1
}

await_child() {
    local monitor=$1 pidfile=$2 pid
    for ((i=0; i<300; i++)); do
        if [[ -r $pidfile ]] && read -r pid < "$pidfile" && child_of "$pid" "$monitor"; then
            printf '%s\n' "$pid"
            return 0
        fi
        kill -0 "$monitor" 2>/dev/null || return 1
        sleep 0.1
    done
    return 1
}

broker_monitor= codex_monitor= claude_monitor=
broker_pid= codex_pid= claude_pid=
ready=0

# SIGTERM the actual service child: signalling runuser itself would enforce
# runuser's two-second kill timeout and could interrupt an active task.
stop_one() {
    local monitor=$1 pid=$2
    [[ -n $monitor ]] || return 0
    if [[ -n $pid ]] && child_of "$pid" "$monitor"; then
        kill -TERM "$pid" 2>/dev/null || true
    elif (( ! ready )); then
        kill -TERM "$monitor" 2>/dev/null || true
    fi
    wait "$monitor" 2>/dev/null || true
}

cleanup() {
    trap - EXIT INT TERM
    stop_one "$broker_monitor" "$broker_pid"
    stop_one "$codex_monitor" "$codex_pid"
    stop_one "$claude_monitor" "$claude_pid"
}
trap cleanup EXIT
trap 'exit 0' INT TERM

run_as worker "$AR_CODEX_USER" "$AR_CODEX_HOME" "$AR_CODEX_TMP" "$AR_CODEX_PATH" \
    "$codex_dir/worker.pid" _launch-worker --socket "$AR_CODEX_WORKER" &
codex_monitor=$!
run_as worker "$AR_CLAUDE_USER" "$AR_CLAUDE_HOME" "$AR_CLAUDE_TMP" "$AR_CLAUDE_PATH" \
    "$claude_dir/worker.pid" _launch-worker --socket "$AR_CLAUDE_WORKER" &
claude_monitor=$!
codex_pid=$(await_child "$codex_monitor" "$codex_dir/worker.pid")
claude_pid=$(await_child "$claude_monitor" "$claude_dir/worker.pid")
for ((i=0; i<300; i++)); do
    [[ -S $AR_CODEX_WORKER && -S $AR_CLAUDE_WORKER ]] && break
    kill -0 "$codex_monitor" "$claude_monitor" 2>/dev/null || exit 1
    sleep 0.1
done
[[ -S $AR_CODEX_WORKER && -S $AR_CLAUDE_WORKER ]]

run_as broker "$AR_BROKER_USER" "$AR_BROKER_HOME" "$AR_BROKER_TMP" "$AR_BROKER_PATH" \
    "$broker_dir/broker.pid" api serve --shared-config "$AR_ROUTES" &
broker_monitor=$!
broker_pid=$(await_child "$broker_monitor" "$broker_dir/broker.pid")

# Report readiness only after the real broker answers and its UID matches the
# operating-system owner of the configured state directory.
for ((i=0; i<300; i++)); do
    if /usr/bin/python3 - "$AR_OPERATOR" "$AR_STATE" <<'PY' 2>/dev/null
import json, os, socket, struct, sys
stream = socket.socket(socket.AF_UNIX)
stream.settimeout(1)
stream.connect(sys.argv[1])
peer = struct.unpack("3i", stream.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))
assert peer[1] == os.stat(sys.argv[2]).st_uid
stream.sendall(b'{"jsonrpc":"2.0","id":1,"method":"ping","params":{}}\n')
assert json.loads(stream.makefile().readline())["result"] == {"ok": True}
PY
    then
        ready=1
        break
    fi
    kill -0 "$broker_monitor" 2>/dev/null || exit 1
    sleep 0.1
done
(( ready )) || exit 1
if [[ -n ${NOTIFY_SOCKET:-} ]]; then
    /usr/bin/systemd-notify --ready --status="Agent Run broker and workers are ready"
fi

# Unexpected exit of any constituent process tears down the unit after drain.
if wait -n "$broker_monitor" "$codex_monitor" "$claude_monitor"; then :; fi
echo "an Agent Run service process stopped unexpectedly" >&2
exit 1
