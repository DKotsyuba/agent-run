#!/usr/bin/env bash
# Prepare the shared broker, restart its service, and wait for the same epoch.
set -Eeuo pipefail

binary=${AGENT_RUN_BINARY:-agent-run}
platform=$(uname -s)
case "$platform" in
    Linux)
        manager=${AGENT_RUN_SYSTEMCTL:-/usr/bin/systemctl}
        target=${AGENT_RUN_SYSTEMD_UNIT:-agent-run.service}
        ;;
    Darwin)
        manager=${AGENT_RUN_LAUNCHCTL:-/bin/launchctl}
        target=${AGENT_RUN_LAUNCHD_TARGET:-gui/$(id -u)/com.agent-run.api}
        ;;
    *) printf '%s\n' 'Only Linux systemd and macOS launchd are supported.' >&2; exit 2 ;;
esac
if [[ ! -x $manager ]] || ! command -v "$binary" >/dev/null 2>&1; then
    printf '%s\n' 'Agent Run or the service manager is unavailable.' >&2
    exit 2
fi
if ! epoch=$("$binary" "$@" maintenance prepare --epoch-only); then
    printf '%s\n' 'Preparation did not reach readiness; inspect maintenance status.' >&2
    exit 2
fi
case $epoch in ''|*[!0-9]*) printf '%s\n' 'The maintenance epoch is invalid.' >&2; exit 2 ;; esac
if [[ $platform == Linux ]]; then
    if ! "$manager" restart -- "$target"; then
        printf '%s\n' 'Restart was not confirmed. Inspect maintenance status before deferring.' >&2
        exit 1
    fi
else
    if ! "$manager" kill SIGTERM "$target"; then
        printf '%s\n' 'Restart was not confirmed. Inspect maintenance status before deferring.' >&2
        exit 1
    fi
fi
"$binary" "$@" maintenance wait "$epoch"
