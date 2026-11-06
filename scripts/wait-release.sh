#!/bin/sh
# Foreground forwarder; Rust owns identity, bounds, events and notifier delivery.
set -eu
script_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$script_root"
if [ -n "${AGENT_RUN_RELEASE_WATCH_XTASK:-}" ]; then
  case "$AGENT_RUN_RELEASE_WATCH_XTASK" in
    /*) exec "$AGENT_RUN_RELEASE_WATCH_XTASK" release wait "$@" ;;
    *) printf '%s\n' 'AGENT_RUN_RELEASE_WATCH_XTASK must be an absolute path' >&2; exit 2 ;;
  esac
fi
exec cargo xtask release wait "$@"
