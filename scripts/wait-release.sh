#!/bin/sh
# Foreground forwarder; Rust owns identity, bounds, events and notifier delivery.
set -eu
script_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$script_root"
exec cargo xtask release wait "$@"
