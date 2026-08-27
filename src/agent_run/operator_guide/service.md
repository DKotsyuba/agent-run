# service

Only opencode has a managed, long-running service; no other runtime does.

## Starting it

Start via `~/.agent-run/bin/agent-run-keychain`, which exports the service
password from the system Keychain before invoking `agent-run service
start`. A bare `agent-run` binary invocation — skipping the keychain
wrapper — starts a service that comes up but silently yields an EMPTY model
roster. This is a known trap: if `agent-run models` shows nothing for
opencode right after a start, check which binary actually launched it
before debugging anything else.

The running service's descriptor lives at
`<home>/runtimes/opencode/home/service.json`.

## Config-change restart order (exact order matters)

1. Rematerialize: run the adapter's `materialize` step (either through a
   normal `agent-run service start --runtime opencode` product path, or
   the documented Python snippet that calls the opencode adapter's
   materialize function directly). This regenerates the on-disk config the
   service actually reads.
2. `kill -TERM` the old service pid (from `service.json`).
3. Start the service again.

`start_service` never regenerates config itself — step 1 must happen first,
or the restarted service comes back up with the old, pre-edit config. Doing
step 2 before step 1 is also wrong: it just kills a service that still has
nothing new to serve when restarted.

## Never touch a foreign listener

Before killing anything on the service's port, confirm the pid in
`service.json` is actually agent-run's — never send a signal to a listener
you have not confirmed by descriptor first.
