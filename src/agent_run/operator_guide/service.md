# service

Only opencode has a managed, long-running service; no other runtime does.

## Starting it

Start with `agent-run service start --runtime opencode`. Configure the service
password through the runtime's declared environment/file auth. A deployment
may wrap this command with its own credential helper; such wrappers are not
part of agent-run.

If `agent-run models` shows nothing for opencode after startup, verify that the
configured credential reached the service process before debugging the model
roster.

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
