# Native release recovery

`cargo xtask release update` first refuses durable active agents and archived
`workflow_runs` owners whose PID cannot be proven gone (an unobservable or
access-denied writer counts as live, exactly like the Python release script),
writes
`<prefix>/deploy.json`, retains a state/config backup under `<prefix>/backups`,
then atomically switches `current`. `--force` skips only the active-agent
query: it does not stop a daemon, cancel work, or make an unsafe deployment
safe. Establish quiescence first.

Run `cargo xtask release rollback --prefix PREFIX --home HOME` only on a
quiescent disposable or operator-approved home. It restores the journal's
previous pointer and retained state/config files. The command never registers
launchd jobs and never targets the owner home implicitly.
