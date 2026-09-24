# Managed background services

Broker services are independent foreground processes shared by agents. They are
not harness children and do not receive provider account credentials. Service
configuration, schema-18 ownership storage and snapshot restoration are available;
broker warmup, idle monitoring and MCP bindings are still being integrated in
this unreleased version. A declaration alone does not yet start a service.

## Configuration contract

Schema-2 configuration accepts up to 32 named services:

```toml
[services.index]
command = "/absolute/path/to/index-server"
args = ["--foreground", "--socket", "/absolute/path/to/index.sock"]
cwd = "/absolute/path/to/project"
env_from = ["INDEX_TOKEN"]
startup_timeout_seconds = 60
idle_timeout_seconds = 1800
monitor_interval_seconds = 5
stop_grace_seconds = 2
readiness = { command = "/absolute/path/to/check-index", args = [], timeout_seconds = 2 }
```

The example is generic, not a built-in integration or an executable supplied by
agent-run. Commands must stay in the foreground. Self-daemonizing commands need
an explicit supported foreground mode; a healthy socket or a PID file is not
enough to adopt an unrelated existing daemon.

Paths are absolute, arguments are literal, and there is no implicit shell. If a
shell is required, name it as the command and supply the script as an argument.
Use nonsecret arguments and inherit secrets through `env_from`; no inline
environment-value map or provider credential bridge is accepted. Environment
names beginning with `AGENT_RUN_` are reserved. Host environment values are not
part of the stored service definition or its revision.

Startup timeout is 1–300 seconds; one readiness probe is 1–30 seconds; health
interval is 1–60 seconds; TERM grace is 0–30 seconds. Idle timeout is 1–86400
seconds and defaults to 1800. Unknown fields and invalid bounds fail validation.
Readiness is an application check, distinct from proof of process ownership.

## Durable ownership

Schema 18 records service generations, agent startup gates, service leases and
captured process identities. There can be only one non-stopped generation for
a service id. A process identity is the PID plus its immutable platform start
token, so reuse does not overwrite an older captured member. Ownership records
retain the original leader and whether capture began from a verified root.
Attempt and service ownership cannot both claim the same PID/token.

Restoration never treats a database row as permission to kill a current PID:
every signal still checks the kernel token and birth time. Missing historical
snapshots remain unknown; migration does not invent a historical process tree.
The original root is immutable and cannot be replaced by a later observation.
See [process identity](process-identity.md) for platform limits.

## Broker lifecycle contract

The integration must warm every configured service before a new harness starts.
Concurrent agents share one generation. Pending or active agents retain leases;
unresolved process ownership retains them even after an agent is marked lost.
The inactivity clock starts when the **last active agent finishes**, not at its
launch. A new agent resets it; a long-running agent cannot lose its services
because thirty minutes elapsed. Expiry sends TERM, then bounded KILL to verified
owned processes. Service health failures block new launches; changing a service
configuration must not restart it underneath active agents.

Harness cleanup owns every observed harness descendant, including an accidental
daemon. Only a process launched independently as a broker service belongs to
the shared service lifecycle. Declaring a service does not turn a single-client
stdio MCP server into a multi-client server: its agents still need a supported
client or network transport to reach the shared backend.
