# Managed background services

Broker services are independent foreground processes shared by agents. They are
not harness children and do not receive provider account credentials. Service
configuration, schema-20 ownership storage, snapshot restoration and the resident
broker lifecycle are implemented. The broker starts declared services only when
an agent needs admission; it does not start them merely because the API opens.
MCP clients must already be configured to connect to the declared backend;
per-run MCP credential bindings are a separate capability.

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
reuse_existing = true
```

The example is generic, not a built-in integration or an executable supplied by
agent-run. Commands must stay in the foreground. Self-daemonizing commands need
an explicit supported foreground mode; a healthy socket or a PID file is not
enough to acquire ownership of an unrelated existing daemon.

`reuse_existing` is an opt-in, disabled by default. When enabled, the broker runs
the configured application check before launching a process. A successful check
uses the existing endpoint as an **external** service; no service PID is captured
or claimed. A negative initial check starts the ordinary owned foreground
process. If that new process exits before becoming ready, the broker first proves
cleanup of its captured tree and then repeats the external check: another
launcher may have won the race. It never uses a healthy external endpoint to
excuse unresolved cleanup of its own attempt.

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
The probe receives `AGENT_RUN_SERVICE_OWNERSHIP` (`managed` or `external`),
`AGENT_RUN_SERVICE_ID` and `AGENT_RUN_SERVICE_GENERATION`. In managed mode it also
receives `AGENT_RUN_SERVICE_PID`; external mode omits that variable. An opt-in
probe must support both modes and verify the intended application endpoint and
protocol without starting a service itself. In managed mode it should check the actual
application endpoint, associating it with the expected process when unrelated
servers could share the address. Probe output is discarded; only bounded static
failure codes enter durable state.

## Durable ownership

Schema 18 introduced service generations, agent startup gates, service leases and
captured process identities. There can be only one non-stopped generation for
a service id. A process identity is the PID plus its immutable platform start
token, so reuse does not overwrite an older captured member. Ownership records
retain the original leader and whether capture began from a verified root.
Attempt and service ownership cannot both claim the same PID/token.

Schema 20 adds explicit `managed`/`external` ownership. Every historical
generation remains `managed`. External readiness confers no process authority:
idle expiry or a configuration change retires only the broker's observation and
leases. It never signals the external daemon. A cleaned bootstrap which lost a
startup race retains its original identity and cleanup proof for audit; the
external daemon's identity is never substituted into that ownership record.

Readiness probes have separate transient ownership records tied to their service
generation. A pipe gates exec until that identity is committed. Broker restart
cleans interrupted probes before starting more work; confirmed probe records
are removed so health checks do not accumulate history indefinitely.

Restoration never treats a database row as permission to kill a current PID:
every signal still checks the kernel token and birth time. Missing historical
snapshots remain unknown; migration does not invent a historical process tree.
The original root is immutable and cannot be replaced by a later observation.
See [process identity](process-identity.md) for platform limits.

## Broker lifecycle contract

The broker warms every configured service before a new harness starts.
Concurrent agents share one generation. Pending or active agents retain leases;
unresolved process ownership retains them even after an agent is marked lost.
The inactivity clock starts when the **last active agent finishes** and its
process ownership is resolved, using the actual final lease-release time.
Delayed cleanup cannot consume the idle grace. A new agent resets the clock;
a running agent cannot lose its services because thirty minutes elapsed.
Expiry sends TERM, then bounded KILL to verified
owned processes; external generations only stop being monitored. Service health failures block new launches; changing a service
configuration must not restart it underneath active agents.

Admission creates a durable pending gate in the same transaction as the agent.
The supervisor waits for that gate with its original timeout and cancellation
rules; disconnecting the submitting CLI does not abandon warmup. Spawn admission
rechecks ready gates and leases atomically. A revision conflict with active
leases rejects the new gate. Unready services cannot start model execution.

An internal native bootstrap records its exact identity and process snapshot
before exec. It verifies its originating broker and cannot exec after its
generation was retired. Broker restart restores only recorded generations,
checks their identities again and never adopts a daemon merely because a socket
responds. External generations are rechecked through their configured application
probe after restart. Harness admission requires a successful external observation
no older than three monitor intervals; missing or stale health refuses admission.
Services can survive a broker restart; the replacement broker resumes
health and idle monitoring from SQLite. A broker which stays down cannot enforce
idle expiry until it returns. Foreground service exit marks the generation
unhealthy and cleans captured children. Uncertain ownership remains unresolved.

Harness cleanup owns every observed harness descendant, including an accidental
daemon. Only a process launched independently as a broker service belongs to
the shared service lifecycle. Declaring a service does not turn a single-client
stdio MCP server into a multi-client server: its agents still need a supported
client or network transport to reach the shared backend.

## CodeGraph 1.6.0 example

The native archive includes `services/codegraph-probe.cjs` as an external
integration example. It checks the daemon's socket hello against the PID supplied
by agent-run in managed mode and completes a real `codegraph_status` MCP call.
In external mode it reads the existing daemon record and checks the socket hello
against that record's PID, the configured version and the protocol. It never
starts a daemon. This example is qualified against
CodeGraph **1.6.0** and uses its private `CODEGRAPH_DAEMON_INTERNAL` entry point;
re-qualify the command and probe before changing that version.

Initialize the intended project with CodeGraph first. Point to its installed
platform bundle, whose launcher execs its bundled Node in the foreground:

```toml
[services.codegraph]
command = "/usr/bin/env"
args = ["CODEGRAPH_DAEMON_INTERNAL=1", "DO_NOT_TRACK=1", "CODEGRAPH_NO_UPDATE_CHECK=1",
        "/absolute/path/to/codegraph-bundle/bin/codegraph", "serve", "--mcp",
        "--path", "/absolute/path/to/project"]
cwd = "/absolute/path/to/project"
reuse_existing = true
idle_timeout_seconds = 1800
monitor_interval_seconds = 5
readiness = { command = "/absolute/path/to/codegraph-bundle/node", args = ["/absolute/path/to/agent-run/current/services/codegraph-probe.cjs", "/absolute/path/to/project", "1.6.0"], timeout_seconds = 5 }
```

Keep the normal per-agent CodeGraph MCP client configured with `serve --mcp
--path /absolute/path/to/project`; do not set `CODEGRAPH_DAEMON_INTERNAL` for
those clients. They connect to the prewarmed backend. The readiness traffic also
keeps CodeGraph's own idle watchdog active while agent-run retains the service.
CodeGraph's own watchdog remains a backstop when the broker is down.
If a Desktop MCP client has already started that backend, the broker reuses it
without claiming it or shutting it down at its own idle deadline.

`agent-run doctor` reports cold services as expected information, stale health
as a warning, and unhealthy or mismatched process ownership as an error. It
reads state without launching probes or starting services.

Before enabling reuse on an existing installation, upgrade the broker and paired
database to schema 20 and use the matching shipped probe. Old binaries reject
the new setting. The normal paired migration preserves historical ownership as
managed; see [migration instructions](provider-migration.md).
