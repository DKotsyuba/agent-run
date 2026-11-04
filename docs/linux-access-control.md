# Linux access and shared-service deployment

This guide describes deployment choices for a Linux host. The account names and
paths below are examples; choose names and paths for your own system. Agent Run
does not create Linux users, groups, provider logins or Bridge grants for you.

## Choose a trust boundary

| Arrangement | Who can use the broker | When to use it |
| --- | --- | --- |
| Per-user broker | The account that owns its private Unix socket | One person or one automation account. |
| Shared broker, separate workers | Members of the operator group, and any explicitly granted client sockets | Several cooperating accounts that are allowed to control the same jobs. |
| Separate brokers | Each broker's own account or group | Users or teams must not see or control one another's jobs. |

The shared broker is a **single trust domain**. An operator can submit work to
either configured worker, inspect shared task state and results, and request
cancellation. Work may use provider credits and the filesystem and network
access granted to the selected worker and native agent. A shared group is not
a tenant boundary, a read-only observer role, or a per-provider permission
policy. Use separate brokers and state directories when those boundaries are
required.

Linux enforces access to the local sockets and files by UID, GID, directory
search permission and file mode. Agent Run also retains per-execution
capabilities and process-identity checks for its internal supervisor protocol;
these protect task ownership and do not replace host account administration.
The service does not expose a TCP endpoint by default. If an administrator adds
a remote proxy, that proxy needs its own authentication, transport protection
and authorization.

## Per-user installation

Install the native release and start the broker as the same ordinary Linux
account that owns its Agent Run home:

```sh
agent-run init
agent-run doctor
agent-run api serve
```

The default home is `~/.agent-run`. Keep the home and provider credentials
private, typically mode `0700` for directories and `0600` for credential
files. Do not run a user broker as root to make an inaccessible path work.
The native Codex or Claude Code CLI must be installed and authenticated for
that account; Agent Run does not automatically copy a login from another UID.
See [account references](account-registry.md) and
[provider configuration](provider-config-v2.md).

## One shared systemd service

The optional files in `systemd/` in a native release are a starting point for
one systemd unit. They start a small root-owned service wrapper, which uses the
OS account-switching utility to run the broker and each worker as an
unprivileged account. The broker does not need to read a worker's private home.
The example requires systemd, Bash, Python 3, `runuser`, and the usual coreutils.
Review the files and adapt them before enabling the unit.

The following example uses existing accounts `run-operator`, `codex-agent` and
`claude-agent`, and a shared group `agent-run`. Create or select those accounts
according to your distribution's account policy. Add only trusted operators
and the worker accounts to the shared group:

```sh
sudo groupadd --system agent-run
sudo usermod -aG agent-run run-operator
sudo usermod -aG agent-run codex-agent
sudo usermod -aG agent-run claude-agent
```

Group membership changes do not alter the supplementary groups of already
running processes. Restart the relevant login sessions or service processes
after changing membership. Do not remove a pre-existing group or replace an
account's other memberships.

### Files and sockets

| Resource | Example owner and group | Typical mode | Purpose |
| --- | --- | --- | --- |
| Shared state directory | `run-operator:agent-run` | `2750` | Broker-owned state; setgid makes new entries inherit the group. |
| Shared `state.db`, WAL and shared-memory files | `run-operator:agent-run` | `0640` | Workers can read state; the broker owns writes. |
| `agents/` and each shared result directory | `run-operator:agent-run` | `2750` for `agents/`; group-writable run directories | Broker creates each run directory; its worker writes result artifacts. |
| Broker and worker socket directories | respective account:`agent-run` | `2750` | Group access to the shared operator, writer and launcher sockets. |
| Optional client socket directory | `run-operator:<client-primary-group>` | `2750` | A pre-existing client session can connect through its account group. |
| Worker private home and temporary directory | respective worker:its primary group | `0700` | Native runtime files and temporary data. |
| Provider or Bridge credential file | its reading account:its primary group | `0600` | Secret read only by the account that needs it. |

The socket inode needs group write permission for group members to connect;
the provided unit's `UMask=0007` and setgid socket directories supply it. Every
parent directory must also grant search (`x`) to the intended account. Inspect
the resulting inode ownership and modes rather than assuming that `umask`,
an existing directory or an ACL produced the intended result. Do not make
private worker homes or credential files group-readable to repair a socket.

For a **new, empty** installation, the broker account can initialize the state
before sharing it. `init` makes its home private, so apply the intended group
and mode only after initialization and before starting the service:

```sh
sudo install -d -o run-operator -g agent-run -m 0700 /var/lib/agent-run
sudo -u run-operator /usr/local/bin/agent-run --home /var/lib/agent-run init
sudo chgrp agent-run /var/lib/agent-run /var/lib/agent-run/state.db
sudo chmod 2750 /var/lib/agent-run
sudo chmod 0640 /var/lib/agent-run/state.db
sudo install -d -o run-operator -g agent-run -m 2750 /var/lib/agent-run/agents
sudo install -d -o run-operator -g run-operator -m 0700 /var/lib/agent-run/tmp
```

These commands are examples for paths that do not already contain an
installation. Do not run them over an existing state directory without first
reviewing its owner, active service, backup and recovery procedure. Configure
the schema-2 provider catalog and profiles in this home. In a shared setup,
the configured harness `home` paths must be writable by the worker account
that runs that harness, for example a private subdirectory of each worker's
own `~/.agent-run`.

Create each worker's private Agent Run home and temporary directory under its
own account. Do not replace existing homes or credentials. Install and sign in
to each native CLI as its worker account. For custom API providers, register a
credential reference such as `file:/absolute/private/path`; the file must be
readable by the relevant worker. Enter secrets with a protected editor or other
local secret-management tool, not in shell history or configuration examples.
For per-user native logins, Agent Run also supports `agent-run login` and
`agent-run auth`; use the account and runtime described in the account guide.

### Install the unit

After installing a release, its `current` directory contains the executable and
the reviewed `systemd/` examples. For a fresh system-wide deployment, an
administrator can copy the exact installed binary and service files to
root-owned locations, then edit the two configuration files:

```sh
release_dir="$HOME/.agent-run/standalone/current"
sudo install -o root -g root -m 0755 "$release_dir/bin/agent-run" /usr/local/bin/agent-run
sudo install -d -o root -g root -m 0755 /etc/agent-run /usr/local/libexec
sudo install -o root -g root -m 0755 "$release_dir/systemd/agent-run-service.sh" /usr/local/libexec/agent-run-service
sudo install -o root -g root -m 0644 "$release_dir/systemd/agent-run.service" /etc/systemd/system/agent-run.service
sudo install -o root -g root -m 0644 "$release_dir/systemd/service.env.example" /etc/agent-run/service.env
sudo install -o root -g root -m 0644 "$release_dir/systemd/routes.toml.example" /etc/agent-run/routes.toml
```

Do not overwrite existing service files without reviewing and backing them up.
Set the account names, home directories, CLI `PATH` values, state directory,
group and sockets in `service.env`; make the worker socket and private-home
paths match `routes.toml`. The wrapper accepts socket directories only under
`/run/agent-run/`. The account used by the broker must own the shared state.
The shared configuration should reference credentials rather than contain
their values.

```sh
sudo systemctl daemon-reload
sudo systemctl enable --now agent-run.service
sudo systemctl status agent-run.service
```

The unit waits for active tasks to drain on a planned stop. Updating the native
release does not automatically replace the system-wide executable or restart
the unit: stop or drain the service, preserve a working binary and state backup,
install the new reviewed binary, and start it explicitly. Keep the old release
until rollback and schema compatibility are verified.

### Verify the boundary

Check the effective IDs and groups of both worker processes and the broker;
the wrapper itself is root, while model processes must run as their configured
worker. From an authorized account, connect to its configured client socket
and check that `agent-run` can list models and start an allowed task. From an
account outside the group, a connection to the shared operator socket should be
denied by the OS. Also check that one worker cannot read the other worker's
credential file. Test with actual accounts and modes on the target host.

For example, inspect the configured files and process membership without
printing credentials:

```sh
systemctl status agent-run.service
namei -l /run/agent-run/broker/api.sock
stat -c '%U:%G %a %n' /run/agent-run/broker/api.sock /var/lib/agent-run/state.db
id codex-agent
id claude-agent
```

`namei` and `stat` show filesystem permissions; supplementary groups of a
running process may differ from a freshly queried account. A client socket
grants access to the common broker, not just to one provider. If a model CLI or
its sandbox needs access to a project, grant that worker the necessary project
permissions separately. Do not widen the broker state or credentials to make
an unrelated project accessible.

## Optional notifications and current limits

Bridge is a separate service with its own per-UID sockets and credentials.
When enabled, the broker account uses its own producer credential and Bridge
endpoints; a worker's credential is not a substitute. Agent Run stores one
configured native store per harness. Do not enable automatic bindings for
multiple native store namespaces of the same harness until per-chat namespace
binding is available: the wrong store can misroute a notification. A message
accepted by Bridge is not, by itself, proof that a user read it.

Provider quota collectors are optional. With `limits_source = "none"`, quota
availability is unknown; it must not be treated as unlimited. Linux release
and host qualification depend on the checks recorded for each release; do not
assume every architecture or host configuration has been tested.
