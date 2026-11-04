# Shared Agent Run service

This document describes the optional shared systemd service and its deployment
boundaries. Platform qualification is established by the release checks.

## Required behavior

- One systemd service serves a common Agent Run environment for Codex and
  Claude. Codex and Claude continue to run as their existing distinct users.
- The operating system owns access policy: users, groups, file/socket modes,
  service configuration and native account credentials. Do not add a separate
  Agent Run role model, UID allowlist, or privileged authorization manager.
- Keep ordinary per-user behavior and macOS support. The systemd setup is an
  optional deployment; the shared execution protocol must not depend on Linux.
- Preserve existing task identities, capability and process-ownership checks,
  cancellation, uncertain-outcome handling, and native provider permissions.
- Each model account reads its own credentials. The broker does not need
  access to either account's private home. Existing explicit-finish checks can
  send secret-shaped launch values to the broker in memory over the writer
  connection; these values are not written to the database or logs.

## Service topology

One systemd unit starts an unprivileged broker and one unprivileged launcher
worker under each model account. Use the host's normal account-switch
utility to start those long-lived processes and let Unix supplementary groups
and socket/file permissions control which accounts can connect. No Agent Run
process needs to run as root to handle a request. The unit and account/group
configuration are deployment concerns, not a new application permissions API.

The broker owns one database and maintains the existing logical agent list,
admission, pool state, request replay and single-writer semantics. Its workers
only start the existing fixed supervisor under their own account. They do not
accept arbitrary commands. The broker records the launch capability and process
identity before releasing a supervisor, and all writes continue to use the
existing run capability and durable ownership evidence. A lost reply is not
permission to start another model process.

The shared state directory and writer socket use a group chosen by the host
administrator. The broker owns the database, and model users receive only the
read access needed by the existing read-only store. Artifact files use ordinary
group permissions; the account homes and credential files remain private. The
broker's operator endpoint is also group-accessible. Its members share control
and visibility over the same Agent Run environment, including the ability to
request work under the configured model accounts.

Each worker reclaims its own expired runtime homes after checking the shared
database and the broker's validated configuration references. Paths are
resolved again under the worker's account, including accessible symbolic links;
the broker does not need to enter that account's private directory. The shared
operator endpoint exposes the configuration's path references to its authorized
group, but not their contents. Invalid or unavailable configuration or database
evidence stops cleanup.

System services must drain active work on a planned stop. A restart, crash or
lost reply must not be reported as success or cause an unproven duplicate
launch. Recovery and permissions must be tested on actual processes before the
service is installed.

## Validation and limits

The test suite exercises shared workers, Unix permissions, process cleanup and
recovery with isolated fixtures. A full Linux gate needs a protected socket
fixture directory; see [releasing.md](releasing.md). The release workflow
requires separate native Linux x86-64 and macOS Apple-silicon checks.

Before relying on notifications to a particular chat, verify the corresponding
Bridge route and wake behavior on that installation. The current delivery
configuration has one store per harness; enabling hooks for multiple stores of
the same harness needs per-chat namespace binding.
