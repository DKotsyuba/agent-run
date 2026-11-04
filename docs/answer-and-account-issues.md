# Answer delivery and shared-account issues

These issues were observed with the shared service in version 0.29.1. They are
independent of any particular account name, credential, host or chat.

## 1. Large completed answers fail in the MCP presentation

An explicit `finish` accepts a summary of up to 64 KiB and seals the completed
answer in the execution's managed artifact directory. The MCP `answer` view has
a smaller, 14,336-byte content budget. When a verified answer exceeds that
budget, the 0.29.1 renderer returns `presentation_failed` instead of the
available artifact's metadata. Reducing the transcript page size cannot recover
a single oversized message either.

The `finish` receipt hashes the canonical finish request; `answer_chunk` requires
the hash of the sealed answer bytes. These are different proofs. A caller that
cannot obtain the latter from `answer` cannot use that receipt as a substitute.

The fix keeps short answers inline and returns bounded, verified retrieval
metadata for longer ones: execution sequence, answer size, answer hash and a
reference to the existing managed artifact, with `answer_chunk` as a fallback
when a local file is not accessible. It preserves the existing finish and artifact
limits; no separate storage or upload protocol is needed for the observed cases.
The existing filesystem retention protects the artifact while its history is
retained and reclaims eligible orphan trees after that history is removed. SQLite
`VACUUM` alone does not delete history or external files.

## 2. Shared-service continuation crosses the wrong user boundary

The broker and account-owned workers run as different Unix users. During
`resume`, the 0.29.1 broker attempts to inspect a worker's private runtime
home. An operating-system permission denial is reported as if the retained
assets no longer verified, even when the owning worker can access the home.

Automatic account fallback has the opposite boundary problem: a worker attempts
to load the broker's private configuration to recheck current policy. If it
cannot read that configuration, fallback stops with
`current_config_unavailable`, even when a second account is enabled and usable.
An API billing error alone does not establish a subscription's state.

The fix uses existing shared sockets and Unix users/groups for common resources,
while keeping account homes and credentials private. The owning worker verifies
retained assets and native history before continuation admission; frozen identity,
idempotency and final pre-spawn checks remain in force. The broker rechecks current
policy and returns only decisions, not raw configuration, environment values,
credential references or paths.

Tests must cover distinct real UIDs, a successful continuation, refused
tampering without consuming the parent continuation, concurrent resumes and
safe fallback after a classified account failure. Continue testing ordinary
single-user operation on macOS and Linux.
