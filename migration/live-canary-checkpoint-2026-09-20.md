# Parallel live-canary checkpoint — 20 September 2026

The sealed Rust broker candidate remains
`/Users/pluto/projects/agent-run/.canary-runtime-CnEIkl/agent-run`, SHA-256
`7f959c3c85370d899acbf3b8e3e55bd81a1fdd684f39895e8bf2367dacb77151`.
Its isolated home is `/private/tmp/agent-run-live-launchd.CnEIkl/home`.

The broker ran successfully in the managed shell and answered socket ping, but
real Codex, Claude and GLM canaries could not authenticate because that process
could not read the user's native auth/Keychain state. Two separate launchd
bootstrap attempts returned `Input/output error`; `launchctl submit` also
refused the temporary job. No provider canary reached a model.

Production is unchanged and available on Python release
`c9904f9843ba4a0772bdfa8bac5259f18fad9dc3`, schema 16, with zero active agents.
The Rust candidate exists in the production releases directory only as an
inactive immutable release; the production pointer, plist, config and job were
not switched.

The next step is to start the isolated broker from the owner's ordinary Terminal
so it inherits already-authorized native authentication, then run the three
canaries through its private socket. The exact foreground command is:

```sh
env -u CLAUDE_CONFIG_DIR \
  HOME=/Users/pluto \
  CODEX_HOME=/Users/pluto/.codex \
  PATH=/Users/pluto/.nvm/versions/node/v24.4.0/bin:/Users/pluto/.local/bin:/usr/bin:/bin \
  /Users/pluto/projects/agent-run/.canary-runtime-CnEIkl/agent-run \
  --home /private/tmp/agent-run-live-launchd.CnEIkl/home api serve
```

Keep that Terminal process running until Codex, Claude and GLM have terminal
verdicts and the broker is explicitly stopped.

## Owner-terminal result

The owner started the same isolated broker from an ordinary Terminal. All three
real provider canaries then completed through its private socket:

| Runtime | Model | Agent | Result | Exact answer | SHA-256 |
|---|---|---|---|---|---|
| Codex | `gpt-5.6-luna` | `ag-20260920-185726-297246d513` | `succeeded` | `CODEX_CANARY_OK` | `61a9308cf700b3ce80c6fead4b5fcec6462e8e56526988d68b3c743669e1975c` |
| Claude | `sonnet` | `ag-20260920-185706-d83261c725` | `succeeded` | `CLAUDE_CANARY_OK` | `4b647199756d38e9ed53c60f8edb5dc63a12062f3dfb16a20fdeef895dad56b6` |
| GLM | `glm-5.3-flash` | `ag-20260920-185702-265438c7d2` | `succeeded` | `GLM_CANARY_OK` | `97f04f1256dd5827f67f09a393e3b135749d26d1b10fd470c3144a5394639951` |

Each answer was read back through the Rust CLI with proof version 2, exact byte
size and matching digest. Provider transcripts contain the submitted task and
provider-produced answer; Codex additionally recorded a streamed provider
message reference. All child process groups and descendants were confirmed
gone after completion. Production remained unchanged.
