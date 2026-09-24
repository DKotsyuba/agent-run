# Account registry and protected references

Schema v17 stores one global `AccountId`, auth family, nonsecret credential
reference, and enabled/disabled status per physical account. Provider-local
TOML bindings may share that id. Registration rejects duplicate ids and
duplicate auth-family/reference storage identities; it never inspects token
bytes. Disabling an account prevents future lease selection and keeps prior
attempts and history readable. Native Codex references require the `openai`
auth family; native Claude Code references require `anthropic`.

Register an existing store, list metadata, or disable future selection:

```sh
agent-run accounts register --id <id> --auth-family <family> --reference <reference>
agent-run accounts list
agent-run accounts disable <id>
```

Listing returns ids, families, status, and source kinds without full
references. These commands do not authenticate or refresh a native login.

Reference forms are `native:codex`, `native:claude-code`,
`named:codex:<label>`, `named:claude-code:<label>`,
`env:<UPPERCASE_NAME>`, `file:<absolute-path>`, and
`keychain:<service>:<account>`. They are storage locations, never secret
values. Native and named forms preserve the harness's own protected login and
refresh lifecycle. File, environment, and Keychain values are read only when
an authorized custom request is prepared.

`agent-run auth` accepts a provider-local label or a global account id for a
native-login provider. If the same spelling names the label of one binding
and the global id of another, login refuses it as ambiguous before touching
either protected credential store; use an unambiguous label or id.

The quota-side Rust boundary is `Store::list_accounts()` or
`Store::account(id)`, followed by
`ProviderConfig::resolve_catalog(account_records)` and
`AuthorizedRequest::new(catalog, provider, model, account)`. Construction
checks the selected enabled account and explicit provider/model binding.
`AuthorizedRequest::send` accepts only URLs on the configured custom
scheme/host/port, constructs authorization inside Rust, and disables HTTP
redirects. `CredentialReader` is injectable for synthetic tests;
`SystemCredentialReader` uses the existing host environment, file, or
platform Keychain. Native providers cannot mint an exportable HTTP token.
The quota consumer must keep prepared request headers out of Lua, logs,
snapshots, and response bodies.

Custom gateway `auth_header = "bearer"` is the default; the explicit
`x_api_key` form is available for compatible Messages gateways. This
inference-gateway header choice does not define GLM's separate quota-endpoint
Authorization behavior; that endpoint and its authorized origin belong to
the quota collector integration.
