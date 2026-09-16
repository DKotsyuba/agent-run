# Engines inventory: Claude, GLM, Qwen adapters + shared adapter layer

> **Historical analysis — the addresses below are not current.** This inventory
> records the state of the port at the time it was written, when the Rust code
> was a single `rust/` crate. That tree no longer exists; the port is now the
> `crates/` workspace, and the parity figures quoted here were superseded long
> ago. The addresses are kept verbatim because rewriting them would attach an
> old measurement to code it never described. For what is covered today, see
> `migration/status.md`, `migration/baseline/test-map.csv` and
> `migration/evidence/qualification-scope.md`.

Area owner: EN. Scope: `src/agent_run/adapters/{base,registry,environment,version,plugin_skills,continuation,omniroute}.py`,
`adapters/claude/*` (not `limits.py`), `adapters/glm/*`, `adapters/qwen/*`.
Rust counterparts actually live in `rust/src/adapters/{stream,materialize,plugins,io,mod}.rs`,
`rust/src/profiles.rs`, `rust/src/policy.rs`, plus scattered hits in `rust/src/{config,service,cli}.rs`
and `rust/src/capacity/sources.rs`.

**Correction to the brief's file map:** Claude/GLM/Qwen argv construction, stream-json decoding, and
result classification are in `rust/src/adapters/stream.rs` (`plan()` L17-190, `run()` L201-424,
`result_text()` L191-200) — verified directly by reading the file. `rust/src/adapters/codex.rs`
(709 lines) is Codex-only; it has zero `Adapter::Claude|Glm|Qwen` references (confirmed by grep).
An earlier draft of this investigation mislabeled these functions as living in `codex.rs`; that was wrong.

## 1. Summary

- **Overall behavioral parity: roughly 55-60%.** The core "launch a CLI, stream stdout, classify the
  result" path is genuinely ported and reasonably faithful for Claude, GLM, and Qwen. What's missing
  clusters in three places: credential resolution (Keychain), the OmniRoute integration (0% ported),
  and defense-in-depth (secret redaction, diagnostics, fail-closed config validation).
- **The read-only-Claude-omits-Bash "gap" named in the task brief does not exist.** Both Python
  (`adapters/claude/adapter.py:239`) and Rust (`stream.rs:78-86`) exclude `Bash` from read-only roles.
  Python's own test (`tests/test_claude_adapter.py:507-508`) asserts this. No work package is needed here.
- **Three blocking gaps for daily use, all silent (no error, just different behavior):**
  1. No macOS Keychain credential fallback for GLM or Qwen/OmniRoute — the owner's actual auth path.
  2. OmniRoute quota collection is entirely unimplemented (accepted as a config string, then errors
     `"selected quota source has not been ported"` at collection time).
  3. Host-environment inheritance changed from Python's deny-list (inherit everything except
     secret-shaped names) to Rust's allow-list of 13 vars — this silently drops arbitrary host env
     (proxies, `NODE_OPTIONS`, project-specific vars) that Python would have passed through.
- **One confirmed functional regression:** GLM's 1M-context model suffix (`glm-5.3[1m]` etc.) and
  `ANTHROPIC_MODEL` export are not ported, so those models silently run at Claude Code's 200k
  auto-compact ceiling instead of their real window.
- **Security-relevant gap:** Python redacts secret-shaped keys and literal known secrets from every
  raw stream line before persisting it (`claude/stream.py:sanitize_line`); Rust has zero redaction
  code anywhere (`grep -rn "redact|sanitize" rust/src/` — no hits). Transcripts persisted to SQLite via
  `mod.rs::journal()` are unredacted.
- **Test coverage on the Rust side is thin:** `rust/tests/` has exactly one hit for glm/qwen
  (`domain_config.rs:160-161`, reserved-settings-keys only). No Rust test exercises Claude/GLM/Qwen
  argv construction, streaming, or OmniRoute.

## 2. Module map

| Python module | Responsibility | Rust counterpart | Status | Concrete gaps |
|---|---|---|---|---|
| `adapters/base.py` (260L) | `LaunchPlan`, `Capability` enum, `RuntimeAdapter`/`EventSink` protocol | `mod.rs::LaunchPlan` (L16-22), `mod.rs::capabilities()` (L67-86) | partial | Rust's plan is an in-process struct (no cross-process payload contract, architecturally fine for a single-process async design); `Capability` is a `Vec<&str>` not an enum, so no compile-time exhaustiveness check when a new capability is added |
| `adapters/registry.py` (113L) | Dynamic `module:attribute` adapter import, API-version + capability contract check | `config.rs::Adapter::parse` (L17-45, static 4-variant enum) | different-by-design | Rust has no plugin-loading; this is a deliberate scope cut (4 compiled adapters), not a bug — flag if the owner ever wants third-party adapters |
| `adapters/environment.py` (79L) | `host_environment()`: inherit all host env except secret-shaped names + 21-name credential-carrier denylist; preserve Rust toolchain homes | `materialize.rs::environment()` (L209-399) | different-by-design, **compatibility-breaking** | Rust uses a 13-name allow-list (`materialize.rs:219-233`) instead of Python's deny-list (`environment.py:11-37,60-68`); any host env var outside that allow-list (proxies, `NODE_OPTIONS`, direnv vars, project config) silently disappears for the child, where Python would have passed it through |
| `adapters/version.py` (120L) | `observe_binary_version()`: bounded `--version` probe via `select()`, kills owned pgid on timeout | `capacity/sources.rs::capture()` (L294-346, generic bounded subprocess capture reused at L676) | ported | Different mechanism (tokio timeout vs `select()` loop) but same contract; not adapter-specific in Rust |
| `adapters/plugin_skills.py` (95L) | Shared skill-ownership resolution (plugin-owned vs runtime-owned dirs), `unlisted_plugin_skills()` fail-closed check for whole-plugin-load adapters | `materialize.rs:418-426` (inline plugin-then-catalog skill source resolution) | partial | `unlisted_plugin_skills()` (`plugin_skills.py:73-95`, used by `glm/adapter.py:97-102`) has no Rust equivalent — a GLM run can silently receive an undeclared skill through a whole-loaded plugin |
| `adapters/continuation.py` (31L) | `cli_resume_plan()`: shared resume-argv rewrite (strip `--session-id`, append `--resume`) for Claude/Qwen | Inlined in `stream.rs:169-171,180-182` | ported | Same logic, not factored into a shared function; low risk but two call sites can drift |
| `adapters/omniroute.py` (349L) | `pool_samples()`: docker-exec into the OmniRoute container, query `provider_connections`/`key_value` sqlite tables, average the `opencode-go` pool, staleness (5400s)/overflow(>64 rows)/malformed-data handling | none | **missing** | Zero implementation. `capacity/sources.rs:540` accepts `"omniroute"` as a config string; `sources.rs:594-601`'s `collect()` match has no arm for it and falls to `Err(Unsupported("selected quota source has not been ported"))`. Qwen's own `limits()` (which in Python returns this pool, `qwen/adapter.py:351-359`) is unreachable in Rust — `sources.rs:529`: `Adapter::Qwen => Err("codexbar has no Qwen provider")` |
| `adapters/claude/adapter.py` (428L) | `ClaudeAdapter`: validate/materialize/probe/models/limits/prepare/launch | `stream.rs::plan()`+`run()`, `mod.rs::validate()`/`capabilities()`, `materialize.rs::materialize()` | partial | See §3 per-behavior breakdown |
| `adapters/claude/session.py` (377L) | Process lifecycle: nonblocking bounded stdin, process-group cancel (SIGINT→poll→SIGKILL), outcome sealing on `wait()` | `adapters/io.rs::Process` (spawn/send/text/reap, L36-193) | partial | Async/tokio instead of a thread-based session; cancellation goes through `OwnedProcess`/polling (`stream.rs:222`), not directly comparable line-for-line but same contract |
| `adapters/claude/stream.py` (354L) | `StreamDecoder`: parses stream-json lines, `sanitize_line`/`_redact`, `classify_failure`/`_AUTH_MARKERS`, cut_off vs no_answer finalize, diagnostic events for malformed/unknown lines | `stream.rs::run()` inline match (L292-421), `result_text()` (L191-200) | partial | Missing: redaction (§1), auth-failure marker classification, cut_off/no_answer/empty_result distinction, soft diagnostics for malformed/unknown/duplicate lines (Rust hard-errors instead) |
| `adapters/claude/materialize.py` (158L) | Renders `settings.json`, MCP config, per-skill plugin dirs, declared-plugin snapshots | `materialize.rs::materialize()` (L400-582, `Adapter::Claude\|Glm` arm L545-553) + `plugins.rs::install()` | ported | Functionally equivalent; snapshot format is Rust-specific (`.agent-run-rust-snapshot.json`, not interchangeable with Python's, per `MIGRATION_STATUS.md`) |
| `adapters/claude/launch_io.py` (90L) | `abort_launch`, `open_runtime_log` (0600, append-only raw+redacted line log), `known_secrets` | none found | **missing** | No private runtime-stream log file exists in Rust; `journal()` (`mod.rs:104-122`) writes straight to SQLite, no on-disk raw log, no redaction |
| `adapters/claude/auth.py` (94L) | `AUTH_ENV_NAMES`, `claude_config_dir` (scoped `CLAUDE_CONFIG_DIR`), `claude_login_environment` | `materialize.rs::environment()` L252-262, `cli.rs:435-445` (`claude auth login` flow) | ported | Env allow-list is inlined generically rather than a named constant; functionally equivalent for the account-scoped case |
| `adapters/claude/stderr.py` (81L) | `StderrTail`: bounded, redacted stderr tail capture used in failure diagnostics | `io.rs::Process` only counts stderr bytes (`stderr_bytes: AtomicU64`, L33,87-97) | **missing** | Stderr text is fully discarded in Rust; failure diagnostics lose the tail Python surfaces |
| `adapters/claude/constants.py` (34L) | `READ_TOOLS`/`WRITE_TOOLS`/`SHELL_TOOLS`/`NETWORK_TOOLS`/`ALWAYS_DISALLOWED`, `MODEL_ALIASES`, `MODEL_DESCRIPTIONS`, `KNOWN_HOOK_EVENTS`, `SUPPORTED_EFFORTS` | Inlined literals in `stream.rs:74-142`, `mod.rs:54-56` | partial | Tool-set logic is ported correctly (verified, see §3); `KNOWN_HOOK_EVENTS` validation (`adapter.py:88-93`) has no Rust equivalent — any hook event string is accepted; `MODEL_DESCRIPTIONS`/roster metadata not found in Rust `models()` path |
| `adapters/glm/adapter.py` (161L) | `GlmAdapter(ClaudeAdapter)`: auth pair, `[1m]` million-context suffix for `glm-5.3`/`glm-5.3-flash`/`glm-5.2`, `ANTHROPIC_MODEL` export, `unlisted_plugin_skills` validation | `materialize.rs:366-377` (env only), `config.rs` enum | partial | Million-context suffix rewrite and `ANTHROPIC_MODEL` export: **not found anywhere in `rust/src`** (grepped for `1m]`, `glm-5`, `MILLION`, `ANTHROPIC_MODEL` — zero hits). `unlisted_plugin_skills` check also absent |
| `adapters/glm/auth.py` (131L) | macOS Keychain lookup (`com.pluto.agent-run.glm` / `GLM_CODING_KEY`), 300s cache, `reset_keychain_cache()`, `DEFAULT_BASE_URL` | `materialize.rs:369-377` (base URL hardcoded; requires `ANTHROPIC_AUTH_TOKEN`/`ANTHROPIC_API_KEY` already present in resolved env) | partial | Keychain fallback and caching: **entirely missing** |
| `adapters/qwen/adapter.py` (512L) | Full lifecycle incl. sandboxed one-shot launch, error-only-result detection (`[API Error:` prefix) | `stream.rs:36-72` (argv), `materialize.rs:378-386,554-581` (env+settings), `stream.rs:392-400` (API-error classification) | ported (core path) | Core argv/settings/error-classification genuinely ported; see auth and OmniRoute gaps below |
| `adapters/qwen/auth.py` (68L) | Keychain lookup for OmniRoute API key (`com.pluto.agent-run.opencode.omniroute` / `OMNIROUTE_API_KEY`), default base URL `http://127.0.0.1:20128/v1` | none found (grepped for "keychain", "20128") | **missing** | No Keychain fallback, no default OmniRoute base URL; Rust requires `OPENAI_API_KEY`/`OPENAI_BASE_URL` explicitly declared or the run fails closed (`materialize.rs:378-386`) |
| `adapters/qwen/plugins.py` (99L) | Copy plugin dirs into `.qwen`-style home, `{plugin:NAME}` token expansion | `plugins.rs::install()` (L103-217), `expand()` (L218-234) | ported | Same mechanism, generalized across adapters |
| `adapters/qwen/skills.py` (109L) | Skill tree snapshot + prune of stale adapter-owned skills + context-note paragraph | `materialize.rs:418-445` (tree copy), `:572-578` (context note) | partial | No selective-prune pass found; likely moot since Rust rebuilds the whole home fresh per `Publisher::new()`, but not verified equivalent for an update-in-place scenario |

## 3. Public contract surface

### Claude (native CLI)

| Surface | Python | Rust | Status |
|---|---|---|---|
| argv shape | `claude/adapter.py:262-298`: `--print --output-format stream-json --input-format stream-json --verbose --model <id> --permission-mode <default\|acceptEdits> --setting-sources "" --strict-mcp-config --settings <home>/settings.json [--mcp-config ...] [--plugin-dir ...]* [--add-dir ...]* --tools <csv> --allowedTools <csv> --disallowedTools <csv> [--effort <e>] --append-system-prompt <text> --session-id <uuid>` | `stream.rs:121-149`: same flag set and order, `--resume <id>` appended when resuming (`:180-182`) | ported |
| tool sets (read-only vs write) | `constants.py:14-19` + `adapter.py:238-249`: read-only = `Read,Grep,Glob[,Skill]`; write adds `Edit,Write,NotebookEdit,Bash`; network adds `WebFetch,WebSearch`; `--disallowedTools` gets `WebFetch,WebSearch` unless network | `stream.rs:74-115`: same composition, same Bash-only-on-write gating (verified match — no divergence) | ported |
| settings.json / hooks | `claude/materialize.py:34-62`: native settings merged with generated `hooks` object, validated against `CLAUDE_RESERVED_ROOTS` | `materialize.rs:545-548`, reserved-key check via `config.rs::CLAUDE_RESERVED` (L552,557) | ported (field set of `CLAUDE_RESERVED` vs Python's not cross-checked field-for-field — unverified) |
| MCP config | `claude/materialize.py:65-94`: strict `mcpServers` JSON, fails closed on unresolved name | `materialize.rs:446-461,549`: same shape | ported |
| Auth | `claude/auth.py`: env names `CLAUDE_CODE_OAUTH_TOKEN`/`ANTHROPIC_API_KEY`/`ANTHROPIC_AUTH_TOKEN` (`constants.py:20`), `CLAUDE_CONFIG_DIR` scoping per account, interactive login flow | `materialize.rs:252-262` (account-scoped `CLAUDE_CONFIG_DIR`), `cli.rs:435-445` (login) | ported |
| macOS Keychain | Not applicable for plain Claude (uses native `claude login`, not Keychain) — n/a | n/a | n/a |
| stream-json events | `system`,`assistant`,`user`,`result`,`stream_event` (`message_start`,`content_block_delta`) — `stream.py` full decoder + redaction + diagnostics | `stream.rs:292-421`: same event types handled, same journal split logic; **no redaction, no soft diagnostics** | partial |
| result/error classification | `stream.py:203-354`: `auth_failed` (marker scan), `max_turns`, `cut_off`, `no_answer`, `empty_result`, `provider_error` | `stream.rs:381-405`: `success`, `max_turns` (`error_max_turns`), generic `runtime_failed`, `missing_result` on bad EOF | partial — fewer distinguishable failure kinds |
| resume/steer | `--resume <session>`, live steer via stdin `user` message injection, `--session-id` fresh per non-resume launch | `stream.rs:180-182` (resume), `:238-248` (steer via `process.send`) | ported |
| read-only vs write | See tool sets above | Same | ported |

### GLM (claude CLI pointed at api.z.ai)

| Surface | Python | Rust | Status |
|---|---|---|---|
| Everything Claude has | Inherited verbatim via subclassing `ClaudeAdapter` (`glm/adapter.py:76-158`, docstring L1-14 states explicitly "argv shape, stream decoding, session lifecycle, hook and plugin semantics -- is the claude adapter's, unmodified") | Inherited via shared `match kind { Claude \| Glm => ... }` branches throughout `stream.rs`/`materialize.rs`/`mod.rs` | ported (same architecture as Python) |
| Auth | `glm/auth.py`: macOS Keychain (`com.pluto.agent-run.glm`/`GLM_CODING_KEY`) first, env fallback, 300s cache | `materialize.rs:366-377`: env only (`ANTHROPIC_AUTH_TOKEN`/`ANTHROPIC_API_KEY` must be pre-declared) | **missing Keychain fallback** |
| Base URL | `glm/auth.py:DEFAULT_BASE_URL` = `https://api.z.ai/api/anthropic` | `materialize.rs:369-371`: same URL hardcoded | ported |
| Million-context suffix | `glm/adapter.py:46-48,145-153`: appends `[1m]` to `--model` for `glm-5.3`/`glm-5.3-flash`/`glm-5.2`, live-verified 2026-08-30 | none | **missing — functional regression** |
| `ANTHROPIC_MODEL` export | `glm/adapter.py:40,155-157` | none | **missing** |
| Whole-plugin skill validation | `glm/adapter.py:97-102` (`unlisted_plugin_skills`) | none | **missing** |

### Qwen (sandboxed headless CLI)

| Surface | Python | Rust | Status |
|---|---|---|---|
| argv shape | `qwen/adapter.py` (not individually cited by line here; confirmed via cross-agent read): `-p <task> --output-format stream-json --approval-mode <plan\|yolo> --sandbox --model <id>` | `stream.rs:36-72`: identical shape | ported |
| macOS Git bootstrap | Not present in Python's `qwen/adapter.py` per full read by the parallel research pass — **this is a Rust-only addition**, not a Python behavior being ported | `stream.rs:38-58`: `xcrun --find git` PATH bootstrap, errors if missing | different-by-design (Rust added this; verify it doesn't reject configurations Python accepted) |
| settings.json | `.qwen/settings.json` with `context`, `tools.sandbox`, `security.auth.selectedType`, `mcpServers`, `hooks` | `materialize.rs:554-581`: same fields | ported |
| Auth | `qwen/auth.py`: Keychain (`com.pluto.agent-run.opencode.omniroute`/`OMNIROUTE_API_KEY`), default base URL `http://127.0.0.1:20128/v1` (the local OmniRoute router) | `materialize.rs:378-386`: requires `OPENAI_API_KEY`+`OPENAI_BASE_URL` pre-declared, no default, no Keychain | **missing Keychain fallback and default base URL** |
| Result/error classification | `[API Error:` prefix → `provider_error` (`qwen/adapter.py`) | `stream.rs:392-400`: same check, same outcome | ported |
| Native `permissions.deny` hardening | `command_policy.py::render_qwen_denials` (second enforcement layer beyond PATH shim) | none beyond the generic PATH-refusal shim (`materialize.rs:583-620`) | **missing** |
| Capabilities | No `steer`/`effort`/`output_schema`/`fast` (validated in `mod.rs:34-36`, matches Python's Qwen restrictions) | Same | ported |
| Resume | `--resume <session>` | `stream.rs:180-182` | ported |
| OmniRoute integration | `omniroute.py::pool_samples()` feeds Qwen's `limits()` | none — unreachable (`sources.rs:529`) | **missing** |

## 4. Test mapping

| Python test file | Behaviors covered | Rust test covering it |
|---|---|---|
| `tests/test_claude_adapter.py` | Argv construction, read-only excludes Bash (`:507-508`, verified directly), write includes Bash (`:530-536`, verified directly), MCP config, settings.json, effort validation | none |
| `tests/test_claude_session.py` | Session lifecycle: stdin writes, cancel (SIGINT→SIGKILL), `wait()` outcome sealing | none |
| `tests/test_claude_stream.py` | `StreamDecoder`, `sanitize_line`/redaction, terminal-event shaping, diagnostics | none |
| `tests/test_claude_developer_environment.py` | Compatibility checks for ignored Claude environment presets | none |
| `tests/test_claude_uds.py` | Unix-domain-socket steer/delivery transport, timeout/missing-session/unreachable-socket classification (overlaps delivery area) | none |
| `tests/test_glm_adapter.py` | GLM auth, million-context suffix, prepare/validate reusing Claude adapter | none |
| `tests/test_qwen_adapter.py` | Qwen argv, materialize, sandbox, error-only-result detection | none |
| `tests/test_omniroute_current_cache.py` | `pool_samples()` current-vs-stale-cache regression | none |
| `tests/test_adapters_base.py` | `LaunchPlan`, `RuntimeHealth`, base adapter contract | none |
| `tests/test_adapter_environment.py` | `host_environment()` deny-list behavior | none — this is exactly the behavior Rust diverges on (§1); a Rust port of this test would catch the allow-list regression |
| `tests/test_adapter_home.py` | Generated home layout / managed-file writes | `rust/tests/domain_config.rs` (partial, reserved-keys only) |
| `tests/test_adapter_versions.py` | `observe_binary_version()` bounded probe | none |
| `tests/test_resume_adapters.py` | Cross-adapter resume/continuation semantics incl. Qwen `--resume` | none |
| `tests/test_plugin_integration.py` | Plugin/skill materialization end-to-end | none |
| `tests/stub_engine_adapter.py` | Test fixture (stub adapter), not a test itself | n/a |
| `tests/test_command_policy.py` (shared with Codex agent) | `render_claude_denials`/`render_qwen_denials` Bash denial-pattern rendering | none (Rust has only the generic PATH-refusal shim) |
| `tests/test_native_settings.py` (shared with core agent) | Reserved-root enforcement for native settings across claude/qwen/glm/codex | `rust/tests/domain_config.rs:160-161` (partial: apiKeyHelper/tools reserved-key checks) |

## 5. Work packages

Ordered by dependency and blocking status.

### EN-1 — Keychain credential fallback for GLM and Qwen/OmniRoute [blocking]

**What:** Port `adapters/glm/auth.py` (131L, whole file) and `adapters/qwen/auth.py` (68L, whole file):
macOS Keychain lookups (`com.pluto.agent-run.glm`/`GLM_CODING_KEY`,
`com.pluto.agent-run.opencode.omniroute`/`OMNIROUTE_API_KEY`), 300s in-process cache, env-var fallback,
default base URLs (`https://api.z.ai/api/anthropic` already hardcoded in Rust; `http://127.0.0.1:20128/v1`
is not). Wire into `materialize.rs::environment()` L367-386 (replace the current
"error if not already declared" branches with keychain-then-env resolution).

**Rust target files:** new `rust/src/adapters/keychain.rs` (macOS `Security` framework binding or shell
out to `/usr/bin/security`), edits to `materialize.rs:366-386`.

**Dependencies:** none within this area; check whether a Keychain-access helper already exists
elsewhere in the Rust tree (Codex auth path) before writing a new one — grep `security find-generic-password`.

**Acceptance:** port `tests/test_glm_adapter.py`'s Keychain-fallback cases and `tests/test_qwen_adapter.py`'s
auth cases to `rust/tests/`; add a test asserting env-var still wins when Keychain has nothing (matches
Python's fallback order).

**Est. LOC:** 300-450. **Blocking**: this is the owner's actual credential path per their own memory
notes on OmniRoute/GLM account policy; without it every GLM/Qwen run needs a manually exported env var.

### EN-2 — OmniRoute pool-quota collector [blocking]

**What:** Port `adapters/omniroute.py` in full (349L): `docker exec` into the `omniroute` container,
run the embedded Node/better-sqlite3 script against `provider_connections`/`key_value`, parse rows,
average the `opencode-go` pool per window (`session_5h`, `weekly`, `mcp_monthly`), apply the 5400s
staleness bound and 64-row overflow cap, reject malformed data. Wire into `capacity/sources.rs::collect()`
(add an `("omniroute", _)` arm before the catch-all at L594-601) and give Qwen a working `limits()` path
(replace the `codexbar` "no Qwen provider" error at `sources.rs:529` with a call into the new collector
when the runtime's `limits_source` is `"omniroute"`).

**Rust target files:** new `rust/src/capacity/omniroute.rs`, edits to `rust/src/capacity/sources.rs`
(~L525-530, ~L594-601), possibly `config.rs` if source-name plumbing needs adjustment.

**Dependencies:** capacity/delivery area owns `capacity/sources.rs`'s overall shape — coordinate
before adding a new module there; this WP only adds one source implementation, doesn't restructure.

**Acceptance:** port `tests/test_omniroute_current_cache.py` in full (it's specifically the
current-vs-stale-cache regression test) plus a fresh-data/malformed-data/overflow test matching
`omniroute.py`'s `CapacitySourceError` cases (`omniroute_unavailable`, `omniroute_malformed_data`,
`omniroute_result_overflow`).

**Est. LOC:** 400-600. **Blocking**: the owner relies on OmniRoute-fed Qwen quota visibility
day-to-day (per their own capacity-monitoring workflow); today it silently reports "not been ported".

### EN-3 — Host-environment inheritance: switch allow-list to deny-list [blocking]

**What:** Replace `materialize.rs::environment()`'s fixed 13-name allow-list (L219-233) with Python's
model from `adapters/environment.py` (79L, whole file): inherit all host env vars except those matching
the secret-name regex (`(key|token|secret|password|credential)`, case-insensitive) and a fixed 21-name
credential-carrier set (AWS/Azure/gcloud/Docker/git/npm/pip/ssh/kubeconfig/etc. config-file vars).
Preserve the existing explicit overrides (`HOME`, `CODEX_HOME`, `CLAUDE_CONFIG_DIR`, managed Rust
toolchain vars) — those still win, same as today.

**Rust target files:** `rust/src/adapters/materialize.rs:209-399` (rewrite the collection loop at
L219-237, keep everything after L238 that layers on top).

**Dependencies:** none.

**Acceptance:** port `tests/test_adapter_environment.py` (whole file) — it directly tests
`host_environment()`'s deny-list behavior; add a case for a var outside both the old allow-list and the
deny-list (e.g. `HTTP_PROXY`, `NODE_OPTIONS`) to prove it now passes through.

**Est. LOC:** 150-250. **Blocking**: silent, compatibility-breaking difference — any host env var a
daily workflow depends on being visible to a launched Claude/Codex/GLM/Qwen process (proxies, tool
configs, project env) currently vanishes with no error.

### EN-4 — GLM million-context suffix + ANTHROPIC_MODEL export [blocking]

**What:** Port `adapters/glm/adapter.py:41-48,145-158`: for `glm-5.3`/`glm-5.3-flash`/`glm-5.2`, append
`[1m]` to the `--model` argv value (not the request/roster/persisted model id — argv only) and set
`ANTHROPIC_MODEL` env to the same suffixed value.

**Rust target files:** `rust/src/adapters/stream.rs` (near L116-120, where `Adapter::Claude` already
has a `fable` special-case — add the GLM million-context case alongside it) or `materialize.rs`
depending on where the env var is easiest to inject post-argv-build.

**Dependencies:** none; small, self-contained.

**Acceptance:** port the relevant cases from `tests/test_glm_adapter.py` (argv contains `[1m]` suffix
for the three listed models, `ANTHROPIC_MODEL` env set, other models pass through unsuffixed).

**Est. LOC:** 60-120. **Blocking** despite small size: it's a silent quality regression for any
`glm-5.x` user today (200k ceiling instead of 1M), and it's cheap to fix — do it early.

### EN-5 — Secret redaction for stream lines and persisted transcripts [blocking]

**What:** Port `adapters/claude/stream.py`'s `_redact`/`_safe_event_data`/`sanitize_line`
(lines 1-70, plus the `is_secret_env_name` import from `environment.py:11,40-43`, which EN-3 already
ports): strip known literal secret values and structurally redact secret-shaped JSON keys from every
raw stream line before it is journaled or would be logged. Apply this in `stream.rs::run()` before
each `journal()` call (L311,325-332,338,341,358-365) and to whatever the equivalent of a raw-line log
would be (there currently isn't one — see EN-8 for that separate gap).

**Rust target files:** new `rust/src/adapters/redact.rs`, call sites in `stream.rs` around the
`journal()` calls listed above.

**Dependencies:** none; can land independently of EN-1/EN-2.

**Acceptance:** port `tests/test_claude_stream.py`'s `SanitizeLineTests` cases — literal-secret
substitution, key-shaped redaction, numeric-field-not-redacted-even-if-key-says-"token".

**Est. LOC:** 150-250. **Blocking**: without this, any secret a child process echoes (a
misconfigured tool printing an env var, an error message containing a token) is persisted verbatim
into the long-lived SQLite transcript store — a real data-handling regression, not a cosmetic one.

### EN-6 — Auth-failure classification and stderr tail capture [later]

**What:** Port `adapters/claude/stream.py:203-235` (`_AUTH_MARKERS`, `classify_failure`) — scan result
text for phrases like "failed to authenticate", "oauth token has expired", "please run /login" and
classify as `auth_failed` instead of generic `runtime_failed`. Port `adapters/claude/stderr.py`
(81L, whole file) — bounded, redacted stderr tail capture (depends on EN-5's redaction primitive) —
and thread it into the failure path so `no_answer`/`provider_error` cases carry diagnostic text.

**Rust target files:** `stream.rs:381-405` (classification), `io.rs::Process` (add a bounded stderr
tail buffer alongside the existing byte counter at L33,87-97).

**Dependencies:** EN-5 (reuses the redaction primitive for the stderr tail).

**Acceptance:** port the `classify_failure` cases from `tests/test_claude_stream.py` and the
`StderrTail` bounding/redaction tests from wherever they live (likely `test_claude_session.py` or
`test_claude_stream.py` — confirm on read).

**Est. LOC:** 250-400. **Later**: real diagnostic-quality loss but not a functional blocker — runs
still complete and fail correctly, just with a less specific failure kind and no stderr context.

### EN-7 — Fail-closed config validation: KNOWN_HOOK_EVENTS + unlisted_plugin_skills [later]

**What:** Port two validation checks that currently accept configs Python would reject:
`adapters/claude/constants.py:31-34` + `adapter.py:88-93` (hook event name must be a known Claude Code
event) and `adapters/plugin_skills.py:73-95` + `glm/adapter.py:97-102` (`unlisted_plugin_skills`: a
GLM/Claude plugin that's loaded whole must have every skill it ships declared in
`runtimes.<name>.skills`, or config load fails closed).

**Rust target files:** wherever runtime config validation happens for Claude/GLM — likely a new
`validate()` extension in `mod.rs` or `config.rs`; check with the core/config area owner before adding,
since `config.rs` is shared.

**Dependencies:** coordinate with the core/config area (this touches `config.rs` validation, which
they own).

**Acceptance:** port the relevant `tests/test_claude_adapter.py`/`test_glm_adapter.py` cases for
unknown hook events and undeclared plugin skills.

**Est. LOC:** 120-200. **Later**: these are safety nets against misconfiguration, not runtime bugs —
a bad config currently either works anyway (harmless) or fails elsewhere less clearly.

### EN-8 — Stream diagnostics: soft-fail malformed/duplicate lines, distinguish EOF failure kinds [later]

**What:** Port `adapters/claude/stream.py:301-354`: instead of hard-erroring on a second terminal
result line or unparsable JSON (current Rust behavior: `stream.rs:371-373` returns `Err`, `io.rs:75`
treats bad JSON as `Event::Failure`), emit a soft diagnostic and continue where Python does. Also port
the `cut_off`/`no_answer`/`empty_result` distinction on abnormal EOF (`stream.py:335-358`) to replace
Rust's single generic `missing_result` (`stream.rs:264`).

**Rust target files:** `stream.rs:254-280` (EOF handling), `:370-373` (duplicate result), `io.rs:75`
region (malformed-JSON handling — check exact line, this is in the frame-reading task not shown in
full above).

**Dependencies:** none, but do after EN-6 since both touch the same failure-classification code paths
— combining review is cheaper than two separate diffs to `stream.rs`'s result handling.

**Acceptance:** port `tests/test_claude_stream.py`'s diagnostic-event tests (`malformed_json_line`,
`unknown_type:*`, `duplicate_terminal_line`) and the `cut_off`/`no_answer`/`empty_result` cases.

**Est. LOC:** 200-350. **Later**: robustness improvement against a chatty/buggy CLI version; current
behavior (hard error) is safe, just less informative and slightly more fragile.

### EN-9 — Qwen native permissions.deny hardening layer [later]

**What:** Port the Qwen-specific half of `command_policy.py` (function `render_qwen_denials`, exact
lines not confirmed — read `command_policy.py` first, it's owned by the Codex agent but this specific
function is Qwen's) — a second denial-enforcement layer via Qwen's native `permissions.deny` settings
field, on top of the already-ported generic PATH-refusal shim (`materialize.rs:583-620`).

**Rust target files:** `materialize.rs:554-581` (Qwen settings block — add a `permissions.deny` field).

**Dependencies:** read `command_policy.py` fully first (owned by the Codex/command-policy agent per
the task split — coordinate before touching, since the function may be shared plumbing).

**Acceptance:** port the Qwen-specific cases from `tests/test_command_policy.py`.

**Est. LOC:** 100-180. **Later**: defense-in-depth on top of an already-working denial mechanism.

## 6. Risks and unknowns

- **The task brief's example gap ("read-only Claude/GLM omits Bash") is not confirmed by evidence** —
  both implementations agree, and Python's own test suite asserts the exclusion. If the owner has
  observed different real-world behavior, it likely traces to a specific profile `.md` file outside
  this repo (`rust/src/profiles.rs:120-123` loads profiles from an external `profiles_dir()`, not
  checked here) rather than to the Claude adapter code — worth asking for the exact profile name before
  spending a WP on it.
- **`native_settings.py`'s exact reserved-key list was not read** in this pass; `config.rs::CLAUDE_RESERVED`
  (L552) was compared only by presence, not field-for-field. Marked `unverified`.
- **`command_policy.py` (Codex agent's area) was only grepped, not read**, for the Qwen-specific
  `render_qwen_denials` function referenced in EN-9 — confirm ownership and exact behavior before
  starting that WP.
- **Two research passes independently read `rust/src/adapters/stream.rs`**; one mislabeled its
  contents as `codex.rs`. This document uses `stream.rs` throughout, verified by direct reads
  (`plan()` at file offset matching L17-190, `run()` at L201-424, confirmed zero `Adapter::Claude`
  references in the real `codex.rs` by grep). If a future pass cites `codex.rs` for Claude/GLM/Qwen
  behavior, treat it as suspect until re-verified.
- **Rust test coverage for this whole area is effectively zero** beyond one reserved-keys check
  (`rust/tests/domain_config.rs:160-161`). Every WP above should land its own tests; there is no
  existing Rust harness to extend, only Python fixtures to port.
- **`tests/test_capacity_sources.py`/`test_capacity_topology.py`/`test_doctor.py`/`test_config.py`**
  likely have incidental GLM/Qwen/OmniRoute coverage but belong primarily to the capacity/config areas
  — flagged for cross-reference, not claimed here.
