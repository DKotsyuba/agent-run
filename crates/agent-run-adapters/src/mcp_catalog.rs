//! Bounded MCP discovery for Claude's native deny rules, without an MCP intermediary.
//!
//! The complement is a launch-time snapshot: tools added by a server after discovery
//! are not covered. Codex uses its native positive allowlist instead of this module.

use crate::{
    LaunchPlan,
    io::{ENGINE_FRAME, Event, Process},
    provider::ProviderLaunchPlan,
};
use agent_run_config::role_plan::ResolvedMcp;
use agent_run_domain::{Error, Result};
use agent_run_platform::process::OwnershipSnapshot;
use serde_json::{Value, json};
use std::{collections::BTreeSet, time::Duration};

/// Maximum discovery time per server, excluding bounded process teardown.
const DISCOVERY_TIMEOUT: Duration = Duration::from_secs(15);
/// Maximum enumeration time across all servers, plus bounded cancellation cleanup.
const TOTAL_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(30);
/// Grace before escalating captured discovery processes from TERM to KILL.
const CLEANUP_GRACE: Duration = Duration::from_millis(250);
/// Bounds catalog size independently of pagination and response frame size.
const MAX_TOOLS: usize = 4096;
/// Bounds cursor traversal, including an otherwise endless sequence of unique cursors.
const MAX_PAGES: usize = 64;
/// Bounds both frame traffic and the eventual native permission argument.
const MAX_RULE_BYTES: usize = 64 * 1024;
/// Bounds all JSON data read during a server's handshake and catalog enumeration.
const MAX_CATALOG_BYTES: usize = 4 * ENGINE_FRAME;

/// Owns a temporary discovery child until cleanup and reaping finish.
struct CatalogChild {
    /// Process with the same cwd and environment as the eventual native harness.
    process: Process,
    /// Cancellation must still clean up while asynchronous teardown is in progress.
    armed: bool,
}

impl Drop for CatalogChild {
    /// Terminates verified identities on cancellation or panic; never signals a reused PID.
    fn drop(&mut self) {
        if self.armed {
            let _ = self.process.owner.cleanup_blocking(CLEANUP_GRACE);
            // Tokio retains responsibility for eventual reaping if exit is not visible yet.
            let _ = self.process.child.try_wait();
        }
    }
}

/// Produces a payload-free preparation failure; server output and secrets never escape.
fn failure(reason: &'static str) -> Error {
    Error::Runtime(format!("MCP tool catalog discovery: {reason}"))
}

/// Accepts only literal native tool-name characters, excluding globs and normalization guesses.
fn literal_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'-'))
}

/// Adds native Claude deny rules derived from each frozen effective allowlist.
///
/// `None` skips discovery; an empty list denies the entire server. Nonempty lists
/// require successful authenticated stdio discovery of every page and every allowed
/// name. All children are bounded and cleaned on success, failure and cancellation.
/// The launch argv changes atomically only after every restricted server succeeds.
/// This function must run before every Claude/GLM attempt, including resume. Native
/// MCP connections remain direct; hot catalog additions require another launch.
/// `observer` must durably persist each root/member snapshot for recovery after
/// supervisor death; observer failures reject discovery and clean the child.
pub async fn apply_claude_tool_filters<F>(plan: &mut ProviderLaunchPlan, observer: F) -> Result<()>
where
    F: Fn(&OwnershipSnapshot) -> Result<()> + Send + Sync + Clone + 'static,
{
    tokio::time::timeout(TOTAL_DISCOVERY_TIMEOUT, apply_filters(plan, observer))
        .await
        .map_err(|_| failure("total discovery deadline exceeded"))?
}

/// Builds a complete native complement under the caller's aggregate deadline.
/// Arguments change only after every server and size check succeeds.
async fn apply_filters<F>(plan: &mut ProviderLaunchPlan, observer: F) -> Result<()>
where
    F: Fn(&OwnershipSnapshot) -> Result<()> + Send + Sync + Clone + 'static,
{
    if !plan
        .role
        .mcp
        .iter()
        .any(|server| server.allowed_tools.is_some())
    {
        return Ok(());
    }
    // Reject ambiguous server/tool separators even on unrestricted peers: native
    // permission strings do not carry separate structured server and tool fields.
    for server in &plan.role.mcp {
        if !literal_name(&server.id) || server.id.contains("__") || server.id.ends_with('_') {
            return Err(failure("server name cannot be represented unambiguously"));
        }
    }
    let positions: Vec<_> = plan
        .launch
        .args
        .iter()
        .enumerate()
        .filter_map(|(index, arg)| (arg == "--disallowedTools").then_some(index + 1))
        .collect();
    if positions.len() != 1 || positions[0] >= plan.launch.args.len() {
        return Err(failure("native deny argument is unavailable"));
    }
    let mut denied = BTreeSet::new();
    for server in &plan.role.mcp {
        let Some(allowed) = &server.allowed_tools else {
            continue;
        };
        if allowed.iter().any(|name| !literal_name(name)) {
            return Err(failure("allowed tool name cannot be represented literally"));
        }
        if allowed.is_empty() {
            denied.insert(format!("mcp__{}__*", server.id));
            continue;
        }
        let catalog = discover(server, &plan.launch, DISCOVERY_TIMEOUT, observer.clone()).await?;
        if allowed.iter().any(|name| !catalog.contains(name)) {
            return Err(failure(
                "configured allowed tool is absent from the catalog",
            ));
        }
        for tool in catalog {
            if !allowed.contains(&tool) {
                denied.insert(format!("mcp__{}__{tool}", server.id));
            }
        }
        if denied.len() > MAX_TOOLS
            || denied.iter().map(|s| s.len() + 1).sum::<usize>() > MAX_RULE_BYTES
        {
            return Err(failure("native deny rules exceed the size limit"));
        }
    }
    if denied.len() > MAX_TOOLS {
        return Err(failure("native deny rule limit exceeded"));
    }
    let mut argument = plan.launch.args[positions[0]].clone();
    for rule in denied {
        if !argument.is_empty() {
            argument.push(',');
        }
        argument.push_str(&rule);
    }
    if argument.len() > MAX_RULE_BYTES {
        return Err(failure("native deny rules exceed the size limit"));
    }
    plan.launch.args[positions[0]] = argument;
    Ok(())
}

/// Runs one direct, temporary stdio connection with frozen launch credentials and a deadline.
/// Missing inherited variables and native argument interpolation are rejected rather
/// than probing a differently configured server. An exec gate prevents backend startup
/// before the first durable observer checkpoint; it exits on parent EOF. Teardown
/// must be positively verified before returning a discovered catalog.
async fn discover<F>(
    server: &ResolvedMcp,
    launch: &LaunchPlan,
    timeout: Duration,
    observer: F,
) -> Result<BTreeSet<String>>
where
    F: Fn(&OwnershipSnapshot) -> Result<()> + Send + Sync + Clone + 'static,
{
    if server.transport != "stdio" {
        return Err(failure("restricted discovery requires stdio transport"));
    }
    if server
        .env_from
        .iter()
        .any(|name| !launch.environment.contains_key(name))
    {
        return Err(failure("declared environment variable is unavailable"));
    }
    if server.command.contains("${") || server.args.iter().any(|arg| arg.contains("${")) {
        return Err(failure(
            "native argument interpolation is unsupported for restricted discovery",
        ));
    }
    let mut args = vec![
        "-c".into(),
        "IFS= read -r gate && [ \"$gate\" = agent-run-mcp-catalog ] || exit 1; exec \"$@\"".into(),
        "agent-run-mcp-catalog".into(),
        server.command.clone(),
    ];
    args.extend(server.args.clone());
    let probe = LaunchPlan {
        binary: "/bin/sh".into(),
        args,
        cwd: launch.cwd.clone(),
        environment: launch.environment.clone(),
        initial_input: None,
    };
    let mut child = CatalogChild {
        process: Process::spawn(&probe).map_err(|_| failure("server could not be started"))?,
        armed: true,
    };
    child
        .process
        .observe_ownership(observer)
        .map_err(|_| failure("process ownership could not be persisted"))?;
    let result = tokio::time::timeout(timeout, async {
        child
            .process
            .text("agent-run-mcp-catalog\n")
            .await
            .map_err(|_| failure("ownership gate could not be released"))?;
        catalog(&mut child.process).await
    })
    .await
    .map_err(|_| failure("deadline exceeded"))
    .and_then(|result| result);
    let cleanup = child.process.owner.cleanup(CLEANUP_GRACE).await;
    child
        .process
        .checkpoint_ownership()
        .map_err(|_| failure("final process ownership could not be persisted"))?;
    child.process.reap().await;
    let reaped = child
        .process
        .child
        .try_wait()
        .is_ok_and(|status| status.is_some());
    if !cleanup.is_ok_and(|proof| proof.confirmed) || !reaped {
        return Err(failure("process cleanup could not be verified"));
    }
    child.armed = false;
    result
}

/// Performs MCP initialization and all pages of tools/list; never calls a server tool.
async fn catalog(process: &mut Process) -> Result<BTreeSet<String>> {
    let mut bytes = 0;
    let initialized = exchange(
        process,
        1,
        "initialize",
        json!({
            "protocolVersion":"2025-11-25", "capabilities":{},
            "clientInfo":{"name":"agent-run-tool-discovery","version":env!("CARGO_PKG_VERSION")}
        }),
        &mut bytes,
    )
    .await?;
    if !matches!(
        initialized.get("protocolVersion").and_then(Value::as_str),
        Some("2025-11-25" | "2025-06-18" | "2025-03-26" | "2024-11-05")
    ) || !initialized
        .pointer("/capabilities/tools")
        .is_some_and(Value::is_object)
        || !["name", "version"].iter().all(|key| {
            initialized
                .get("serverInfo")
                .and_then(|info| info.get(key))
                .and_then(Value::as_str)
                .is_some_and(|value| !value.is_empty())
        })
    {
        return Err(failure(
            "server did not negotiate a supported tools capability",
        ));
    }
    process
        .send(&json!({"jsonrpc":"2.0","method":"notifications/initialized"}))
        .await
        .map_err(|_| failure("initialization write failed"))?;
    let mut tools = BTreeSet::new();
    let mut cursors = BTreeSet::new();
    let mut params = json!({});
    for page in 0..MAX_PAGES {
        let result = exchange(process, page as u64 + 2, "tools/list", params, &mut bytes).await?;
        let listed = result
            .get("tools")
            .and_then(Value::as_array)
            .ok_or_else(|| failure("tools page is malformed"))?;
        for tool in listed {
            let name = tool
                .get("name")
                .and_then(Value::as_str)
                .filter(|name| literal_name(name))
                .ok_or_else(|| failure("catalog tool name cannot be represented literally"))?;
            if tool.pointer("/inputSchema/type").and_then(Value::as_str) != Some("object") {
                return Err(failure("catalog tool schema is malformed"));
            }
            if !tools.insert(name.to_owned()) {
                return Err(failure("catalog contains duplicate tool names"));
            }
            if tools.len() > MAX_TOOLS {
                return Err(failure("catalog tool limit exceeded"));
            }
        }
        match result.get("nextCursor") {
            None => return Ok(tools),
            Some(Value::String(cursor)) if !cursor.is_empty() && cursor.len() <= 4096 => {
                if !cursors.insert(cursor.clone()) {
                    return Err(failure("pagination cursor repeated"));
                }
                params = json!({"cursor":cursor});
            }
            _ => return Err(failure("pagination cursor is malformed")),
        }
    }
    Err(failure("catalog page limit exceeded"))
}

/// Correlates one MCP JSON-RPC response, bounding traffic and rejecting catalog mutation.
/// Server pings receive empty replies; all other unsolicited requests are declined.
/// Notifications are discarded, never retained, and errors never copy server text.
async fn exchange(
    process: &mut Process,
    id: u64,
    method: &str,
    params: Value,
    bytes: &mut usize,
) -> Result<Value> {
    process
        .send(&json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}))
        .await
        .map_err(|_| failure("request write failed"))?;
    for _ in 0..128 {
        let Event::Json(value) = process.next().await else {
            return Err(failure("server closed or returned malformed JSON"));
        };
        *bytes += serde_json::to_vec(&value)
            .map_err(|_| failure("invalid JSON response"))?
            .len();
        if *bytes > MAX_CATALOG_BYTES {
            return Err(failure("catalog byte limit exceeded"));
        }
        if value.get("jsonrpc").and_then(Value::as_str) != Some("2.0") || !value.is_object() {
            return Err(failure("invalid JSON-RPC envelope"));
        }
        if let Some(method) = value.get("method") {
            let method = method
                .as_str()
                .ok_or_else(|| failure("invalid server method"))?;
            if value.get("result").is_some() || value.get("error").is_some() {
                return Err(failure("invalid server request envelope"));
            }
            if method == "notifications/tools/list_changed" {
                return Err(failure("catalog changed during discovery"));
            }
            if let Some(request_id) = value.get("id") {
                if !request_id.is_string() && !request_id.is_number() {
                    return Err(failure("invalid server request id"));
                }
                let response = if method == "ping" {
                    json!({"jsonrpc":"2.0","id":request_id,"result":{}})
                } else {
                    json!({"jsonrpc":"2.0","id":request_id,"error":{"code":-32601,"message":"Client capability unavailable"}})
                };
                process
                    .send(&response)
                    .await
                    .map_err(|_| failure("server request reply failed"))?;
            }
            continue;
        }
        if value.get("id").and_then(Value::as_u64) != Some(id)
            || value.get("error").is_some()
            || !value.get("result").is_some_and(Value::is_object)
        {
            return Err(failure("request failed or returned an invalid response"));
        }
        return Ok(value["result"].clone());
    }
    Err(failure("server notification limit exceeded"))
}

#[cfg(test)]
mod tests {
    //! Finite stdio fixtures verify native filtering and cleanup without a model turn.

    use super::*;
    use agent_run_config::{
        profiles::Profile,
        role_plan::{McpSelectionSource, ResolvedRolePlan},
    };
    use agent_run_platform::process;
    use std::{collections::BTreeMap, path::Path};

    /// Creates a ten-second maximum-life Bash MCP fixture and a matching frozen launch.
    /// The fixture requires inherited credentials/cwd and records requests and its tree.
    fn fixture(root: &Path, mode: &str) -> ProviderLaunchPlan {
        let script = root.join("catalog.sh");
        std::fs::write(&script, r#"
[ "$REQUIRED_TOKEN" = "fixture-secret" ] || exit 11
[ "$PWD" = "$EXPECTED_CWD" ] || exit 12
printf '%s\n' "$$" > "$PID_FILE"
if [ "$MODE" = hang ]; then
  trap '' TERM
  /bin/sleep 8 &
  printf '%s\n' "$!" >> "$PID_FILE"
fi
initialized=0
pages=0
while [ "$SECONDS" -lt 10 ] && IFS= read -r -t 8 line; do
  printf '%s\n' "$line" >> "$TRACE_FILE"
  case "$line" in
    *'"method":"initialize"'*)
      [ "$MODE" = hang ] && continue
      printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2025-11-25","capabilities":{"tools":{}},"serverInfo":{"name":"fixture","version":"1"}}}' ;;
    *'"method":"notifications/initialized"'*) initialized=1 ;;
    *'"method":"tools/list"'*)
      [ "$initialized" = 1 ] || exit 13
      pages=$((pages + 1))
      case "$MODE" in
        malformed) printf '%s\n' 'not json'; continue ;;
        duplicate) printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"tools":[{"name":"alpha","inputSchema":{"type":"object"}},{"name":"alpha","inputSchema":{"type":"object"}}]}}'; continue ;;
        unsafe) printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"tools":[{"name":"alpha*","inputSchema":{"type":"object"}}]}}'; continue ;;
        error) printf '%s\n' '{"jsonrpc":"2.0","id":2,"error":{"code":-1,"message":"fixture-secret"}}'; continue ;;
        changed) printf '%s\n' '{"jsonrpc":"2.0","method":"notifications/tools/list_changed"}'; continue ;;
        cursor_loop)
          printf '{"jsonrpc":"2.0","id":%s,"result":{"tools":[],"nextCursor":"same"}}\n' "$((pages + 1))"
          continue ;;
        ping)
          printf '%s\n' '{"jsonrpc":"2.0","id":"server-ping","method":"ping"}'
          IFS= read -r -t 2 reply || exit 14
          printf '%s\n' "$reply" >> "$TRACE_FILE"
          case "$reply" in *'"result":{}'*) ;; *) exit 15 ;; esac ;;
      esac
      if [ "$pages" = 1 ]; then
        printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"tools":[{"name":"alpha","inputSchema":{"type":"object"}}],"nextCursor":"next"}}'
      else
        case "$line" in *'"cursor":"next"'*) ;; *) exit 16 ;; esac
        printf '%s\n' '{"jsonrpc":"2.0","id":3,"result":{"tools":[{"name":"beta","inputSchema":{"type":"object"}}]}}'
      fi ;;
    *) exit 17 ;;
  esac
done
"#).unwrap();
        let server = ResolvedMcp {
            id: "fixture_server".into(),
            transport: "stdio".into(),
            command: "/bin/bash".into(),
            args: vec![script.to_string_lossy().into_owned()],
            env_from: vec!["REQUIRED_TOKEN".into()],
            approval_mode: "auto".into(),
            allowed_tools: Some(vec!["alpha".into()]),
            selection: McpSelectionSource::Profile,
        };
        let profile = Profile {
            name: "review".into(),
            body: String::new(),
            write: false,
            network: false,
            revision: "test".into(),
            canonical: true,
            allow_external_read_roots: false,
            read_roots: vec![],
            skills: vec![],
            mcp: vec![server.id.clone()],
            mcp_tools: BTreeMap::from([(server.id.clone(), vec!["alpha".into()])]),
            required_constraints: BTreeSet::new(),
        };
        ProviderLaunchPlan {
            launch: LaunchPlan {
                binary: "/bin/false".into(), args: vec!["--disallowedTools".into(), "WebFetch,WebSearch".into()],
                cwd: root.into(), environment: BTreeMap::from([
                    ("REQUIRED_TOKEN".into(), "fixture-secret".into()),
                    ("EXPECTED_CWD".into(), root.to_string_lossy().into_owned()),
                    ("PID_FILE".into(), root.join("pids").to_string_lossy().into_owned()),
                    ("TRACE_FILE".into(), root.join("trace").to_string_lossy().into_owned()),
                    ("MODE".into(), mode.into()),
                ]), initial_input: None,
            },
            native_model: "fixture".into(),
            role: ResolvedRolePlan {
                worker_mcp: false,
        role_name: "review".into(), role_revision: "test".into(), prompt: String::new(),
                write: false, network: false, allow_external_read_roots: false, read_roots: vec![],
                skills: vec![], mcp: vec![server], required_constraints: BTreeSet::new(),
                auth_mode: "native".into(), auth_reference: None, config_revision: "test".into(),
            },
            runtime: serde_json::from_value(json!({
                "enabled":true,"adapter":"claude","binary":"/bin/false","home":root,"models":["fixture"]
            })).unwrap(), profile,
        }
    }

    /// Asserts every recorded fixture PID is gone or a non-running zombie after cleanup.
    fn assert_stopped(root: &Path) {
        for pid in std::fs::read_to_string(root.join("pids")).unwrap().lines() {
            match process::inspect(pid.parse().unwrap()) {
                Ok(identity) => assert!(identity.zombie, "fixture {pid} survived cleanup"),
                Err(error) => assert!(
                    matches!(error.raw_os_error(), Some(libc::ESRCH | libc::ENOENT)),
                    "fixture {pid} liveness unknown: {error}"
                ),
            }
        }
    }

    /// Uses every page, preserves builtin denies/auth/cwd and answers only harmless server pings.
    #[tokio::test]
    async fn native_complement_discovers_all_pages_and_cleans_up() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let mut plan = fixture(&root, "ping");
        apply_claude_tool_filters(&mut plan, |_| Ok(()))
            .await
            .unwrap();
        assert_eq!(
            plan.launch.args[1],
            "WebFetch,WebSearch,mcp__fixture_server__beta"
        );
        let trace = std::fs::read_to_string(root.join("trace")).unwrap();
        assert_eq!(trace.matches("\"method\":\"tools/list\"").count(), 2);
        assert!(trace.contains("notifications/initialized"));
        assert!(trace.contains("server-ping"));
        assert!(!trace.contains("tools/call"));
        assert_stopped(&root);
    }

    /// Omitting a cap never probes; an empty cap uses the native whole-server wildcard.
    #[tokio::test]
    async fn omitted_and_empty_caps_do_not_start_a_probe() {
        let temp = tempfile::tempdir().unwrap();
        let mut plan = fixture(temp.path(), "hang");
        plan.role.mcp[0].allowed_tools = None;
        apply_claude_tool_filters(&mut plan, |_| Ok(()))
            .await
            .unwrap();
        assert_eq!(plan.launch.args[1], "WebFetch,WebSearch");
        plan.role.mcp[0].allowed_tools = Some(vec![]);
        apply_claude_tool_filters(&mut plan, |_| Ok(()))
            .await
            .unwrap();
        assert_eq!(
            plan.launch.args[1],
            "WebFetch,WebSearch,mcp__fixture_server__*"
        );
        assert!(!temp.path().join("pids").exists());
    }

    /// Incomplete, mutating, malformed or ambiguous catalogs cannot mutate native arguments.
    #[tokio::test]
    async fn invalid_catalogs_fail_closed_without_payloads_or_leaks() {
        for mode in [
            "malformed",
            "duplicate",
            "unsafe",
            "error",
            "changed",
            "cursor_loop",
            "missing",
        ] {
            let temp = tempfile::tempdir().unwrap();
            let root = temp.path().canonicalize().unwrap();
            let mut plan = fixture(&root, mode);
            if mode == "missing" {
                plan.role.mcp[0].allowed_tools = Some(vec!["typo".into()]);
            }
            let before = plan.launch.args.clone();
            let error = apply_claude_tool_filters(&mut plan, |_| Ok(()))
                .await
                .unwrap_err();
            assert!(!error.to_string().contains("fixture-secret"));
            assert_eq!(plan.launch.args, before, "{mode}");
            assert_stopped(&root);
        }
    }

    /// A deadline and cancellation both terminate a stalled discovery child and its descendant.
    #[tokio::test]
    async fn timeout_and_cancellation_clean_the_discovery_tree() {
        for cancel in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let root = temp.path().canonicalize().unwrap();
            let plan = fixture(&root, "hang");
            if cancel {
                let task = tokio::spawn(async move {
                    discover(&plan.role.mcp[0], &plan.launch, DISCOVERY_TIMEOUT, |_| {
                        Ok(())
                    })
                    .await
                });
                tokio::time::timeout(Duration::from_secs(2), async {
                    loop {
                        if std::fs::read_to_string(root.join("pids"))
                            .is_ok_and(|pids| pids.lines().count() == 2)
                        {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                })
                .await
                .unwrap();
                task.abort();
                assert!(task.await.unwrap_err().is_cancelled());
            } else {
                let error = discover(
                    &plan.role.mcp[0],
                    &plan.launch,
                    Duration::from_millis(350),
                    |_| Ok(()),
                )
                .await
                .unwrap_err();
                assert!(error.to_string().contains("deadline exceeded"), "{error}");
            }
            assert_stopped(&root);
        }
    }

    /// Unsafe native names, missing credentials and argv expansion fail before any subprocess.
    #[tokio::test]
    async fn native_ambiguity_and_environment_mismatch_are_rejected() {
        let temp = tempfile::tempdir().unwrap();
        for id in ["server.dot", "server__ambiguous", "server_"] {
            let mut plan = fixture(temp.path(), "hang");
            plan.role.mcp[0].id = id.into();
            assert!(
                apply_claude_tool_filters(&mut plan, |_| Ok(()))
                    .await
                    .is_err()
            );
        }
        let mut plan = fixture(temp.path(), "hang");
        plan.launch.environment.remove("REQUIRED_TOKEN");
        assert!(
            apply_claude_tool_filters(&mut plan, |_| Ok(()))
                .await
                .unwrap_err()
                .to_string()
                .contains("environment")
        );
        let mut plan = fixture(temp.path(), "hang");
        plan.role.mcp[0].args.push("${TOKEN}".into());
        assert!(
            apply_claude_tool_filters(&mut plan, |_| Ok(()))
                .await
                .unwrap_err()
                .to_string()
                .contains("interpolation")
        );
        assert!(!temp.path().join("pids").exists());
    }

    /// Refused durable ownership must terminate the bootstrap before executing the MCP backend.
    #[tokio::test]
    async fn ownership_refusal_keeps_backend_behind_exec_gate() {
        use std::sync::{
            Arc,
            atomic::{AtomicI32, Ordering},
        };
        let temp = tempfile::tempdir().unwrap();
        let mut plan = fixture(temp.path(), "hang");
        let captured = Arc::new(AtomicI32::new(0));
        let pid = captured.clone();
        let error = apply_claude_tool_filters(&mut plan, move |snapshot| {
            pid.store(snapshot.leader.pid, Ordering::SeqCst);
            Err(failure("fixture observer rejected ownership"))
        })
        .await
        .unwrap_err();
        assert!(error.to_string().contains("ownership"));
        assert!(
            !temp.path().join("pids").exists(),
            "backend executed before persistence"
        );
        let pid = captured.load(Ordering::SeqCst);
        assert!(pid > 1, "observer never received bootstrap identity");
        match process::inspect(pid) {
            Ok(identity) => assert!(identity.zombie),
            Err(error) => assert!(matches!(
                error.raw_os_error(),
                Some(libc::ESRCH | libc::ENOENT)
            )),
        }
    }
}
