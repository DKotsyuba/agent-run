//! Compact MiniJinja presentation for every public MCP tool result.
//!
//! MCP successes render as one short plain-text page per tool from the
//! repo-owned templates in `assets/mcp/`, embedded at build time (never
//! loaded from the filesystem or hot-reloaded). The private socket and the
//! CLI keep their structured JSON contracts; only the MCP presentation is
//! text. `start`/`resume` additionally keep a tiny structured
//! `{"agent_id": ..., "run_id": ...}` so the PostToolUse binding hook can
//! bind the exact execution from JSON (see `hooks::bind`); no other tool
//! mirrors its result into structured content. Failures render as one
//! short typed line, never a JSON dump.

use crate::{Error, Result};
use minijinja::{AutoEscape, Environment, UndefinedBehavior};
use rmcp::model::{CallToolResult, Content};
use serde_json::{json, Value};
use std::sync::OnceLock;

/// The embedded per-tool templates, keyed by public tool name.
const TEMPLATES: &[(&str, &str)] = &[
    ("start", include_str!("../../../../assets/mcp/start.txt.j2")),
    (
        "resume",
        include_str!("../../../../assets/mcp/resume.txt.j2"),
    ),
    (
        "cancel",
        include_str!("../../../../assets/mcp/cancel.txt.j2"),
    ),
    ("steer", include_str!("../../../../assets/mcp/steer.txt.j2")),
    (
        "list_agents",
        include_str!("../../../../assets/mcp/list_agents.txt.j2"),
    ),
    (
        "transcript",
        include_str!("../../../../assets/mcp/transcript.txt.j2"),
    ),
    (
        "answer",
        include_str!("../../../../assets/mcp/answer.txt.j2"),
    ),
    (
        "models",
        include_str!("../../../../assets/mcp/models.txt.j2"),
    ),
    (
        "capacity_order",
        include_str!("../../../../assets/mcp/capacity_order.txt.j2"),
    ),
    (
        "limits",
        include_str!("../../../../assets/mcp/limits.txt.j2"),
    ),
    ("doc", include_str!("../../../../assets/mcp/doc.txt.j2")),
];

/// The shared embedded error template.
const ERROR_TEMPLATE: &str = include_str!("../../../../assets/mcp/error.txt.j2");

/// Returns the process-wide template environment for MCP presentation.
///
/// Block tags trim their surrounding newlines, the templates' own trailing
/// newline is kept, undefined values are strict (a context/template
/// mismatch fails loudly instead of printing nothing), and auto-escaping is
/// explicitly off because every page is plain text. Registration can only
/// fail for an invalid repo-owned asset, which is a build-time programming
/// defect (mirroring the tool-registry parse), never a caller condition.
fn environment() -> &'static Environment<'static> {
    static ENVIRONMENT: OnceLock<Environment> = OnceLock::new();
    ENVIRONMENT.get_or_init(|| {
        let mut environment = Environment::new();
        environment.set_trim_blocks(true);
        environment.set_lstrip_blocks(true);
        environment.set_keep_trailing_newline(true);
        environment.set_undefined_behavior(UndefinedBehavior::Strict);
        environment.set_auto_escape_callback(|_| AutoEscape::None);
        for (name, source) in TEMPLATES {
            environment
                .add_template(name, source)
                .unwrap_or_else(|error| panic!("MCP template {name} is invalid: {error}"));
        }
        environment
            .add_template("error", ERROR_TEMPLATE)
            .expect("MCP error template is valid");
        environment
    })
}

/// Builds the MCP success result for one public tool value.
///
/// `delegation_guide` already is rendered text and passes through. Every
/// other tool renders its compact template over a normalized projection of
/// `value`. `start`/`resume` keep only their stable agent and exact run ids
/// next to the text so automatic PostToolUse binding stays JSON-extractable
/// without mirroring the full result. A template/projection failure yields
/// the typed compact error line, never JSON and never a silent blank.
pub fn success_result(tool: &str, value: &Value) -> CallToolResult {
    let identity = match tool {
        "start" | "resume" => {
            let mut identity = json!({"agent_id": value["agent_id"]});
            if let Some(run_id) = value.get("run_id") {
                identity["run_id"] = run_id.clone();
            }
            Some(identity)
        }
        _ => None,
    };
    let result = match (tool, value) {
        ("delegation_guide", Value::String(text)) => {
            CallToolResult::success(vec![Content::text(text.clone())])
        }
        ("delegation_guide", _) => error_result("RuntimeError", "delegation guide must be text"),
        (_, Value::Object(_)) => match render(tool, value) {
            Ok(text) => CallToolResult::success(vec![Content::text(text)]),
            // A projection/template defect is a typed compact failure, never
            // JSON, a silent blank, or a success page hiding the failure.
            Err(error) => error_result(
                "RuntimeError",
                &format!("tool result presentation failed: {error}"),
            ),
        },
        _ => error_result("RuntimeError", "tool result must be an object"),
    };
    // The one deliberate structured mirror, kept even on a presentation
    // failure: machine identity for the PostToolUse binding hook, never the
    // full result object.
    let mut result = result;
    if let Some(identity) = identity {
        result.structured_content = Some(identity);
    }
    result
}

/// Builds the MCP failure result as one short typed text line.
///
/// The kind and (whitespace-normalized) message stay machine-greppable;
/// no structured mirror and no result dump accompany it.
pub fn error_result(kind: &str, message: &str) -> CallToolResult {
    let mut result = CallToolResult::success(vec![Content::text(
        render("error", &json!({"kind": kind, "message": prose(message)}))
            .unwrap_or_else(|error| format!("agent-run error {kind}: {error}")),
    )]);
    result.is_error = Some(true);
    result
}

/// Renders one named template over its normalized context.
fn render(name: &str, value: &Value) -> Result<String> {
    environment()
        .get_template(name)
        .map_err(|error| Error::Runtime(format!("MCP template {name} is missing: {error}")))?
        .render(context(name, value))
        .map_err(|error| Error::Runtime(format!("MCP template {name} failed: {error}")))
}

/// Builds the normalized compact context one template renders.
///
/// Each entry renames exactly the fields its template prints, so the
/// templates never depend on incidental dispatcher shapes and strict
/// undefined behavior catches drift. Unknown names have no template and
/// fail into the typed compact error line.
fn context(name: &str, value: &Value) -> Value {
    match name {
        "start" | "resume" => start_context(value),
        "cancel" => json!({"agent": agent_fields(value), "kind": "cancel"}),
        "steer" => json!({
            "agent_id": value["agent_id"], "command_id": value["command_id"],
            "run_id": value["run_id"].as_str().unwrap_or_default(),
            "kind": value["kind"].as_str().unwrap_or("steer"),
            "state": value["state"].as_str().unwrap_or("queued"),
        }),
        "list_agents" => list_context(value),
        "transcript" => transcript_context(value),
        "answer" => answer_context(value),
        "models" => models_context(value),
        "capacity_order" => order_context(value),
        "limits" => limits_context(value),
        "doc" => json!({"text": value["text"]}),
        _ => value.clone(),
    }
}

/// Extracts the compact shared agent facts one template prints per agent.
///
/// Optional facts default to empty strings (falsy for the templates' guards)
/// instead of absent keys, so strict undefined behavior cannot turn a merely
/// sparse view into a rendering failure.
fn agent_fields(view: &Value) -> Value {
    let text = |key: &str| view[key].as_str().unwrap_or_default().to_owned();
    let status = text("status");
    json!({
        "id": view["agent_id"], "status": status,
        "run_id": text("run_id"),
        "terminal": matches!(status.as_str(),
            "succeeded" | "failed" | "lost" | "timed_out" | "cancelled"),
        "runtime": view["runtime"], "model": view["model"], "profile": view["profile"],
        "phase": text("phase"), "effort": text("effort"),
        "task": text("task_summary"),
        "failure_kind": text("failure_kind"),
        "failure_text": text("failure_text"),
        "bound": view["delivery"]["bound"].as_bool().unwrap_or(false),
        "delivery_state": view["delivery"]["state"].as_str().unwrap_or("not_created"),
        "delivery_error": view["delivery"]["last_error"]
            .as_str()
            .unwrap_or_default()
            .to_owned(),
        "ambiguous": view["delivery"]["ambiguous"].as_bool().unwrap_or(false),
        "warned": view["warned"].as_bool().unwrap_or(false),
        "cleanup_unconfirmed": view["cleanup"]
            .as_object()
            .is_some_and(|cleanup| !cleanup["confirmed"].as_bool().unwrap_or(false)),
        "process_state": view["process_state"].as_str().unwrap_or_default().to_owned(),
    })
}

/// Start/resume context: the durable id plus the just-committed snapshot.
fn start_context(value: &Value) -> Value {
    let mut fields = agent_fields(&value["agent"]);
    fields["agent_id"] = value["agent_id"].clone();
    fields["run_id"] = json!(value["run_id"].as_str().unwrap_or_default());
    fields["created"] = value["created"].clone();
    fields["attempt_id"] = json!(value["attempt_id"].as_str().unwrap_or_default());
    fields["parent_agent_id"] = json!(value["agent"]["parent_run_id"]
        .as_str()
        .or_else(|| value["agent"]["parent_agent_id"].as_str())
        .or_else(|| value["parent_agent_id"].as_str())
        .unwrap_or_default());
    fields
}

/// List context: exact total, returned page, and continuation facts.
fn list_context(value: &Value) -> Value {
    json!({
        "total": value["total"], "returned": value["items"].as_array().map(Vec::len),
        "offset": value["offset"],
        "next_offset": value["next_offset"].as_u64(),
        "complete": value["complete"].as_bool().unwrap_or(true),
        "agents": value["items"].as_array().into_iter().flatten()
            .map(agent_fields).collect::<Vec<_>>(),
    })
}

/// Transcript context: every requested message with role/name/sequence.
fn transcript_context(value: &Value) -> Value {
    json!({
        "agent_id": value["agent_id"],
        "run_id": value["run_id"].as_str().unwrap_or_default(),
        "next_cursor": value["next_cursor"].as_i64(),
        "complete": value["complete"].as_bool().unwrap_or(true),
        "messages": value["messages"].as_array().into_iter().flatten().map(|message| {
            json!({
                "seq": message["seq"], "role": message["role"],
                "name": message["name"].as_str(), "content": message["content"],
            })
        }).collect::<Vec<_>>(),
        "count": value["messages"].as_array().map(Vec::len),
    })
}

/// Answer context: availability plus the full inline text or retrieval facts.
fn answer_context(value: &Value) -> Value {
    json!({
        "agent_id": value["agent_id"], "status": value["status"],
        "run_id": value["run_id"].as_str().unwrap_or_default(),
        "available": value["available"].as_bool().unwrap_or(false),
        "inline_complete": value["inline_complete"].as_bool().unwrap_or(false),
        "content": value["content"].as_str(),
        "path": value["path"].as_str(),
        "relative_path": value["relative_path"].as_str(),
        "size_bytes": value["size_bytes"].as_u64(),
        "kind": value["kind"].as_str(), "media_type": value["media_type"].as_str(),
    })
}

/// Models context: schema-2 provider catalog or the legacy runtime roster.
fn models_context(value: &Value) -> Value {
    if value["providers"].as_array().is_some() {
        json!({
            "schema": 2,
            "capacity_revision": value["capacity_revision"],
            "profiles": value["profiles"].as_array().into_iter().flatten()
                .map(|role| json!({
                    "name": role["name"],
                    "write": role["write"].as_bool().unwrap_or(false).to_string(),
                    "network": role["network"].as_bool().unwrap_or(false).to_string(),
                    "roots": nonempty(&role["read_roots"].as_array().into_iter().flatten()
                        .filter_map(Value::as_str).collect::<Vec<_>>().join(", ")),
                    "constraints": nonempty(&role["required_constraints"].as_array()
                        .into_iter().flatten().filter_map(Value::as_str)
                        .collect::<Vec<_>>().join(", ")),
                })).collect::<Vec<_>>(),
            "providers": providers_context(value),
        })
    } else {
        json!({
            "schema": 1,
            "runtimes": value.as_object().into_iter().flatten().map(|(name, runtime)| {
                json!({
                    "name": name, "available": runtime["available"],
                    "reason": runtime["reason"].as_str(),
                    "models": runtime["models"].as_array().into_iter().flatten()
                        .map(|model| model["id"].clone()).collect::<Vec<_>>(),
                })
            }).collect::<Vec<_>>(),
        })
    }
}

/// Provider/model projection shared by the `models` and guide layouts.
fn providers_context(value: &Value) -> Vec<Value> {
    value["providers"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|provider| {
            let models: Vec<Value> = provider["models"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|model| {
                    let profiles = model["profiles"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .filter_map(Value::as_str)
                        .collect::<Vec<_>>()
                        .join(", ");
                    json!({
                        "id": model["model"],
                        "native_model": model["native_model"].as_str(),
                        "status": model["quota"]["status"],
                        "evidence": model["quota"]["evidence"],
                        "guidance": model["recommendations"]
                            .as_array().into_iter().flatten()
                            .filter_map(Value::as_str).map(prose).collect::<Vec<_>>(),
                        "profiles": if profiles.is_empty() { "none".to_owned() } else { profiles },
                        "params": param_line(&model["params"], &model["allowed_params"]),
                        "restrictions": nonempty(&model["restrictions"]
                            .as_array().into_iter().flatten()
                            .filter_map(Value::as_str).collect::<Vec<_>>().join(", ")),
                    })
                })
                .collect();
            // When every model admits the same nonempty profile set, state it
            // once at the provider instead of repeating the list per model.
            let common = models
                .iter()
                .map(|model| model["profiles"].as_str().unwrap_or("none"))
                .reduce(|left, right| if left == right { left } else { "" })
                .filter(|common| !common.is_empty() && *common != "none")
                .map(str::to_owned);
            let mut provider = json!({
                "id": provider["provider"], "harness": provider["harness"],
                "guidance": provider["recommendations"]
                    .as_array().into_iter().flatten()
                    .filter_map(Value::as_str).map(prose).collect::<Vec<_>>(),
                "models": models,
            });
            // Null (not removal) so strict-undefined templates still see a
            // defined, falsy key after hoisting.
            provider["profiles"] = common
                .as_deref()
                .map_or(Value::Null, |common| json!(common));
            if common.is_some() {
                for model in provider["models"].as_array_mut().into_iter().flatten() {
                    model["profiles"] = Value::Null;
                }
            }
            provider
        })
        .collect()
}

/// Capacity-order context: schema-2 provider order or the legacy routes.
fn order_context(value: &Value) -> Value {
    if value["providers"].as_array().is_some() {
        json!({
            "schema": 2,
            "capacity_revision": value["capacity_revision"],
            "providers": value["providers"].as_array().into_iter().flatten().map(|provider| {
                json!({
                    "id": provider["provider"],
                    "score": provider["score"].as_f64(),
                    "multiplier": provider["priority_multiplier"].as_f64(),
                    "models": provider["models"].as_array().into_iter().flatten().map(|model| {
                        json!({
                            "id": model["model"], "status": model["quota"]["status"],
                            "evidence": model["quota"]["evidence"],
                        })
                    }).collect::<Vec<_>>(),
                })
            }).collect::<Vec<_>>(),
        })
    } else {
        json!({
            "schema": 1,
            "routes": value["routes"].as_array().into_iter().flatten().map(|route| {
                json!({
                    "runtime": route["runtime"], "priority": route["priority"],
                    "aliases": nonempty(&route["aliases"].as_array().into_iter().flatten()
                        .filter_map(Value::as_str).collect::<Vec<_>>().join(", ")),
                })
            }).collect::<Vec<_>>(),
            "unavailable": nonempty(&value["unavailable_runtimes"].as_array()
                .into_iter().flatten().filter_map(Value::as_str)
                .collect::<Vec<_>>().join(", ")),
        })
    }
}

/// Limits context: the diagnostic rows this API already exposes publicly.
///
/// Reset horizons are derived against the read's own `observed_at` clock as
/// readable spans, so no raw fractional epoch reaches the text.
fn limits_context(value: &Value) -> Value {
    let observed_at = value["observed_at"].as_f64();
    json!({
        "items": value["items"].as_array().into_iter().flatten().map(|item| {
            let key = &item["key"];
            json!({
                "runtime": key["runtime"], "lane": key["lane"], "window": key["window"],
                "account": item["account"].as_str(), "pool": item["pool"].as_str(),
                "known": item["known"].as_bool().unwrap_or(false),
                "remaining_percent": item["remaining_percent"].as_f64(),
                "resets_in": observed_at
                    .zip(item["reset_at"].as_f64())
                    .map(|(observed_at, reset_at)| span(reset_at - observed_at)),
            })
        }).collect::<Vec<_>>(),
    })
}

/// Renders one finite non-negative duration in compact human units.
///
/// Sub-second spans round up to `1s`; anything from seconds upward keeps
/// its largest whole unit (`45s`, `2m`, `2h`, `3d`), and longer spans stay
/// in days. A non-finite or negative input renders as `0s` rather than a
/// misleading clock reading.
fn span(seconds: f64) -> String {
    if !seconds.is_finite() || seconds <= 0.0 {
        return "0s".to_owned();
    }
    let whole = [(86_400.0, "d"), (3_600.0, "h"), (60.0, "m"), (1.0, "s")];
    for (unit, suffix) in whole {
        if seconds >= unit {
            return format!("{}{suffix}", (seconds / unit).ceil());
        }
    }
    "1s".to_owned()
}

/// Renders configured default and allowed params as one compact fragment.
fn param_line(defaults: &Value, allowed: &Value) -> Option<String> {
    let mut parts = Vec::new();
    for (name, value) in defaults.as_object().into_iter().flatten() {
        // Render the scalar itself, not its JSON encoding.
        let scalar = match value {
            Value::String(text) => text.clone(),
            other => other.to_string(),
        };
        parts.push(format!("{name}={scalar}"));
    }
    for (name, values) in allowed.as_object().into_iter().flatten() {
        let choices = values
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .collect::<Vec<_>>()
            .join("|");
        parts.push(if choices.is_empty() {
            format!("allowed {name}")
        } else {
            format!("allowed {name}: {choices}")
        });
    }
    nonempty(&parts.join("; "))
}

/// Returns `None` for an empty string so templates can omit empty lines.
fn nonempty(text: &str) -> Option<String> {
    (!text.is_empty()).then(|| text.to_owned())
}

/// Normalizes operator prose to single spaces, preserving other Unicode.
fn prose(text: &str) -> String {
    text.chars()
        .map(|character| {
            if character.is_control() || character.is_whitespace() {
                ' '
            } else {
                character
            }
        })
        .collect::<String>()
        .split(' ')
        .filter(|word| !word.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::{error_result, success_result};
    use serde_json::{json, Value};

    /// Minimal agent view proving sparse optional metadata remains renderable.
    fn agent_view() -> Value {
        json!({
            "agent_id": "ag-1", "runtime": "glm-user", "model": "glm-5.3",
            "profile": "review", "status": "running", "phase": "running",
            "effort": "high", "task_summary": "fix the parser",
            "delivery": {"bound": false},
        })
    }

    /// Renders one tool success and returns its single text content.
    fn text(tool: &str, value: &Value) -> String {
        let result = success_result(tool, value);
        let content = serde_json::to_value(&result.content).unwrap();
        let page = content[0]["text"].as_str().unwrap().to_owned();
        assert_eq!(result.is_error, Some(false), "{tool}: {page}");
        page
    }

    /// Start and resume retain stable and exact ids without a full JSON mirror.
    #[test]
    fn start_and_resume_keep_binding_identity_and_honest_status() {
        for tool in ["start", "resume"] {
            let run = if tool == "resume" { "ag-2" } else { "ag-1" };
            let value = json!({
                "agent_id": "ag-1", "run_id": run, "created": true, "attempt_id": "att_9",
                "agent": agent_view(),
            });
            let result = success_result(tool, &value);
            assert_eq!(
                result.structured_content,
                Some(json!({"agent_id": "ag-1", "run_id": run})),
                "tiny identity mirror only"
            );
            let page = text(tool, &value);
            assert!(page.contains("agent-run"), "{page}");
            assert!(page.contains("- Agent: ag-1"), "{page}");
            assert!(page.contains(&format!("- Run: {run}")), "{page}");
            assert!(page.contains("NOT a completion"), "{page}");
            assert!(page.contains("- Status: running (running)"), "{page}");
            assert!(page.contains("glm-user/glm-5.3 profile review"), "{page}");
            assert!(page.contains("not bound"), "{page}");
            assert!(page.ends_with('\n') && !page.ends_with("\n\n"), "{page:?}");
        }
    }

    /// The real binding hook normalizer extracts the id from the rendered
    /// start envelope exactly as a host would deliver it.
    #[test]
    fn posttooluse_binding_extracts_the_rendered_start_envelope() {
        let value = json!({"agent_id": "ag-2026-1", "created": true, "agent": agent_view()});
        let envelope = serde_json::to_value(success_result("start", &value)).unwrap();
        let payload = agent_run_core::hooks::bind::normalize(
            &json!({
                "hook_event_name": "PostToolUse", "session_id": "s-1",
                "tool_response": envelope,
            }),
            true,
            "claude_uds",
        )
        .expect("binding normalizer accepts the rendered envelope");
        assert_eq!(payload.agent_id.as_deref(), Some("ag-2026-1"));
        let resumed =
            json!({"agent_id":"ag-root", "run_id":"ag-child", "created":true,"agent":agent_view()});
        let payload = agent_run_core::hooks::bind::normalize(
            &json!({"session_id":"s-1","tool_response":success_result("resume", &resumed)}),
            true,
            "claude_uds",
        )
        .unwrap();
        assert_eq!(payload.agent_id.as_deref(), Some("ag-child"));
    }

    /// Resume reads lineage from the real nested agent view; other tools
    /// cannot bypass their templates by returning a preformatted scalar.
    #[test]
    fn resume_lineage_and_non_guide_result_shapes_are_preserved() {
        let mut agent = agent_view();
        agent["parent_agent_id"] = json!("ag-legacy");
        agent["parent_run_id"] = json!("ag-parent");
        let page = text(
            "resume",
            &json!({"agent_id": "ag-root", "run_id":"ag-child", "created": true, "agent": agent}),
        );
        assert!(page.contains("Previous run: ag-parent"), "{page}");
        assert!(!page.contains("ag-legacy"), "{page}");
        for (name, value) in [
            ("models", json!("unvalidated text")),
            ("delegation_guide", json!({})),
        ] {
            let result = success_result(name, &value);
            assert_eq!(result.is_error, Some(true), "{name}");
            assert!(serde_json::to_value(result.content).unwrap()[0]["text"]
                .as_str()
                .unwrap()
                .contains("RuntimeError"));
        }
    }

    /// Cancel and steer report acceptance without claiming a terminal state.
    #[test]
    fn cancel_and_steer_report_pending_acceptance() {
        let cancel = text("cancel", &agent_view());
        assert!(cancel.contains("agent-run cancel accepted"), "{cancel}");
        assert!(cancel.contains("- Agent: ag-1"), "{cancel}");
        assert!(cancel.contains("requested, not yet confirmed"), "{cancel}");
        assert!(!cancel.contains("succeeded"), "{cancel}");
        let steer = text(
            "steer",
            &json!({"command_id": 7, "agent_id": "ag-1", "kind": "steer", "state": "queued"}),
        );
        assert!(steer.contains("agent-run steer accepted"), "{steer}");
        assert!(steer.contains("steer #7 (queued)"), "{steer}");
        assert!(steer.contains("applies to the active run"), "{steer}");
        let mut terminal = agent_view();
        terminal["status"] = json!("cancelled");
        let finished = text("cancel", &terminal);
        assert!(finished.contains("already terminal"));
        assert!(!finished.contains("watch list_agents"));
    }

    /// list_agents keeps the exact total, page size, and continuation.
    #[test]
    fn list_agents_keeps_total_and_continuation() {
        let page = text(
            "list_agents",
            &json!({
                "items": [agent_view(), agent_view()], "total": 5, "offset": 0,
                "limit": 2, "next_offset": 2, "complete": false, "revision": 9,
            }),
        );
        assert!(page.contains("2 of 5 matching (offset 0)"), "{page}");
        assert!(
            page.contains("- ag-1: running (running) — glm-user/glm-5.3 profile review"),
            "{page}"
        );
        assert!(page.contains("task: fix the parser"), "{page}");
        assert!(
            page.contains("next offset: 2 — more matching rows remain"),
            "{page}"
        );
    }

    /// transcript preserves content verbatim and states its cursor contract.
    #[test]
    fn transcript_preserves_content_and_cursor() {
        let page = text(
            "transcript",
            &json!({
                "agent_id": "ag-1", "complete": false, "next_cursor": 4,
                "messages": [
                    {"seq": 1, "role": "user", "content": "please\nfix\tit"},
                    {"seq": 2, "role": "tool_call", "name": "shell", "content": "{\"cmd\":1}"},
                ],
            }),
        );
        assert!(page.contains("[1] user: please\nfix\tit"), "{page}");
        assert!(
            page.contains("[2] tool_call (shell): {\"cmd\":1}"),
            "{page}"
        );
        assert!(page.contains("continues at cursor 4"), "{page}");
        assert!(page.contains("next_cursor: 4"), "{page}");
    }

    /// answer shows availability honestly and keeps retrieval facts.
    #[test]
    fn answer_is_honest_about_availability_and_retrieval() {
        let missing = text(
            "answer",
            &json!({"agent_id": "ag-1", "status": "running", "available": false,
                    "inline_complete": false}),
        );
        assert!(missing.contains("NOT available"), "{missing}");
        let partial = text(
            "answer",
            &json!({
                "agent_id": "ag-1", "status": "succeeded", "available": true,
                "inline_complete": false, "kind": "agent_answer",
                "media_type": "text/markdown", "relative_path": "answers/a.md",
                "path": "/home/ag-1/agents/ag-1/answers/a.md",
                "size_bytes": 9000, "content": "partial only",
            }),
        );
        assert!(
            partial.contains("inline text absent or partial"),
            "{partial}"
        );
        assert!(
            partial.contains(
                "agent_answer (text/markdown) at /home/ag-1/agents/ag-1/answers/a.md, 9000 bytes"
            ),
            "{partial}"
        );
        let full = text(
            "answer",
            &json!({"agent_id": "ag-1", "status": "succeeded", "available": true,
                    "inline_complete": true, "content": "the whole answer\n"}),
        );
        assert!(full.contains("complete inline text below"), "{full}");
        assert!(full.contains("the whole answer"), "{full}");
    }

    /// One representative schema-2 catalog renders compactly with the
    /// configured facts, hoisted profiles, and no account or endpoint.
    #[test]
    fn models_renders_compact_guidance_and_grants() {
        // The bulky per-role asset arrays a real catalog carries; the text
        // page omits them, which is where its compactness comes from.
        let bulky = |prefix: &str, count: usize| {
            (0..count)
                .map(|index| json!({"id": format!("{prefix}-{index}")}))
                .collect::<Vec<_>>()
        };
        let model = json!({
            "model": "gpt-main", "native_model": "gpt-native",
            "params": {"effort": "medium"}, "allowed_params": {"effort": ["medium", "high"]},
            "restrictions": ["web_tools_disabled"],
            "recommendations": ["broad coding"],
            "profiles": ["code", "review"],
            "quota": {"status": "available", "evidence": "fresh"},
        });
        let mut other = model.clone();
        other["model"] = json!("gpt-review");
        let catalog = json!({
            "schema_version": 2, "config_revision": "abc123",
            "capacity_revision": 7,
            "profiles": [
                {"name": "code", "write": true, "network": false,
                 "read_roots": ["/tmp"], "required_constraints": [],
                 "skills": bulky("skill", 12), "mcp": bulky("server", 6),
                 "revision": "r1", "allow_external_read_roots": false},
                {"name": "review", "write": false, "network": false,
                 "read_roots": [], "required_constraints": ["filesystem_write_isolation"],
                 "skills": bulky("skill", 12), "mcp": bulky("server", 6),
                 "revision": "r2", "allow_external_read_roots": false},
            ],
            "providers": [{
                "provider": "codex", "harness": "codex",
                "recommendations": ["native subscription"],
                "models": [model, other],
            }],
        });
        let page = text("models", &catalog);
        assert!(
            page.contains("- code: write=true, network=false\n"),
            "{page}"
        );
        assert!(
            page.contains(
                "- review: write=false, network=false, constraints: filesystem_write_isolation"
            ),
            "{page}"
        );
        assert!(
            page.contains("provider codex (harness codex) — all models admit: code, review"),
            "{page}"
        );
        assert!(
            page.contains("provider guidance: native subscription"),
            "{page}"
        );
        assert!(
            page.contains("- gpt-main (gpt-native): available, evidence fresh"),
            "{page}"
        );
        assert!(
            page.contains("params: effort=medium; allowed effort: medium|high"),
            "{page}"
        );
        assert!(page.contains("restrictions: web_tools_disabled"), "{page}");
        assert!(page.contains("model guidance: broad coding"), "{page}");
        assert!(!page.contains("acct-"), "{page}");
        assert!(!page.contains("https://"), "{page}");
        assert!(
            page.len() < catalog.to_string().len(),
            "compact: {} vs {}",
            page.len(),
            catalog.to_string().len()
        );
    }

    /// Legacy rosters, both capacity-order schemas, limits, and doc render.
    #[test]
    fn legacy_and_remaining_tools_render_their_essentials() {
        let legacy = text(
            "models",
            &json!({"codex": {"available": true, "reason": null,
                              "models": [{"id": "gpt-5"}]}}),
        );
        assert!(legacy.contains("schema 1 runtime rosters"), "{legacy}");
        assert!(legacy.contains("- codex: available"), "{legacy}");
        assert!(legacy.contains("model: gpt-5"), "{legacy}");
        let order = text(
            "capacity_order",
            &json!({"schema_version": 2, "capacity_revision": 4, "providers": [
                {"provider": "glm", "score": 60.0, "priority_multiplier": 1.5,
                 "models": [{"model": "glm-5.3",
                             "quota": {"status": "available", "evidence": "fresh"}}]},
            ]}),
        );
        assert!(
            order.contains("1. glm (score 60.0, multiplier 1.5)"),
            "{order}"
        );
        assert!(
            order.contains("- glm-5.3: available, evidence fresh"),
            "{order}"
        );
        assert!(order.contains("not model ability"), "{order}");
        let legacy_order = text(
            "capacity_order",
            &json!({"routes": [{"runtime": "codex", "priority": 9.5,
                                "aliases": ["codex/main"]}],
                    "unavailable_runtimes": ["claude"]}),
        );
        assert!(
            legacy_order.contains("1. codex (priority 9.5) aliases: codex/main"),
            "{legacy_order}"
        );
        assert!(
            legacy_order.contains("unavailable runtimes: claude"),
            "{legacy_order}"
        );
        let limits = text(
            "limits",
            &json!({"observed_at": 1000.0, "items": [
                {"key": {"runtime": "glm", "lane": "glm-5.3", "window": "5h"},
                 "account": "work", "pool": "work::glm-5.3", "known": true,
                 "remaining_percent": 60.0, "reset_at": 4600.0},
                {"key": {"runtime": "codex", "lane": "codex", "window": "5h"},
                 "known": false},
            ]}),
        );
        assert!(
            limits.contains(
                "- glm/glm-5.3/5h [work pool work::glm-5.3]: 60.0% remaining, resets in 1h"
            ),
            "{limits}"
        );
        assert!(
            limits.contains("- codex/codex/5h: no current sample (stale or expired)"),
            "{limits}"
        );
        let doc = text("doc", &json!({"topic": "config", "text": "guide prose\n"}));
        assert_eq!(doc, "guide prose\n");
    }

    /// Empty catalog states, legacy empties, and errors are explicit.
    #[test]
    fn empty_states_and_errors_are_explicit() {
        let empty_models = text(
            "models",
            &json!({"providers": [], "profiles": [],
                    "config_revision": "empty", "capacity_revision": 3}),
        );
        assert!(
            empty_models.contains("no providers are currently configured"),
            "{empty_models}"
        );
        let empty_legacy = text("models", &json!({}));
        assert!(
            empty_legacy.contains("no enabled runtimes"),
            "{empty_legacy}"
        );
        let empty_agents = text(
            "list_agents",
            &json!({"items": [], "total": 0, "offset": 0, "complete": true}),
        );
        assert!(empty_agents.contains("0 of 0 matching"), "{empty_agents}");
        let empty_limits = text("limits", &json!({"items": []}));
        assert!(
            empty_limits.contains("no stored capacity samples"),
            "{empty_limits}"
        );
        let error = error_result("ValidationError", "unknown arguments: ['x']\nsecond line");
        assert_eq!(error.is_error, Some(true));
        assert_eq!(error.structured_content, None);
        let content = serde_json::to_value(&error.content).unwrap();
        assert_eq!(
            content[0]["text"],
            "agent-run error ValidationError: unknown arguments: ['x'] second line\n"
        );
    }
}
