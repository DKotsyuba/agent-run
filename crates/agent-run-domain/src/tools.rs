//! The canonical public-tool registry shared by every transport.

use crate::MachineCode;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::OnceLock;

/// Reviewed MCP hints, carried by the schema-first assets and shared discovery.
/// Hints describe effects; they confer no authority or unconditional replay safety.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ToolAnnotations {
    /// Whether the operation leaves application state unchanged.
    pub read_only_hint: bool,
    /// Whether it may terminate work or remove/replace existing state.
    pub destructive_hint: bool,
    /// Whether identical arguments guarantee no additional effect; optional
    /// request keys alone are insufficient for an unconditional true hint.
    pub idempotent_hint: bool,
    /// Whether configured engines, hosts or other external entities participate.
    pub open_world_hint: bool,
}

/// One callable method in the schema-first registry, optionally advertised.
///
/// Input schemas preserve the frozen Python oracle plus explicitly tested
/// product extensions. Descriptions and typed effect annotations come from the
/// same reviewed assets; current snapshots pin them together. Error classes and
/// dispatch defaults are derived metadata, separate from MCP discovery.
/// Call-only replacements retain their original schemas and dispatch semantics.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ToolDefinition {
    /// Stable method name shared by CLI, MCP, and the Unix socket API.
    pub name: String,
    /// Canonical replacement for a call-only compatibility method. Omission
    /// advertises the method; a named replacement hides it from all discovery
    /// while preserving its original argument schema and dispatcher behavior.
    /// This internal routing metadata never appears in the MCP tool object.
    #[serde(default, skip_serializing)]
    pub legacy_for: Option<String>,
    /// Transport audience. Omission means the orchestrator MCP surface (and,
    /// like every method, the operator socket and CLI); `socket` marks a
    /// service integration method served only by the operator socket and CLI,
    /// never by orchestrator MCP discovery or dispatch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub surface: Option<ToolSurface>,
    /// Discovery guidance, including the completion-notice contract for start.
    pub description: String,
    /// Reviewed effect hints from this asset, never transport-local defaults.
    pub annotations: ToolAnnotations,
    /// JSON Schema for the sole object argument.
    pub input_schema: Value,
    /// Optional MCP result schema; absent for the current text-first profile.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_schema: Option<Value>,
    /// Optional product result declaration; absent on the current SDK wire.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result_shape: Option<Value>,
}

/// Narrower transport audience of a registry method.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolSurface {
    /// Service integration method (Agent Pool, Hub, operator CLI): served by
    /// the operator socket and listed by its `tools` method, but absent from
    /// orchestrator MCP discovery and refused by orchestrator MCP dispatch.
    Socket,
}

/// One input argument derived from a tool's JSON Schema.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ArgumentDefinition<'a> {
    /// The object property name accepted by the tool.
    pub name: &'a str,
    /// The JSON Schema accepted for the property value.
    pub schema: &'a Value,
    /// Whether the property must be supplied by the caller.
    pub required: bool,
    /// The dispatch default used when the optional property is omitted.
    pub default: Option<ArgumentDefault>,
}

/// A JSON-compatible default applied by dispatch for an omitted optional argument.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArgumentDefault {
    /// The omitted value is interpreted as JSON null.
    Null,
    /// The omitted value is interpreted as this boolean.
    Bool(bool),
    /// The omitted value is interpreted as this signed integer.
    Integer(i64),
    /// The omitted value is interpreted as this finite number.
    Number(u64),
    /// The omitted value is interpreted as an empty array.
    EmptyArray,
}

impl ArgumentDefault {
    /// Converts this registry default into its public JSON representation.
    pub fn json(self) -> Value {
        match self {
            Self::Null => Value::Null,
            Self::Bool(value) => Value::Bool(value),
            Self::Integer(value) => json!(value),
            Self::Number(value) => json!(value),
            Self::EmptyArray => json!([]),
        }
    }
}

impl ToolDefinition {
    /// Returns the declared arguments in the schema's stable property order.
    ///
    /// Every public tool accepts exactly one JSON object. This method exposes
    /// its properties, required set, type schema, and dispatch defaults for
    /// CLI wiring and validation without maintaining a second argument table.
    pub fn arguments(&self) -> Vec<ArgumentDefinition<'_>> {
        let required = self
            .input_schema
            .get("required")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .collect::<Vec<_>>();
        self.input_schema
            .get("properties")
            .and_then(Value::as_object)
            .into_iter()
            .flatten()
            .map(|(name, schema)| ArgumentDefinition {
                name,
                schema,
                required: required.contains(&name.as_str()),
                default: argument_default(self.name.as_str(), name),
            })
            .collect()
    }

    /// Lists the stable public error classes this operation can surface.
    ///
    /// Validation is always possible at the object boundary. Other classes
    /// are restricted to the durable operation each tool invokes; transports
    /// render these same machine codes using their protocol-specific envelope.
    pub fn error_classes(&self) -> &'static [MachineCode] {
        match self.name.as_str() {
            "start" => &[
                MachineCode::ValidationError,
                MachineCode::PathEscapeError,
                MachineCode::RequestConflict,
                MachineCode::SelectionBusy,
                MachineCode::NoEligibleAccount,
                MachineCode::QuotaExhausted,
                MachineCode::CapacityExhausted,
                MachineCode::Unsupported,
                MachineCode::RuntimeError,
                MachineCode::IOError,
                MachineCode::StorageError,
            ],
            "start_batch" => &[
                MachineCode::BatchOutcomeUnknown,
                MachineCode::StoreIdentityMismatch,
                MachineCode::PreconditionFailed,
                MachineCode::ValidationError,
                MachineCode::PathEscapeError,
                MachineCode::RequestConflict,
                MachineCode::SelectionBusy,
                MachineCode::NoEligibleAccount,
                MachineCode::QuotaExhausted,
                MachineCode::BindingCooldown,
                MachineCode::CapacityExhausted,
                MachineCode::StateTransitionError,
                MachineCode::BrokerUnavailable,
                MachineCode::Unsupported,
                MachineCode::RuntimeError,
                MachineCode::IOError,
                MachineCode::StorageError,
            ],
            "batch_receipt" => &[
                MachineCode::ValidationError,
                MachineCode::StoreIdentityMismatch,
                MachineCode::Unsupported,
                MachineCode::AnswerIntegrityError,
                MachineCode::RuntimeError,
                MachineCode::IOError,
                MachineCode::StorageError,
            ],
            "history_pins_list" => &[
                MachineCode::ValidationError,
                MachineCode::StoreIdentityMismatch,
                MachineCode::BrokerUnavailable,
                MachineCode::RuntimeError,
                MachineCode::IOError,
                MachineCode::StorageError,
            ],
            "history_pins_release" => &[
                MachineCode::ValidationError,
                MachineCode::RequestConflict,
                MachineCode::StoreIdentityMismatch,
                MachineCode::BrokerUnavailable,
                MachineCode::RuntimeError,
                MachineCode::IOError,
                MachineCode::StorageError,
            ],
            "transcript_exact" => &[
                MachineCode::ValidationError,
                MachineCode::StoreIdentityMismatch,
                MachineCode::TranscriptExportAborted,
                MachineCode::Unsupported,
                MachineCode::RuntimeError,
                MachineCode::IOError,
                MachineCode::StorageError,
            ],
            "initial_task" => &[
                MachineCode::ValidationError,
                MachineCode::StoreIdentityMismatch,
                MachineCode::Unsupported,
                MachineCode::RuntimeError,
                MachineCode::IOError,
                MachineCode::StorageError,
            ],
            "status" => &[
                MachineCode::ValidationError,
                MachineCode::StoreIdentityMismatch,
                MachineCode::Unsupported,
                MachineCode::RuntimeError,
                MachineCode::AnswerIntegrityError,
                MachineCode::IOError,
                MachineCode::StorageError,
            ],
            "resume" => &[
                MachineCode::SequenceMismatch,
                MachineCode::ValidationError,
                MachineCode::AgentNotFound,
                MachineCode::StateTransitionError,
                MachineCode::RequestConflict,
                MachineCode::Unsupported,
                MachineCode::RuntimeError,
                MachineCode::IOError,
                MachineCode::StorageError,
            ],
            "cancel" | "steer" => &[
                MachineCode::ValidationError,
                MachineCode::AgentNotFound,
                MachineCode::StateTransitionError,
                MachineCode::Unsupported,
                MachineCode::IOError,
                MachineCode::StorageError,
            ],
            "answer" => &[
                MachineCode::ValidationError,
                MachineCode::AgentNotFound,
                MachineCode::AnswerIntegrityError,
                MachineCode::IOError,
                MachineCode::StorageError,
            ],
            "answer_chunk" => &[
                MachineCode::ValidationError,
                MachineCode::AgentNotFound,
                MachineCode::ExecutionChanged,
                MachineCode::AnswerHashMismatch,
                MachineCode::AnswerUnavailable,
                MachineCode::AnswerIntegrityError,
                MachineCode::PathEscapeError,
                MachineCode::RuntimeError,
                MachineCode::IOError,
                MachineCode::StorageError,
            ],
            "capabilities" => &[
                MachineCode::ValidationError,
                MachineCode::Unsupported,
                MachineCode::RuntimeError,
                MachineCode::IOError,
                MachineCode::StorageError,
            ],
            "agent_integration" => &[
                MachineCode::ValidationError,
                MachineCode::AgentNotFound,
                MachineCode::AnswerIntegrityError,
                MachineCode::RuntimeError,
                MachineCode::IOError,
                MachineCode::StorageError,
            ],
            "list_agents" | "transcript" => &[
                MachineCode::ValidationError,
                MachineCode::AgentNotFound,
                MachineCode::IOError,
                MachineCode::StorageError,
            ],
            "delegation_guide" => &[
                MachineCode::ValidationError,
                MachineCode::Unsupported,
                MachineCode::RuntimeError,
                MachineCode::IOError,
                MachineCode::StorageError,
            ],
            "doc" => &[MachineCode::ValidationError],
            _ => unreachable!("registry contains only the pinned public tools"),
        }
    }
}

/// Returns the sole callable registry, including call-only compatibility
/// methods, parsed once from the checked-in public asset.
///
/// `assets/tools.json` preserves the Python-compatible schemas and carries
/// current operator guidance in each description. The domain crate owns parsing,
/// lookup, argument metadata, and error declarations, so no transport can acquire
/// an independent schema or name list.
pub fn registry() -> &'static [ToolDefinition] {
    static REGISTRY: OnceLock<Vec<ToolDefinition>> = OnceLock::new();
    REGISTRY
        .get_or_init(|| {
            serde_json::from_str(include_str!("../../../assets/tools.json"))
                .expect("assets/tools.json must be a valid public tool registry")
        })
        .as_slice()
}

/// Returns the separate worker-capability registry from its sole asset.
/// This table never adds operator tools or accepts capability material as input.
pub fn worker_registry() -> &'static [ToolDefinition] {
    static REGISTRY: OnceLock<Vec<ToolDefinition>> = OnceLock::new();
    REGISTRY
        .get_or_init(|| {
            serde_json::from_str(include_str!("../../../assets/worker_tools.json"))
                .expect("worker_tools.json must be a valid worker registry")
        })
        .as_slice()
}

/// Serializes the worker-only registry used by live MCP and contract exports.
pub fn worker_tools_json() -> Vec<Value> {
    worker_registry()
        .iter()
        .map(|tool| serde_json::to_value(tool).expect("tool definition is JSON"))
        .collect()
}

/// Finds an advertised or call-only compatibility method by its stable name.
pub fn tool(name: &str) -> Option<&'static ToolDefinition> {
    registry().iter().find(|definition| definition.name == name)
}

/// Reports whether a method is one of the public tools.
pub fn is_tool(name: &str) -> bool {
    tool(name).is_some()
}

/// Reports whether a method belongs to the orchestrator MCP surface: an
/// advertised or call-only registry method that is not socket-only.
pub fn is_orchestrator_tool(name: &str) -> bool {
    tool(name).is_some_and(|definition| definition.surface.is_none())
}

/// Renders the orchestrator MCP discovery projection, omitting call-only
/// compatibility entries and socket-only service methods. Direct lookup
/// retains their original schemas for legacy calls.
pub fn tools_json() -> Vec<Value> {
    registry()
        .iter()
        .filter(|definition| definition.legacy_for.is_none() && definition.surface.is_none())
        .cloned()
        .map(orchestrator_tool_json)
        .collect()
}

/// Projects one MCP definition with the same receipt shape as tool discovery.
pub fn orchestrator_tool_json(mut definition: ToolDefinition) -> Value {
    if definition.name == "start_batch"
        && let Some(schema) = &mut definition.output_schema
        && let Some(properties) = schema["properties"].as_object_mut()
    {
        properties.remove("release_token");
    }
    serde_json::to_value(definition).expect("tool definition is JSON")
}

/// Renders the operator-socket `tools` listing: every advertised method,
/// including socket-only service methods (each carrying `"surface":"socket"`),
/// so socket clients such as Hub keep detecting them with one `tools` call.
pub fn socket_tools_json() -> Vec<Value> {
    registry()
        .iter()
        .filter(|definition| definition.legacy_for.is_none())
        .cloned()
        .map(|mut definition| {
            if definition.name == "list_agents" {
                definition.input_schema["properties"]["limit"] =
                    json!({"type":"integer","minimum":1,"maximum":1000,"default":100});
            }
            serde_json::to_value(definition).expect("tool definition is JSON")
        })
        .collect()
}

/// Returns the only non-schema defaults that Python dispatch applies.
fn argument_default(tool: &str, argument: &str) -> Option<ArgumentDefault> {
    match (tool, argument) {
        ("start", "write" | "fast") | ("list_agents", "active") => {
            Some(ArgumentDefault::Bool(false))
        }
        ("start", "read_roots" | "required_constraints") => Some(ArgumentDefault::EmptyArray),
        ("start", "effort" | "output_schema" | "orchestrator" | "request_id" | "account")
        | ("resume", "request_id" | "orchestrator")
        | ("list_agents", "orchestrator" | "after_revision")
        | ("doc", "topic")
        | ("delegation_guide", "provider" | "profile" | "model") => Some(ArgumentDefault::Null),
        ("resume" | "cancel" | "steer" | "answer" | "transcript", "run_id") => {
            Some(ArgumentDefault::Null)
        }
        ("history_pins_list", "limit") => Some(ArgumentDefault::Integer(100)),
        ("history_pins_list", "after_pin_id") => Some(ArgumentDefault::Null),
        (
            "history_pins_list" | "history_pins_release" | "start_batch" | "status",
            "expected_store_id",
        ) => Some(ArgumentDefault::Null),
        ("status", "include_view") => Some(ArgumentDefault::Bool(false)),
        ("start_batch" | "batch_receipt", "retain_history") => Some(ArgumentDefault::Bool(false)),
        ("capabilities", "include_store_id") => Some(ArgumentDefault::Bool(false)),
        ("capabilities", "include_lineage_holds" | "include_initial_task") => {
            Some(ArgumentDefault::Bool(false))
        }
        ("answer_chunk" | "initial_task", "max_bytes") => Some(ArgumentDefault::Integer(65536)),
        ("initial_task", "offset") => Some(ArgumentDefault::Integer(0)),
        ("list_agents", "offset") | ("transcript", "cursor") => Some(ArgumentDefault::Integer(0)),
        ("list_agents", "after_message_revision") => Some(ArgumentDefault::Null),
        ("transcript", "view") => Some(ArgumentDefault::Null),
        ("list_agents", "limit") => Some(ArgumentDefault::Integer(100)),
        ("transcript", "limit") => Some(ArgumentDefault::Integer(200)),
        ("list_agents", "wait_seconds") => Some(ArgumentDefault::Number(0)),
        ("transcript", "tail_blocks") | ("transcript", "before_cursor") => {
            Some(ArgumentDefault::Null)
        }
        _ => None,
    }
}
