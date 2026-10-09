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
            "resume" => &[
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
            "list_pools" => &[
                MachineCode::ValidationError,
                MachineCode::IOError,
                MachineCode::StorageError,
            ],
            "list_agents" | "transcript" => &[
                MachineCode::ValidationError,
                MachineCode::AgentNotFound,
                MachineCode::IOError,
                MachineCode::StorageError,
            ],
            "capacity_order" | "models" | "limits" | "delegation_guide" => &[
                MachineCode::ValidationError,
                MachineCode::Unsupported,
                MachineCode::RuntimeError,
                MachineCode::IOError,
                MachineCode::StorageError,
            ],
            "start_pool" => &[
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
            "pool_post" | "pool_replace" | "pool" => &[
                MachineCode::ValidationError,
                MachineCode::AgentNotFound,
                MachineCode::StateTransitionError,
                MachineCode::RequestConflict,
                MachineCode::CapacityExhausted,
                MachineCode::SelectionBusy,
                MachineCode::NoEligibleAccount,
                MachineCode::QuotaExhausted,
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

/// Renders the shared discovery projection, omitting call-only compatibility
/// entries. Direct lookup retains their original schemas for legacy calls.
pub fn tools_json() -> Vec<Value> {
    registry()
        .iter()
        .filter(|definition| definition.legacy_for.is_none())
        .cloned()
        .map(|definition| serde_json::to_value(definition).expect("tool definition is JSON"))
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
        | ("models" | "delegation_guide", "provider" | "profile" | "model")
        | ("capacity_order", "model") => Some(ArgumentDefault::Null),
        ("start_pool", "acceptance" | "orchestrator")
        | ("pool_replace", "start")
        | ("pool", "after_seq" | "before_seq" | "limit") => Some(ArgumentDefault::Null),
        ("resume" | "cancel" | "steer" | "answer" | "transcript", "run_id") => {
            Some(ArgumentDefault::Null)
        }
        ("list_pools", "state") => Some(ArgumentDefault::Null),
        ("list_pools", "limit") => Some(ArgumentDefault::Integer(50)),
        ("list_pools" | "list_agents", "offset") | ("transcript", "cursor") => {
            Some(ArgumentDefault::Integer(0))
        }
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
