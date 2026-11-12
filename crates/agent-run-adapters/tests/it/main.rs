//! Single integration-test binary: each module is a former `tests/*.rs` crate.
//! One binary links dependencies once; nextest still runs every test in its own process.

#[path = "../adapter_contract.rs"]
mod adapter_contract;
#[path = "../authorized_request.rs"]
mod authorized_request;
#[path = "../claude_session.rs"]
mod claude_session;
#[path = "../codex_adapter_long_tail.rs"]
mod codex_adapter_long_tail;
#[path = "../codex_environment.rs"]
mod codex_environment;
#[path = "../codex_errors.rs"]
mod codex_errors;
#[path = "../codex_permissions.rs"]
mod codex_permissions;
#[path = "../codex_runtime.rs"]
mod codex_runtime;
#[path = "../codex_session.rs"]
mod codex_session;
#[path = "../codex_trust.rs"]
mod codex_trust;
#[path = "../command_policy.rs"]
mod command_policy;
#[path = "../control_pressure.rs"]
mod control_pressure;
#[path = "../environment.rs"]
mod environment;
#[path = "../glm.rs"]
mod glm;
#[path = "../harness_drain.rs"]
mod harness_drain;
#[path = "../harness_exit.rs"]
mod harness_exit;
#[path = "../keychain.rs"]
mod keychain;
#[path = "../managed_native_fixtures.rs"]
mod managed_native_fixtures;
#[path = "../native_settings.rs"]
mod native_settings;
#[path = "../ownership_gate.rs"]
mod ownership_gate;
#[path = "../plugin_integration.rs"]
mod plugin_integration;
#[path = "../provider_launch.rs"]
mod provider_launch;
#[path = "../redaction.rs"]
mod redaction;
#[path = "../snapshots.rs"]
mod snapshots;
