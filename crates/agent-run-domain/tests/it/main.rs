//! Single integration-test binary: each module is a former `tests/*.rs` crate.
//! One binary links dependencies once; nextest still runs every test in its own process.

#[path = "../canonical.rs"]
mod canonical;
#[path = "../contracts.rs"]
mod contracts;
#[path = "../display_name.rs"]
mod display_name;
#[path = "../health.rs"]
mod health;
#[path = "../initial_task.rs"]
mod initial_task;
#[path = "../quota_decision_fence.rs"]
mod quota_decision_fence;
#[path = "../tool_registry.rs"]
mod tool_registry;
#[path = "../transcript_exact.rs"]
mod transcript_exact;
#[path = "../transcript_options.rs"]
mod transcript_options;
