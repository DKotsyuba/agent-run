//! Single integration-test binary: each module is a former `tests/*.rs` crate.
//! One binary links dependencies once; nextest still runs every test in its own process.

#[path = "../accounts.rs"]
mod accounts;
#[path = "../common/mod.rs"]
mod common;
#[path = "../config_compat.rs"]
mod config_compat;
#[path = "../domain_config.rs"]
mod domain_config;
#[path = "../managed_services.rs"]
mod managed_services;
#[path = "../mcp_controls.rs"]
mod mcp_controls;
#[path = "../native_settings.rs"]
mod native_settings;
#[path = "../profiles_policy.rs"]
mod profiles_policy;
#[path = "../provider_config.rs"]
mod provider_config;
#[path = "../provider_migration.rs"]
mod provider_migration;
#[path = "../role_plan.rs"]
mod role_plan;
#[path = "../unlimited_executions.rs"]
mod unlimited_executions;
#[path = "../workspace_roots.rs"]
mod workspace_roots;
