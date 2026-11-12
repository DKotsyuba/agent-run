//! Single integration-test binary: each module is a former `tests/*.rs` crate.
//! One binary links dependencies once; nextest still runs every test in its own process.

#[path = "../agent_identity.rs"]
mod agent_identity;
#[path = "../capacity_collectors.rs"]
mod capacity_collectors;
#[path = "../capacity_service.rs"]
mod capacity_service;
#[path = "../claude_contract.rs"]
mod claude_contract;
#[path = "../claude_initial_proof.rs"]
mod claude_initial_proof;
#[path = "../claude_result_native.rs"]
mod claude_result_native;
#[path = "../claude_stream_identity.rs"]
mod claude_stream_identity;
#[path = "../codex_adapter_prepare.rs"]
mod codex_adapter_prepare;
#[path = "../codex_app_server.rs"]
mod codex_app_server;
#[path = "../codex_effective_params.rs"]
mod codex_effective_params;
#[path = "../codex_finish_idle.rs"]
mod codex_finish_idle;
#[path = "../codex_grant.rs"]
mod codex_grant;
#[path = "../codex_resume.rs"]
mod codex_resume;
#[path = "../commands.rs"]
mod commands;
#[path = "../common/mod.rs"]
mod common;
#[path = "../completion.rs"]
mod completion;
#[path = "../delivery.rs"]
mod delivery;
#[path = "../delivery_empty.rs"]
mod delivery_empty;
#[path = "../dispatch.rs"]
mod dispatch;
#[path = "../explicit_finish.rs"]
mod explicit_finish;
#[path = "../generic_batch.rs"]
mod generic_batch;
#[path = "../golden_homes.rs"]
mod golden_homes;
#[path = "../health_ping.rs"]
mod health_ping;
#[path = "../hooks.rs"]
mod hooks;
#[path = "../housekeeping.rs"]
mod housekeeping;
#[path = "../initial_task_wiring.rs"]
mod initial_task_wiring;
#[path = "../lifecycle_reconcile.rs"]
mod lifecycle_reconcile;
#[path = "../logging.rs"]
mod logging;
#[path = "../native_cache_integration.rs"]
mod native_cache_integration;
#[path = "../native_owner.rs"]
mod native_owner;
#[path = "../native_publication.rs"]
mod native_publication;
#[path = "../native_tree_cache.rs"]
mod native_tree_cache;
#[path = "../observation.rs"]
mod observation;
#[path = "../persist_batches.rs"]
mod persist_batches;
#[path = "../persist_diagnostics.rs"]
mod persist_diagnostics;
#[path = "../persist_recovery.rs"]
mod persist_recovery;
#[path = "../pool_enrollment.rs"]
mod pool_enrollment;
#[path = "../pool_push_stream.rs"]
mod pool_push_stream;
#[path = "../provider_catalog.rs"]
mod provider_catalog;
#[path = "../provider_ranking.rs"]
mod provider_ranking;
#[path = "../quota_adapter_v1.rs"]
mod quota_adapter_v1;
#[path = "../quota_admission_v2.rs"]
mod quota_admission_v2;
#[path = "../quota_client_v1.rs"]
mod quota_client_v1;
#[path = "../quota_projection_v2.rs"]
mod quota_projection_v2;
#[path = "../reconciliation_receipts.rs"]
mod reconciliation_receipts;
#[path = "../redaction.rs"]
mod redaction;
#[path = "../research_reports.rs"]
mod research_reports;
#[path = "../resume_stream.rs"]
mod resume_stream;
#[path = "../runtime_storage.rs"]
mod runtime_storage;
#[path = "../service.rs"]
mod service;
#[path = "../session_continuity.rs"]
mod session_continuity;
#[path = "../status_wiring.rs"]
mod status_wiring;
#[path = "../storage_gc.rs"]
mod storage_gc;
#[path = "../supervisor_main.rs"]
mod supervisor_main;
#[path = "../unlimited_executions.rs"]
mod unlimited_executions;
#[path = "../verification.rs"]
mod verification;
#[path = "../version_probe.rs"]
mod version_probe;
#[path = "../wait.rs"]
mod wait;
#[path = "../writer_service.rs"]
mod writer_service;
