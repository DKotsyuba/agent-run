//! Single integration-test binary: each module is a former `tests/*.rs` crate.
//! One binary links dependencies once; nextest still runs every test in its own process.

#[path = "../account_registry.rs"]
mod account_registry;
#[path = "../admission.rs"]
mod admission;
#[path = "../attempt_binding_r2.rs"]
mod attempt_binding_r2;
#[path = "../attempt_launch.rs"]
mod attempt_launch;
#[path = "../common/mod.rs"]
mod common;
#[path = "../context.rs"]
mod context;
#[path = "../diagnostics.rs"]
mod diagnostics;
#[path = "../f1_noop_reads.rs"]
mod f1_noop_reads;
#[path = "../f3_terminal_stats.rs"]
mod f3_terminal_stats;
#[path = "../f6_f7_diagnostics.rs"]
mod f6_f7_diagnostics;
#[path = "../health.rs"]
mod health;
#[path = "../incidents.rs"]
mod incidents;
#[path = "../initial_task.rs"]
mod initial_task;
#[path = "../journal.rs"]
mod journal;
#[path = "../lineage.rs"]
mod lineage;
#[path = "../lineage_holds.rs"]
mod lineage_holds;
#[path = "../outbox.rs"]
mod outbox;
#[path = "../owner_v2.rs"]
mod owner_v2;
#[path = "../ownership.rs"]
mod ownership;
#[path = "../pool_chat.rs"]
mod pool_chat;
#[path = "../pool_schema.rs"]
mod pool_schema;
#[path = "../process_ownership.rs"]
mod process_ownership;
#[path = "../projection_proof.rs"]
mod projection_proof;
#[path = "../projections.rs"]
mod projections;
#[path = "../provider_admission.rs"]
mod provider_admission;
#[path = "../provider_writer_paths.rs"]
mod provider_writer_paths;
#[path = "../retention.rs"]
mod retention;
#[path = "../retired_quota_writer.rs"]
mod retired_quota_writer;
#[path = "../run_stats.rs"]
mod run_stats;
#[path = "../runtime_storage.rs"]
mod runtime_storage;
#[path = "../service_restart.rs"]
mod service_restart;
#[path = "../sqlite_busy.rs"]
mod sqlite_busy;
#[path = "../state.rs"]
mod state;
#[path = "../state_migrations.rs"]
mod state_migrations;
#[path = "../status_snapshot.rs"]
mod status_snapshot;
#[path = "../store_corpus.rs"]
mod store_corpus;
#[path = "../store_identity.rs"]
mod store_identity;
#[path = "../terminal_delivery.rs"]
mod terminal_delivery;
#[path = "../transcript_blocks.rs"]
mod transcript_blocks;
#[path = "../unlimited_executions.rs"]
mod unlimited_executions;
#[path = "../worker_finish.rs"]
mod worker_finish;
#[path = "../worker_notifications.rs"]
mod worker_notifications;
#[path = "../writer_apply.rs"]
mod writer_apply;
#[path = "../writer_fence.rs"]
mod writer_fence;
#[path = "../writer_local_ops.rs"]
mod writer_local_ops;
#[path = "../writer_ops.rs"]
mod writer_ops;
#[path = "../writer_queue.rs"]
mod writer_queue;
#[path = "../writer_scope.rs"]
mod writer_scope;
