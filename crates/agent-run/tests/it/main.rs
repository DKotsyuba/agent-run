//! Single integration-test binary: each module is a former `tests/*.rs` crate.
//! One binary links dependencies once; nextest still runs every test in its own process.

#[path = "../batch_socket_smoke.rs"]
mod batch_socket_smoke;
#[path = "../bridge_capabilities_live.rs"]
mod bridge_capabilities_live;
#[path = "../../../agent-run-core/tests/common/bridge.rs"]
mod bridge_fixture;
#[path = "../broker_client.rs"]
mod broker_client;
#[path = "../capacity_cli.rs"]
mod capacity_cli;
#[path = "../claude_account_home.rs"]
mod claude_account_home;
#[path = "../cli_accounts.rs"]
mod cli_accounts;
#[path = "../cli_parity.rs"]
mod cli_parity;
#[path = "../cli_seam.rs"]
mod cli_seam;
#[path = "../config_migrate.rs"]
mod config_migrate;
#[path = "../context_hook_process.rs"]
mod context_hook_process;
#[path = "../doctor.rs"]
mod doctor;
#[path = "../doctor_contract.rs"]
mod doctor_contract;
#[path = "../end_to_end.rs"]
mod end_to_end;
#[path = "../fake_engine.rs"]
mod fake_engine;
#[path = "../hooks.rs"]
mod hooks;
#[path = "../init_offline.rs"]
mod init_offline;
#[path = "../launch.rs"]
mod launch;
#[path = "../launchd.rs"]
mod launchd;
#[path = "../mcp_parity.rs"]
mod mcp_parity;
#[path = "../opencode_live.rs"]
mod opencode_live;
#[path = "../operator_answer.rs"]
mod operator_answer;
#[path = "../operator_cli_mode.rs"]
mod operator_cli_mode;
#[path = "../operator_transcript.rs"]
mod operator_transcript;
#[path = "../operator_transcript_retry.rs"]
mod operator_transcript_retry;
#[path = "../protocol.rs"]
mod protocol;
#[path = "../provider_initial.rs"]
mod provider_initial;
#[path = "../provider_login.rs"]
mod provider_login;
#[path = "../provider_public_boundary.rs"]
mod provider_public_boundary;
#[path = "../resources.rs"]
mod resources;
#[path = "../resume_retry.rs"]
mod resume_retry;
#[path = "../shared_worker.rs"]
mod shared_worker;
#[path = "../shared_worker_uid.rs"]
mod shared_worker_uid;
#[path = "../socket_api_planned.rs"]
mod socket_api_planned;
#[path = "../socket_concurrency.rs"]
mod socket_concurrency;
#[path = "../socket_operator.rs"]
mod socket_operator;
#[path = "../socket_protocol.rs"]
mod socket_protocol;
#[path = "../storage_admin.rs"]
mod storage_admin;
#[path = "../support/test_broker.rs"]
mod test_broker;
#[path = "../worker_pool_wire.rs"]
mod worker_pool_wire;
#[path = "../writer_load.rs"]
mod writer_load;
#[path = "../writer_socket.rs"]
mod writer_socket;
