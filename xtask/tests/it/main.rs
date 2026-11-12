//! Single integration-test binary: each module is a former `tests/*.rs` crate.
//! One binary links dependencies once; nextest still runs every test in its own process.

#[path = "../archive.rs"]
mod archive;
#[path = "../ci_workflow.rs"]
mod ci_workflow;
#[path = "../deploy_recovery.rs"]
mod deploy_recovery;
#[path = "../installer.rs"]
mod installer;
#[path = "../release.rs"]
mod release;
#[path = "../release_wait.rs"]
mod release_wait;
