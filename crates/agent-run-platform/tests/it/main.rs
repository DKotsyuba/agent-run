//! Single integration-test binary: each module is a former `tests/*.rs` crate.
//! One binary links dependencies once; nextest still runs every test in its own process.

#[path = "../answer_proof.rs"]
mod answer_proof;
#[path = "../launch.rs"]
mod launch;
#[path = "../launch_capability.rs"]
mod launch_capability;
#[path = "../launch_cleanup.rs"]
mod launch_cleanup;
#[path = "../launch_tail.rs"]
mod launch_tail;
#[path = "../lifecycle_tail.rs"]
mod lifecycle_tail;
#[path = "../process_identity.rs"]
mod process_identity;
#[path = "../publish.rs"]
mod publish;
#[path = "../release.rs"]
mod release;
#[path = "../safe_fs.rs"]
mod safe_fs;
