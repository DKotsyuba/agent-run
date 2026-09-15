use agent_run::process::{self, ProcessState};
#[test]
fn current_process_birth_is_observed_and_reuse_is_distinct() {
    let identity = process::inspect(std::process::id() as i32).unwrap();
    assert_eq!(
        process::observe(
            Some(identity.pid),
            Some(&identity.token),
            Some(identity.birth)
        ),
        ProcessState::Alive
    );
    let wrong = if cfg!(target_os = "linux") {
        "linux:wrong:0"
    } else {
        "darwin:0:0"
    };
    assert_eq!(
        process::observe(Some(identity.pid), Some(wrong), Some(identity.birth)),
        ProcessState::Reused
    );
}
#[test]
fn missing_or_unproven_identity_is_not_automatically_death() {
    assert_eq!(process::observe(None, None, None), ProcessState::NotStarted);
    assert_eq!(
        process::observe(Some(std::process::id() as i32), None, None),
        ProcessState::Unknown
    );
    assert_eq!(process::observe(Some(0), None, None), ProcessState::Unknown);
}
