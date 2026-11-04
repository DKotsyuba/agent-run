//! Manual, content-free check of an already provisioned Bridge producer grant.

use std::{
    io::Read,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    time::Duration,
};

/// The real credential stays in the broker account and is never printed.
#[test]
#[ignore = "manual check requires the configured broker account and Bridge"]
fn bridge_producer_capabilities() {
    let home = PathBuf::from(std::env::var_os("AGENT_RUN_BRIDGE_CHECK_HOME").unwrap());
    let (config, _) = agent_run_config::provider_config::ProviderConfig::load(&home).unwrap();
    let bridge = config.delivery.bridge.unwrap();
    let parent = agent_run_platform::fs::Dir::open(bridge.token_file.parent().unwrap()).unwrap();
    let file = parent
        .open_file(Path::new(bridge.token_file.file_name().unwrap()))
        .unwrap();
    let metadata = file.metadata().unwrap();
    // SAFETY: geteuid only reads this process's effective user identity.
    let uid = unsafe { libc::geteuid() };
    assert!(metadata.is_file());
    assert_eq!(metadata.uid(), uid);
    assert_eq!(metadata.mode() & 0o077, 0);
    let mut token = String::new();
    file.take(16_385).read_to_string(&mut token).unwrap();
    assert!(token.len() <= 16_384);
    let registration = std::env::var_os("AGENT_RUN_BRIDGE_TEST_REGISTRATION")
        .map(PathBuf::from)
        .unwrap_or_else(|| bridge.registration_socket.clone());
    let cfg = bridge_sdk::Config::new(
        &registration,
        &bridge.core_socket,
        bridge_sdk::bridge_protocol::Secret::new(token.trim().to_owned()),
        Duration::from_secs(5),
    )
    .unwrap_or_else(|_| panic!("Bridge client configuration failed"));
    let client = bridge_sdk::Client::new(cfg)
        .unwrap_or_else(|_| panic!("Bridge client initialization failed"));
    let info = client
        .capabilities("agent-run-producer-capability-check")
        .unwrap_or_else(|error| {
            use bridge_sdk::ClientError;
            match error {
                ClientError::InvalidConfig(_) => panic!("Bridge capability: invalid config"),
                ClientError::InvalidRequest(_) => panic!("Bridge capability: invalid request"),
                ClientError::Service(error) => {
                    panic!("Bridge capability service class: {:?}", error.class)
                }
                ClientError::Transport {
                    outcome, reason, ..
                } => panic!("Bridge capability transport: {:?}, {}", outcome, reason),
            }
        });
    assert_eq!(info.api_version, 1);
    assert!(info.grant.direct_address);
    assert!(
        info.grant
            .allowed_modes
            .contains(&bridge_sdk::bridge_protocol::Mode::Message)
    );
    assert!(
        info.grant
            .allowed_policies
            .contains(&bridge_sdk::bridge_protocol::PolicyKind::KeepAll)
    );
    assert!(
        info.operations
            .iter()
            .any(|operation| operation == "publish")
    );
    assert!(
        info.operations
            .iter()
            .any(|operation| operation == "status")
    );
    for kind in [
        "agent-run.completion",
        "agent-run.worker-report",
        "agent-run.pool-completion",
        "agent-run.pool-attention",
    ] {
        assert!(
            info.grant
                .allowed_kinds
                .iter()
                .any(|allowed| allowed == kind)
        );
    }
    println!("bridge_producer_capabilities=ok");
}
