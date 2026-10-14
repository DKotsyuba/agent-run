//! Disposable SDK-protocol service for outbox tests; no native carrier or production credentials.
use bridge_sdk::bridge_protocol::*;
use std::{
    os::unix::fs::PermissionsExt,
    path::Path,
    sync::{Arc, Mutex},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::UnixListener,
    sync::Notify,
};

/// Own one synthetic Bridge endpoint and its observed publications; dropping aborts its task.
pub struct BridgeFixture {
    /// Exact decoded publications, excluding request credentials.
    pub published: Arc<Mutex<Vec<PublishParams>>>,
    /// Signalled when a publication arrives, before its optional acknowledgement pause.
    #[allow(dead_code)] // Shared with broker tests that only observe publications.
    pub started: Arc<Notify>,
    /// Releases a paused publication reply; one permit serves one publication.
    #[allow(dead_code)] // Shared fixture control is exercised by the core outbox tests.
    pub release: Arc<Notify>,
    /// The only owned fixture service task.
    task: tokio::task::JoinHandle<()>,
}

impl Drop for BridgeFixture {
    /// Abort the private fixture server; never signal or modify an external process/service.
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Append a protected synthetic Bridge deployment to this disposable home's config.
/// `paused` holds each publish reply until released, permitting cancellation/lease/config races.
/// Panics on malformed fixture setup or protocol; never contacts a native chat or provider.
pub fn start(home: &Path, paused: bool) -> BridgeFixture {
    let socket = home.join("bridge-fixture.sock");
    let listener = UnixListener::bind(&socket).unwrap();
    let token = home.join("bridge-fixture-token");
    std::fs::write(&token, "synthetic-bridge-fixture-only").unwrap();
    std::fs::set_permissions(&token, std::fs::Permissions::from_mode(0o600)).unwrap();
    let path = home.join("config.toml");
    let original = std::fs::read_to_string(&path).unwrap();
    std::fs::write(path, format!("{original}\n[delivery.bridge]\nregistration_socket={socket:?}\ncore_socket={socket:?}\ntoken_file={token:?}\nhost_id='local'\ncodex_store='codex-main'\nclaude_store='claude-main'\n")).unwrap();
    let published = Arc::new(Mutex::new(Vec::<PublishParams>::new()));
    let observed = published.clone();
    let started = Arc::new(Notify::new());
    let arrival = started.clone();
    let release = Arc::new(Notify::new());
    let resume = release.clone();
    let task = tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let mut stream = BufReader::new(stream);
            let mut line = String::new();
            stream.read_line(&mut line).await.unwrap();
            let request = decode_request(line.trim_end().as_bytes()).unwrap();
            let result = match &request.operation {
                Operation::Capabilities(_) => OperationResult::Capabilities(Capabilities {
                    api_version: 1,
                    supported_versions: vec![1],
                    operations: vec!["publish".into(), "status".into()],
                    evidence_variants: vec![],
                    native_contracts: vec![],
                    modes: vec![Mode::Message],
                    policies: vec![PolicyKind::KeepAll],
                    reply_policies: vec![ReplyPolicy::SelfActor],
                    limits: Limits {
                        registration_request_bytes: 524288,
                        core_request_bytes: 524288,
                        response_bytes: 524288,
                        payload_bytes: 65536,
                        metadata_entries: 16,
                        rendered_manifest_bytes: 98304,
                        open_routes: 0,
                        assertions: 0,
                    },
                    grant: ProducerConstraints {
                        allowed_modes: vec![Mode::Message],
                        allowed_policies: vec![PolicyKind::KeepAll],
                        allowed_kinds: vec!["*".into()],
                        reply_policies: vec![ReplyPolicy::SelfActor],
                        native_scopes: vec![],
                        direct_address: true,
                    },
                    features: Features {
                        consume_assertion: false,
                        delivery_scopes: false,
                        host_observations: false,
                    },
                    retention: Retention {
                        durable_keys: true,
                        closed_tombstones: true,
                        credential_independent_identity: true,
                    },
                }),
                Operation::Publish(params) => {
                    observed.lock().unwrap().push(params.clone());
                    arrival.notify_one();
                    if paused {
                        resume.notified().await;
                    }
                    OperationResult::Publish(PublishResult {
                        replayed: false,
                        receipt: receipt(params),
                    })
                }
                _ => panic!("unexpected fixture operation"),
            };
            let mut response = encode_response(&Response::ok(&request.request_id, result)).unwrap();
            response.push(b'\n');
            // Cancellation may close a paused caller before its reply; the fixture owns no external effect.
            let _ = stream.get_mut().write_all(&response).await;
        }
    });
    BridgeFixture {
        published,
        started,
        release,
        task,
    }
}

/// Return exact synthetic custody for the normalized publication, with no native-delivery proof.
fn receipt(params: &PublishParams) -> EventReceipt {
    let Destination::Address { address } = &params.destination else {
        panic!("direct address expected");
    };
    EventReceipt {
        event_id: params.event_id.clone(),
        producer: "agent-run".into(),
        destination: FrozenDestination {
            address: address.clone(),
            route_key: None,
            caller_scope: None,
            destination_scope: None,
        },
        envelope_hash: "a".repeat(64),
        accepted_at_ms: 1,
        acceptance_seq: 1,
        intent_hash: publication_intent_fingerprint(params).unwrap(),
    }
}
