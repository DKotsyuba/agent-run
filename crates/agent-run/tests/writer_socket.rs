//! The broker's private writer socket against a real writer service and a real
//! disposable schema-30 database: kernel-derived authority, the hello contract,
//! frame bounds and the producer lane's replay-safe, ordered submission. Every
//! database, home and socket is private to one test.

use agent_run::transport::writer_socket::serve_listener;
use agent_run_config::config::Config;
use agent_run_core::{
    domain::{AgentId, StartRequest},
    persist::{self, Persist, Producer},
    writer::{WriterConfig, WriterService},
    writer_limits::ConnectionLimits,
    writer_server::WriterEndpoint,
};
use agent_run_domain::storage::SubmitResult;
use agent_run_platform::{fs, process};
use agent_run_store::ops;
use serde_json::json;
use std::{path::PathBuf, time::Duration};
use tokio::sync::oneshot;

/// One running writer service and socket server over a private home.
struct Fixture {
    /// Home directory (kept alive for the test).
    _temp: tempfile::TempDir,
    /// Canonical private home.
    home: PathBuf,
    /// `<home>/writer.sock`.
    socket: PathBuf,
    /// Admitted execution owned by this test process.
    id: AgentId,
    /// The launch capability secret (64 lowercase hex characters).
    capability: String,
    /// Stops the socket server.
    stop: Option<oneshot::Sender<()>>,
    /// The server task; awaited at teardown so its result is observed.
    server: Option<tokio::task::JoinHandle<agent_run::Result<()>>>,
    /// The writer service, shut down at the end of the test.
    service: Option<WriterService>,
}

impl Fixture {
    /// Admits one `starting` execution whose startup owner is THIS process,
    /// then starts the writer service and the socket server.
    async fn new(limits: ConnectionLimits) -> Self {
        Self::with_assembly(limits, None).await
    }

    /// [`Self::new`] with an explicit frame-assembly budget.
    async fn with_assembly(
        limits: ConnectionLimits,
        assembly: Option<agent_run_core::writer_limits::AssemblyBudget>,
    ) -> Self {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().canonicalize().unwrap();
        fs::private_dir(&home).unwrap();
        let binary = if std::path::Path::new("/usr/bin/true").is_file() {
            "/usr/bin/true"
        } else {
            "/bin/true"
        };
        std::fs::write(
            home.join("config.toml"),
            format!(
                "schema_version=1\n[runtimes.mock]\nenabled=true\nadapter='claude'\nbinary={}\nhome={}\nmodels=['fixture']\nlimits_source='none'\n",
                toml::Value::String(binary.into()),
                toml::Value::String(home.join("runtimes/mock").to_string_lossy().into_owned())
            ),
        )
        .unwrap();
        let config = Config::load(&home).unwrap();
        let mut request: StartRequest = serde_json::from_value(json!({
            "runtime": "mock", "model": "fixture", "profile": "review",
            "task": "fixture task", "workdir": home,
        }))
        .unwrap();
        request.validate().unwrap();
        let capability = format!(
            "{}{}",
            uuid::Uuid::new_v4().simple(),
            uuid::Uuid::new_v4().simple()
        );
        let id = {
            let mut store = agent_run_store::fixture::initialize(&home).unwrap();
            let (id, _) = store
                .admit_with_config_revision(
                    &request,
                    &config,
                    "fixture-revision",
                    &json!({"rust_identity_version": 1}),
                    None,
                )
                .unwrap();
            store
                .record_supervisor_launch(&id, &agent_run_core::fs::sha256(capability.as_bytes()))
                .unwrap();
            let me = process::inspect(std::process::id() as i32).unwrap();
            store
                .record_startup_owner(&id, &serde_json::to_value(me).unwrap())
                .unwrap();
            id
        };
        let service = WriterService::start(&home, WriterConfig::default()).unwrap();
        let socket = home.join("writer.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let mut endpoint = WriterEndpoint::new(
            service.handle(),
            process::inspect(std::process::id() as i32).unwrap(),
            &home,
        );
        endpoint.limits = limits;
        if let Some(assembly) = assembly {
            endpoint.assembly = assembly;
        }
        let (stop, stopped) = oneshot::channel::<()>();
        let server = tokio::spawn(async move {
            serve_listener(listener, endpoint, async {
                let _ = stopped.await;
            })
            .await
        });
        Self {
            _temp: temp,
            home,
            socket,
            id,
            capability,
            stop: Some(stop),
            server: Some(server),
            service: Some(service),
        }
    }

    /// A supervisor producer claiming `capability`.
    fn producer(&self, capability: &str) -> Producer {
        Producer::connect(
            &self.socket,
            persist::supervisor_hello(&self.id, &self.id, capability.to_owned()),
            persist::supervisor_producer_id(&self.id),
        )
    }

    /// Shuts the server and the writer down and proves nothing leaked: the
    /// serve loop returned success, the listener is gone, the writer thread was
    /// joined and its lease is free for a new service.
    async fn finish(mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(server) = self.server.take() {
            server
                .await
                .expect("serve task did not panic")
                .expect("serve loop ended cleanly");
        }
        assert!(
            tokio::net::UnixStream::connect(&self.socket).await.is_err(),
            "the listener must be closed after shutdown"
        );
        if let Some(service) = self.service.take() {
            let (boot_id, now) = agent_run_platform::monotonic::now().unwrap();
            service
                .shutdown(agent_run_domain::storage::MonoDeadline {
                    boot_id,
                    ns: now + 5_000_000_000,
                })
                .await
                .expect("writer drained and joined");
        }
        // A clean shutdown released the home-wide lease, so a fresh service
        // starts at once.
        let again = WriterService::start(&self.home, WriterConfig::default())
            .expect("the writer lease was released");
        let (boot_id, now) = agent_run_platform::monotonic::now().unwrap();
        again
            .shutdown(agent_run_domain::storage::MonoDeadline {
                boot_id,
                ns: now + 5_000_000_000,
            })
            .await
            .unwrap();
    }
}

/// The supervisor identity is the kernel's, the capability is the hello's: a
/// correct pair commits ownership and journal rows, which a read-only handle
/// then sees; the same operation is applied once.
#[tokio::test]
async fn supervisor_owns_and_journals_through_the_socket() {
    let fixture = Fixture::new(ConnectionLimits::default()).await;
    let producer = fixture.producer(&fixture.capability);
    let me = process::inspect(std::process::id() as i32).unwrap();
    producer
        .call(
            ops::RecordSupervisorOwner {
                execution: fixture.id.clone(),
                pid: me.pid,
                identity: me.token.clone(),
                process_group_id: None,
                birth_time: Some(me.birth),
                at: agent_run_core::domain::now(),
            },
            Duration::from_secs(10),
        )
        .await
        .expect("owner committed");
    let mut persist = Persist::open(&fixture.home, &fixture.id, producer).unwrap();
    persist
        .event_durable(&fixture.id, "fixture_event", &json!({"n": 1}))
        .await
        .expect("journal committed");
    let count: i64 = persist
        .conn
        .query_row(
            "SELECT COUNT(*) FROM events WHERE agent_id=? AND kind='fixture_event'",
            [fixture.id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 1);
    let owner: Option<i64> = persist
        .conn
        .query_row(
            "SELECT supervisor_pid FROM agents WHERE id=?",
            [fixture.id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(owner, Some(i64::from(me.pid)));
    drop(persist);
    fixture.finish().await;
}

/// A wrong capability never gets a hello acknowledgement, so nothing it could
/// submit reaches the writer: the producer ends without a committed receipt and
/// mutates nothing.
#[tokio::test]
async fn wrong_capability_is_refused_at_the_hello() {
    let fixture = Fixture::new(ConnectionLimits::default()).await;
    let wrong = "0".repeat(64);
    let producer = fixture.producer(&wrong);
    let me = process::inspect(std::process::id() as i32).unwrap();
    let outcome = producer
        .call_raw(
            ops::RecordSupervisorOwner {
                execution: fixture.id.clone(),
                pid: me.pid,
                identity: me.token.clone(),
                process_group_id: None,
                birth_time: Some(me.birth),
                at: agent_run_core::domain::now(),
            },
            Duration::from_secs(2),
        )
        .await;
    assert!(
        !matches!(outcome, SubmitResult::Committed(_)),
        "a refused hello must not commit: {outcome:?}"
    );
    let reader = agent_run_store::writer::StoreReader::open(&fixture.home).unwrap();
    let owner: Option<i64> = reader
        .conn()
        .query_row(
            "SELECT supervisor_pid FROM agents WHERE id=?",
            [fixture.id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(owner, None, "a refused authority must not mutate");
    drop(reader);
    fixture.finish().await;
}

/// Shape-valid but unverifiable owner claims (well-formed 64-hex capability for
/// a real execution, a wrong capability, an unknown execution, a stale identity)
/// are refused at the hello without ever taking a reserved owner slot, so
/// repeated abuse cannot deny the real owner, who still connects.
#[tokio::test]
async fn unverified_owner_claims_never_hold_owner_slots() {
    use agent_run_core::{
        storage_client::Hello,
        storage_wire::{self as wire, Message},
    };
    use tokio::net::UnixStream;

    let limits = ConnectionLimits {
        preauth: 4,
        owner: 2,
        operator: 2,
    };
    let fixture = Fixture::new(limits).await;
    let claim = |execution: &str, capability: &str| Hello {
        protocol: agent_run_domain::storage::WRITE_PROTOCOL,
        role: "supervisor".into(),
        logical: Some(fixture.id.to_string()),
        execution: Some(execution.to_owned()),
        attempt: None,
        generation: None,
        capability_hex: Some(capability.to_owned()),
    };
    let unknown = "ag-20260101-000000-ffffffffff";
    let fakes = [
        claim(fixture.id.as_str(), &"0".repeat(64)),
        claim(fixture.id.as_str(), &"f".repeat(64)),
        claim(unknown, &fixture.capability),
        claim(unknown, &"a".repeat(64)),
    ];
    // Many rounds, every fake kept open to the end: none may be acknowledged.
    let mut held = Vec::new();
    for round in 0..4 {
        for (index, hello) in fakes.iter().enumerate() {
            let mut stream = UnixStream::connect(&fixture.socket).await.unwrap();
            wire::write_frame(
                &mut stream,
                Message::Hello,
                0,
                1,
                &serde_json::to_vec(hello).unwrap(),
            )
            .await
            .unwrap();
            let answer = tokio::time::timeout(
                Duration::from_secs(5),
                wire::read_header(&mut stream, |m, _| {
                    (m == Message::HelloOk).then_some(wire::HELLO_MAX)
                }),
            )
            .await
            .unwrap_or_else(|_| panic!("round {round} claim {index}: refusal must be prompt"));
            assert!(
                !matches!(answer, Ok(Some(_))),
                "round {round} claim {index}: an unverified claim was acknowledged"
            );
            held.push(stream);
        }
    }
    // The real owner still gets a slot and commits ownership.
    let me = process::inspect(std::process::id() as i32).unwrap();
    fixture
        .producer(&fixture.capability)
        .call(
            ops::RecordSupervisorOwner {
                execution: fixture.id.clone(),
                pid: me.pid,
                identity: me.token.clone(),
                process_group_id: None,
                birth_time: Some(me.birth),
                at: agent_run_core::domain::now(),
            },
            Duration::from_secs(10),
        )
        .await
        .expect("the real owner connects under abuse");
    drop(held);
    fixture.finish().await;
}

/// An operation outside the authenticated authority's matrix is refused by the
/// writer even though it decodes: a supervisor cannot register an account.
#[tokio::test]
async fn supervisor_cannot_submit_operator_operations() {
    let fixture = Fixture::new(ConnectionLimits::default()).await;
    let producer = fixture.producer(&fixture.capability);
    let outcome = producer
        .call_raw(
            ops::DisableAccount {
                account: "forged".parse().unwrap(),
            },
            Duration::from_secs(10),
        )
        .await;
    match outcome {
        SubmitResult::Rejected(rejection) => assert!(
            matches!(
                rejection.code,
                agent_run_domain::storage::RejectCode::Unauthorized
                    | agent_run_domain::storage::RejectCode::StaleAuthority
            ),
            "{rejection:?}"
        ),
        other => panic!("unexpected {other:?}"),
    }
    fixture.finish().await;
}

/// A command-line process is authenticated by its kernel identity alone: it can
/// register and disable an account through the socket, while an execution-scoped
/// operation of the same process is refused by the authority matrix.
#[tokio::test]
async fn cli_manages_accounts_through_the_socket_and_nothing_else() {
    use agent_run_domain::catalog::{AccountId, AccountStatus, AuthFamily};
    let fixture = Fixture::new(ConnectionLimits::default()).await;
    let cli = Producer::connect(
        &fixture.socket,
        persist::cli_hello(),
        persist::cli_producer_id(),
    );
    let record = agent_run_domain::catalog::AccountRecord {
        account_id: "socket-account".parse::<AccountId>().unwrap(),
        auth_family: "anthropic".parse::<AuthFamily>().unwrap(),
        secret_ref: "env:SOCKET_ACCOUNT_KEY".parse().unwrap(),
        status: AccountStatus::Enabled,
    };
    cli.call(
        ops::RegisterAccount {
            record: record.clone(),
        },
        Duration::from_secs(10),
    )
    .await
    .expect("account registered");
    let reader = agent_run_store::writer::StoreReader::open(&fixture.home).unwrap();
    assert_eq!(
        reader
            .list_accounts()
            .unwrap()
            .iter()
            .filter(|a| a.account_id == record.account_id && a.status == AccountStatus::Enabled)
            .count(),
        1
    );
    cli.call(
        ops::DisableAccount {
            account: record.account_id.clone(),
        },
        Duration::from_secs(10),
    )
    .await
    .expect("account disabled");
    assert!(
        reader
            .list_accounts()
            .unwrap()
            .iter()
            .any(|a| a.account_id == record.account_id && a.status == AccountStatus::Disabled)
    );
    // A supervisor-scoped row is outside a command-line process's authority.
    let refused = cli
        .call_raw(
            ops::AppendJournal {
                execution: fixture.id.clone(),
                fragments: vec![],
            },
            Duration::from_secs(5),
        )
        .await;
    assert!(
        !matches!(refused, SubmitResult::Committed(_)),
        "{refused:?}"
    );
    drop(reader);
    fixture.finish().await;
}

/// A forged opcode alone never reaches the protected critical reserve: a
/// command-line connection announcing either an unauthorized critical row or
/// the permitted command opcode (whose body could be ordinary steer), with a
/// body that does not fit ordinary capacity, is refused at once
/// (`BytesFull`) instead of holding protected assembly while it stalls, and a
/// supervisor whose recorded startup identity was revoked after hello is also
/// refused before reading its stalled body. Restoring the exact identity lets
/// the real supervisor commit ownership.
#[tokio::test]
async fn a_forged_critical_opcode_cannot_take_the_protected_reserve() {
    use agent_run_core::{
        storage_client::Hello,
        storage_wire::{self as wire, Message},
    };
    use agent_run_domain::storage::NotEnqueuedReason;
    use tokio::{io::AsyncWriteExt, net::UnixStream};

    // 200 KB total, 100 KB of it only critical frames may use: ordinary frames
    // may take at most 100 KB.
    let assembly = agent_run_core::writer_limits::AssemblyBudget::new(200_000, 100_000);
    let fixture = Fixture::with_assembly(ConnectionLimits::default(), Some(assembly)).await;
    let hello = Hello {
        protocol: agent_run_domain::storage::WRITE_PROTOCOL,
        role: "cli".into(),
        logical: None,
        execution: None,
        attempt: None,
        generation: None,
        capability_hex: None,
    };
    let me = process::inspect(std::process::id() as i32).unwrap();
    let broker = agent_run_core::broker_writer::BrokerWriter::new(
        fixture.service.as_ref().unwrap().handle(),
    )
    .lane("fixture-owner");
    for (kind, revoke) in [
        (ops::OpKindRef::FinishTerminal, false),
        (ops::OpKindRef::EnqueueCommand, false),
        (ops::OpKindRef::RememberProcesses, true),
    ] {
        let hello = if revoke {
            persist::supervisor_hello(&fixture.id, &fixture.id, fixture.capability.clone())
        } else {
            hello.clone()
        };
        let mut stream = UnixStream::connect(&fixture.socket).await.unwrap();
        wire::write_frame(
            &mut stream,
            Message::Hello,
            0,
            1,
            &serde_json::to_vec(&hello).unwrap(),
        )
        .await
        .unwrap();
        let ack = wire::read_header(&mut stream, |m, _| {
            (m == Message::HelloOk).then_some(wire::HELLO_MAX)
        })
        .await
        .unwrap()
        .expect("valid hello acknowledged");
        wire::read_body(&mut stream, &ack).await.unwrap();
        if revoke {
            broker
                .local(
                    ops::RecordStartupOwner {
                        execution: fixture.id.clone(),
                        identity: json!({"pid":me.pid,"birth":me.birth,"token":"revoked"}),
                    },
                    Duration::from_secs(5),
                )
                .await
                .expect("recorded owner revoked after hello");
        }
        // A header without current owner authority must not reserve 150 KB.
        let forged = kind.wire_id();
        let header = wire::encode_header(&wire::Header {
            message: Message::Request,
            op_kind: forged,
            payload_len: 150_000,
            seq: 2,
        });
        stream.write_all(&header).await.unwrap();
        let reply = tokio::time::timeout(
            Duration::from_secs(3),
            wire::read_header(&mut stream, |m, _| {
                (m == Message::Reply).then_some(wire::REPLY_MAX)
            }),
        )
        .await
        .expect("the forged header is refused promptly, not left stalling")
        .unwrap()
        .expect("a refusal reply");
        let body = wire::read_body(&mut stream, &reply).await.unwrap();
        let refusal: SubmitResult = serde_json::from_slice(&body).unwrap();
        assert!(
            matches!(
                refusal,
                SubmitResult::NotEnqueued(NotEnqueuedReason::BytesFull)
            ),
            "{refusal:?}"
        );
        drop(stream);
        if revoke {
            broker
                .local(
                    ops::RecordStartupOwner {
                        execution: fixture.id.clone(),
                        identity: serde_json::to_value(&me).unwrap(),
                    },
                    Duration::from_secs(5),
                )
                .await
                .expect("exact owner restored");
        }
    }
    // The supervisor's real critical operation is unaffected.
    fixture
        .producer(&fixture.capability)
        .call(
            ops::RecordSupervisorOwner {
                execution: fixture.id.clone(),
                pid: me.pid,
                identity: me.token.clone(),
                process_group_id: None,
                birth_time: Some(me.birth),
                at: agent_run_core::domain::now(),
            },
            Duration::from_secs(10),
        )
        .await
        .expect("a real critical operation still commits");
    fixture.finish().await;
}

/// One operation envelope applied at most once: the identical frame resent on
/// the same connection, and again on a NEW connection after the acknowledgement
/// was lost (the first reply was never read), is answered from the stored
/// receipt with the same identity, digest and operation kind, `replayed` set,
/// and the journal gains exactly one row.
#[tokio::test]
async fn identical_envelope_is_applied_once_across_a_lost_acknowledgement() {
    use agent_run_core::{
        storage_client::{Hello, HelloOk},
        storage_wire::{self as wire, Message},
    };
    use agent_run_domain::storage::{MonoDeadline, OperationId, ProducerId, ProducerKind};
    use tokio::net::UnixStream;

    let fixture = Fixture::new(ConnectionLimits::default()).await;
    let me = process::inspect(std::process::id() as i32).unwrap();
    // Ownership first, through the ordinary lane.
    fixture
        .producer(&fixture.capability)
        .call(
            ops::RecordSupervisorOwner {
                execution: fixture.id.clone(),
                pid: me.pid,
                identity: me.token.clone(),
                process_group_id: None,
                birth_time: Some(me.birth),
                at: agent_run_core::domain::now(),
            },
            Duration::from_secs(10),
        )
        .await
        .expect("owner committed");
    // One fixed envelope: operation id, producer, ordinal and deadline never change.
    let (boot_id, now) = agent_run_platform::monotonic::now().unwrap();
    let envelope = ops::WriteEnvelope::new(
        OperationId::new(&boot_id, now),
        ProducerId {
            kind: ProducerKind::Supervisor,
            scope: Some(fixture.id.to_string()),
            instance: "replay-instance".into(),
        },
        1,
        MonoDeadline {
            boot_id,
            ns: now + 30_000_000_000,
        },
        ops::RemoteOp::from(ops::AppendJournal {
            execution: fixture.id.clone(),
            fragments: vec![ops::JournalFragment::Event {
                kind: "replay_event".into(),
                data: json!({"n": 1}),
                attempt: None,
            }],
        }),
    )
    .unwrap();
    let (kind, body) = envelope.to_wire();
    let body = body.to_vec();
    let hello = Hello {
        protocol: agent_run_domain::storage::WRITE_PROTOCOL,
        role: "supervisor".into(),
        logical: Some(fixture.id.to_string()),
        execution: Some(fixture.id.to_string()),
        attempt: None,
        generation: None,
        capability_hex: Some(fixture.capability.clone()),
    };
    /// Connects to the writer socket, introduces itself with `hello` and returns
    /// the acknowledged stream.
    async fn connect(socket: &std::path::Path, hello: &Hello) -> UnixStream {
        let mut stream = UnixStream::connect(socket).await.unwrap();
        wire::write_frame(
            &mut stream,
            Message::Hello,
            0,
            1,
            &serde_json::to_vec(hello).unwrap(),
        )
        .await
        .unwrap();
        let header = wire::read_header(&mut stream, |m, _| {
            (m == Message::HelloOk).then_some(wire::HELLO_MAX)
        })
        .await
        .unwrap()
        .expect("hello acknowledged");
        let ok: HelloOk =
            serde_json::from_slice(&wire::read_body(&mut stream, &header).await.unwrap()).unwrap();
        assert_eq!(
            ok.boot_id,
            agent_run_platform::monotonic::boot_id().unwrap()
        );
        stream
    }
    /// Sends one request frame (`kind`, `seq`, `body`) and reads its reply.
    async fn exchange(stream: &mut UnixStream, kind: u16, seq: u64, body: &[u8]) -> SubmitResult {
        wire::write_frame(stream, Message::Request, kind, seq, body)
            .await
            .unwrap();
        let header = wire::read_header(stream, |m, _| {
            (m == Message::Reply).then_some(wire::REPLY_MAX)
        })
        .await
        .unwrap()
        .expect("reply");
        assert_eq!(header.seq, seq);
        serde_json::from_slice(&wire::read_body(stream, &header).await.unwrap()).unwrap()
    }
    let count = |home: &std::path::Path| -> i64 {
        agent_run_store::writer::StoreReader::open(home)
            .unwrap()
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM events WHERE agent_id=? AND kind='replay_event'",
                [fixture.id.as_str()],
                |row| row.get(0),
            )
            .unwrap()
    };

    let mut first_connection = connect(&fixture.socket, &hello).await;
    let SubmitResult::Committed(first) = exchange(&mut first_connection, kind, 2, &body).await
    else {
        panic!("first submission must commit");
    };
    assert!(!first.replayed);
    assert_eq!(count(&fixture.home), 1);
    // Same connection, identical frame: the stored receipt, no second row.
    let SubmitResult::Committed(second) = exchange(&mut first_connection, kind, 3, &body).await
    else {
        panic!("replay must be answered from the receipt");
    };
    assert!(second.replayed);
    assert_eq!(second.operation_id, first.operation_id);
    assert_eq!(second.digest, first.digest);
    assert_eq!(second.op_kind, first.op_kind);
    assert_eq!(second.commit_seq, first.commit_seq);
    assert_eq!(count(&fixture.home), 1);
    drop(first_connection);

    // Lost acknowledgement: send, never read the reply, drop the connection.
    // A second envelope is applied once even though its reply was lost.
    let (boot_id, now) = agent_run_platform::monotonic::now().unwrap();
    let lost = ops::WriteEnvelope::new(
        OperationId::new(&boot_id, now),
        ProducerId {
            kind: ProducerKind::Supervisor,
            scope: Some(fixture.id.to_string()),
            instance: "replay-instance".into(),
        },
        2,
        MonoDeadline {
            boot_id,
            ns: now + 30_000_000_000,
        },
        ops::RemoteOp::from(ops::AppendJournal {
            execution: fixture.id.clone(),
            fragments: vec![ops::JournalFragment::Event {
                kind: "replay_event".into(),
                data: json!({"n": 2}),
                attempt: None,
            }],
        }),
    )
    .unwrap();
    let (lost_kind, lost_body) = lost.to_wire();
    let lost_body = lost_body.to_vec();
    let mut doomed = connect(&fixture.socket, &hello).await;
    wire::write_frame(&mut doomed, Message::Request, lost_kind, 2, &lost_body)
        .await
        .unwrap();
    drop(doomed);
    // Wait until the writer has committed it (the reply went nowhere).
    for _ in 0..200 {
        if count(&fixture.home) == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(
        count(&fixture.home),
        2,
        "the unacknowledged write committed"
    );
    let mut retry = connect(&fixture.socket, &hello).await;
    let SubmitResult::Committed(answered) = exchange(&mut retry, lost_kind, 2, &lost_body).await
    else {
        panic!("the retry must be answered from the receipt");
    };
    assert!(answered.replayed);
    assert_eq!(count(&fixture.home), 2, "the retry applied nothing again");
    drop(retry);
    fixture.finish().await;
}
