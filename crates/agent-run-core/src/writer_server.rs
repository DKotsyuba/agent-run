//! The server side of one writer-socket connection.
//!
//! Every process other than the resident broker (detached supervisors,
//! managed-service executors, CLI commands, capacity collectors) reaches the
//! one authoritative SQLite writer through a private Unix socket. This module
//! is the bounded, fail-closed adapter between one such connection and the
//! resident [`crate::writer::WriterHandle`]; the broker's accept loop (kernel peer identity,
//! socket binding, shutdown) lives in the `agent-run` transport and calls
//! [`crate::writer_server::serve_connection`] with the kernel-attested [`crate::writer_server::PeerIdentity`].
//!
//! * identity comes from the kernel, never from the peer: the hello's role,
//!   execution, attempt and capability are only claims that select which
//!   [`agent_run_domain::storage::WriteAuthority`] is built, and the writer re-checks the recorded
//!   capability hash and process identity inside every write transaction;
//! * capacity is bounded before anything is read: connection slots per
//!   authority class (a pre-authentication pool, a reserved owner pool that
//!   operators can never consume, an operator pool) and the byte budget of the
//!   frame being assembled, both released on drop ([`crate::writer_limits`]);
//! * a frame is refused from its 24-byte header when its kind is not a
//!   wire-decodable operation or its body would exceed that operation's bound,
//!   and a request is handed to the writer only after its whole body was
//!   received, verified and its digest recomputed;
//! * a request frame never carries authority, priority or class: the class is
//!   computed from the verified operation and the authority from the
//!   connection;
//! * replies carry the writer's outcome verbatim (committed receipt, durable or
//!   non-durable rejection, not enqueued, unknown), bounded to 72 KiB;
//! * a verified supervisor hello may carry its pinned attempt's finish
//!   launch-secret context, which is installed in the writer handle's memory
//!   for exactly as long as this connection keeps that claim; no other role
//!   or unverified claim installs one.
//!
//! This module never executes SQL, never opens the database for writing and
//! never holds a statement across an await.

use crate::{
    domain::AgentId,
    storage_client::{Hello, HelloOk},
    storage_wire::{self as wire, Message},
    writer::WriterHandle,
    writer_limits::{AssemblyBudget, ConnectionLimits, ConnectionSlots, SlotClass, SlotPermit},
};
use agent_run_domain::storage::{
    MAX_REPLY_BYTES, OpStatus, OperationId, PayloadDigest, SubmitResult, UnknownCause,
    WRITE_PROTOCOL, WriteAuthority, WriteClass,
};
use agent_run_platform::{monotonic, process::Identity};
use agent_run_store::ops::{OpKindRef, WriteEnvelope};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::io::{AsyncRead, AsyncWrite, ReadHalf, WriteHalf};

/// Longest a new socket may take to deliver a complete hello.
const HELLO_DEADLINE: Duration = Duration::from_secs(5);
/// Longest the body of an announced frame may take to arrive.
const BODY_DEADLINE: Duration = Duration::from_secs(30);
/// Longest the body of a frame admitted from the protected critical reserve may
/// take to arrive.
const PROTECTED_BODY_DEADLINE: Duration = Duration::from_secs(5);
/// Longest a reply may take to be written to a live peer.
const REPLY_DEADLINE: Duration = Duration::from_secs(15);
/// Longest an operator connection may idle between frames; owners never idle out.
const OPERATOR_IDLE: Duration = Duration::from_secs(60);

/// Kernel-attested identity of the process on the other end of a connection.
#[derive(Debug, Clone)]
pub struct PeerIdentity {
    /// Effective user id of the peer, equal to this process's user.
    pub uid: u32,
    /// Live identity (pid, birth, token, group) of the peer process.
    pub process: Identity,
}

/// Everything a connection needs from the broker.
#[derive(Clone)]
pub struct WriterEndpoint {
    /// The resident writer.
    pub handle: WriterHandle,
    /// Live identity of this broker process: the recorded launch root of every
    /// service generation it starts.
    pub broker: Identity,
    /// Connection pool sizes.
    pub limits: ConnectionLimits,
    /// Frame-assembly byte budget.
    pub assembly: AssemblyBudget,
    /// Home whose read-only store verifies a process owner's recorded
    /// capability and identity before it may take a reserved owner slot.
    pub home: PathBuf,
}

impl WriterEndpoint {
    /// An endpoint with the provisional default pools and budget, verifying
    /// owners against the store of `home`.
    pub fn new(handle: WriterHandle, broker: Identity, home: &Path) -> Self {
        Self {
            handle,
            broker,
            home: home.to_path_buf(),
            limits: ConnectionLimits::default(),
            assembly: AssemblyBudget::new(
                AssemblyBudget::DEFAULT_TOTAL,
                AssemblyBudget::DEFAULT_CRITICAL_RESERVE,
            ),
        }
    }
}

/// Why a hello was refused; always closes the connection without a reply.
#[derive(Debug)]
enum Refusal {
    /// The peer is not a same-user live process, or its hello is invalid.
    Identity,
    /// The pool for the claimed class is exhausted.
    Capacity,
}

/// One authenticated connection: its authority template and capacity permit.
struct Session {
    /// Authority built from the hello and the kernel identity; only the pinned
    /// attempt of a supervisor changes, and only through a re-hello.
    authority: WriteAuthority,
    /// Capacity class of the connection.
    class: SlotClass,
    /// Held until the connection ends.
    _permit: SlotPermit,
}

/// Serves one connection from its hello to its close.
///
/// `peer` is the kernel-attested identity of the other end; `preauth` is the
/// pre-authentication slot already taken for this socket, released only after
/// the verified-class slot was obtained. Any protocol violation, identity
/// mismatch or exhausted pool closes the connection without a reply.
/// Protected frame bytes require a currently recorded owner, checked again
/// after the header; operator and revoked-owner frames use ordinary capacity
/// or receive `BytesFull` before their body is read.
///
/// # Errors
/// The transport failed, a deadline passed or a frame was malformed; the
/// connection is then closed. A request whose body was completely received
/// before the failure may still have been applied: its acknowledgement can be
/// lost, and the producer resolves that by resubmitting the identical
/// operation, which the writer answers from the stored receipt.
pub async fn serve_connection<S>(
    stream: S,
    peer: PeerIdentity,
    endpoint: Arc<WriterEndpoint>,
    slots: ConnectionSlots,
    preauth: SlotPermit,
) -> std::io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (mut input, mut output) = tokio::io::split(stream);
    // Hello: bounded in time and size, before any other frame is considered.
    let Some(header) = tokio::time::timeout(
        HELLO_DEADLINE,
        wire::read_header(&mut input, |message, _| {
            (message == Message::Hello).then_some(wire::HELLO_MAX)
        }),
    )
    .await
    .map_err(|_| std::io::ErrorKind::TimedOut)?
    .map_err(frame_io)?
    else {
        return Ok(());
    };
    let body = tokio::time::timeout(HELLO_DEADLINE, wire::read_body(&mut input, &header))
        .await
        .map_err(|_| std::io::ErrorKind::TimedOut)?
        .map_err(frame_io)?;
    let hello: Hello = serde_json::from_slice(&body)
        .map_err(|_| std::io::Error::from(std::io::ErrorKind::InvalidData))?;
    // Shape and kernel identity first; the reserved owner slot is taken only
    // after the recorded capability and process identity were verified against
    // the store, so an unverified peer never occupies protected capacity.
    let (authority, class) = match claim(&hello, &peer, &endpoint) {
        Ok(claimed) => claimed,
        Err(_) => return Ok(()),
    };
    if class == SlotClass::Owner && !verified_owner(&endpoint, &authority).await {
        return Ok(());
    }
    let mut session = match admit(authority, class, &slots) {
        Ok(session) => session,
        Err(_) => return Ok(()),
    };
    // A verified supervisor's launch-secret context lives exactly as long as
    // this connection and its current attempt claim.
    let mut guard = GuardLease::new(endpoint.handle.clone());
    guard.update(&session.authority, hello.finish_guard.as_ref());
    // The verified-class slot is held before the pre-auth slot is released.
    drop(preauth);
    let ok = HelloOk {
        epoch: endpoint.handle.health().epoch,
        boot_id: monotonic::boot_id()?,
        mono_ns: monotonic::now_ns(),
    };
    reply(
        &mut output,
        Message::HelloOk,
        header.seq,
        &serde_json::to_vec(&ok)?,
    )
    .await?;
    loop {
        let idle = match session.class {
            SlotClass::Owner => None,
            SlotClass::Operator => Some(OPERATOR_IDLE),
        };
        let next = wire::read_header(&mut input, |message, kind| match message {
            Message::Hello => Some(wire::HELLO_MAX),
            Message::Status => Some(wire::STATUS_MAX),
            Message::Request => OpKindRef::from_wire_id(kind)
                .filter(|kind| kind.is_remote())
                .map(OpKindRef::wire_max),
            _ => None,
        });
        let header = match idle {
            Some(limit) => match tokio::time::timeout(limit, next).await {
                Ok(result) => result,
                Err(_) => return Ok(()),
            },
            None => next.await,
        };
        let Some(header) = header.map_err(frame_io)? else {
            return Ok(());
        };
        match header.message {
            Message::Hello => {
                let body = read_body(&mut input, &header, BODY_DEADLINE).await?;
                let hello: Hello = serde_json::from_slice(&body)
                    .map_err(|_| std::io::Error::from(std::io::ErrorKind::InvalidData))?;
                // Only the attempt claim of a supervisor may move; anything else
                // changing mid-connection is a new identity and is refused.
                let Some(authority) = repin(&session.authority, &hello) else {
                    return Ok(());
                };
                session.authority = authority;
                guard.update(&session.authority, hello.finish_guard.as_ref());
                let ok = HelloOk {
                    epoch: endpoint.handle.health().epoch,
                    boot_id: monotonic::boot_id()?,
                    mono_ns: monotonic::now_ns(),
                };
                reply(
                    &mut output,
                    Message::HelloOk,
                    header.seq,
                    &serde_json::to_vec(&ok)?,
                )
                .await?;
            }
            Message::Status => {
                let body = read_body(&mut input, &header, BODY_DEADLINE).await?;
                let status = status(&endpoint, &session.authority, &body).await;
                let bytes = bounded(&status).unwrap_or_else(|| b"{\"NotFound\":null}".to_vec());
                reply(&mut output, Message::StatusReply, header.seq, &bytes).await?;
            }
            Message::Request => {
                let kind = OpKindRef::from_wire_id(header.op_kind)
                    .expect("read_header accepted only known remote kinds");
                // Reserve the frame's memory BEFORE reading a single payload byte.
                // Ordinary capacity first. The protected critical reserve is only
                // a fallback for a recorded owner and a row it may submit.
                // Operators cannot prove cancel versus steer from a header, so
                // even their permitted command opcode uses ordinary capacity.
                // Revalidate owner identity before spending protected bytes;
                // an earlier hello is not authority after revocation. A frame that
                // needed it must arrive within a short deadline, so a stalled
                // header releases the protected bytes quickly.
                let len = header.payload_len as usize;
                let permitted_critical = session.class == SlotClass::Owner
                    && kind.class() == WriteClass::Critical
                    && kind.permits(&session.authority);
                let reserved = match endpoint.assembly.reserve(len, false) {
                    Some(permit) => Some((permit, BODY_DEADLINE)),
                    None if permitted_critical
                        && verified_owner(&endpoint, &session.authority).await =>
                    {
                        endpoint
                            .assembly
                            .reserve(len, true)
                            .map(|permit| (permit, PROTECTED_BODY_DEADLINE))
                    }
                    None => None,
                };
                let Some((_assembly, deadline)) = reserved else {
                    // Out of assembly memory: refuse without reading and close,
                    // because the unread body cannot be skipped safely.
                    let refusal = SubmitResult::NotEnqueued(
                        agent_run_domain::storage::NotEnqueuedReason::BytesFull,
                    );
                    let _ = reply(
                        &mut output,
                        Message::Reply,
                        header.seq,
                        &serde_json::to_vec(&refusal)?,
                    )
                    .await;
                    return Ok(());
                };
                let body = read_body(&mut input, &header, deadline).await?;
                let result = match WriteEnvelope::from_wire(header.op_kind, &body) {
                    Err(rejection) => SubmitResult::Rejected(rejection),
                    Ok(envelope) => {
                        drop(body);
                        endpoint
                            .handle
                            .submit(session.authority.clone(), envelope)
                            .await
                    }
                };
                let bytes = bounded(&result).unwrap_or_else(|| {
                    // An unboundable reply cannot be delivered; the operation
                    // may have committed, so the honest answer is "unknown".
                    serde_json::to_vec(&SubmitResult::Unknown(UnknownCause::Transport))
                        .expect("static reply")
                });
                reply(&mut output, Message::Reply, header.seq, &bytes).await?;
            }
            _ => return Ok(()),
        }
    }
}

/// Builds the connection's authority claim from the hello and the kernel
/// identity.
///
/// A claimed role that the peer cannot satisfy, malformed identifiers or a
/// protocol mismatch refuse the connection. The capability secret is hashed
/// here and never retained or logged. Nothing here proves the claim against
/// recorded facts: see [`verified_owner`].
fn claim(
    hello: &Hello,
    peer: &PeerIdentity,
    endpoint: &WriterEndpoint,
) -> std::result::Result<(WriteAuthority, SlotClass), Refusal> {
    if hello.protocol != WRITE_PROTOCOL {
        return Err(Refusal::Identity);
    }
    let process = &peer.process;
    let (authority, class) = match hello.role.as_str() {
        "supervisor" => {
            let logical = parse_agent(hello.logical.as_deref())?;
            let execution = parse_agent(hello.execution.as_deref())?;
            let capability = capability_hash(hello.capability_hex.as_deref())?;
            (
                WriteAuthority::Supervisor {
                    logical,
                    execution,
                    attempt: hello.attempt.clone(),
                    capability_sha256: capability,
                    pid: process.pid,
                    birth: process.birth,
                    token: process.token.clone(),
                },
                SlotClass::Owner,
            )
        }
        "service_exec" => {
            let generation = hello
                .generation
                .clone()
                .filter(|generation| !generation.is_empty() && generation.len() <= 128)
                .ok_or(Refusal::Identity)?;
            let capability = capability_hash(hello.capability_hex.as_deref())?;
            (
                WriteAuthority::ServiceExec {
                    generation,
                    capability_sha256: capability,
                    pid: process.pid,
                    birth: process.birth,
                    token: process.token.clone(),
                    root_pid: endpoint.broker.pid,
                    root_birth: endpoint.broker.birth,
                    root_token: endpoint.broker.token.clone(),
                },
                SlotClass::Owner,
            )
        }
        "cli" => (
            WriteAuthority::Cli {
                uid: peer.uid,
                pid: process.pid,
                birth: process.birth,
                token: process.token.clone(),
            },
            SlotClass::Operator,
        ),
        "collector" => (
            WriteAuthority::Collector {
                uid: peer.uid,
                pid: process.pid,
                birth: process.birth,
                token: process.token.clone(),
            },
            SlotClass::Operator,
        ),
        _ => return Err(Refusal::Identity),
    };
    Ok((authority, class))
}

/// Takes the connection's verified-class slot.
fn admit(
    authority: WriteAuthority,
    class: SlotClass,
    slots: &ConnectionSlots,
) -> std::result::Result<Session, Refusal> {
    let permit = slots.verified(class).ok_or(Refusal::Capacity)?;
    Ok(Session {
        authority,
        class,
        _permit: permit,
    })
}

/// Whether the recorded facts of the store confirm a process owner's claim:
/// the execution's (or service generation's) recorded capability hash, lineage
/// root or launch root, and the recorded startup owner, supervisor or
/// executor identity must equal the claimed capability and the kernel-attested
/// peer, with exact birth. Read-only and off the executor; any failure to
/// verify refuses the peer, which then never leaves the bounded pre-auth pool.
async fn verified_owner(endpoint: &WriterEndpoint, authority: &WriteAuthority) -> bool {
    let (home, authority) = (endpoint.home.clone(), authority.clone());
    tokio::task::spawn_blocking(move || {
        agent_run_store::writer::StoreReader::open_with(&home, Duration::from_millis(250))
            .and_then(|reader| owner_matches(&reader.conn, &authority))
            .unwrap_or(false)
    })
    .await
    .unwrap_or(false)
}

/// Compares a process owner's claim with the recorded rows (see
/// [`verified_owner`]); mirrors the writer's in-transaction scope predicates.
fn owner_matches(conn: &rusqlite::Connection, authority: &WriteAuthority) -> crate::Result<bool> {
    use rusqlite::OptionalExtension;
    use serde_json::Value;
    match authority {
        WriteAuthority::Supervisor {
            logical,
            execution,
            capability_sha256,
            pid,
            birth,
            token,
            ..
        } => {
            type Row = (
                Option<String>,
                String,
                Option<i32>,
                Option<String>,
                Option<f64>,
                Option<String>,
                Option<f64>,
            );
            let row: Option<Row> = conn
                .query_row(
                    "SELECT supervisor_capability_sha256,CASE WHEN root_agent_id='' THEN id ELSE root_agent_id END,\
                     supervisor_pid,supervisor_identity,supervisor_birth_time,startup_owner_pid_identity,startup_owner_birth_time \
                     FROM agents WHERE id=?",
                    [execution.as_str()],
                    |row| {
                        Ok((
                            row.get(0)?,
                            row.get(1)?,
                            row.get(2)?,
                            row.get(3)?,
                            row.get(4)?,
                            row.get(5)?,
                            row.get(6)?,
                        ))
                    },
                )
                .optional()?;
            let Some((capability, root, sup_pid, sup_token, sup_birth, startup, startup_birth)) =
                row
            else {
                return Ok(false);
            };
            if capability.as_deref() != Some(capability_sha256.as_str()) || root != logical.as_str()
            {
                return Ok(false);
            }
            if sup_pid == Some(*pid)
                && sup_token.as_deref() == Some(token.as_str())
                && sup_birth == Some(*birth)
            {
                return Ok(true);
            }
            if sup_pid.is_some() {
                return Ok(false);
            }
            let owner = startup.unwrap_or_default();
            Ok(match serde_json::from_str::<Value>(&owner) {
                Ok(evidence) if evidence.is_object() => {
                    evidence["pid"].as_i64() == Some(i64::from(*pid))
                        && evidence["token"].as_str() == Some(token.as_str())
                        && evidence["birth"].as_f64().or(startup_birth) == Some(*birth)
                }
                _ => owner == *token && startup_birth == Some(*birth),
            })
        }
        WriteAuthority::ServiceExec {
            generation,
            capability_sha256,
            pid,
            birth,
            token,
            root_pid,
            root_birth,
            root_token,
        } => {
            let row: Option<(Option<String>, String, Option<String>)> = conn
                .query_row(
                    "SELECT exec_capability_sha256,broker_identity_json,process_identity_json FROM managed_service_generations WHERE id=?",
                    [generation.as_str()],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .optional()?;
            let Some((capability, root, process)) = row else {
                return Ok(false);
            };
            let root: Value = serde_json::from_str(&root).unwrap_or(Value::Null);
            if capability.as_deref() != Some(capability_sha256.as_str())
                || root["pid"].as_i64() != Some(i64::from(*root_pid))
                || root["token"].as_str() != Some(root_token.as_str())
                || root["birth"].as_f64() != Some(*root_birth)
            {
                return Ok(false);
            }
            Ok(
                match process.map(|raw| serde_json::from_str::<Value>(&raw).unwrap_or(Value::Null))
                {
                    None => true,
                    Some(identity) => {
                        identity["pid"].as_i64() == Some(i64::from(*pid))
                            && identity["token"].as_str() == Some(token.as_str())
                            && identity["birth"].as_f64() == Some(*birth)
                    }
                },
            )
        }
        _ => Ok(true),
    }
}

/// The authority after a re-hello: only a supervisor's pinned attempt may
/// differ; the identity, capability and execution must be exactly the same.
fn repin(current: &WriteAuthority, hello: &Hello) -> Option<WriteAuthority> {
    let WriteAuthority::Supervisor {
        logical,
        execution,
        capability_sha256,
        pid,
        birth,
        token,
        ..
    } = current
    else {
        return None;
    };
    if hello.protocol != WRITE_PROTOCOL
        || hello.role != "supervisor"
        || hello.logical.as_deref() != Some(logical.as_str())
        || hello.execution.as_deref() != Some(execution.as_str())
        || capability_hash(hello.capability_hex.as_deref())
            .ok()
            .as_deref()
            != Some(capability_sha256.as_str())
    {
        return None;
    }
    Some(WriteAuthority::Supervisor {
        logical: logical.clone(),
        execution: execution.clone(),
        attempt: hello.attempt.clone(),
        capability_sha256: capability_sha256.clone(),
        pid: *pid,
        birth: *birth,
        token: token.clone(),
    })
}

/// Parses one required agent identifier of a hello.
fn parse_agent(value: Option<&str>) -> std::result::Result<AgentId, Refusal> {
    value
        .and_then(|text| text.parse().ok())
        .ok_or(Refusal::Identity)
}

/// SHA-256 (lowercase hex) of a well-formed capability secret: exactly 64
/// lowercase hexadecimal characters.
fn capability_hash(secret: Option<&str>) -> std::result::Result<String, Refusal> {
    let secret = secret.ok_or(Refusal::Identity)?;
    if secret.len() != 64
        || !secret
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(Refusal::Identity);
    }
    Ok(crate::fs::sha256(secret.as_bytes()))
}

/// Looks up one operation's status for the connection's own authority.
async fn status(endpoint: &WriterEndpoint, authority: &WriteAuthority, body: &[u8]) -> OpStatus {
    /// The body of a status request: the operation identity and the payload
    /// digest it was submitted with.
    #[derive(serde::Deserialize)]
    struct Lookup {
        /// Identity of the operation being asked about.
        operation_id: OperationId,
        /// Payload digest the operation was submitted with.
        digest: PayloadDigest,
    }
    match serde_json::from_slice::<Lookup>(body) {
        Ok(lookup) => {
            endpoint
                .handle
                .status(authority.clone(), &lookup.operation_id, &lookup.digest)
                .await
        }
        // A malformed lookup reveals nothing; "not found" is never proof of
        // non-commit.
        Err(_) => OpStatus::NotFound {
            writer_epoch: endpoint.handle.health().epoch,
        },
    }
}

/// Encodes a reply and checks the 72 KiB bound; `None` when it does not fit.
fn bounded<T: serde::Serialize>(value: &T) -> Option<Vec<u8>> {
    let bytes = serde_json::to_vec(value).ok()?;
    (bytes.len() <= MAX_REPLY_BYTES.max(wire::REPLY_MAX)).then_some(bytes)
}

/// Reads one announced body within `deadline`.
async fn read_body<R: AsyncRead + Unpin>(
    input: &mut ReadHalf<R>,
    header: &wire::Header,
    deadline: Duration,
) -> std::io::Result<Vec<u8>> {
    tokio::time::timeout(deadline, wire::read_body(input, header))
        .await
        .map_err(|_| std::io::Error::from(std::io::ErrorKind::TimedOut))?
        .map_err(frame_io)
}

/// Writes one reply frame within [`REPLY_DEADLINE`].
async fn reply<W: AsyncWrite + Unpin>(
    output: &mut WriteHalf<W>,
    message: Message,
    seq: u64,
    body: &[u8],
) -> std::io::Result<()> {
    tokio::time::timeout(
        REPLY_DEADLINE,
        wire::write_frame(output, message, 0, seq, body),
    )
    .await
    .map_err(|_| std::io::Error::from(std::io::ErrorKind::TimedOut))?
}

/// Maps a framing error to an I/O error without losing its closed description.
fn frame_io(error: wire::FrameError) -> std::io::Error {
    match error {
        wire::FrameError::Io(error) => error,
        other => std::io::Error::new(std::io::ErrorKind::InvalidData, other.to_string()),
    }
}

/// Source of process-unique writer connection identities for guard ownership.
static NEXT_CONNECTION: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

/// The finish launch-secret context one connection installed in the writer
/// handle. Only a supervisor authority with a pinned attempt installs one;
/// changing or dropping the claim, or closing the connection, retires it,
/// and retiring never removes a context a newer connection installed.
struct GuardLease {
    /// Writer handle that holds the transient contexts.
    handle: WriterHandle,
    /// This connection's identity.
    connection: u64,
    /// Execution and attempt of the context this connection installed.
    installed: Option<(AgentId, String)>,
}

impl GuardLease {
    /// A lease that has installed nothing yet.
    fn new(handle: WriterHandle) -> Self {
        Self {
            handle,
            connection: NEXT_CONNECTION.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            installed: None,
        }
    }

    /// Replaces this connection's context with the one carried by its latest
    /// verified hello; any other role, an unpinned attempt or an absent
    /// context leaves none installed.
    fn update(
        &mut self,
        authority: &WriteAuthority,
        context: Option<&crate::storage_client::FinishGuardContext>,
    ) {
        self.retire();
        if let (
            WriteAuthority::Supervisor {
                execution,
                attempt: Some(attempt),
                ..
            },
            Some(context),
        ) = (authority, context)
        {
            self.handle
                .install_finish_guard(execution, attempt, self.connection, &context.0);
            self.installed = Some((execution.clone(), attempt.clone()));
        }
    }

    /// Retires the context this connection installed, if it is still current.
    fn retire(&mut self) {
        if let Some((execution, attempt)) = self.installed.take() {
            self.handle
                .retire_finish_guard(&execution, &attempt, self.connection);
        }
    }
}

impl Drop for GuardLease {
    /// A closed connection's context no longer guards any finish.
    fn drop(&mut self) {
        self.retire();
    }
}
