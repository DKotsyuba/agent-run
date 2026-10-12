//! Protocol types of the single resident SQLite writer.
//!
//! These DTOs are shared by the broker-side writer service, the store that
//! executes typed operations and every producer that submits them over the
//! private writer socket. They carry identities, bounded results and closed
//! diagnostics only: never SQL text, arguments, secrets or free-form errors.
//! Operation payloads themselves live in the store crate because they embed
//! store and platform types.

use crate::{
    Error, QuotaAdmissionError,
    canonical::{dumps, hex_digest, sha256_hex},
    domain::AgentId,
    error::{PersistenceOperation, invalid},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use sha2::{Digest, Sha256};

/// Version of the writer request/receipt protocol; a peer speaking another
/// version is refused with [`RejectCode::ProtocolUnsupported`].
pub const WRITE_PROTOCOL: u16 = 1;
/// Maximum bytes of a receipt result in its canonical `ensure_ascii` JSON
/// encoding (the exact bytes stored in `operation_receipts.result_json`).
pub const MAX_RESULT_BYTES: usize = 65_536;
/// Maximum bytes of any encoded [`SubmitResult`] or [`OpStatus`] reply.
pub const MAX_REPLY_BYTES: usize = 73_728;
/// Maximum bytes of a [`StorageRejection::message`] in canonical encoding.
pub const MAX_MESSAGE_BYTES: usize = 512;
/// Maximum bytes of a [`RejectionPayload`] in canonical encoding.
pub const MAX_PAYLOAD_BYTES: usize = 16_384;
/// Maximum age of an operation identity, in monotonic nanoseconds (7 days).
/// Older identities are refused with [`RejectCode::StaleOperation`].
pub const MAX_OPERATION_AGE_NS: u64 = 7 * 86_400 * 1_000_000_000;
/// Maximum distance of a deadline from "now", in nanoseconds (24 hours).
pub const MAX_DEADLINE_HORIZON_NS: u64 = 86_400 * 1_000_000_000;
/// Tolerated future skew of an operation issue stamp, in nanoseconds (1 s).
pub const MAX_ISSUE_SKEW_NS: u64 = 1_000_000_000;
/// Maximum total bytes of a [`ProducerId`] in canonical encoding.
const MAX_PRODUCER_BYTES: usize = 200;

/// Returns the 16-hex-digit boot tag of one boot session identifier: the
/// first eight bytes of its SHA-256 in lowercase hex. The same `boot_id`
/// always yields the same tag; distinct boots yield distinct tags with
/// overwhelming probability.
pub fn boot_tag(boot_id: &str) -> String {
    hex_digest(&Sha256::digest(boot_id.as_bytes())[..8])
}

/// Durable identity of one storage operation.
///
/// Exactly `"<boot_tag>.<issued_ns>.<uuid>"`: a 16-hex boot tag of the boot
/// session that issued it, the issuing `CLOCK_MONOTONIC` nanoseconds as 16
/// hex digits and a 32-hex random UUID v4 (66 ASCII bytes). The issue stamp
/// is immutable identity: the writer refuses an identity from another boot or
/// older than [`MAX_OPERATION_AGE_NS`] regardless of any fresh deadline, so a
/// retired operation can never execute again. A retry of the same operation
/// reuses the same identity; a new operation always gets a new one.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct OperationId(String);

impl OperationId {
    /// Mints a fresh identity for the current boot `boot_id` at monotonic
    /// `issued_ns` (both from the platform monotonic clock helper).
    pub fn new(boot_id: &str, issued_ns: u64) -> Self {
        Self(format!(
            "{}.{issued_ns:016x}.{}",
            boot_tag(boot_id),
            uuid::Uuid::new_v4().simple()
        ))
    }

    /// Validates the exact format; any other text is a typed validation error.
    /// The 66-byte length is checked before anything else and the three
    /// segments are then read at fixed offsets, so an oversized or
    /// delimiter-heavy input is refused without allocation.
    pub fn parse(text: &str) -> crate::Result<Self> {
        let bytes = text.as_bytes();
        let hex = |range: std::ops::Range<usize>| {
            bytes[range]
                .iter()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(b))
        };
        if bytes.len() != 66
            || bytes[16] != b'.'
            || bytes[33] != b'.'
            || !hex(0..16)
            || !hex(17..33)
            || !hex(34..66)
        {
            return Err(invalid("invalid operation id"));
        }
        Ok(Self(text.to_owned()))
    }

    /// The identity text.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The 16-hex boot tag of the issuing boot.
    pub fn boot_tag(&self) -> &str {
        &self.0[..16]
    }

    /// The issuing monotonic nanoseconds.
    pub fn issued_ns(&self) -> u64 {
        u64::from_str_radix(&self.0[17..33], 16).expect("validated hex")
    }

    /// Whether this identity is stale for the current boot `boot_id` at
    /// monotonic `now_ns`: issued by another boot, older than
    /// [`MAX_OPERATION_AGE_NS`] or stamped more than [`MAX_ISSUE_SKEW_NS`] in
    /// the future. Wall-clock time is never consulted.
    pub fn is_stale(&self, boot_id: &str, now_ns: u64) -> bool {
        self.boot_tag() != boot_tag(boot_id)
            || self.issued_ns() > now_ns.saturating_add(MAX_ISSUE_SKEW_NS)
            || now_ns.saturating_sub(self.issued_ns()) > MAX_OPERATION_AGE_NS
    }
}

impl TryFrom<String> for OperationId {
    type Error = Error;
    /// Validates owned text with [`OperationId::parse`].
    fn try_from(text: String) -> crate::Result<Self> {
        Self::parse(&text)
    }
}

impl From<OperationId> for String {
    /// Returns the identity text.
    fn from(id: OperationId) -> Self {
        id.0
    }
}

/// SHA-256 of an operation's canonical `ensure_ascii` JSON, 64 lowercase hex.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct PayloadDigest(String);

impl PayloadDigest {
    /// Digests one JSON value with the canonical encoding used on the wire.
    pub fn of(value: &Value) -> Self {
        Self(sha256_hex(value, true))
    }

    /// Validates 64 lowercase hex digits.
    pub fn parse(text: &str) -> crate::Result<Self> {
        if text.len() != 64
            || !text
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(invalid("invalid payload digest"));
        }
        Ok(Self(text.to_owned()))
    }

    /// The digest text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for PayloadDigest {
    type Error = Error;
    /// Validates owned text with [`PayloadDigest::parse`].
    fn try_from(text: String) -> crate::Result<Self> {
        Self::parse(&text)
    }
}

impl From<PayloadDigest> for String {
    /// Returns the digest text.
    fn from(digest: PayloadDigest) -> Self {
        digest.0
    }
}

/// Kind of process or subsystem that submits operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProducerKind {
    /// A subsystem of the resident broker itself.
    Broker,
    /// A detached execution supervisor.
    Supervisor,
    /// A managed-service executor process.
    ServiceExec,
    /// A same-user command-line process.
    Cli,
    /// A capacity/quota collector process.
    Collector,
}

/// Stable identity of one ordered producer.
///
/// Operations of one producer are admitted and executed in strictly
/// increasing `ordinal` order (gaps allowed). `scope` names the producer's
/// subsystem or execution (for example `delivery` or an agent id) and
/// `instance` is a per-process random identifier, so a restarted process is a
/// new producer. Total canonical size is at most 200 bytes.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProducerId {
    /// Producer kind.
    pub kind: ProducerKind,
    /// Optional subsystem or execution scope.
    pub scope: Option<String>,
    /// Per-process instance identifier, nonempty.
    pub instance: String,
}

impl ProducerId {
    /// Validates non-empty instance and the 200-byte bound.
    pub fn validate(&self) -> crate::Result<()> {
        if self.instance.trim().is_empty() || self.key().len() > MAX_PRODUCER_BYTES {
            return Err(invalid("invalid producer id"));
        }
        Ok(())
    }

    /// Canonical key used for durable producer watermarks.
    pub fn key(&self) -> String {
        String::from_utf8(dumps(
            &serde_json::to_value(self).expect("producer json"),
            true,
        ))
        .expect("ascii")
    }
}

/// Scheduling class, always derived from the verified operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WriteClass {
    /// Ownership, cancel, cleanup, terminal and observed-result writes.
    Critical,
    /// Ordinary journal, admission, pool and delivery writes.
    Normal,
    /// Statistics, quota, retention and other background writes.
    Background,
}

/// Absolute same-host deadline: a `CLOCK_MONOTONIC` nanosecond value of the
/// boot session `boot_id`. A pending write keeps its original deadline across
/// reconnects, resubmissions and cancellation; Rust `Instant` is never
/// serialized.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MonoDeadline {
    /// Boot session identifier the nanoseconds belong to.
    pub boot_id: String,
    /// Absolute monotonic nanoseconds.
    pub ns: u64,
}

impl MonoDeadline {
    /// Whether the deadline has passed (or belongs to another boot) at the
    /// current boot `boot_id` and monotonic `now_ns`.
    pub fn expired(&self, boot_id: &str, now_ns: u64) -> bool {
        self.boot_id != boot_id || now_ns >= self.ns
    }
}

/// Authenticated authority of one submission.
///
/// Values are constructed only by the broker or the authenticated transport,
/// never decoded from a request frame. The writer re-checks the recorded
/// scope (capability hash, process identity, execution) inside each write
/// transaction; a claimed role alone grants nothing.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WriteAuthority {
    /// An in-process broker subsystem.
    Broker,
    /// A same-user command-line peer.
    Cli {
        /// Peer user id.
        uid: u32,
        /// Kernel peer process id.
        pid: i32,
        /// Peer process birth time.
        birth: f64,
        /// Peer process identity token.
        token: String,
    },
    /// A same-user capacity collector peer.
    Collector {
        /// Peer user id.
        uid: u32,
        /// Kernel peer process id.
        pid: i32,
        /// Peer process birth time.
        birth: f64,
        /// Peer process identity token.
        token: String,
    },
    /// A detached execution supervisor.
    Supervisor {
        /// Lineage root of the execution.
        logical: AgentId,
        /// Execution (agent) id.
        execution: AgentId,
        /// Attempt pinned at hello, re-pinned after failover; excluded from the fingerprint.
        attempt: Option<String>,
        /// SHA-256 of the launch capability received over the private pipe.
        capability_sha256: String,
        /// Kernel peer process id.
        pid: i32,
        /// Peer process birth time.
        birth: f64,
        /// Peer process identity token.
        token: String,
    },
    /// A managed-service executor.
    ServiceExec {
        /// Service generation id.
        generation: String,
        /// SHA-256 of the executor capability.
        capability_sha256: String,
        /// Kernel peer process id.
        pid: i32,
        /// Peer process birth time.
        birth: f64,
        /// Peer process identity token.
        token: String,
        /// Recorded launch root process id.
        root_pid: i32,
        /// Recorded launch root birth time.
        root_birth: f64,
        /// Recorded launch root identity token.
        root_token: String,
    },
    /// An authenticated worker tool call routed by the broker's API socket.
    Worker {
        /// Execution (agent) id.
        execution: AgentId,
        /// Exact attempt.
        attempt: String,
    },
}

impl WriteAuthority {
    /// Closed kind label stored with receipts.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Broker => "broker",
            Self::Cli { .. } => "cli",
            Self::Collector { .. } => "collector",
            Self::Supervisor { .. } => "supervisor",
            Self::ServiceExec { .. } => "service_exec",
            Self::Worker { .. } => "worker",
        }
    }

    /// SHA-256 over the canonical identity fields; stored in receipts so that
    /// replay and status are answered only to the original authority. The
    /// mutable supervisor `attempt` is excluded, so replay survives a re-pin.
    pub fn fingerprint(&self) -> String {
        let mut value = serde_json::to_value(self).expect("authority json");
        if let Self::Supervisor { .. } = self {
            value.as_object_mut().expect("object").remove("attempt");
        }
        sha256_hex(&value, true)
    }
}

/// Committed outcome of one operation, durable under its identity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WriteReceipt {
    /// Operation identity.
    pub operation_id: OperationId,
    /// Digest of the operation payload.
    pub digest: PayloadDigest,
    /// Closed operation kind name.
    pub op_kind: String,
    /// Submitting producer.
    pub producer: ProducerId,
    /// Producer ordinal.
    pub ordinal: u64,
    /// Execution scope when the receipt is scoped, else `None`.
    pub scope: Option<AgentId>,
    /// Writer epoch that committed it.
    pub writer_epoch: u64,
    /// Monotonically increasing receipt sequence.
    pub commit_seq: u64,
    /// Unix commit time (diagnostic only).
    pub committed_at: f64,
    /// True when answered from the stored receipt instead of a fresh execution.
    /// A replayed receipt never authorizes a relaunch or resend.
    pub replayed: bool,
    /// Bounded typed result (<= [`MAX_RESULT_BYTES`] canonical bytes).
    pub result: Value,
}

impl WriteReceipt {
    /// Decodes the typed result of the operation.
    pub fn output<O: DeserializeOwned>(&self) -> crate::Result<O> {
        Ok(serde_json::from_value(self.result.clone())?)
    }
}

/// Canonical `ensure_ascii` byte length of a JSON value.
pub fn encoded_len(value: &Value) -> usize {
    dumps(value, true).len()
}

/// Verifies that an encoded reply stays within [`MAX_REPLY_BYTES`].
pub fn check_reply<T: Serialize>(reply: &T) -> crate::Result<()> {
    if encoded_len(&serde_json::to_value(reply)?) > MAX_REPLY_BYTES {
        return Err(invalid("writer reply exceeds its byte bound"));
    }
    Ok(())
}

/// Stage at which a submission was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RejectStage {
    /// Before acceptance (format, size, identity, order).
    Gate,
    /// Static authority matrix or in-transaction scope predicate.
    Authorization,
    /// `BEGIN IMMEDIATE` did not acquire; nothing executed.
    Acquire,
    /// A statement failed and the rollback was confirmed.
    Statement,
    /// `COMMIT` failed and the rollback was confirmed.
    Commit,
}

/// Closed refusal codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RejectCode {
    /// Recomputed digest differs from the claimed digest.
    DigestMismatch,
    /// Unknown protocol version or operation kind/header mismatch.
    ProtocolUnsupported,
    /// The database schema is not the supported version.
    SchemaUnsupported,
    /// The request or result exceeds its byte bound.
    PayloadTooLarge,
    /// The deadline is from another boot or beyond the horizon.
    StaleDeadline,
    /// The operation identity is from another boot or too old.
    StaleOperation,
    /// The authority may not submit this operation.
    Unauthorized,
    /// The recorded process/capability no longer matches the authority.
    StaleAuthority,
    /// The exact execution scope no longer exists.
    UnknownScope,
    /// The same identity was used with a different digest/producer/ordinal/authority.
    ReplayConflict,
    /// The producer ordinal does not exceed its durable watermark.
    OrderViolation,
    /// A public input contract was violated.
    Validation,
    /// A guarded state or identity conflict.
    Conflict,
    /// A referenced record does not exist.
    NotFound,
    /// A lifecycle transition is not permitted.
    Transition,
    /// Stored evidence failed an integrity check.
    Integrity,
    /// Typed provider admission refusal (payload in [`RejectionPayload`]).
    QuotaAdmission,
    /// Active-agent capacity is exhausted (`Error::Capacity`).
    Capacity,
    /// A path escaped its owned root (`Error::PathEscape`).
    PathEscape,
    /// The operation or path is not supported (yet).
    Unsupported,
    /// The write lock could not be acquired; nothing executed.
    StorageUnavailable,
    /// A storage failure whose rollback was confirmed.
    StorageRolledBack,
}

/// Closed persistence diagnostics: operation, stage, SQLite codes and timing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistenceDiag {
    /// Closed persistence boundary.
    pub operation: PersistenceOperation,
    /// Stage of the failure.
    pub phase: RejectStage,
    /// SQLite primary result code.
    pub sqlite_primary: Option<i32>,
    /// SQLite extended result code.
    pub sqlite_extended: Option<i32>,
    /// Elapsed milliseconds.
    pub wait_ms: u64,
    /// Attempts including the failing one.
    pub attempts: u32,
}

/// Typed domain payload preserved across the writer boundary.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RejectionPayload {
    /// Provider admission refusal including `SelectionStale` rerank data.
    QuotaAdmission(QuotaAdmissionError),
}

/// A refused submission.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StorageRejection {
    /// Closed code.
    pub code: RejectCode,
    /// Refusal stage.
    pub stage: RejectStage,
    /// Closed, bounded human text without SQL, arguments or secrets.
    pub message: String,
    /// True when stored as a durable rejected receipt (replays return it).
    pub durable: bool,
    /// Optional closed persistence diagnostics.
    pub persistence: Option<PersistenceDiag>,
    /// Optional typed domain payload.
    pub domain: Option<RejectionPayload>,
}

impl StorageRejection {
    /// Builds a non-durable refusal. The message keeps its longest character
    /// prefix whose canonical `ensure_ascii` JSON string encoding, including
    /// both quotes, is at most [`MAX_MESSAGE_BYTES`]; the cost is computed per
    /// character in one linear
    /// pass, so a large untrusted string cannot cause quadratic work.
    pub fn new(code: RejectCode, stage: RejectStage, message: impl Into<String>) -> Self {
        let message = bounded_message(&message.into());
        Self {
            code,
            stage,
            message,
            durable: false,
            persistence: None,
            domain: None,
        }
    }

    /// Maps a typed domain error returned by an operation into a refusal at
    /// `stage`, keeping the quota payload and persistence diagnostics.
    /// Source text of SQL/IO errors is never copied.
    pub fn from_error(error: &Error, stage: RejectStage) -> Self {
        let (code, message) = match error {
            Error::QuotaAdmission(payload) => {
                let mut refusal = Self::new(RejectCode::QuotaAdmission, stage, payload.kind());
                refusal.domain = Some(RejectionPayload::QuotaAdmission(payload.clone()));
                return refusal;
            }
            Error::Persistence {
                operation,
                elapsed_ms,
                attempts,
                phase,
                source,
            } => {
                let mut refusal = Self::from_error(source, stage);
                let (primary, extended) = sqlite_codes(source);
                refusal.persistence = Some(PersistenceDiag {
                    operation: *operation,
                    phase: phase.unwrap_or(stage),
                    sqlite_primary: primary,
                    sqlite_extended: extended,
                    wait_ms: *elapsed_ms,
                    attempts: *attempts,
                });
                return refusal;
            }
            Error::OwnershipCheckpoint { source, .. } => return Self::from_error(source, stage),
            Error::Validation(text) => (RejectCode::Validation, text.clone()),
            Error::PathEscape(text) => (RejectCode::PathEscape, text.clone()),
            Error::NotFound(text) => (RejectCode::NotFound, text.clone()),
            Error::Transition(text) => (RejectCode::Transition, text.clone()),
            Error::Conflict => (RejectCode::Conflict, error.to_string()),
            Error::Capacity => (RejectCode::Capacity, error.to_string()),
            Error::Unsupported(text) => (RejectCode::Unsupported, text.clone()),
            Error::AnswerIntegrity(text) | Error::Integrity(text) => {
                (RejectCode::Integrity, text.clone())
            }
            Error::Json(_) => (RejectCode::Validation, "invalid JSON payload".to_owned()),
            Error::Sql(_)
            | Error::Io(_)
            | Error::Runtime(_)
            | Error::BrokerUnavailable
            | Error::Broker { .. }
            | Error::Bootstrap { .. } => (
                RejectCode::StorageRolledBack,
                "storage operation failed".to_owned(),
            ),
        };
        Self::new(code, stage, message)
    }

    /// Converts back into a typed domain error preserving the class, the quota
    /// payload and the closed persistence diagnostics, including `Some(phase)`.
    /// When the diagnostics
    /// carry an extended SQLite code, the wrapped source is a reconstructed
    /// `Error::Sql` failure with exactly that code, so machine codes and the
    /// primary/extended pair survive the writer boundary.
    pub fn into_error(self) -> Error {
        let inner = match (&self.code, self.domain.clone()) {
            (_, Some(RejectionPayload::QuotaAdmission(payload))) => Error::QuotaAdmission(payload),
            (RejectCode::Conflict | RejectCode::ReplayConflict, _) => Error::Conflict,
            (RejectCode::Capacity, _) => Error::Capacity,
            (RejectCode::PathEscape, _) => Error::PathEscape(self.message.clone()),
            (RejectCode::NotFound | RejectCode::UnknownScope, _) => {
                Error::NotFound(self.message.clone())
            }
            (RejectCode::Transition, _) => Error::Transition(self.message.clone()),
            (RejectCode::Integrity, _) => Error::Integrity(self.message.clone()),
            (RejectCode::Unsupported | RejectCode::SchemaUnsupported, _) => {
                Error::Unsupported(self.message.clone())
            }
            (RejectCode::StorageUnavailable | RejectCode::StorageRolledBack, _) => {
                Error::Runtime(self.message.clone())
            }
            _ => Error::Validation(self.message.clone()),
        };
        match self.persistence {
            Some(diag) => Error::Persistence {
                operation: diag.operation,
                elapsed_ms: diag.wait_ms,
                attempts: diag.attempts,
                phase: Some(diag.phase),
                source: Box::new(match diag.sqlite_extended {
                    Some(code) => Error::Sql(rusqlite::Error::SqliteFailure(
                        rusqlite::ffi::Error::new(code),
                        None,
                    )),
                    None => inner,
                }),
            },
            None => inner,
        }
    }
}

/// Returns the longest character prefix of `text` whose canonical
/// `ensure_ascii` JSON string encoding, including both surrounding quotes, is
/// at most [`MAX_MESSAGE_BYTES`]. Each character's escaped width matches
/// `canonical::dumps` (1 for printable ASCII, 2 for short escapes, 6 per
/// `\uXXXX` unit and 12 for a surrogate pair) and is computed once, so the work
/// is linear in the retained prefix.
fn bounded_message(text: &str) -> String {
    // The two quotes count toward the bound.
    let mut used = 2;
    let mut end = 0;
    for (index, character) in text.char_indices() {
        let width = match character {
            '"' | '\\' | '\u{8}' | '\t' | '\n' | '\u{c}' | '\r' => 2,
            ' '..='~' => 1,
            '\u{0}'..='\u{1f}' | '\u{7f}'..='\u{ffff}' => 6,
            _ => 12,
        };
        if used + width > MAX_MESSAGE_BYTES {
            break;
        }
        used += width;
        end = index + character.len_utf8();
    }
    text[..end].to_owned()
}

/// Extracts SQLite primary/extended codes from a (possibly wrapped) error.
fn sqlite_codes(error: &Error) -> (Option<i32>, Option<i32>) {
    match error {
        Error::Sql(rusqlite::Error::SqliteFailure(failure, _)) => (
            Some(failure.extended_code & 0xff),
            Some(failure.extended_code),
        ),
        Error::Persistence { source, .. } | Error::OwnershipCheckpoint { source, .. } => {
            sqlite_codes(source)
        }
        _ => (None, None),
    }
}

/// Reasons proving that a submission was never accepted or never claimed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NotEnqueuedReason {
    /// Job count limit reached.
    QueueFull,
    /// Byte budget reached.
    BytesFull,
    /// Per-producer or per-execution limit reached.
    ProducerFull,
    /// Remaining capacity is reserved for critical work.
    ReservedForCritical,
    /// New admissions are closed.
    AdmissionsClosed,
    /// The writer is shutting down.
    Draining,
    /// The writer is degraded and recovering.
    Degraded,
    /// The deadline had already passed at the gate.
    DeadlineExpired,
    /// The deadline passed while queued; the job was never claimed.
    DeadlineBeforeStart,
}

/// Causes of an unknown outcome; the caller resolves them by status/resubmit
/// with the same identity and never by repeating a native action.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnknownCause {
    /// `COMMIT` failed in a way that may still have committed.
    CommitUncertain,
    /// `ROLLBACK` failed or autocommit could not be confirmed.
    RollbackUncertain,
    /// The caller's deadline passed while the job was executing.
    DeadlineDuringExecution,
    /// The writer thread stopped while the job was executing.
    WriterStopped,
    /// The transport lost the reply.
    Transport,
}

/// Outcome of one submission.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "outcome", content = "value", rename_all = "snake_case")]
pub enum SubmitResult {
    /// Committed now or answered from the durable receipt.
    Committed(WriteReceipt),
    /// Refused; see [`StorageRejection::durable`].
    Rejected(StorageRejection),
    /// Proven never accepted or never claimed.
    NotEnqueued(NotEnqueuedReason),
    /// Outcome not known; never proof of rollback.
    Unknown(UnknownCause),
}

/// Status of one operation identity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", content = "value", rename_all = "snake_case")]
pub enum OpStatus {
    /// Durable committed receipt.
    Committed(WriteReceipt),
    /// Durable rejected receipt.
    Rejected(StorageRejection),
    /// Accepted and waiting in the queue.
    Queued,
    /// Currently executing.
    Executing,
    /// No receipt and not in flight; never proof of non-commit.
    NotFound {
        /// Current writer epoch.
        writer_epoch: u64,
    },
}

/// The executing job, for health reporting.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InFlight {
    /// Operation kind name.
    pub op_kind: String,
    /// Class.
    pub class: WriteClass,
    /// Milliseconds since execution started.
    pub elapsed_ms: u64,
}

/// Degraded writer state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DegradedState {
    /// Closed reason label.
    pub reason: String,
    /// Monotonic nanoseconds when degradation began.
    pub since_ns: u64,
    /// Completed recovery attempts.
    pub recovery_attempts: u32,
    /// Closed label of the last probe outcome.
    pub last_probe: Option<String>,
    /// Up to 16 operation identities with uncertain outcome.
    pub uncertain: Vec<OperationId>,
}

/// Outcome counters since writer start.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutcomeCounters {
    /// Committed (including replays).
    pub committed: u64,
    /// Rejected.
    pub rejected: u64,
    /// Not enqueued.
    pub not_enqueued: u64,
    /// Unknown.
    pub unknown: u64,
}

/// Writer health snapshot.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WriterHealth {
    /// Writer epoch.
    pub epoch: u64,
    /// Whether new admissions are accepted.
    pub admissions_open: bool,
    /// Accepted FIFO jobs, including currently executing SQL.
    pub accepted_jobs: u32,
    /// Retained payload byte credits belonging to accepted work.
    pub accepted_bytes: u64,
    /// SQL worker is poisoned; no new callback can execute.
    pub poisoned: bool,
    /// Replies actually observed by facade callers, including replay replies.
    pub counters: OutcomeCounters,
}

/// Outcome of one maintenance job (outside business transactions, no receipt).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum MaintenanceOutcome {
    /// `wal_checkpoint` result triple.
    Checkpoint {
        /// SQLite busy flag.
        busy: i64,
        /// WAL frames.
        log: i64,
        /// Checkpointed frames.
        checkpointed: i64,
    },
    /// Incremental vacuum result.
    Vacuumed {
        /// Whether pages were reclaimed.
        reclaimed: bool,
    },
    /// Interrupted at its deadline; never success.
    Interrupted,
    /// Refused (for example while degraded).
    Refused(StorageRejection),
}

#[cfg(test)]
/// Unit checks of operation identity format and staleness, attempt-stable
/// authority fingerprints and lossless rejection mapping.
mod tests {
    use super::*;

    /// A minted id parses back unchanged, exposes its issue stamp and becomes
    /// stale for another boot, past the age bound or when stamped far ahead;
    /// malformed text is refused.
    #[test]
    fn operation_id_round_trips_and_detects_staleness() {
        let id = OperationId::new("boot-a", 5_000);
        assert_eq!(OperationId::parse(id.as_str()).unwrap(), id);
        assert_eq!(id.issued_ns(), 5_000);
        assert!(!id.is_stale("boot-a", 6_000));
        assert!(id.is_stale("boot-b", 6_000));
        assert!(id.is_stale("boot-a", 5_000 + MAX_OPERATION_AGE_NS + 1));
        assert!(id.is_stale("boot-a", 0) == (5_000 > MAX_ISSUE_SKEW_NS));
        assert!(OperationId::parse("not-an-id").is_err());
    }

    /// Malformed identities are refused by the up-front exact-length and
    /// fixed-offset checks: a large delimiter-heavy string, a 66-byte string
    /// with misplaced delimiters and uppercase hex.
    #[test]
    fn operation_id_refuses_malformed_and_oversized_text() {
        let valid = OperationId::new("boot-a", 7).as_str().to_owned();
        assert!(OperationId::parse(&".".repeat(1 << 20)).is_err());
        assert!(OperationId::parse(&format!("{valid}.")).is_err());
        let mut shifted = valid.clone().into_bytes();
        shifted.swap(16, 15);
        assert!(OperationId::parse(std::str::from_utf8(&shifted).unwrap()).is_err());
        assert!(OperationId::parse(&valid.to_uppercase()).is_err());
        assert!(OperationId::parse(&"\u{e9}".repeat(33)).is_err());
        assert!(OperationId::parse(&valid).is_ok());
    }

    /// Re-pinning the supervisor attempt keeps the fingerprint, while any
    /// process identity change (here the pid) changes it.
    #[test]
    fn fingerprint_ignores_supervisor_attempt_only() {
        let base = |attempt: Option<&str>, pid| WriteAuthority::Supervisor {
            logical: "ag-20261011-000000-0000000001".parse().unwrap(),
            execution: "ag-20261011-000000-0000000001".parse().unwrap(),
            attempt: attempt.map(str::to_owned),
            capability_sha256: "c".repeat(64),
            pid,
            birth: 1.0,
            token: "t".into(),
        };
        assert_eq!(
            base(Some("a1"), 7).fingerprint(),
            base(Some("a2"), 7).fingerprint()
        );
        assert_ne!(
            base(Some("a1"), 7).fingerprint(),
            base(Some("a1"), 8).fingerprint()
        );
    }

    /// Closed persistence diagnostics, including the extended SQLite code,
    /// survive the round trip from a domain error to a refusal and back.
    #[test]
    fn rejection_round_trip_keeps_persistence_and_sqlite_codes() {
        let busy = Error::Sql(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(5),
            None,
        ));
        let refusal = StorageRejection::from_error(
            &Error::persistence(
                PersistenceOperation::CommandWrite,
                std::time::Duration::from_millis(9),
                busy,
            ),
            RejectStage::Statement,
        );
        let diag = refusal.persistence.clone().unwrap();
        assert_eq!(
            (diag.wait_ms, diag.sqlite_primary, diag.sqlite_extended),
            (9, Some(5), Some(5))
        );
        match refusal.into_error() {
            Error::Persistence {
                operation: PersistenceOperation::CommandWrite,
                elapsed_ms: 9,
                source,
                ..
            } => assert!(matches!(
                *source,
                Error::Sql(rusqlite::Error::SqliteFailure(failure, None)) if failure.extended_code == 5
            )),
            other => panic!("unexpected error {other:?}"),
        }
    }

    /// Message bounding keeps the longest prefix whose canonical encoding,
    /// quotes included, is at most 512 bytes: adding the next character would
    /// exceed the bound, for control, non-ASCII, astral and plain characters.
    #[test]
    fn rejection_message_is_bounded_by_canonical_bytes() {
        let bounded = |text: &str| {
            StorageRejection::new(RejectCode::Validation, RejectStage::Gate, text).message
        };
        let encoded = |s: &str| encoded_len(&Value::String(s.to_owned()));
        let text = "a\u{1}é😀\"".repeat(300);
        let kept = bounded(&text);
        assert!(encoded(&kept) <= MAX_MESSAGE_BYTES);
        let next = text[kept.len()..].chars().next().unwrap();
        assert!(encoded(&format!("{kept}{next}")) > MAX_MESSAGE_BYTES);
        assert_eq!(bounded("plain"), "plain");
    }

    /// Exact boundary: a message encoding to 512 canonical bytes is kept
    /// whole, while 513 bytes drop the last character, with control
    /// (`\u0001`, 6 bytes) and non-BMP (surrogate pair, 12 bytes) content.
    #[test]
    fn rejection_message_boundary_is_512_canonical_bytes() {
        let bounded = |text: &str| {
            StorageRejection::new(RejectCode::Validation, RejectStage::Gate, text).message
        };
        let encoded = |s: &str| encoded_len(&Value::String(s.to_owned()));
        let controls = "\u{1}".repeat(85);
        let astral = format!("{}abcdef", "😀".repeat(42));
        let plain = "a".repeat(510);
        for exact in [&controls, &astral, &plain] {
            assert_eq!(encoded(exact), 512);
            assert_eq!(&bounded(exact), exact);
            let over = format!("{exact}z");
            assert_eq!(encoded(&over), 513);
            assert_eq!(&bounded(&over), exact);
        }
        let astral_over = format!("{}abcde😀", "😀".repeat(42));
        assert_eq!(bounded(&astral_over), format!("{}abcde", "😀".repeat(42)));
    }

    /// Every closed domain class survives the refusal round trip with its
    /// public machine code, including Capacity and PathEscape.
    #[test]
    fn rejection_round_trip_keeps_public_classes() {
        let cases = [
            Error::Capacity,
            Error::PathEscape("escape".into()),
            Error::Conflict,
            Error::NotFound("missing".into()),
            Error::Transition("transition".into()),
            Error::Integrity("integrity".into()),
            Error::Unsupported("unsupported".into()),
            Error::Validation("invalid".into()),
        ];
        for error in cases {
            let kind = error.public().kind;
            let back = StorageRejection::from_error(&error, RejectStage::Statement).into_error();
            assert_eq!(back.public().kind, kind);
        }
    }

    /// A persistence error with a known phase keeps that phase (not the
    /// supplied stage) for Acquire, Statement and Commit, together with its
    /// SQLite codes, elapsed time and attempts, through the round trip.
    #[test]
    fn rejection_round_trip_keeps_each_persistence_phase() {
        for phase in [
            RejectStage::Acquire,
            RejectStage::Statement,
            RejectStage::Commit,
        ] {
            let error = Error::Persistence {
                operation: PersistenceOperation::OwnershipWrite,
                elapsed_ms: 7,
                attempts: 3,
                phase: Some(phase),
                source: Box::new(Error::Sql(rusqlite::Error::SqliteFailure(
                    rusqlite::ffi::Error::new(5),
                    None,
                ))),
            };
            let refusal = StorageRejection::from_error(&error, RejectStage::Gate);
            assert_eq!(refusal.persistence.as_ref().unwrap().phase, phase);
            match refusal.into_error() {
                Error::Persistence {
                    operation: PersistenceOperation::OwnershipWrite,
                    elapsed_ms: 7,
                    attempts: 3,
                    phase: Some(back),
                    source,
                } => {
                    assert_eq!(back, phase);
                    assert!(
                        matches!(*source, Error::Sql(rusqlite::Error::SqliteFailure(failure, None)) if failure.extended_code == 5)
                    );
                }
                other => panic!("unexpected {other:?}"),
            }
        }
    }

    /// The legacy constructor has no phase, so the refusal records the
    /// supplied stage; replay totals keep the phase; a legacy or `Acquire`
    /// phase keeps the old busy-acquire retryability.
    #[test]
    fn legacy_phase_totals_and_retry_are_preserved() {
        let busy = || {
            Error::Sql(rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(5),
                None,
            ))
        };
        let legacy = Error::persistence(
            PersistenceOperation::OwnershipAcquire,
            std::time::Duration::from_millis(2),
            busy(),
        );
        assert!(matches!(legacy, Error::Persistence { phase: None, .. }));
        assert!(legacy.retryable_ownership_acquire());
        let refusal = StorageRejection::from_error(&legacy, RejectStage::Commit);
        assert_eq!(refusal.persistence.unwrap().phase, RejectStage::Commit);
        let phased = Error::Persistence {
            operation: PersistenceOperation::OwnershipAcquire,
            elapsed_ms: 1,
            attempts: 1,
            phase: Some(RejectStage::Acquire),
            source: Box::new(busy()),
        };
        assert!(phased.retryable_ownership_acquire());
        let totals = phased.persistence_totals(std::time::Duration::from_millis(40), 4);
        assert!(matches!(
            totals,
            Error::Persistence {
                phase: Some(RejectStage::Acquire),
                attempts: 4,
                elapsed_ms: 40,
                ..
            }
        ));
    }

    /// Replay is permitted only for ownership acquire with SQLITE_BUSY
    /// (including the extended BUSY_SNAPSHOT code 517) and a legacy or
    /// `Acquire` phase; every other recorded phase, operation, code or source
    /// is denied even when the operation and code would otherwise match.
    #[test]
    fn retry_requires_acquire_phase_busy_and_sql_source() {
        let sql = |code| {
            Error::Sql(rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(code),
                None,
            ))
        };
        let error = |operation, phase, source| Error::Persistence {
            operation,
            elapsed_ms: 1,
            attempts: 1,
            phase,
            source: Box::new(source),
        };
        let acquire = PersistenceOperation::OwnershipAcquire;
        for (phase, code, retry) in [
            (None, 5, true),
            (None, 517, true),
            (Some(RejectStage::Acquire), 5, true),
            (Some(RejectStage::Acquire), 517, true),
            (Some(RejectStage::Acquire), 6, false),
            (Some(RejectStage::Gate), 5, false),
            (Some(RejectStage::Authorization), 5, false),
            (Some(RejectStage::Statement), 5, false),
            (Some(RejectStage::Statement), 517, false),
            (Some(RejectStage::Commit), 5, false),
            (Some(RejectStage::Commit), 517, false),
        ] {
            assert_eq!(
                error(acquire, phase, sql(code)).retryable_ownership_acquire(),
                retry,
                "{phase:?} {code}"
            );
        }
        let unknown = error(
            acquire,
            Some(RejectStage::Acquire),
            Error::Runtime("busy".into()),
        );
        assert!(!unknown.retryable_ownership_acquire());
        let write = error(
            PersistenceOperation::OwnershipWrite,
            Some(RejectStage::Acquire),
            sql(5),
        );
        assert!(!write.retryable_ownership_acquire());
        let wrapped = Error::ownership_checkpoint(
            crate::error::OwnershipStage::Streaming,
            error(acquire, Some(RejectStage::Commit), sql(5)),
        );
        assert!(!wrapped.retryable_ownership_acquire());
    }

    /// The writer fallback label is a closed, serialized operation that
    /// round-trips through refusals with its phase, codes, wait and attempts
    /// and is never replayable, even at the acquire phase with SQLITE_BUSY.
    #[test]
    fn writer_apply_label_round_trips_and_never_retries() {
        assert_eq!(
            serde_json::to_value(PersistenceOperation::WriterApply).unwrap(),
            serde_json::json!("writer_apply")
        );
        assert_eq!(
            serde_json::from_value::<PersistenceOperation>(serde_json::json!("writer_apply"))
                .unwrap(),
            PersistenceOperation::WriterApply
        );
        let error = Error::Persistence {
            operation: PersistenceOperation::WriterApply,
            elapsed_ms: 12,
            attempts: 1,
            phase: Some(RejectStage::Acquire),
            source: Box::new(Error::Sql(rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(5),
                None,
            ))),
        };
        assert!(!error.retryable_ownership_acquire());
        let refusal = StorageRejection::from_error(&error, RejectStage::Statement);
        let diag = refusal.persistence.clone().unwrap();
        assert_eq!(diag.operation, PersistenceOperation::WriterApply);
        assert_eq!(diag.phase, RejectStage::Acquire);
        assert_eq!((diag.wait_ms, diag.attempts), (12, 1));
        match refusal.into_error() {
            Error::Persistence {
                operation,
                phase,
                elapsed_ms,
                ..
            } => {
                assert_eq!(operation, PersistenceOperation::WriterApply);
                assert_eq!(phase, Some(RejectStage::Acquire));
                assert_eq!(elapsed_ms, 12);
            }
            other => panic!("unexpected {other:?}"),
        }
    }
}
