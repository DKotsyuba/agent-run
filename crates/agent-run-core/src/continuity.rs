//! Native conversation evidence for explicit continuation.
//!
//! A resumed attempt attaches a new process to the native session a previous,
//! cleaned-up attempt wrote. The evidence is minted once, by the supervisor at
//! that attempt's cleanup boundary ([`seal`]): the harness's own history file
//! for the exact session, found inside the storage root the attempt actually
//! used, read through an anchored no-follow descriptor with a hard byte bound,
//! structurally checked, and recorded as root, relative path, length, SHA-256
//! and record count. OpenCode seals the private SQLite main/WAL plus settled
//! session/message semantics; its startup rechecks semantics after checkpoints.
//! A resume and the supervisor handoff first [`verify`]
//! that recorded seal against the same file; they never mint a new baseline
//! from whatever history exists later. Every failure is a typed
//! `continuation_unavailable` refusal; the history is only read.
//!
//! Structural checks: every record is one complete line of JSON, the session
//! is named consistently, and native tool calls follow a chronological
//! lifecycle — each recognized call has a nonempty unique id, each result
//! answers exactly one earlier pending call, and no call is left pending.

use crate::{Error, Result, fs};
use agent_run_domain::catalog::HarnessId;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    path::{Component, Path, PathBuf},
};

/// Largest native history file read for a proof (64 MiB).
const MAX_HISTORY_BYTES: usize = 64 * 1024 * 1024;

/// The verified native history of one session, as recorded at an attempt's
/// cleanup boundary and re-checked before any continuation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistorySeal {
    /// Harness whose history format was checked.
    pub harness: HarnessId,
    /// Native session id the history belongs to.
    pub session: String,
    /// Canonical storage root the attempt used (`CODEX_HOME` or the Claude
    /// config directory), or the private OpenCode data root.
    pub root: PathBuf,
    /// History file path relative to `root`, without `..` or links.
    pub relative: PathBuf,
    /// Exact byte length.
    pub bytes: u64,
    /// Lowercase SHA-256 of the exact bytes.
    pub sha256: String,
    /// Number of complete JSONL records or projected SQLite messages.
    pub records: usize,
    /// OpenCode-only WAL presence and semantic evidence; absent in historical seals.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sqlite: Option<SqliteSeal>,
}

/// Exact bytes of an authoritative native SQLite sidecar; SHM is never history.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileSeal {
    /// Bounded byte length, including zero for a present empty WAL.
    pub bytes: u64,
    /// SHA-256 of those exact bytes.
    pub sha256: String,
}

/// Native SQLite continuation evidence bound by the enclosing history seal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SqliteSeal {
    /// `None` proves the WAL held no frames (absent or empty); `Some` proves
    /// its exact nonempty bytes. Older seals may record an empty WAL as
    /// `Some` with zero bytes, which compares equal to `None`.
    pub wal: Option<FileSeal>,
    /// Hash of the settled native session and full projected message rows.
    /// Checked after native startup, whose checkpoint may change physical files.
    pub semantic_sha256: String,
    /// Qualified OpenCode version that wrote the sealed session. Absent only in
    /// seals minted before versions were recorded, which could only seal
    /// [`OPENCODE_LEGACY_VERSION`]; see [`SqliteSeal::opencode_version`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
}

impl SqliteSeal {
    /// The OpenCode version a continuation must run under: the recorded one,
    /// or the only version an unversioned (older) seal could have proven.
    pub fn opencode_version(&self) -> &str {
        self.version.as_deref().unwrap_or(OPENCODE_LEGACY_VERSION)
    }
}

/// The single OpenCode version Run qualified before seals recorded a version.
pub const OPENCODE_LEGACY_VERSION: &str = "2.0.28";

/// Whether a native table or column is part of a qualified OpenCode schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Presence {
    /// Must exist; absence is an incompatible schema, never an empty value.
    Required,
    /// This version is known to lack it. Absence is expected; if a later
    /// migration added it anyway it is still checked.
    KnownAbsent,
}

/// Native SQLite layout of one qualified OpenCode release, as far as the
/// settled-session proof reads it.
#[derive(Debug)]
struct OpenCodeSchema {
    /// Exact `session_v2.version` and `/api/info` version string.
    version: &'static str,
    /// Tables holding a session's not-yet-consumed input; each present one must
    /// have no row for the session.
    pending: &'static [(&'static str, Presence)],
    /// `session_v2.time_suspended`, which must be null when present.
    suspension: Presence,
}

/// OpenCode releases whose server protocol and native session schema are
/// qualified. 2.0.28 stays: its schema and protocol were qualified against a
/// source build, existing seals name it, and no other code path depends on an
/// official binary. 2.3.0 drops `session_pending` (pending input lives only in
/// `session_inbox`) and adds `session_v2.time_suspended`.
const OPENCODE_SCHEMAS: &[OpenCodeSchema] = &[
    OpenCodeSchema {
        version: "2.0.28",
        pending: &[
            ("session_inbox", Presence::Required),
            ("session_pending", Presence::Required),
        ],
        suspension: Presence::KnownAbsent,
    },
    OpenCodeSchema {
        version: "2.3.0",
        pending: &[
            ("session_inbox", Presence::Required),
            ("session_pending", Presence::KnownAbsent),
        ],
        suspension: Presence::Required,
    },
];

/// Returns the qualified OpenCode version set, in ascending release order.
pub fn opencode_qualified_versions() -> impl Iterator<Item = &'static str> {
    OPENCODE_SCHEMAS.iter().map(|schema| schema.version)
}

/// Whether `version` is exactly one qualified OpenCode release.
pub fn opencode_version_qualified(version: &str) -> bool {
    opencode_schema(version).is_some()
}

/// Selects the exact schema of a qualified OpenCode version.
fn opencode_schema(version: &str) -> Option<&'static OpenCodeSchema> {
    OPENCODE_SCHEMAS
        .iter()
        .find(|schema| schema.version == version)
}

/// Builds the typed refusal for an unprovable continuation.
fn unavailable(reason: impl std::fmt::Display) -> Error {
    Error::Unsupported(format!("continuation_unavailable: {reason}"))
}

/// Mints the seal of `session`'s native history in `root`, the storage the
/// finished attempt used: the run's `CODEX_HOME` for Codex
/// (`sessions/**/rollout-*-<id>.jsonl`), the Claude config directory for
/// Claude Code (`projects/*/<id>.jsonl`). Call only at the attempt's cleanup
/// boundary, once its process group is gone.
///
/// OpenCode additionally binds main/WAL file presence and rejects pending
/// inputs, compaction, revert, or unsettled projected assistant/tool state.
/// Refuses with `continuation_unavailable` when the file is missing or
/// ambiguous, not a regular file reachable without links, over the byte
/// bound, truncated, not line-delimited JSON, names another session, or has
/// an invalid or unfinished tool lifecycle.
pub fn seal(harness: HarnessId, root: &Path, session: &str) -> Result<HistorySeal> {
    if session.is_empty() || session.contains(['/', '\0']) || session.contains("..") {
        return Err(unavailable("native session id is not a plain identifier"));
    }
    let root = root
        .canonicalize()
        .map_err(|_| unavailable("native session storage is missing"))?;
    let matches = match harness {
        HarnessId::OpenCode => return seal_sqlite(&root, session),
        HarnessId::Codex => find(&root, Path::new("sessions"), 4, &|name| {
            name.starts_with("rollout-") && name.ends_with(&format!("-{session}.jsonl"))
        }),
        HarnessId::ClaudeCode => find(&root, Path::new("projects"), 1, &|name| {
            name == format!("{session}.jsonl")
        }),
    };
    let [relative] = matches.as_slice() else {
        return Err(unavailable(if matches.is_empty() {
            "no native history for the recorded session"
        } else {
            "native history for the session is ambiguous"
        }));
    };
    let bytes = read(&root, relative)?;
    let records = check(harness, &bytes, session)?;
    Ok(HistorySeal {
        harness,
        session: session.to_owned(),
        root,
        relative: relative.clone(),
        bytes: bytes.len() as u64,
        sha256: fs::sha256(&bytes),
        records,
        sqlite: None,
    })
}

/// Re-reads the sealed file (anchored at the recorded root, no links, bounded)
/// and refuses unless its length and digest equal the seal, it still passes
/// the structural checks, and it belongs to `harness` and `session`.
pub fn verify(seal: &HistorySeal, harness: HarnessId, session: &str) -> Result<()> {
    if seal.harness != harness || seal.session != session {
        return Err(unavailable("recorded history seal names another session"));
    }
    if harness == HarnessId::OpenCode {
        if seal.relative != Path::new("opencode.db") || seal.sqlite.is_none() {
            return Err(unavailable(
                "OpenCode database manifest is missing or foreign",
            ));
        }
        let mut current = seal_sqlite(&seal.root, session)?;
        let mut seal = seal.clone();
        if let Some(recorded) = seal.sqlite.as_mut() {
            recorded.wal = frames(recorded.wal.take());
        }
        // An unversioned seal proves the legacy version through its semantic
        // hash; compare it as recorded instead of failing on the new field.
        if let (Some(recorded), Some(now)) = (seal.sqlite.as_ref(), current.sqlite.as_mut())
            && recorded.version.is_none()
            && now.version.as_deref() == Some(OPENCODE_LEGACY_VERSION)
        {
            now.version = None;
        }
        return if current == seal {
            Ok(())
        } else {
            Err(unavailable("OpenCode history changed since cleanup"))
        };
    }
    if seal.sqlite.is_some() {
        return Err(unavailable("SQLite evidence belongs to another harness"));
    }
    let bytes = read(&seal.root, &seal.relative)?;
    if bytes.len() as u64 != seal.bytes || fs::sha256(&bytes) != seal.sha256 {
        return Err(changed(seal, &bytes));
    }
    check(harness, &bytes, session)?;
    Ok(())
}

/// Builds the refusal for history whose bytes no longer match its seal.
///
/// The reason stays content-free but says whether the sealed bytes are still
/// an intact prefix (something appended to the session after cleanup) or were
/// rewritten (truncated, compacted or edited), with both byte lengths. Either
/// way the continuation is refused; this only makes the cause diagnosable.
fn changed(seal: &HistorySeal, bytes: &[u8]) -> Error {
    let now = bytes.len();
    let prefix = usize::try_from(seal.bytes)
        .ok()
        .and_then(|sealed| bytes.get(..sealed));
    if now as u64 > seal.bytes && prefix.is_some_and(|prefix| fs::sha256(prefix) == seal.sha256) {
        unavailable(format!(
            "native history changed since it was sealed: grew from {} to {now} bytes after the intact sealed prefix",
            seal.bytes
        ))
    } else {
        unavailable(format!(
            "native history changed since it was sealed: rewritten ({} sealed bytes, {now} now)",
            seal.bytes
        ))
    }
}

/// Drops an empty WAL record: an absent and an empty WAL both mean "no frames".
///
/// OpenCode 2.3.0 checkpoints and deletes its WAL on a graceful exit, and the
/// read-only proof connection then recreates an empty one (SQLite creates the
/// WAL/SHM pair for readers when the directory is writable). Neither changes
/// the database content, so both must yield the same evidence.
fn frames(wal: Option<FileSeal>) -> Option<FileSeal> {
    wal.filter(|wal| wal.bytes != 0)
}

/// Reads the private main database and optional WAL through anchored no-follow
/// descriptors under a combined byte bound. No checkpoint or migration is run.
/// An empty WAL is reported as absent; see [`frames`].
fn sqlite_files(root: &Path) -> Result<(FileSeal, Option<FileSeal>)> {
    let directory =
        fs::Dir::open(root).map_err(|_| unavailable("OpenCode data root is unreadable"))?;
    let main = directory
        .read(Path::new("opencode.db"), MAX_HISTORY_BYTES)
        .map_err(|_| unavailable("OpenCode database is missing, linked or oversized"))?;
    if main.is_empty() {
        return Err(unavailable("OpenCode database is empty"));
    }
    directory
        .optional(Path::new("opencode.db-shm"), 1024 * 1024)
        .map_err(|_| unavailable("OpenCode coordination file is linked or invalid"))?;
    let wal = directory
        .optional(Path::new("opencode.db-wal"), MAX_HISTORY_BYTES - main.len())
        .map_err(|_| unavailable("OpenCode WAL is linked or oversized"))?;
    Ok((
        FileSeal {
            bytes: main.len() as u64,
            sha256: fs::sha256(&main),
        },
        frames(wal.map(|bytes| FileSeal {
            bytes: bytes.len() as u64,
            sha256: fs::sha256(&bytes),
        })),
    ))
}

/// Settled-session evidence of one native OpenCode session.
#[derive(Debug, PartialEq, Eq)]
struct SqliteState {
    /// Hash over the selected session columns and full projected messages.
    semantic_sha256: String,
    /// Number of projected messages.
    records: usize,
    /// Qualified version that wrote the session.
    version: String,
}

/// Whether the snapshot's schema has `table`, or `table.column` when given.
fn sqlite_has(
    transaction: &rusqlite::Transaction<'_>,
    table: &str,
    column: Option<&str>,
) -> Result<bool> {
    let count: i64 = match column {
        None => transaction.query_row(
            "SELECT COUNT(*) FROM sqlite_schema WHERE type='table' AND name=?1",
            [table],
            |row| row.get(0),
        ),
        Some(column) => transaction.query_row(
            "SELECT COUNT(*) FROM pragma_table_info(?1) WHERE name=?2",
            [table, column],
            |row| row.get(0),
        ),
    }
    .map_err(|_| unavailable("OpenCode schema cannot be inspected"))?;
    Ok(count != 0)
}

/// Reads a WAL-aware query-only snapshot of one settled native session written
/// by a qualified OpenCode version, using that version's schema. Pending input,
/// suspension, compaction, revert, unfinished assistant/tool output, missing
/// sessions, unqualified versions or incompatible schemas refuse. A table or
/// column may be missing only where the session's version is known to lack it.
/// Full native JSON stays in memory; only its hash/count/version leave this
/// function. No provider credentials are queried.
fn sqlite_state(root: &Path, session: &str) -> Result<SqliteState> {
    let connection = sqlite_access::reader_connection(
        &root.join("opencode.db"),
        std::time::Duration::from_millis(100),
    )
    .map_err(|_| unavailable("OpenCode database cannot be read safely"))?;
    let transaction = connection
        .unchecked_transaction()
        .map_err(|_| unavailable("OpenCode snapshot could not begin"))?;
    let state:String=transaction.query_row("SELECT json_object('id',id,'directory',directory,'agent',agent,'model',model,'permission',permission,'version',version,'idle',time_idle,'outcome',idle_outcome,'compacting',time_compacting,'revert',revert) FROM session_v2 WHERE id=?",[session],|row|row.get(0)).map_err(|_|unavailable("OpenCode session is missing or incompatible"))?;
    let state: Value =
        serde_json::from_str(&state).map_err(|_| unavailable("OpenCode session is malformed"))?;
    let schema = state["version"]
        .as_str()
        .and_then(opencode_schema)
        .ok_or_else(|| unavailable("OpenCode session version is not qualified"))?;
    if state["agent"] != "agent-run"
        || state["idle"].is_null()
        || !state["compacting"].is_null()
        || !state["revert"].is_null()
        || !matches!(
            state["outcome"].as_str(),
            Some("succeeded" | "failed" | "interrupted")
        )
    {
        return Err(unavailable("OpenCode session is not structurally settled"));
    }
    if sqlite_has(&transaction, "session_v2", Some("time_suspended"))? {
        let suspended: bool = transaction
            .query_row(
                "SELECT time_suspended IS NOT NULL FROM session_v2 WHERE id=?1",
                [session],
                |row| row.get(0),
            )
            .map_err(|_| unavailable("OpenCode suspension state is incompatible"))?;
        if suspended {
            return Err(unavailable("OpenCode session is suspended"));
        }
    } else if schema.suspension == Presence::Required {
        return Err(unavailable("OpenCode suspension state is incompatible"));
    }
    for (table, presence) in schema.pending {
        if !sqlite_has(&transaction, table, None)? {
            if *presence == Presence::Required {
                return Err(unavailable("OpenCode pending-input state is incompatible"));
            }
            continue;
        }
        // Table names come only from the static qualified schema table above.
        let pending: i64 = transaction
            .query_row(
                &format!("SELECT COUNT(*) FROM {table} WHERE session_id=?1"),
                [session],
                |row| row.get(0),
            )
            .map_err(|_| unavailable("OpenCode pending-input state is incompatible"))?;
        if pending != 0 {
            return Err(unavailable("OpenCode session has pending input"));
        }
    }
    let mut statement=transaction.prepare("SELECT id,type,seq,data FROM session_message WHERE session_id=? ORDER BY seq,id LIMIT 4097").map_err(|_|unavailable("OpenCode message schema is incompatible"))?;
    let mut cursor = statement
        .query([session])
        .map_err(|_| unavailable("OpenCode messages are unreadable"))?;
    let mut messages = Vec::new();
    let mut bytes = 0usize;
    while let Some(row) = cursor
        .next()
        .map_err(|_| unavailable("OpenCode message scan failed"))?
    {
        let id: String = row
            .get(0)
            .map_err(|_| unavailable("OpenCode message id is missing"))?;
        let kind: String = row
            .get(1)
            .map_err(|_| unavailable("OpenCode message type is missing"))?;
        let sequence: i64 = row
            .get(2)
            .map_err(|_| unavailable("OpenCode message order is missing"))?;
        let raw: String = row
            .get(3)
            .map_err(|_| unavailable("OpenCode message data is missing"))?;
        bytes = bytes.saturating_add(raw.len());
        if messages.len() >= 4096 || bytes > MAX_HISTORY_BYTES {
            return Err(unavailable("OpenCode projected history exceeds bound"));
        }
        let data: Value =
            serde_json::from_str(&raw).map_err(|_| unavailable("OpenCode message is malformed"))?;
        if kind == "assistant" {
            if data.pointer("/time/completed").is_none_or(Value::is_null) {
                return Err(unavailable("OpenCode assistant output is unfinished"));
            }
            let parts = data["content"]
                .as_array()
                .ok_or_else(|| unavailable("OpenCode assistant content is missing"))?;
            let mut tools = std::collections::BTreeSet::new();
            for part in parts.iter().filter(|part| part["type"] == "tool") {
                let id = part["id"]
                    .as_str()
                    .filter(|id| !id.is_empty())
                    .ok_or_else(|| unavailable("OpenCode tool id is missing"))?;
                if !tools.insert(id)
                    || !matches!(
                        part.pointer("/state/status").and_then(Value::as_str),
                        Some("completed" | "error")
                    )
                {
                    return Err(unavailable("OpenCode tool lifecycle is unresolved"));
                }
            }
        }
        messages.push(serde_json::json!({"id":id,"type":kind,"seq":sequence,"data":data}));
    }
    if messages.is_empty() {
        return Err(unavailable("OpenCode history has no projected input"));
    }
    Ok(SqliteState {
        semantic_sha256: fs::sha256(
            &serde_json::to_vec(&(state, &messages))
                .map_err(|_| unavailable("OpenCode history cannot be hashed"))?,
        ),
        records: messages.len(),
        version: schema.version.to_owned(),
    })
}

/// Mints physical and semantic SQLite evidence only at verified process cleanup.
/// A second no-follow manifest read rejects files changing during the SQL snapshot.
fn seal_sqlite(root: &Path, session: &str) -> Result<HistorySeal> {
    let (main, wal) = sqlite_files(root)?;
    let state = sqlite_state(root, session)?;
    if sqlite_files(root)? != (main.clone(), wal.clone()) {
        return Err(unavailable("OpenCode database changed during sealing"));
    }
    Ok(HistorySeal {
        harness: HarnessId::OpenCode,
        session: session.into(),
        root: root.into(),
        relative: "opencode.db".into(),
        bytes: main.bytes,
        sha256: main.sha256,
        records: state.records,
        sqlite: Some(SqliteSeal {
            wal,
            semantic_sha256: state.semantic_sha256,
            version: Some(state.version),
        }),
    })
}

/// Rechecks the same logical session after owned native startup, before input.
/// Physical files may have checkpointed after the supervisor's pre-start verify;
/// native session/message data, row count, root and identity must remain equal.
/// `running` is the verified `/api/info` version of the started server: a
/// session sealed under one OpenCode version never resumes under another.
pub fn verify_opencode_started(
    seal: &HistorySeal,
    root: &Path,
    session: &str,
    running: &str,
) -> Result<()> {
    if seal.harness != HarnessId::OpenCode
        || seal.session != session
        || root.canonicalize().ok().as_deref() != Some(seal.root.as_path())
        || seal.relative != Path::new("opencode.db")
    {
        return Err(unavailable(
            "OpenCode startup history belongs to another session/root",
        ));
    }
    let expected = seal
        .sqlite
        .as_ref()
        .ok_or_else(|| unavailable("OpenCode semantic proof is missing"))?;
    if running != expected.opencode_version() {
        return Err(unavailable(format!(
            "OpenCode version changed since the session was sealed ({} sealed, {running} running)",
            expected.opencode_version()
        )));
    }
    let state = sqlite_state(root, session)?;
    if state.semantic_sha256 != expected.semantic_sha256
        || state.records != seal.records
        || state.version != expected.opencode_version()
    {
        return Err(unavailable("OpenCode session changed during startup"));
    }
    Ok(())
}

/// Reports whether sealed native history contains this attempt's admitted input.
/// Codex binds the `turn/start` id to its exact wire text; Claude binds the
/// runner-supplied user-message UUID to its single exact text block.
///
/// Provenance is the turn id, never text equality, so an older turn with
/// identical text (an explicit-resume parent, a repeated control text) can
/// never stand in for this one. The file is re-read and must still match
/// the seal; a changed or unreadable history is an error, not `false`.
/// `false` means the turn never reached native history (for example a
/// usage-limit rejection before the user input was recorded).
pub fn admitted_turn_recorded(seal: &HistorySeal, turn: &str, input_sha256: &str) -> Result<bool> {
    if !matches!(seal.harness, HarnessId::Codex | HarnessId::ClaudeCode) || turn.is_empty() {
        return Ok(false);
    }
    let bytes = read(&seal.root, &seal.relative)?;
    if bytes.len() as u64 != seal.bytes || fs::sha256(&bytes) != seal.sha256 {
        return Err(changed(seal, &bytes));
    }
    let text =
        std::str::from_utf8(&bytes).map_err(|_| unavailable("native history is not UTF-8"))?;
    let mut claude_seen = false;
    let mut claude_admitted = false;
    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        let record: Value = serde_json::from_str(line)
            .map_err(|_| unavailable("native history has a malformed record"))?;
        let admitted = match seal.harness {
            HarnessId::Codex => {
                let payload = &record["payload"];
                record["type"] == "response_item"
                    && payload["type"] == "message"
                    && payload["role"] == "user"
                    && payload["internal_chat_message_metadata_passthrough"]["turn_id"] == turn
                    && payload["content"].as_array().is_some_and(|items| {
                        items.iter().any(|item| {
                            item["type"] == "input_text"
                                && item["text"]
                                    .as_str()
                                    .is_some_and(|text| fs::sha256(text.as_bytes()) == input_sha256)
                        })
                    })
            }
            HarnessId::ClaudeCode => {
                if record["type"] == "user" && record["uuid"] == turn {
                    if claude_seen {
                        return Err(unavailable("native history repeats the admitted user ID"));
                    }
                    claude_seen = true;
                    claude_admitted = record["sessionId"] == seal.session
                        && record["isSidechain"] != true
                        && record["message"]["role"] == "user"
                        && record["message"]["content"]
                            .as_array()
                            .is_some_and(|items| {
                                matches!(items.as_slice(), [item]
                                if item["type"] == "text"
                                    && item["text"].as_str().is_some_and(|text|
                                        fs::sha256(text.as_bytes()) == input_sha256))
                            });
                }
                false
            }
            HarnessId::OpenCode => false,
        };
        if admitted {
            return Ok(true);
        }
    }
    Ok(claude_admitted)
}

/// Reads `relative` under `root` through the anchored no-follow directory
/// primitive, refusing escapes, links, non-regular files and more than the
/// byte bound (the bound applies to the bytes actually read).
fn read(root: &Path, relative: &Path) -> Result<Vec<u8>> {
    if relative.is_absolute()
        || relative
            .components()
            .any(|part| !matches!(part, Component::Normal(_)))
    {
        return Err(unavailable("native history path escapes its storage root"));
    }
    let bytes = fs::Dir::open(root)
        .and_then(|dir| dir.read(relative, MAX_HISTORY_BYTES))
        .map_err(|_| unavailable("native history is unreadable, linked or oversized"))?;
    if bytes.is_empty() {
        return Err(unavailable("native history is empty"));
    }
    Ok(bytes)
}

/// Lists paths (relative to `root`) under `start`, at most `depth` directory
/// levels deep and never following directory links, whose file name
/// satisfies `wanted`.
fn find(root: &Path, start: &Path, depth: usize, wanted: &dyn Fn(&str) -> bool) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let Ok(entries) = std::fs::read_dir(root.join(start)) else {
        return found;
    };
    for entry in entries.flatten() {
        let Ok(kind) = entry.file_type() else {
            continue;
        };
        let relative = start.join(entry.file_name());
        if kind.is_dir() && depth > 0 {
            found.extend(find(root, &relative, depth - 1, wanted));
        } else if !kind.is_dir() && entry.file_name().to_str().is_some_and(wanted) {
            found.push(relative);
        }
    }
    found
}

/// Parses complete JSON lines and applies the harness's structural checks;
/// returns the record count.
fn check(harness: HarnessId, bytes: &[u8], session: &str) -> Result<usize> {
    if bytes.last() != Some(&b'\n') {
        return Err(unavailable("native history is truncated"));
    }
    let text =
        std::str::from_utf8(bytes).map_err(|_| unavailable("native history is not UTF-8"))?;
    let mut records = Vec::new();
    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        records.push(
            serde_json::from_str::<Value>(line)
                .map_err(|_| unavailable("native history has a malformed record"))?,
        );
    }
    match harness {
        HarnessId::Codex => codex(&records, session)?,
        HarnessId::ClaudeCode => claude(&records, session)?,
        HarnessId::OpenCode => return Err(unavailable("OpenCode does not use JSONL history")),
    }
    Ok(records.len())
}

/// One recognized native tool record in history order.
enum Tool<'a> {
    /// A call that opens a lifecycle; `None` when its id is absent.
    Call(Option<&'a str>),
    /// A result that must close an earlier pending call.
    Result(Option<&'a str>),
}

/// Checks tool lifecycles chronologically: ids must be nonempty and unique
/// per call, every result must answer an earlier still-pending call, and no
/// call may remain pending at the end.
fn lifecycle<'a>(tools: impl Iterator<Item = Tool<'a>>) -> Result<()> {
    let mut state: BTreeMap<&str, bool> = BTreeMap::new();
    for tool in tools {
        match tool {
            Tool::Call(Some(id)) if !id.is_empty() => {
                if state.insert(id, true).is_some() {
                    return Err(unavailable("native history repeats a tool call id"));
                }
            }
            Tool::Result(Some(id)) if !id.is_empty() => match state.get_mut(id) {
                Some(pending @ true) => *pending = false,
                _ => {
                    return Err(unavailable(
                        "native history has a tool result without an earlier pending call",
                    ));
                }
            },
            _ => {
                return Err(unavailable(
                    "native history has a tool record without an id",
                ));
            }
        }
    }
    if state.values().any(|pending| *pending) {
        return Err(unavailable("native history has an unresolved tool call"));
    }
    Ok(())
}

/// Codex rollout: every `session_meta` record must name `session` (at least
/// one must); `function_call`/`custom_tool_call`/`local_shell_call` response
/// items open lifecycles that `function_call_output`/`custom_tool_call_output`
/// items close, by `call_id`.
fn codex(records: &[Value], session: &str) -> Result<()> {
    let metas: Vec<&Value> = records
        .iter()
        .filter(|record| record["type"] == "session_meta")
        .collect();
    if metas.is_empty() || metas.iter().any(|meta| meta["payload"]["id"] != session) {
        return Err(unavailable("native history belongs to another session"));
    }
    lifecycle(
        records
            .iter()
            .filter(|record| record["type"] == "response_item")
            .map(|record| &record["payload"])
            .filter_map(|payload| {
                let id = payload["call_id"].as_str();
                match payload["type"].as_str()? {
                    "function_call" | "custom_tool_call" | "local_shell_call" => {
                        Some(Tool::Call(id))
                    }
                    "function_call_output" | "custom_tool_call_output" => Some(Tool::Result(id)),
                    _ => None,
                }
            }),
    )
}

/// Claude transcript: every recorded session id must match, real tool calls
/// must close, and API request snapshots may only repeat known tool events.
fn claude(records: &[Value], session: &str) -> Result<()> {
    let ids: Vec<&Value> = records
        .iter()
        .filter_map(|record| record.get("sessionId"))
        .collect();
    if ids.is_empty() || ids.iter().any(|id| *id != session) {
        return Err(unavailable("native history belongs to another session"));
    }
    claude_request_snapshots(records)?;
    lifecycle(
        records
            .iter()
            .filter(|record| record["type"] != "api-request-blob")
            .filter_map(|record| record["message"]["content"].as_array())
            .flatten()
            .filter_map(|item| match item["type"].as_str()? {
                "tool_use" => Some(Tool::Call(item["id"].as_str())),
                "tool_result" => Some(Tool::Result(item["tool_use_id"].as_str())),
                _ => None,
            }),
    )
}

/// Checks repeated API-request tool records against earlier real conversation
/// events; snapshots cannot introduce or settle a tool call of their own.
/// Each real event is remembered with its record's `cwd`, which bounds the
/// only accepted snapshot rewrite (see [`same_claude_snapshot_call`]).
fn claude_request_snapshots(records: &[Value]) -> Result<()> {
    let mut seen: BTreeMap<(&str, &str), (&Value, Option<&str>)> = BTreeMap::new();
    let mut conversation = false;
    for record in records {
        let snapshot = record["type"] == "api-request-blob";
        if !snapshot
            && match &record["message"]["content"] {
                Value::String(text) => !text.is_empty(),
                Value::Array(items) => !items.is_empty(),
                _ => false,
            }
        {
            conversation = true;
        }
        let Some(items) = record["message"]["content"].as_array() else {
            continue;
        };
        for item in items {
            let (kind, id) = match item["type"].as_str() {
                Some("tool_use") => ("tool_use", item["id"].as_str()),
                Some("tool_result") => ("tool_result", item["tool_use_id"].as_str()),
                _ => continue,
            };
            if !snapshot {
                if let Some(id) = id.filter(|id| !id.is_empty()) {
                    seen.entry((kind, id))
                        .or_insert((item, record["cwd"].as_str()));
                }
                continue;
            }
            let &(previous, cwd) = id
                .filter(|id| !id.is_empty())
                .and_then(|id| seen.get(&(kind, id)))
                .ok_or_else(|| {
                    unavailable("Claude request snapshot references an unrecorded tool event")
                })?;
            if kind == "tool_use" && !same_claude_snapshot_call(previous, cwd, item) {
                return Err(unavailable("Claude request snapshot changes a tool call"));
            }
            if kind == "tool_result"
                && !matches!(
                    (claude_tool_error(previous), claude_tool_error(item)),
                    (Some(first), Some(second)) if first == second
                )
            {
                return Err(unavailable(
                    "Claude request snapshot changes a tool result status",
                ));
            }
        }
    }
    if !conversation {
        return Err(unavailable("Claude history has no conversation messages"));
    }
    Ok(())
}

/// Compares a request-snapshot tool call with the recorded call while ignoring
/// Claude's caller metadata.
///
/// Claude Code (observed in 2.1.311 and 2.1.322) normalizes some recorded
/// calls when it replays them in an API request snapshot. Exactly two
/// semantics-preserving rewrites are accepted (see
/// [`same_claude_snapshot_input`]); every other field, input key, tool and
/// value must still match exactly, so a snapshot can neither introduce nor
/// alter a tool call.
fn same_claude_snapshot_call(recorded: &Value, cwd: Option<&str>, snapshot: &Value) -> bool {
    let (Some(first), Some(second)) = (recorded.as_object(), snapshot.as_object()) else {
        return false;
    };
    first
        .keys()
        .chain(second.keys())
        .filter(|key| key.as_str() != "caller")
        .all(|key| {
            first.get(key) == second.get(key)
                || (key == "input" && same_claude_snapshot_input(first, cwd, second))
        })
}

/// Whether a snapshot's tool input differs from the recorded input only by one
/// observed Claude Code normalization of the same call:
///
/// - `Bash`: `command` becomes exactly `cd <cwd> && <recorded command>`, where
///   `<cwd>` is the absolute `cwd` of the transcript record holding the real
///   call (the same command in the same directory). No other directory,
///   quoting or separator is accepted, nor a call recorded without a `cwd`.
/// - `Edit`: a recorded `replace_all: false`, the tool's default, is omitted.
///
/// Both calls must name the same tool and every other input key must be equal.
fn same_claude_snapshot_input(
    recorded: &serde_json::Map<String, Value>,
    cwd: Option<&str>,
    snapshot: &serde_json::Map<String, Value>,
) -> bool {
    let (Some(first), Some(second)) = (
        recorded.get("input").and_then(Value::as_object),
        snapshot.get("input").and_then(Value::as_object),
    ) else {
        return false;
    };
    let name = recorded.get("name").and_then(Value::as_str);
    if name != snapshot.get("name").and_then(Value::as_str) {
        return false;
    }
    // The single input key a normalization may change, and whether it holds.
    let (key, normalized) = match name {
        Some("Bash") => {
            let replayed = second.get("command").and_then(Value::as_str);
            let original = first.get("command").and_then(Value::as_str);
            let prefixed = cwd.filter(|cwd| cwd.starts_with('/')).and_then(|cwd| {
                replayed?
                    .strip_prefix("cd ")?
                    .strip_prefix(cwd)?
                    .strip_prefix(" && ")
            });
            ("command", original.is_some() && prefixed == original)
        }
        Some("Edit") => (
            "replace_all",
            first.get("replace_all") == Some(&Value::Bool(false))
                && !second.contains_key("replace_all"),
        ),
        _ => return false,
    };
    normalized
        && first
            .keys()
            .chain(second.keys())
            .filter(|other| other.as_str() != key)
            .all(|other| first.get(other) == second.get(other))
}

/// Treats an absent tool-error flag as false; a malformed flag remains unknown.
fn claude_tool_error(item: &Value) -> Option<bool> {
    match item.get("is_error") {
        None => Some(false),
        Some(value) => value.as_bool(),
    }
}

/// Compatibility name for [`seal`] used by the structural unit tests.
#[cfg(test)]
fn prove(harness: HarnessId, root: &Path, session: &str) -> Result<HistorySeal> {
    seal(harness, root, session)
}

#[cfg(test)]
mod tests {
    use super::{
        HarnessId, HistorySeal, Path, opencode_qualified_versions, opencode_version_qualified,
        prove, seal, verify, verify_opencode_started,
    };
    use std::fs;

    /// Writes one Codex rollout for `session` with `body` records.
    fn rollout(root: &std::path::Path, session: &str, body: &str) -> std::path::PathBuf {
        let dir = root.join("sessions/2026/09/23");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!("rollout-2026-09-23T00-00-00-{session}.jsonl"));
        fs::write(&path, body).unwrap();
        path
    }

    /// A complete Codex rollout for its session proves; tampering, another
    /// session, truncation, an open tool call, absence and an escaping link
    /// each refuse as `continuation_unavailable`.
    #[test]
    fn codex_history_must_be_complete_and_owned() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let session = "019a-thread";
        let meta = format!("{{\"type\":\"session_meta\",\"payload\":{{\"id\":\"{session}\"}}}}\n");
        let call = "{\"type\":\"response_item\",\"payload\":{\"type\":\"function_call\",\"call_id\":\"c1\"}}\n";
        let output = "{\"type\":\"response_item\",\"payload\":{\"type\":\"function_call_output\",\"call_id\":\"c1\"}}\n";
        let path = rollout(root, session, &format!("{meta}{call}{output}"));
        assert_eq!(prove(HarnessId::Codex, root, session).unwrap().records, 3);
        let refused = |body: &str| {
            fs::write(&path, body).unwrap();
            let error = prove(HarnessId::Codex, root, session)
                .unwrap_err()
                .to_string();
            assert!(error.contains("continuation_unavailable"), "{error}");
        };
        refused(&format!("{meta}{call}"));
        refused(&format!("{meta}{call}{}", output.trim_end()));
        refused("{\"type\":\"session_meta\",\"payload\":{\"id\":\"other\"}}\n");
        refused(&format!("{meta}not json\n"));
        fs::remove_file(&path).unwrap();
        assert!(prove(HarnessId::Codex, root, session).is_err());
        let outside = tempfile::tempdir().unwrap();
        let foreign = outside.path().join("elsewhere.jsonl");
        fs::write(&foreign, &meta).unwrap();
        std::os::unix::fs::symlink(&foreign, &path).unwrap();
        assert!(prove(HarnessId::Codex, root, session).is_err());
    }

    /// A Claude transcript proves only for its own session with every
    /// `tool_use` answered.
    #[test]
    fn claude_history_must_be_complete_and_owned() {
        let temp = tempfile::tempdir().unwrap();
        let dir = temp.path().join("projects/-tmp-work");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("s1.jsonl");
        let used = "{\"sessionId\":\"s1\",\"message\":{\"content\":[{\"type\":\"tool_use\",\"id\":\"t1\"}]}}\n";
        let result = "{\"sessionId\":\"s1\",\"message\":{\"content\":[{\"type\":\"tool_result\",\"tool_use_id\":\"t1\"}]}}\n";
        fs::write(&path, format!("{used}{result}")).unwrap();
        assert!(prove(HarnessId::ClaudeCode, temp.path(), "s1").is_ok());
        fs::write(&path, used).unwrap();
        assert!(prove(HarnessId::ClaudeCode, temp.path(), "s1").is_err());
        fs::write(&path, "{\"sessionId\":\"s2\"}\n").unwrap();
        assert!(prove(HarnessId::ClaudeCode, temp.path(), "s1").is_err());
        assert!(prove(HarnessId::ClaudeCode, temp.path(), "../s1").is_err());
    }

    /// API request snapshots may repeat completed tools, but cannot prove an
    /// unrecorded call, close a pending call, or weaken the sealed bytes.
    #[test]
    fn claude_request_blobs_do_not_replay_or_hide_tool_events() {
        let temp = tempfile::tempdir().unwrap();
        let dir = temp.path().join("projects/-tmp-work");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("s1.jsonl");
        let record = |kind: &str, item: serde_json::Value| serde_json::json!({"sessionId":"s1","type":kind,"message":{"content":[item]}});
        let call =
            serde_json::json!({"type":"tool_use","id":"t1","name":"fixture","input":{"x":1}});
        let result = serde_json::json!({"type":"tool_result","tool_use_id":"t1","content":"ok"});
        let blob_call = serde_json::json!({"type":"tool_use","id":"t1","name":"fixture","input":{"x":1},"caller":{"type":"direct"}});
        let blob_result = serde_json::json!({"type":"tool_result","tool_use_id":"t1","content":[{"type":"text","text":"ok"}]});
        let write = |records: &[serde_json::Value]| {
            let body: String = records
                .iter()
                .map(|r| format!("{}\n", serde_json::to_string(r).unwrap()))
                .collect();
            fs::write(&path, body).unwrap();
        };
        let valid = vec![
            record("assistant", call.clone()),
            record("user", result.clone()),
            record("api-request-blob", blob_call.clone()),
            record("api-request-blob", blob_result.clone()),
            record("api-request-blob", blob_result.clone()),
        ];
        write(&valid);
        let sealed = seal(HarnessId::ClaudeCode, temp.path(), "s1").unwrap();
        assert!(verify(&sealed, HarnessId::ClaudeCode, "s1").is_ok());
        let mut altered = valid.clone();
        altered[4]["message"]["content"][0]["content"] = serde_json::json!("changed");
        write(&altered);
        assert!(verify(&sealed, HarnessId::ClaudeCode, "s1").is_err());

        let mut changed_call = blob_call.clone();
        changed_call["input"]["x"] = serde_json::json!(2);
        let mut changed_result = blob_result.clone();
        changed_result["is_error"] = serde_json::json!(true);
        let mut invalid_result = blob_result.clone();
        invalid_result["is_error"] = serde_json::json!("invalid");
        let mut wrong_session = record("api-request-blob", blob_result.clone());
        wrong_session["sessionId"] = serde_json::json!("s2");
        let cases = [
            vec![record(
                "api-request-blob",
                serde_json::json!({"type":"text","text":"snapshot only"}),
            )],
            vec![
                serde_json::json!({"sessionId":"s1","type":"progress","message":{}}),
                record(
                    "api-request-blob",
                    serde_json::json!({"type":"text","text":"snapshot only"}),
                ),
            ],
            vec![
                record("assistant", call.clone()),
                record("api-request-blob", blob_call.clone()),
            ],
            vec![
                record("api-request-blob", blob_call.clone()),
                record("api-request-blob", blob_result.clone()),
            ],
            vec![
                record("assistant", call.clone()),
                record("api-request-blob", blob_result.clone()),
            ],
            vec![
                record("api-request-blob", blob_call.clone()),
                record("user", result.clone()),
            ],
            vec![
                record("assistant", call.clone()),
                record("assistant", call.clone()),
                record("user", result.clone()),
            ],
            vec![
                record("assistant", call.clone()),
                record("user", result.clone()),
                record("user", result.clone()),
            ],
            vec![
                record("user", result.clone()),
                record("assistant", call.clone()),
            ],
            vec![
                record("assistant", call.clone()),
                record("user", result.clone()),
                record("api-request-blob", changed_call),
            ],
            vec![
                record("assistant", call.clone()),
                record("user", result.clone()),
                record("api-request-blob", changed_result),
            ],
            vec![
                record("assistant", call.clone()),
                record("user", result.clone()),
                record("api-request-blob", invalid_result),
            ],
            vec![
                record("assistant", call.clone()),
                record("user", result.clone()),
                record("api-request-blob", serde_json::json!({"type":"tool_use"})),
            ],
            vec![
                record("assistant", call.clone()),
                record("user", result.clone()),
                record("api-request-blob", blob_call.clone()),
                record("assistant", call.clone()),
            ],
            vec![
                record("assistant", call.clone()),
                record("user", result.clone()),
                wrong_session,
            ],
        ];
        for records in cases {
            write(&records);
            assert!(seal(HarnessId::ClaudeCode, temp.path(), "s1").is_err());
        }
        write(&[serde_json::json!({
            "sessionId":"s1", "type":"user", "message":{"content":"plain task"}
        })]);
        assert!(seal(HarnessId::ClaudeCode, temp.path(), "s1").is_ok());
    }

    /// Regression for `history_unverifiable` on Claude resumes: Claude Code
    /// replays a recorded `Bash` call in its request snapshot as
    /// `cd <record cwd> && <command>` and drops an `Edit` call's default
    /// `replace_all: false`. Those exact rewrites seal and verify (and
    /// tampering after the seal is still refused); any other change to the
    /// replayed call still refuses at the cleanup boundary.
    #[test]
    fn claude_request_blobs_may_apply_observed_harness_normalizations() {
        let temp = tempfile::tempdir().unwrap();
        let dir = temp.path().join("projects/-work-repo");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("s1.jsonl");
        let cwd = "/work/repo";
        let call = |name: &str, input: serde_json::Value| serde_json::json!({"type":"tool_use","id":"t1","name":name,"input":input});
        let assistant = |item: serde_json::Value, cwd: Option<&str>| {
            let mut record = serde_json::json!({"sessionId":"s1","type":"assistant",
                "message":{"role":"assistant","content":[item]}});
            if let Some(cwd) = cwd {
                record["cwd"] = serde_json::json!(cwd);
            }
            record
        };
        let result = serde_json::json!({"sessionId":"s1","type":"user","cwd":cwd,
            "message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"t1","content":"ok"}]}});
        let blob = |item: serde_json::Value| {
            serde_json::json!({"sessionId":"s1","type":"api-request-blob","hash":"h",
                "message":{"content":[item]}})
        };
        let write = |records: &[serde_json::Value]| {
            let body: String = records
                .iter()
                .map(|r| format!("{}\n", serde_json::to_string(r).unwrap()))
                .collect();
            fs::write(&path, body).unwrap();
        };
        let bash = |command: &str| {
            call(
                "Bash",
                serde_json::json!({"command":command,"description":"List files"}),
            )
        };
        let history =
            |recorded: serde_json::Value, cwd: Option<&str>, replay: serde_json::Value| {
                vec![assistant(recorded, cwd), result.clone(), blob(replay)]
            };

        // The observed harness shape seals, verifies, and still detects later edits.
        write(&history(
            bash("git status"),
            Some(cwd),
            bash("cd /work/repo && git status"),
        ));
        let sealed = seal(HarnessId::ClaudeCode, temp.path(), "s1").unwrap();
        assert!(verify(&sealed, HarnessId::ClaudeCode, "s1").is_ok());
        write(&history(
            bash("git status"),
            Some(cwd),
            bash("cd /work/repo && git status --short"),
        ));
        assert!(verify(&sealed, HarnessId::ClaudeCode, "s1").is_err());
        // A verbatim replay with caller metadata remains accepted.
        let mut with_caller = bash("git status");
        with_caller["caller"] = serde_json::json!({"type":"direct"});
        write(&history(bash("git status"), Some(cwd), with_caller));
        assert!(seal(HarnessId::ClaudeCode, temp.path(), "s1").is_ok());
        // Edit: the default `replace_all: false` may be omitted on replay.
        let edit = |input: serde_json::Value| call("Edit", input);
        let edited = serde_json::json!({"file_path":"/work/repo/a","old_string":"x",
            "new_string":"y","replace_all":false});
        let mut replayed_edit = edited.clone();
        replayed_edit.as_object_mut().unwrap().remove("replace_all");
        write(&history(
            edit(edited.clone()),
            Some(cwd),
            edit(replayed_edit.clone()),
        ));
        assert!(seal(HarnessId::ClaudeCode, temp.path(), "s1").is_ok());
        let mut replace_every = edited.clone();
        replace_every["replace_all"] = serde_json::json!(true);
        let mut changed_edit = replayed_edit.clone();
        changed_edit["new_string"] = serde_json::json!("z");
        let mut replayed_true = replayed_edit.clone();
        replayed_true["replace_all"] = serde_json::json!(true);

        let mut extra_key = bash("cd /work/repo && git status");
        extra_key["input"]["timeout"] = serde_json::json!(1);
        let mut changed_description = bash("cd /work/repo && git status");
        changed_description["input"]["description"] = serde_json::json!("Other");
        let mut renamed = bash("cd /work/repo && git status");
        renamed["name"] = serde_json::json!("Shell");
        let refused = [
            // Another directory, a changed command, or other separators.
            history(
                bash("git status"),
                Some(cwd),
                bash("cd /elsewhere && git status"),
            ),
            history(
                bash("git status"),
                Some(cwd),
                bash("cd /work/repo && rm -rf ."),
            ),
            history(
                bash("git status"),
                Some(cwd),
                bash("cd /work/repo; git status"),
            ),
            history(
                bash("git status"),
                Some(cwd),
                bash("cd '/work/repo' && git status"),
            ),
            history(
                bash("git status"),
                Some(cwd),
                bash("cd /work/repo && git status && rm x"),
            ),
            history(bash("git status"), Some(cwd), bash("rm x && git status")),
            // No recorded or a relative directory proves nothing.
            history(
                bash("git status"),
                None,
                bash("cd /work/repo && git status"),
            ),
            history(
                bash("git status"),
                Some("work/repo"),
                bash("cd work/repo && git status"),
            ),
            // Only `command` may differ, and only for Bash.
            history(bash("git status"), Some(cwd), extra_key),
            history(bash("git status"), Some(cwd), changed_description),
            history(bash("git status"), Some(cwd), renamed),
            history(
                call("Read", serde_json::json!({"command":"git status"})),
                Some(cwd),
                call(
                    "Read",
                    serde_json::json!({"command":"cd /work/repo && git status"}),
                ),
            ),
            // Edit may drop only a recorded `replace_all: false`, nothing else.
            history(edit(replace_every), Some(cwd), edit(replayed_edit.clone())),
            history(edit(edited.clone()), Some(cwd), edit(changed_edit)),
            history(edit(edited.clone()), Some(cwd), edit(replayed_true)),
            history(edit(replayed_edit.clone()), Some(cwd), edit(edited.clone())),
            history(
                call("Write", edited.clone()),
                Some(cwd),
                call("Write", replayed_edit.clone()),
            ),
            // The real record must not be the rewritten one.
            history(
                bash("cd /work/repo && git status"),
                Some(cwd),
                bash("git status"),
            ),
        ];
        for records in refused {
            write(&records);
            let error = seal(HarnessId::ClaudeCode, temp.path(), "s1")
                .unwrap_err()
                .to_string();
            assert!(
                error.contains("Claude request snapshot changes a tool call"),
                "{error}"
            );
        }
    }

    /// A result preceding its tool call cannot prove that later call finished.
    #[test]
    fn tool_results_must_follow_the_calls_they_acknowledge() {
        let temp = tempfile::tempdir().unwrap();
        let body = concat!(
            "{\"type\":\"session_meta\",\"payload\":{\"id\":\"s1\"}}\n",
            "{\"type\":\"response_item\",\"payload\":{\"type\":\"function_call_output\",\"call_id\":\"c1\"}}\n",
            "{\"type\":\"response_item\",\"payload\":{\"type\":\"function_call\",\"call_id\":\"c1\"}}\n"
        );
        rollout(temp.path(), "s1", body);
        assert!(
            prove(HarnessId::Codex, temp.path(), "s1").is_err(),
            "an earlier result must not acknowledge a later pending call"
        );
    }

    /// Codex and Claude lifecycles reject duplicate call ids, results without
    /// an earlier pending call, missing or empty ids, and pending calls.
    #[test]
    fn tool_lifecycles_are_chronological_and_unambiguous() {
        let temp = tempfile::tempdir().unwrap();
        let meta = "{\"type\":\"session_meta\",\"payload\":{\"id\":\"s1\"}}\n";
        let item = |kind: &str, id: Option<&str>| match id {
            Some(id) => format!(
                "{{\"type\":\"response_item\",\"payload\":{{\"type\":\"{kind}\",\"call_id\":\"{id}\"}}}}\n"
            ),
            None => format!("{{\"type\":\"response_item\",\"payload\":{{\"type\":\"{kind}\"}}}}\n"),
        };
        let codex = |body: String| {
            rollout(temp.path(), "s1", &format!("{meta}{body}"));
            prove(HarnessId::Codex, temp.path(), "s1")
        };
        assert!(
            codex(
                item("custom_tool_call", Some("a")) + &item("custom_tool_call_output", Some("a"))
            )
            .is_ok()
        );
        assert!(
            codex(
                item("function_call", Some("a"))
                    + &item("function_call", Some("a"))
                    + &item("function_call_output", Some("a"))
            )
            .is_err()
        );
        assert!(
            codex(
                item("function_call", Some("a"))
                    + &item("function_call_output", Some("a"))
                    + &item("function_call_output", Some("a"))
            )
            .is_err()
        );
        assert!(codex(item("function_call", None) + &item("function_call_output", None)).is_err());
        assert!(
            codex(item("function_call", Some("")) + &item("function_call_output", Some("")))
                .is_err()
        );
        assert!(codex(item("function_call_output", Some("z"))).is_err());
        assert!(codex(item("local_shell_call", Some("b"))).is_err());

        let dir = temp.path().join("projects/-w");
        fs::create_dir_all(&dir).unwrap();
        let claude = |content: &[&str]| {
            let body: String = content
                .iter()
                .map(|item| {
                    format!("{{\"sessionId\":\"c1\",\"message\":{{\"content\":[{item}]}}}}\n")
                })
                .collect();
            fs::write(dir.join("c1.jsonl"), body).unwrap();
            prove(HarnessId::ClaudeCode, temp.path(), "c1")
        };
        let used = "{\"type\":\"tool_use\",\"id\":\"t1\"}";
        let result = "{\"type\":\"tool_result\",\"tool_use_id\":\"t1\"}";
        assert!(claude(&[used, result]).is_ok());
        assert!(claude(&[result, used]).is_err());
        assert!(claude(&[used, used, result]).is_err());
        assert!(claude(&["{\"type\":\"tool_use\"}"]).is_err());
        assert!(claude(&[used, "{\"type\":\"tool_result\",\"tool_use_id\":\"\"}"]).is_err());
    }

    /// A seal verifies only the exact sealed bytes: a same-session edit, a
    /// complete-line truncation or a header-only replacement refuses, and so
    /// does a seal naming another session.
    #[test]
    fn sealed_history_detects_same_session_rewrites() {
        let temp = tempfile::tempdir().unwrap();
        let meta = "{\"type\":\"session_meta\",\"payload\":{\"id\":\"s1\"}}\n";
        let turn = "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"content\":\"nonce-1\"}}\n";
        let path = rollout(temp.path(), "s1", &format!("{meta}{turn}"));
        let sealed = super::seal(HarnessId::Codex, temp.path(), "s1").unwrap();
        assert!(super::verify(&sealed, HarnessId::Codex, "s1").is_ok());
        assert!(super::verify(&sealed, HarnessId::Codex, "s2").is_err());
        for (body, cause) in [
            (meta.to_owned(), "rewritten"),
            (
                format!("{meta}{}", turn.replace("nonce-1", "nonce-2")),
                "rewritten",
            ),
            (format!("{turn}{meta}{turn}"), "rewritten"),
            (format!("{meta}{turn}{turn}"), "grew from"),
        ] {
            fs::write(&path, &body).unwrap();
            let error = super::verify(&sealed, HarnessId::Codex, "s1")
                .unwrap_err()
                .to_string();
            assert!(error.contains("changed since it was sealed"), "{error}");
            // The diagnostic names the cause but never history content.
            assert!(error.contains(cause) && !error.contains("nonce"), "{error}");
        }
    }

    /// The admitted-turn proof is keyed by the attempt's own turn id and
    /// input digest: the exact turn proves it, the same text under another
    /// (older) turn or another digest does not, a meta-only history does
    /// not, a changed file is an error, and an unfinished tool call cannot
    /// even be sealed.
    #[test]
    fn admitted_turn_proof_is_bound_to_turn_id_and_input() {
        let temp = tempfile::tempdir().unwrap();
        let input = "ROLE PREAMBLE\n\nfixture:task";
        let digest = crate::fs::sha256(input.as_bytes());
        let meta = "{\"type\":\"session_meta\",\"payload\":{\"id\":\"s1\"}}\n";
        let user = |turn: &str| {
            format!(
                "{}\n",
                serde_json::json!({"type":"response_item","payload":{"type":"message","role":"user",
                    "content":[{"type":"input_text","text":input}],
                    "internal_chat_message_metadata_passthrough":{"turn_id":turn}}})
            )
        };
        let path = rollout(temp.path(), "s1", &format!("{meta}{}", user("old-turn")));
        let sealed = super::seal(HarnessId::Codex, temp.path(), "s1").unwrap();
        assert!(!super::admitted_turn_recorded(&sealed, "new-turn", &digest).unwrap());
        assert!(super::admitted_turn_recorded(&sealed, "old-turn", &digest).unwrap());
        let other = crate::fs::sha256(b"another input");
        assert!(!super::admitted_turn_recorded(&sealed, "old-turn", &other).unwrap());
        fs::write(&path, meta).unwrap();
        let meta_only = super::seal(HarnessId::Codex, temp.path(), "s1").unwrap();
        assert!(!super::admitted_turn_recorded(&meta_only, "new-turn", &digest).unwrap());
        fs::write(&path, format!("{meta}{}", user("new-turn"))).unwrap();
        assert!(
            super::admitted_turn_recorded(&meta_only, "new-turn", &digest)
                .unwrap_err()
                .to_string()
                .contains("changed since it was sealed")
        );
        let pending = "{\"type\":\"response_item\",\"payload\":{\"type\":\"function_call\",\"call_id\":\"c1\"}}\n";
        fs::write(&path, format!("{meta}{}{pending}", user("new-turn"))).unwrap();
        assert!(super::seal(HarnessId::Codex, temp.path(), "s1").is_err());
    }

    /// Claude continuation proves the runner's exact UUID and UTF-8 input,
    /// never an identical older message, sidechain or request snapshot.
    #[test]
    fn claude_admitted_input_requires_exact_user_identity_and_text() {
        let temp = tempfile::tempdir().unwrap();
        let dir = temp.path().join("projects/-tmp-work");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("s1.jsonl");
        let input = "fixture:exact\ninput ☃";
        let digest = crate::fs::sha256(input.as_bytes());
        let user = |id: &str| {
            serde_json::json!({"type":"user","sessionId":"s1","uuid":id,"isSidechain":false,
                "message":{"role":"user","content":[{"type":"text","text":input}]}})
        };
        let write = |records: &[serde_json::Value]| {
            let body: String = records
                .iter()
                .map(|record| format!("{}\n", serde_json::to_string(record).unwrap()))
                .collect();
            fs::write(&path, body).unwrap();
        };
        write(&[user("older"), user("current")]);
        let sealed = seal(HarnessId::ClaudeCode, temp.path(), "s1").unwrap();
        assert!(super::admitted_turn_recorded(&sealed, "current", &digest).unwrap());
        assert!(!super::admitted_turn_recorded(&sealed, "different", &digest).unwrap());
        assert!(
            !super::admitted_turn_recorded(&sealed, "current", &crate::fs::sha256(b"other"))
                .unwrap()
        );

        let mut sidechain = user("current");
        sidechain["isSidechain"] = serde_json::json!(true);
        let mut blob = user("current");
        blob["type"] = serde_json::json!("api-request-blob");
        let mut multiple = user("current");
        multiple["message"]["content"] = serde_json::json!([
            {"type":"text","text":input}, {"type":"text","text":"extra"}
        ]);
        for record in [sidechain, blob, multiple] {
            write(&[user("older"), record]);
            let sealed = seal(HarnessId::ClaudeCode, temp.path(), "s1").unwrap();
            assert!(!super::admitted_turn_recorded(&sealed, "current", &digest).unwrap());
        }
        write(&[user("current"), user("current")]);
        let repeated = seal(HarnessId::ClaudeCode, temp.path(), "s1").unwrap();
        assert!(super::admitted_turn_recorded(&repeated, "current", &digest).is_err());
        write(&[user("current")]);
        let sealed = seal(HarnessId::ClaudeCode, temp.path(), "s1").unwrap();
        write(&[user("older")]);
        assert!(super::admitted_turn_recorded(&sealed, "current", &digest).is_err());
    }
    /// Creates only the native tables the version-pinned read-only proof needs.
    /// The connection stays open so the test can exercise an authoritative WAL.
    fn native_sqlite(root: &Path) -> rusqlite::Connection {
        native_sqlite_version(root, "2.0.28")
    }

    /// Creates the proof's tables in the shape `version` writes: 2.0.28 keeps
    /// `session_pending`; 2.3.0 has only `session_inbox` (with its native
    /// columns) and adds `time_suspended`/`resume_attempts` to `session_v2`.
    fn native_sqlite_version(root: &Path, version: &str) -> rusqlite::Connection {
        let connection = rusqlite::Connection::open(root.join("opencode.db")).unwrap();
        let (extra_columns, extra_values, pending) = if version == "2.0.28" {
            (
                "",
                "",
                "CREATE TABLE session_inbox(session_id TEXT); CREATE TABLE session_pending(session_id TEXT);",
            )
        } else {
            (
                ",time_suspended INTEGER,resume_attempts INTEGER",
                ",NULL,0",
                "CREATE TABLE session_inbox(id TEXT PRIMARY KEY,session_id TEXT,type TEXT,payload TEXT,delivery TEXT,enqueued_seq INTEGER,time_created INTEGER);",
            )
        };
        connection.execute_batch(&format!("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;
            CREATE TABLE session_v2(id TEXT PRIMARY KEY,directory TEXT,agent TEXT,model TEXT,permission TEXT,version TEXT,time_idle INTEGER,idle_outcome TEXT,time_compacting INTEGER,revert TEXT{extra_columns});
            CREATE TABLE session_message(id TEXT PRIMARY KEY,session_id TEXT,type TEXT,seq INTEGER,data TEXT);
            {pending}
            INSERT INTO session_v2 VALUES('ses_one','/work','agent-run','{{}}','[]','{version}',5,'succeeded',NULL,NULL{extra_values});
            INSERT INTO session_message VALUES('msg_one','ses_one','user',1,'{{\"text\":\"marker\"}}');
            INSERT INTO session_message VALUES('msg_two','ses_one','assistant',2,'{{\"time\":{{\"completed\":5}},\"content\":[{{\"type\":\"tool\",\"id\":\"finish\",\"state\":{{\"status\":\"completed\"}}}}]}}');")).unwrap();
        connection
    }

    /// The qualified set is exact: both qualified releases, nothing nearby.
    #[test]
    fn opencode_qualified_versions_are_exact() {
        assert_eq!(
            opencode_qualified_versions().collect::<Vec<_>>(),
            ["2.0.28", "2.3.0"]
        );
        for version in ["2.0.28", "2.3.0"] {
            assert!(opencode_version_qualified(version), "{version}");
        }
        for version in ["", "2.3", "2.3.1", "2.2.0", "v2.3.0", "2.3.0 ", "2.0.27"] {
            assert!(!opencode_version_qualified(version), "{version:?}");
        }
    }

    /// 2.3.0 seals through `session_inbox` alone, records its version, and
    /// refuses queued input, suspension and every schema gap it does not know.
    #[test]
    fn opencode_230_schema_uses_inbox_and_refuses_suspension() {
        let root = tempfile::tempdir().unwrap();
        let connection = native_sqlite_version(root.path(), "2.3.0");
        let sealed = seal(HarnessId::OpenCode, root.path(), "ses_one").unwrap();
        assert_eq!(
            sealed.sqlite.as_ref().unwrap().version.as_deref(),
            Some("2.3.0")
        );
        verify(&sealed, HarnessId::OpenCode, "ses_one").unwrap();
        verify_opencode_started(&sealed, root.path(), "ses_one", "2.3.0").unwrap();
        // Columns outside the selected set (a fresh serve's bookkeeping) do not
        // change the digest a resume compares after native startup.
        connection
            .execute("UPDATE session_v2 SET resume_attempts=0", [])
            .unwrap();
        verify_opencode_started(&sealed, root.path(), "ses_one", "2.3.0").unwrap();

        let refused =
            |connection: &rusqlite::Connection, change: &str, undo: &str, reason: &str| {
                connection.execute_batch(change).unwrap();
                let error = seal(HarnessId::OpenCode, root.path(), "ses_one")
                    .unwrap_err()
                    .to_string();
                assert!(error.contains(reason), "{change}: {error}");
                connection.execute_batch(undo).unwrap();
            };
        refused(
            &connection,
            "INSERT INTO session_inbox VALUES('msg_q','ses_one','prompt','{}','queue',1,1)",
            "DELETE FROM session_inbox",
            "pending input",
        );
        refused(
            &connection,
            "UPDATE session_v2 SET time_suspended=7",
            "UPDATE session_v2 SET time_suspended=NULL",
            "suspended",
        );
        // A table 2.3.0 is known to lack is still checked if it reappears.
        refused(
            &connection,
            "CREATE TABLE session_pending(session_id TEXT); INSERT INTO session_pending VALUES('ses_one')",
            "DROP TABLE session_pending",
            "pending input",
        );
        refused(
            &connection,
            "ALTER TABLE session_inbox RENAME TO inbox_moved",
            "ALTER TABLE inbox_moved RENAME TO session_inbox",
            "pending-input state is incompatible",
        );
        refused(
            &connection,
            "ALTER TABLE session_v2 DROP COLUMN time_suspended",
            "ALTER TABLE session_v2 ADD COLUMN time_suspended INTEGER",
            "suspension state is incompatible",
        );
        refused(
            &connection,
            "UPDATE session_v2 SET version='2.4.0'",
            "UPDATE session_v2 SET version='2.3.0'",
            "version is not qualified",
        );
        seal(HarnessId::OpenCode, root.path(), "ses_one").unwrap();
    }

    /// 2.0.28 still requires both pending-input tables: a missing
    /// `session_pending` is an incompatible schema, never an empty queue.
    #[test]
    fn opencode_2028_schema_never_treats_missing_pending_as_empty() {
        let root = tempfile::tempdir().unwrap();
        let connection = native_sqlite(root.path());
        let sealed = seal(HarnessId::OpenCode, root.path(), "ses_one").unwrap();
        assert_eq!(
            sealed.sqlite.as_ref().unwrap().version.as_deref(),
            Some("2.0.28")
        );
        connection
            .execute_batch("DROP TABLE session_pending")
            .unwrap();
        let error = seal(HarnessId::OpenCode, root.path(), "ses_one")
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("pending-input state is incompatible"),
            "{error}"
        );
        // A later migration's suspension column is honoured when present.
        connection
            .execute_batch("CREATE TABLE session_pending(session_id TEXT); ALTER TABLE session_v2 ADD COLUMN time_suspended INTEGER; UPDATE session_v2 SET time_suspended=3")
            .unwrap();
        assert!(seal(HarnessId::OpenCode, root.path(), "ses_one").is_err());
    }

    /// 2.3.0 deletes its WAL on a graceful exit and the read-only proof
    /// connection recreates it empty: sealing must not see that as a change,
    /// and an older seal recording an empty WAL still verifies.
    #[test]
    fn opencode_seal_treats_absent_and_empty_wal_alike() {
        let root = tempfile::tempdir().unwrap();
        let connection = native_sqlite_version(root.path(), "2.3.0");
        connection
            .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();
        drop(connection);
        let wal = root.path().join("opencode.db-wal");
        assert!(!wal.exists(), "closing the last connection removes the WAL");
        let sealed = seal(HarnessId::OpenCode, root.path(), "ses_one").unwrap();
        assert!(sealed.sqlite.as_ref().unwrap().wal.is_none());
        verify(&sealed, HarnessId::OpenCode, "ses_one").unwrap();
        fs::write(&wal, b"").unwrap();
        verify(&sealed, HarnessId::OpenCode, "ses_one").unwrap();
        let mut older = sealed.clone();
        older.sqlite.as_mut().unwrap().wal = Some(super::FileSeal {
            bytes: 0,
            sha256: crate::fs::sha256(b""),
        });
        verify(&older, HarnessId::OpenCode, "ses_one").unwrap();
        // Real frames are still bound exactly.
        fs::write(&wal, b"not a frame").unwrap();
        assert!(verify(&sealed, HarnessId::OpenCode, "ses_one").is_err());
    }

    /// A seal binds the OpenCode version: a resume under another qualified
    /// version refuses before any input, and an unversioned seal from an older
    /// Run means exactly the legacy version.
    #[test]
    fn opencode_seal_refuses_resume_under_another_version() {
        let root = tempfile::tempdir().unwrap();
        let _connection = native_sqlite_version(root.path(), "2.3.0");
        let sealed = seal(HarnessId::OpenCode, root.path(), "ses_one").unwrap();
        let error = verify_opencode_started(&sealed, root.path(), "ses_one", "2.0.28")
            .unwrap_err()
            .to_string();
        assert!(
            error.starts_with("continuation_unavailable") && error.contains("version changed"),
            "{error}"
        );
        // A recorded version that disagrees with the native session refuses too.
        let mut forged = sealed.clone();
        forged.sqlite.as_mut().unwrap().version = Some("2.0.28".into());
        assert!(verify(&forged, HarnessId::OpenCode, "ses_one").is_err());
        assert!(verify_opencode_started(&forged, root.path(), "ses_one", "2.0.28").is_err());

        let legacy_root = tempfile::tempdir().unwrap();
        let _legacy = native_sqlite(legacy_root.path());
        let mut legacy =
            serde_json::to_value(seal(HarnessId::OpenCode, legacy_root.path(), "ses_one").unwrap())
                .unwrap();
        legacy["sqlite"].as_object_mut().unwrap().remove("version");
        let legacy: HistorySeal = serde_json::from_value(legacy).unwrap();
        assert_eq!(legacy.sqlite.as_ref().unwrap().opencode_version(), "2.0.28");
        verify(&legacy, HarnessId::OpenCode, "ses_one").unwrap();
        verify_opencode_started(&legacy, legacy_root.path(), "ses_one", "2.0.28").unwrap();
        assert!(verify_opencode_started(&legacy, legacy_root.path(), "ses_one", "2.3.0").is_err());
    }

    /// WAL mutations, foreign sessions and symlink substitutions cannot resume;
    /// a native checkpoint changes physical bytes but preserves post-start semantics.
    #[test]
    fn opencode_sqlite_proof_binds_wal_and_survives_only_semantic_startup() {
        let root = tempfile::tempdir().unwrap();
        let connection = native_sqlite(root.path());
        let seal = seal(HarnessId::OpenCode, root.path(), "ses_one").unwrap();
        assert!(seal.sqlite.as_ref().unwrap().wal.is_some());
        verify(&seal, HarnessId::OpenCode, "ses_one").unwrap();
        assert!(verify(&seal, HarnessId::OpenCode, "ses_other").is_err());
        let encoded = serde_json::to_value(&seal).unwrap();
        let decoded: HistorySeal = serde_json::from_value(encoded).unwrap();
        assert_eq!(seal, decoded);
        connection
            .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();
        assert!(verify(&seal, HarnessId::OpenCode, "ses_one").is_err());
        verify_opencode_started(&seal, root.path(), "ses_one", "2.0.28").unwrap();
        connection
            .execute(
                "UPDATE session_message SET data='{\"text\":\"changed\"}' WHERE id='msg_one'",
                [],
            )
            .unwrap();
        assert!(verify_opencode_started(&seal, root.path(), "ses_one", "2.0.28").is_err());
        drop(connection);
        std::fs::write(root.path().join("foreign-wal"), b"unrelated").unwrap();
        std::os::unix::fs::symlink("foreign-wal", root.path().join("opencode.db-wal")).unwrap();
        assert!(super::seal(HarnessId::OpenCode, root.path(), "ses_one").is_err());
    }

    /// A pending input or unresolved native tool can never mint continuation evidence.
    #[test]
    fn opencode_history_refuses_unsettled_native_state() {
        let root = tempfile::tempdir().unwrap();
        let connection = native_sqlite(root.path());
        connection
            .execute("INSERT INTO session_pending VALUES('ses_one')", [])
            .unwrap();
        assert!(seal(HarnessId::OpenCode, root.path(), "ses_one").is_err());
        connection
            .execute("DELETE FROM session_pending", [])
            .unwrap();
        connection.execute("UPDATE session_message SET data='{\"time\":{\"completed\":5},\"content\":[{\"type\":\"tool\",\"id\":\"finish\",\"state\":{\"status\":\"running\"}}]}' WHERE id='msg_two'",[]).unwrap();
        assert!(seal(HarnessId::OpenCode, root.path(), "ses_one").is_err());
    }
}
