//! Durable physical storage-layout registry for shared runtime assets.
//!
//! One row per canonical runtime home records which shared managed trees the
//! home's layout installed, keyed by the home itself. The row is the pin: a
//! `prepared` row blocks any new continuation admission into that home until
//! it is committed — its registration may only be retried idempotently by its
//! recorded owner, never replaced — and it never expires by age. A
//! `committed` row only describes the mapping while the home exists and pins
//! nothing on its own. There is no blob reference count to drift: physical
//! reclamation is decided later by core against the row states, never by
//! counters maintained here.
//!
//! This is a trusted internal API. Callers are agent-run's own supervisor and
//! compaction paths, which already hold the frozen launch identity; nothing
//! here accepts a wire request or an arbitrary caller-supplied path.

use crate::{ACTIVE_SQL, Store};
use agent_run_domain::{Error, Result, domain::AgentId, domain::now, error::invalid};
use agent_run_platform::fs;
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

/// The only layout contract version this store understands.
const LAYOUT_VERSION: u32 = 1;
/// Upper bound on canonical layout text accepted or stored in one row.
const MAX_LAYOUT_BYTES: usize = 64 * 1024;
/// Upper bound on managed roots mapped by one layout.
const MAX_ROOTS: usize = 256;
/// Upper bound on one canonical runtime home string.
const MAX_HOME_BYTES: usize = 4096;
/// Upper bound on one page of pending rows returned to a recovery caller.
const MAX_PENDING_PAGE: i64 = 1000;

/// One managed tree's placement in the caller-owned shared store.
///
/// Both fields are validated by [`RuntimeStorageLayout::validate`]; the
/// physical store target is always derived later by core from its trusted
/// app home plus this pair, never from a path stored here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SharedRootMapping {
    /// 64 lowercase hexadecimal digits naming the account or compatibility
    /// domain the tree was imported under.
    pub scope: String,
    /// SHA-256 of the tree's exact original canonical manifest bytes.
    pub manifest_sha256: String,
}

/// The versioned, canonical physical layout of one runtime home.
///
/// `roots` maps each relative managed tree path inside the home (for example
/// `assets/plugins`) to the shared object that now backs it. The original
/// frozen index digest stays in `index_sha256`: installing a shared layout
/// changes the physical placement, never the recorded authority, index or
/// native-history proofs. Unknown fields and unknown versions are rejected,
/// so a future contract is a new version, never a silent reinterpretation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeStorageLayout {
    /// Explicit contract version; only `LAYOUT_VERSION` is accepted.
    pub version: u32,
    /// Canonical absolute runtime home this layout belongs to.
    pub runtime_home: String,
    /// SHA-256 of the home's original frozen runtime asset index.
    pub index_sha256: String,
    /// Managed tree root (relative path) to shared object mapping.
    pub roots: BTreeMap<String, SharedRootMapping>,
}

/// The lifecycle state of one registered layout row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LayoutState {
    /// The layout pins its shared references and blocks admission into the
    /// home until it is committed; its own registration may be retried
    /// idempotently by the owner that recorded it.
    Prepared,
    /// The layout describes the installed mapping while the home exists.
    Committed,
}

impl LayoutState {
    /// Returns the exact stored spelling of this state.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Prepared => "prepared",
            Self::Committed => "committed",
        }
    }
}

/// One registry row as validated, typed values.
///
/// The record is built only through this module, which re-validates the
/// stored layout text against its stored digest, so a corrupted row surfaces
/// as an integrity error instead of an unsafe mapping.
#[derive(Debug, Clone)]
pub struct RuntimeStorageLayoutRecord {
    /// Canonical absolute runtime home keying the row.
    pub runtime_home: String,
    /// SHA-256 of the home's original frozen runtime asset index.
    pub index_sha256: String,
    /// Digest of the canonical `layout_json` bytes.
    pub layout_sha256: String,
    /// Current lifecycle state of the row.
    pub state: LayoutState,
    /// Compare-and-swap token of the preparing operation.
    pub operation_token: String,
    /// Agent whose preparation created a still-pending row, if any.
    pub owner_agent_id: Option<String>,
    /// Store clock of the last state change, in Unix seconds.
    pub updated_at: f64,
    /// The validated canonical layout itself.
    layout: RuntimeStorageLayout,
}

impl RuntimeStorageLayoutRecord {
    /// Returns the validated canonical layout this row records.
    pub fn layout(&self) -> &RuntimeStorageLayout {
        &self.layout
    }
}

/// Returns whether `value` is 64 lowercase hexadecimal digits, the shape of
/// every digest and scope stored in a layout.
fn is_lowercase_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

/// Refuses a runtime home that is not an absolute canonical path.
///
/// Empty components, `.` and `..` components, trailing separators and a bare
/// `/` are all refused, so a stored key can only be a real canonical home.
fn validate_canonical_home(home: &str) -> Result<()> {
    let refused = || {
        invalid(
            "runtime home must be an absolute canonical path without '.', '..' or empty \
             components",
        )
    };
    if home.len() > MAX_HOME_BYTES || home.contains('\0') || !home.starts_with('/') {
        return Err(refused());
    }
    // Raw segments, not a components iterator, which normalizes `/a/./b` away.
    let body = &home[1..];
    if body.is_empty()
        || body
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
    {
        return Err(refused());
    }
    Ok(())
}

/// Refuses a managed tree root that is not one safe relative path.
///
/// The root must be nonempty, relative, and free of `.`, `..` and empty
/// components, so a layout can never steer a shared mapping outside the
/// caller-owned trees it names.
fn validate_root(root: &str) -> Result<()> {
    if root.is_empty()
        || root.len() > 512
        || root.starts_with('/')
        || root.contains('\0')
        || root.contains('\\')
        || root
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
    {
        return Err(invalid(
            "managed tree root must be a nonempty relative path without '.', '..' or empty \
             components",
        ));
    }
    Ok(())
}

impl RuntimeStorageLayout {
    /// Validates the complete layout contract.
    ///
    /// Fails for an unsupported version, a non-canonical home, a
    /// non-lowercase digest, an oversized root set, an unsafe root path, or
    /// two roots where one is a component prefix of the other. An empty root
    /// set is accepted: it is the cache-only anchor of a home with no managed
    /// trees. Duplicate roots cannot survive this contract's canonical-bytes
    /// check, which rejects any text that is not already the sorted, compact
    /// encoding.
    pub fn validate(&self) -> Result<()> {
        if self.version != LAYOUT_VERSION {
            return Err(invalid(format!(
                "runtime storage layout version {} is not supported",
                self.version
            )));
        }
        validate_canonical_home(&self.runtime_home)?;
        if !is_lowercase_digest(&self.index_sha256) {
            return Err(invalid(
                "runtime storage layout index digest must be 64 lowercase hexadecimal digits",
            ));
        }
        // An empty root set is a valid anchor: a cache-only home has no
        // managed trees to map, yet its row still names the one physical home
        // the shared store must keep accounting for. Schema 22 is unreleased,
        // so widening the lower bound here changes no stored contract.
        if self.roots.len() > MAX_ROOTS {
            return Err(invalid(format!(
                "runtime storage layout must map at most {MAX_ROOTS} managed roots"
            )));
        }
        for (root, mapping) in &self.roots {
            validate_root(root)?;
            if !is_lowercase_digest(&mapping.scope)
                || !is_lowercase_digest(&mapping.manifest_sha256)
            {
                return Err(invalid(
                    "shared tree scope and manifest digest must be 64 lowercase hexadecimal \
                     digits",
                ));
            }
        }
        // Two roots overlap exactly when one is a proper component ancestor of
        // the other. Checking every root's ancestors against the key set (not
        // adjacent sorted neighbours, which miss `a` beside `a-b` and `a/b`)
        // covers every pair in one pass.
        for root in self.roots.keys() {
            let mut ancestor = String::new();
            let components: Vec<&str> = root.split('/').collect();
            for component in &components[..components.len() - 1] {
                ancestor.push_str(component);
                ancestor.push('/');
                if self.roots.contains_key(ancestor.trim_end_matches('/')) {
                    return Err(invalid(format!(
                        "managed tree root {root} overlaps {}",
                        ancestor.trim_end_matches('/')
                    )));
                }
            }
        }
        Ok(())
    }

    /// Returns the canonical encoding of this layout: sorted keys, compact
    /// separators, no whitespace. This is the only byte form the registry
    /// stores and the only form its digest covers.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>> {
        fs::canonical_json(&serde_json::to_value(self)?)
    }
}

/// Validates canonical layout text and returns it with its SHA-256 digest.
///
/// The text is bounded, parsed strictly (unknown fields are errors), checked
/// against the whole [`RuntimeStorageLayout::validate`] contract, and then
/// re-encoded canonically: text that is not already that exact encoding is
/// refused, as is a caller-supplied digest that does not match the bytes.
fn validate_layout_text(layout_json: &str) -> Result<(RuntimeStorageLayout, String)> {
    if layout_json.len() > MAX_LAYOUT_BYTES {
        return Err(invalid("runtime storage layout exceeds its metadata bound"));
    }
    let layout: RuntimeStorageLayout = serde_json::from_str(layout_json)
        .map_err(|_| invalid("runtime storage layout is malformed"))?;
    layout.validate()?;
    let canonical = layout.canonical_bytes()?;
    if canonical != layout_json.as_bytes() {
        return Err(invalid("runtime storage layout is not in canonical form"));
    }
    let digest = fs::sha256(&canonical);
    Ok((layout, digest))
}

/// Raw column values of one registry row, in storage order.
struct LayoutRow {
    /// Canonical absolute runtime home keying the row.
    runtime_home: String,
    /// SHA-256 of the home's original frozen runtime asset index.
    index_sha256: String,
    /// Canonical layout text exactly as stored.
    layout_json: String,
    /// Digest the row claims for `layout_json`.
    layout_sha256: String,
    /// Stored state spelling: `prepared` or `committed`.
    state: String,
    /// Compare-and-swap token of the preparing operation.
    operation_token: String,
    /// Agent whose preparation created a still-pending row, if any.
    owner_agent_id: Option<String>,
    /// Store clock of the last state change, in Unix seconds.
    updated_at: f64,
}

/// Reads one row's raw columns.
fn read_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<LayoutRow> {
    Ok(LayoutRow {
        runtime_home: row.get(0)?,
        index_sha256: row.get(1)?,
        layout_json: row.get(2)?,
        layout_sha256: row.get(3)?,
        state: row.get(4)?,
        operation_token: row.get(5)?,
        owner_agent_id: row.get(6)?,
        updated_at: row.get(7)?,
    })
}

/// Validates one raw row into a record, proving the stored bytes still match
/// the stored digest before anything is handed back.
fn validated(row: LayoutRow) -> Result<RuntimeStorageLayoutRecord> {
    let LayoutRow {
        runtime_home,
        index_sha256,
        layout_json,
        layout_sha256,
        state,
        operation_token,
        owner_agent_id,
        updated_at,
    } = row;
    let (layout, digest) = validate_layout_text(&layout_json)?;
    if digest != layout_sha256 || layout.runtime_home != runtime_home {
        return Err(Error::Integrity(format!(
            "runtime storage layout row for {runtime_home} failed its own digest check"
        )));
    }
    let state = match state.as_str() {
        "prepared" => LayoutState::Prepared,
        "committed" => LayoutState::Committed,
        other => {
            return Err(Error::Integrity(format!(
                "runtime storage layout row for {runtime_home} has unknown state {other}"
            )));
        }
    };
    Ok(RuntimeStorageLayoutRecord {
        runtime_home,
        index_sha256,
        layout_sha256,
        state,
        operation_token,
        owner_agent_id,
        updated_at,
        layout,
    })
}

/// Columns of [`read_row`], in read order.
const ROW_COLUMNS: &str = "runtime_home,index_sha256,layout_json,layout_sha256,state,\
     operation_token,owner_agent_id,updated_at";

/// One durable agent identity's binding to a canonical runtime home.
///
/// Only recorded evidence is reported: the home exactly as the frozen
/// identity spells it, the agent's current status, and the sealed asset-index
/// digest when the identity carries one. Nothing here canonicalizes, guesses
/// a provider, or touches the filesystem.
#[derive(Debug, Clone)]
pub struct RetainedHomeRef {
    /// Runtime home exactly as recorded in the agent's frozen identity.
    pub runtime_home: String,
    /// Agent id whose identity binds this home.
    pub agent_id: String,
    /// Current durable status of that agent row.
    pub status: String,
    /// Sealed runtime asset-index digest recorded by the identity, if any.
    pub index_sha256: Option<String>,
    /// Finish time of the execution, when it already finished.
    pub finished_at: Option<f64>,
}

/// Returns the one registered layout row for `runtime_home`, if any.
fn row_in(conn: &Connection, runtime_home: &str) -> Result<Option<RuntimeStorageLayoutRecord>> {
    conn.query_row(
        &format!("SELECT {ROW_COLUMNS} FROM runtime_storage_layouts WHERE runtime_home=?1"),
        [runtime_home],
        read_row,
    )
    .optional()?
    .map(validated)
    .transpose()
}

/// An attempt holds nothing only when positively proven on one of two sides:
/// it never claimed a spawn (`prepared` with no recorded process identity),
/// or it finished `cleanup_complete` with a parsed proof that either
/// confirms verified cleanup or records that nothing was ever spawned. Every
/// comparison is `COALESCE`d so an unknown phase can never evaluate to SQL
/// NULL, which `NOT` would silently treat as false; every other row — an
/// unknown phase, `spawning` before a process identity was inspected, a
/// terminal attempt with a null, empty or false proof — still holds the home.
const INERT_ATTEMPT: &str = "((COALESCE(t.phase,'')='prepared' AND t.process_identity IS NULL) \
     OR (COALESCE(t.phase,'')='cleanup_complete' \
         AND (COALESCE(json_extract(t.cleanup_proof_json,'$.confirmed'),0)=1 \
              OR COALESCE(json_extract(t.cleanup_proof_json,'$.never_spawned'),0)=1)))";

/// The stronger subset of [`INERT_ATTEMPT`] a `running` owner must show for
/// every attempt: a confirmed cleanup proof, never a merely `prepared`
/// attempt, which is the account-switch spawn window.
const CLEANED_PROOF: &str = "(COALESCE(t.phase,'')='cleanup_complete' \
     AND (COALESCE(json_extract(t.cleanup_proof_json,'$.confirmed'),0)=1 \
          OR COALESCE(json_extract(t.cleanup_proof_json,'$.never_spawned'),0)=1))";

/// Returns the agent ids whose frozen identity still binds this runtime home
/// and whose hold on it is unresolved: an unfinished status, `lost`, or any
/// attempt that is not provably inert — including released-attempt rows
/// (`ownership_active=0`) whose cleanup was never verified.
fn unsettled_home_holders(tx: &Transaction<'_>, runtime_home: &str) -> Result<Vec<String>> {
    let active = ACTIVE_SQL;
    let inert = INERT_ATTEMPT;
    let mut stmt = tx.prepare(&format!(
        "SELECT a.id FROM agents a \
         WHERE json_extract(a.identity_json,'$.runtime_home')=?1 \
         AND (a.status IN {active} OR a.status='lost' \
              OR EXISTS(SELECT 1 FROM attempts t WHERE t.agent_id=a.id AND NOT ({inert})))"
    ))?;
    let rows = stmt.query_map([runtime_home], |row| row.get::<_, String>(0))?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

/// Returns whether `agent` proves it holds nothing live on its home.
///
/// Cancelling and lost always hold. Any status requires every attempt ever
/// opened to be provably inert per [`INERT_ATTEMPT`]. A `created` or
/// `starting` agent with only inert attempts qualifies — it never spawned —
/// while the same agent still counts as a holder for anyone but itself.
/// A `running` agent is the consolidation window before its terminal commit:
/// it qualifies only with at least one attempt, all of them proven cleaned
/// by [`CLEANED_PROOF`] — never on merely `prepared` attempts, which is the
/// account-switch spawn window.
fn holds_nothing_live(tx: &Transaction<'_>, agent: &AgentId) -> Result<bool> {
    let inert = INERT_ATTEMPT;
    let cleaned = CLEANED_PROOF;
    let unresolved: i64 = tx.query_row(
        &format!(
            "SELECT COUNT(*) FROM agents a WHERE a.id=?1 \
             AND (a.status IN ('cancelling','lost') \
                  OR EXISTS(SELECT 1 FROM attempts t WHERE t.agent_id=a.id \
                            AND NOT ({inert})) \
                  OR (a.status='running' \
                      AND (NOT EXISTS(SELECT 1 FROM attempts t WHERE t.agent_id=a.id) \
                           OR EXISTS(SELECT 1 FROM attempts t WHERE t.agent_id=a.id \
                                     AND NOT ({cleaned})))))"
        ),
        [agent.as_str()],
        |row| row.get(0),
    )?;
    Ok(unresolved == 0)
}

/// Blocks a continuation admission whose frozen parent home has a prepared
/// layout row. Called inside the admission transaction itself, so a layout
/// prepared a moment later or earlier can never race a new child into the
/// home. A committed row, or an identity without a recorded runtime home,
/// admits unchanged.
pub(crate) fn refuse_prepared_resume_home(tx: &Transaction<'_>, identity: &Value) -> Result<()> {
    let Some(home) = identity
        .get("runtime_home")
        .and_then(Value::as_str)
        .filter(|home| !home.trim().is_empty())
    else {
        return Ok(());
    };
    let state: Option<String> = tx
        .query_row(
            "SELECT state FROM runtime_storage_layouts WHERE runtime_home=?1",
            [home],
            |row| row.get(0),
        )
        .optional()?;
    if state.as_deref() == Some(LayoutState::Prepared.as_str()) {
        return Err(Error::Unsupported(format!(
            "continuation_unavailable: runtime home {home} has a prepared storage layout that \
             must be committed before a new continuation is admitted"
        )));
    }
    Ok(())
}

impl Store {
    /// Registers one physical storage layout for its own canonical runtime
    /// home as `prepared` and returns the row with its operation token.
    ///
    /// `layout_json` must already be the canonical encoding of a valid
    /// version-1 layout; anything else is refused without touching the
    /// registry. The write runs in one `IMMEDIATE` transaction. An absent row
    /// is created only when no agent still holds the home unresolved (see
    /// `unsettled_home_holders`); when a holder exists, only that holder
    /// itself, passed as `owner` and proving it holds nothing live, may
    /// register the layout. When a row already exists, this call is strictly
    /// idempotent: a `prepared` row returns itself unchanged — same operation
    /// token, same bytes — only for the same layout digest and the same owner
    /// claim that recorded it, and only while that owner still holds nothing
    /// live; anything else, and always a `committed` row, is
    /// [`Error::Conflict`]. This unit never replaces or drops a registered
    /// layout: re-pointing a home at a different mapping waits for an
    /// explicit safe replacement operation. The row never expires by age;
    /// only commit or an explicit [`Self::remove_runtime_storage_layout`]
    /// clears it.
    pub fn prepare_runtime_storage_layout(
        &mut self,
        layout_json: &str,
        owner: Option<&AgentId>,
    ) -> Result<RuntimeStorageLayoutRecord> {
        let (layout, digest) = validate_layout_text(layout_json)?;
        let runtime_home = layout.runtime_home.clone();
        let token = format!("rt_{}", uuid::Uuid::new_v4().simple());
        let at = now();
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(existing) = row_in(&tx, &runtime_home)? {
            // Retry is idempotent only for the exact recorded values; a
            // committed mapping is never re-opened by this operation. The
            // digest alone proves the layout text: only its exact canonical
            // bytes hash to it.
            if existing.state != LayoutState::Prepared
                || existing.layout_sha256 != digest
                || existing.owner_agent_id.as_deref() != owner.map(AgentId::as_str)
            {
                return Err(Error::Conflict);
            }
            if let Some(recorded) = existing.owner_agent_id.as_deref()
                && !holds_nothing_live(&tx, &recorded.parse()?)?
            {
                return Err(Error::Conflict);
            }
            tx.commit()?;
            return Ok(existing);
        }
        let holders = unsettled_home_holders(&tx, &runtime_home)?;
        if !holders.is_empty() {
            let owner = owner.ok_or(Error::Conflict)?;
            if holders.iter().any(|holder| holder != owner.as_str())
                || !holds_nothing_live(&tx, owner)?
            {
                return Err(Error::Conflict);
            }
        }
        tx.execute(
            "INSERT INTO runtime_storage_layouts(runtime_home,index_sha256,layout_json,\
             layout_sha256,state,operation_token,owner_agent_id,updated_at) \
             VALUES(?1,?2,?3,?4,'prepared',?5,?6,?7)",
            rusqlite::params![
                runtime_home,
                layout.index_sha256,
                layout_json,
                digest,
                token,
                owner.map(AgentId::as_str),
                at
            ],
        )?;
        tx.commit()?;
        Ok(RuntimeStorageLayoutRecord {
            runtime_home,
            index_sha256: layout.index_sha256.clone(),
            layout_sha256: digest,
            state: LayoutState::Prepared,
            operation_token: token,
            owner_agent_id: owner.map(|owner| owner.as_str().to_owned()),
            updated_at: at,
            layout,
        })
    }

    /// Marks one prepared layout `committed` after proving the exact
    /// operation token and expected layout digest.
    ///
    /// Retrying the same token and digest is idempotent, including after the
    /// commit succeeded. Any other token or digest is [`Error::Conflict`] and
    /// overwrites nothing, so a superseded or mistyped commit can never flip
    /// a row prepared by a different operation. A home with no registered
    /// row is a validation error. The committed row describes the mapping
    /// while the home exists; it does not by itself pin the home against
    /// later deletion.
    pub fn commit_runtime_storage_layout(
        &mut self,
        runtime_home: &str,
        operation_token: &str,
        layout_sha256: &str,
    ) -> Result<RuntimeStorageLayoutRecord> {
        if !is_lowercase_digest(layout_sha256) || operation_token.is_empty() {
            return Err(invalid(
                "runtime storage layout commit needs an operation token and a lowercase digest",
            ));
        }
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let existing = row_in(&tx, runtime_home)?.ok_or_else(|| {
            invalid(format!(
                "no runtime storage layout is registered for {runtime_home}"
            ))
        })?;
        if existing.operation_token != operation_token || existing.layout_sha256 != layout_sha256 {
            return Err(Error::Conflict);
        }
        if existing.state == LayoutState::Prepared {
            let at = now();
            tx.execute(
                "UPDATE runtime_storage_layouts SET state='committed',updated_at=?1 \
                 WHERE runtime_home=?2",
                rusqlite::params![at, runtime_home],
            )?;
            tx.commit()?;
            return Ok(RuntimeStorageLayoutRecord {
                state: LayoutState::Committed,
                updated_at: at,
                ..existing
            });
        }
        tx.commit()?;
        Ok(existing)
    }

    /// Returns the registered layout row for `runtime_home`, if any.
    ///
    /// The stored bytes are re-validated against the stored digest, so a
    /// corrupted row is reported as an integrity error rather than returned.
    pub fn runtime_storage_layout(
        &self,
        runtime_home: &str,
    ) -> Result<Option<RuntimeStorageLayoutRecord>> {
        row_in(&self.conn, runtime_home)
    }

    /// Returns up to `limit` pending rows oldest-first for recovery and
    /// garbage collection. `limit` is clamped to at least one and at most one
    /// thousand rows, so the enumeration stays bounded. This performs no
    /// filesystem cleanup of its own; deciding and executing recovery is the
    /// caller's unit.
    pub fn pending_runtime_storage_layouts(
        &self,
        limit: usize,
    ) -> Result<Vec<RuntimeStorageLayoutRecord>> {
        let limit = limit.clamp(1, MAX_PENDING_PAGE as usize) as i64;
        let mut stmt = self.conn.prepare(&format!(
            "SELECT {ROW_COLUMNS} FROM runtime_storage_layouts \
             WHERE state='prepared' ORDER BY updated_at LIMIT ?1"
        ))?;
        let rows = stmt.query_map([limit], read_row)?;
        rows.map(|row| validated(row?)).collect()
    }

    /// Returns one bounded page of every registered layout row, ordered by
    /// runtime home strictly after `after`, with `true` when more rows remain.
    ///
    /// The keyset cursor keeps repeated maintenance passes advancing instead
    /// of re-reading the same first page forever. `limit` is clamped to at
    /// most one thousand rows; a corrupted row is an integrity error, so a
    /// caller deciding retention treats the page as incomplete, never as
    /// proof that anything is unreferenced.
    pub fn runtime_storage_layouts_page(
        &self,
        after: Option<&str>,
        limit: usize,
    ) -> Result<(Vec<RuntimeStorageLayoutRecord>, bool)> {
        let limit = limit.clamp(1, MAX_PENDING_PAGE as usize) as i64;
        let mut stmt = self.conn.prepare(&format!(
            "SELECT {ROW_COLUMNS} FROM runtime_storage_layouts \
             WHERE ?1 IS NULL OR runtime_home > ?1 ORDER BY runtime_home LIMIT ?2"
        ))?;
        let rows = stmt
            .query_map(rusqlite::params![after, limit + 1], read_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let more = rows.len() as i64 > limit;
        let page = rows
            .into_iter()
            .take(limit as usize)
            .map(validated)
            .collect::<Result<Vec<_>>>()?;
        Ok((page, more))
    }

    /// Returns one bounded page of runtime homes bound by durable identities,
    /// ordered by home strictly after `after`, with `true` when more remain.
    ///
    /// Every row comes from a frozen identity's own recorded home and index
    /// digest; homes that no identity records are not invented here. The
    /// keyset cursor keeps repeated maintenance passes advancing instead of
    /// re-reading the same first page forever, and `limit` is clamped to at
    /// most one thousand rows.
    pub fn retained_runtime_homes(
        &self,
        after: Option<&str>,
        limit: usize,
    ) -> Result<(Vec<RetainedHomeRef>, bool)> {
        let limit = limit.clamp(1, MAX_PENDING_PAGE as usize) as i64;
        let mut stmt = self.conn.prepare(
            "SELECT json_extract(identity_json,'$.runtime_home') AS home, id, status, \
             json_extract(identity_json,'$.snapshot_sha256'), finished_at \
             FROM agents \
             WHERE home IS NOT NULL AND home != '' \
               AND (?1 IS NULL OR home > ?1) \
             ORDER BY home LIMIT ?2",
        )?;
        let rows = stmt
            .query_map(rusqlite::params![after, limit + 1], |row| {
                Ok(RetainedHomeRef {
                    runtime_home: row.get(0)?,
                    agent_id: row.get(1)?,
                    status: row.get(2)?,
                    index_sha256: row.get(3)?,
                    finished_at: row.get(4)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let more = rows.len() as i64 > limit;
        Ok((rows.into_iter().take(limit as usize).collect(), more))
    }

    /// Deletes one registered layout row explicitly and reports whether a
    /// row was removed.
    ///
    /// Removal refuses with a validation error while any agent row still
    /// carries this runtime home in its frozen identity, because the mapping
    /// may still describe live storage. The caller must already hold every
    /// proof at once: the physical home is gone, no configuration references
    /// it, and every service reference to it was released. There is no
    /// age-based or cascade deletion; a committed row survives its agent
    /// history and must be removed by this call once all of those proofs
    /// hold.
    pub fn remove_runtime_storage_layout(&mut self, runtime_home: &str) -> Result<bool> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let references: i64 = tx.query_row(
            "SELECT COUNT(*) FROM agents WHERE json_extract(identity_json,'$.runtime_home')=?1",
            [runtime_home],
            |row| row.get(0),
        )?;
        if references > 0 {
            return Err(invalid(format!(
                "runtime storage layout for {runtime_home} is still referenced by {references} \
                 agent row(s); removal requires the physical home to be gone, unreferenced by \
                 configuration and free of service references, all at once"
            )));
        }
        let removed = tx.execute(
            "DELETE FROM runtime_storage_layouts WHERE runtime_home=?1",
            [runtime_home],
        )?;
        tx.commit()?;
        Ok(removed == 1)
    }
}
