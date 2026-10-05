//! Read-only pool discovery, bounded log buffers and pool-tab navigation.
//! Only public `list_pools`, `pool` and `status` reads are issued.

use crate::{
    app::{App, Screen},
    events::{BrokerEvent, Dispatched},
    net::{Broker, SharedBroker},
};
use agent_run_domain::{
    domain::{AgentId, Status},
    pool::{PoolEntryView, PoolId, PoolState, VoteDecision},
    views::{AgentView, ListPoolsView, PoolListMemberView, PoolListView},
};
use ratatui::crossterm::event::{KeyCode, MouseButton, MouseEvent, MouseEventKind};
use serde::Deserialize;
use serde_json::json;
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    time::Duration,
};
use tokio::sync::{mpsc, watch};

/// Maximum retained entries in each of four cached pools (each body is ≤8192 bytes).
pub const ENTRY_CAP: usize = 500;
/// Pool log page from the public read lane; direction comes from the request.
#[derive(Debug, Clone, Deserialize)]
pub struct Page {
    /// Stable pool identity for rejecting stale deliveries.
    pub pool_id: PoolId,
    /// Ascending immutable log entries.
    pub entries: Vec<PoolEntryView>,
    /// Exclusive reverse cursor, absent for forward reads.
    pub before_seq: Option<u64>,
    /// Last durable sequence, independent of page direction.
    pub last_seq: u64,
    /// No further entries in the requested direction.
    pub complete: bool,
    /// Current or frozen status, supplied even by empty polls.
    pub status: PoolStatus,
}
/// Broker status projection; completion evidence never comes from session joins.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct PoolStatus {
    /// Open or formally completed.
    pub state: PoolState,
    /// Current/frozen roster generation.
    pub roster_revision: u32,
    /// Full untrusted goal.
    pub goal: String,
    /// All acceptance criteria.
    pub criteria: Vec<Criterion>,
    /// Exact current proposal and its snapshot, absent before proposal.
    pub current_proposal: Option<Proposal>,
    /// Current roster in slot order.
    pub members: Vec<Member>,
    /// Retired members, available for transcript navigation.
    pub replaced_members: Vec<Retired>,
}
/// One acceptance criterion; display text is untrusted.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Criterion {
    /// Broker criterion identity.
    pub id: String,
    /// Full criterion text.
    pub text: String,
}
/// Snapshot available for exactly this proposal sequence.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Proposal {
    /// Proposal entry sequence.
    pub seq: u64,
    /// Untrusted result snapshot.
    pub snapshot: String,
}
/// Vote decision alone never establishes valid readiness.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Vote {
    /// Raw decision; validity is on the enclosing member.
    pub decision: VoteDecision,
}
/// Full public roster row and derived vote validity.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Member {
    /// Stable transcript identity.
    pub agent_id: AgentId,
    /// One-based roster slot.
    pub slot: u8,
    /// Untrusted display name.
    pub name: String,
    /// Untrusted descriptive role.
    pub role: String,
    /// Broker execution status, frozen when completed.
    pub tip_status: Status,
    /// Verified cleanup of the lineage.
    pub cleanup_complete: bool,
    /// Raw vote if any.
    pub vote: Option<Vote>,
    /// Sole authority for counting readiness.
    pub counts: bool,
    /// Why this vote counts or does not count.
    pub why: String,
}
/// Retired roster row; does not contribute to readiness.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Retired {
    /// Stable retired transcript identity.
    pub agent_id: AgentId,
    /// Reused roster slot.
    pub slot: u8,
    /// Historical untrusted name.
    pub name: String,
    /// Historical descriptive role.
    pub role: String,
    /// Stable identity of the replacement.
    pub replaced_by: AgentId,
}
impl PoolStatus {
    /// Counts only broker-valid votes; raw ready decisions may be stale.
    pub fn ready(&self) -> usize {
        self.members.iter().filter(|m| m.counts).count()
    }
}
/// One pool log and its navigation state, retained across tab and member switches.
pub struct Buffer {
    /// Stable pool identity.
    pub id: PoolId,
    /// Current status; absent while opening.
    pub status: Option<PoolStatus>,
    /// Bounded immutable entries in ascending sequence order.
    pub entries: Vec<PoolEntryView>,
    /// Latest sequence actually loaded, never the server's unseen last_seq.
    pub after: u64,
    /// Durable tail known from the last status page; retained window may end earlier.
    pub last_seq: u64,
    /// Whether all older history is loaded.
    pub history_complete: bool,
    /// In-flight reverse cursor; cleared on response or target switch.
    pub older: Option<u64>,
    /// Tail follow flag.
    pub follow: bool,
    /// Absolute rendered top row while scrolled; prepends preserve the anchor.
    pub offset: usize,
    /// Selected chat entry sequence, absent while no entries exist.
    pub cursor: Option<u64>,
    /// Member cursor (current members then expanded retired history).
    pub member: usize,
    /// Whether member navigation has focus.
    pub roster: bool,
    /// Whether replacement history is expanded.
    pub history: bool,
    /// Entry sequences expanded from the default preview.
    pub expanded: BTreeSet<u64>,
    /// Observed proposal snapshots, bounded with retained log entries.
    pub snapshots: BTreeMap<u64, String>,
    /// Last read failure, sanitized at render time.
    pub error: Option<String>,
    /// Per-entry wrapped rows memoized by width and expansion.
    pub memo: std::cell::RefCell<crate::ui::pools::Memo>,
}
impl Buffer {
    /// Opens an empty pool at its tail; initial reverse read supplies newest entries.
    pub fn new(id: PoolId) -> Self {
        Self {
            id,
            status: None,
            entries: vec![],
            after: 0,
            last_seq: 0,
            history_complete: false,
            older: None,
            follow: true,
            offset: 0,
            cursor: None,
            member: 0,
            roster: false,
            history: false,
            expanded: BTreeSet::new(),
            snapshots: BTreeMap::new(),
            error: None,
            memo: std::cell::RefCell::default(),
        }
    }
    /// Merges overlap once by immutable sequence, freezes completed status, and caps memory.
    /// Reverse pages evict the newest edge; following forward pages evict the oldest.
    /// Scrolled forward pages admit only remaining capacity and defer their cursor
    /// at the last retained entry, preserving the reader's older edge.
    /// The next reverse cursor is the minimum entry, never the API's overlapping next_cursor.
    pub fn merge(&mut self, page: Page, width: usize) -> bool {
        if page.pool_id != self.id {
            return false;
        }
        let previous = crate::ui::pools::rows(self, width);
        let anchor = previous.get(self.offset).map(|row| {
            (
                row.seq,
                previous[..self.offset]
                    .iter()
                    .rev()
                    .take_while(|r| r.seq == row.seq)
                    .count(),
            )
        });
        self.last_seq = self.last_seq.max(page.last_seq);
        let backward = page.before_seq.is_some();
        let status_changed = self.status.as_ref() != Some(&page.status)
            && !self
                .status
                .as_ref()
                .is_some_and(|s| s.state == PoolState::Completed);
        if status_changed {
            if let Some(proposal) = &page.status.current_proposal {
                self.snapshots
                    .insert(proposal.seq, proposal.snapshot.clone());
            }
            self.status = Some(page.status);
        }
        let cleared_error = self.error.take().is_some();
        let finished_older = self.older.take().is_some();
        let mut changed = status_changed || cleared_error || finished_older;
        let old_history = self.history_complete;
        if backward {
            self.history_complete = page.complete;
        }
        for entry in page.entries {
            if !backward && !self.follow && self.entries.len() >= ENTRY_CAP {
                continue;
            }
            match self.entries.binary_search_by_key(&entry.seq, |e| e.seq) {
                Ok(_) => {}
                Err(at) => {
                    if !backward {
                        self.after = self.after.max(entry.seq);
                    }
                    self.entries.insert(at, entry);
                    changed = true;
                }
            }
        }
        // Initial tail reads establish a forward cursor; later reverse pages never rewind it.
        if self.after == 0 {
            self.after = self.entries.last().map_or(page.last_seq, |e| e.seq);
        }
        if self.entries.len() > ENTRY_CAP {
            if backward {
                self.entries.truncate(ENTRY_CAP);
                // Resume forward at the retained edge so evicted tail entries can be read again.
                self.after = self.entries.last().map_or(0, |e| e.seq);
            } else {
                self.entries.drain(..self.entries.len() - ENTRY_CAP);
                self.history_complete = false;
            }
        }
        let retained: BTreeSet<_> = self.entries.iter().map(|e| e.seq).collect();
        self.expanded.retain(|seq| retained.contains(seq));
        self.snapshots.retain(|seq, _| {
            retained.contains(seq)
                || self
                    .status
                    .as_ref()
                    .and_then(|s| s.current_proposal.as_ref())
                    .is_some_and(|p| p.seq == *seq)
        });
        self.memo.borrow_mut().retain(&retained);
        if !self.follow && changed {
            let rows = crate::ui::pools::rows(self, width);
            if let Some((seq, within)) = anchor {
                self.offset = rows
                    .iter()
                    .position(|r| r.seq == seq)
                    .map_or(0, |at| at + within);
            }
        }
        if self.follow {
            self.cursor = self.entries.last().map(|e| e.seq);
        }
        changed || old_history != self.history_complete
    }
    /// Returns current or expanded historical member identity, clamped on roster changes.
    pub fn member_id(&self) -> Option<AgentId> {
        let status = self.status.as_ref()?;
        status
            .members
            .get(self.member)
            .map(|m| m.agent_id.clone())
            .or_else(|| {
                self.history
                    .then(|| {
                        status
                            .replaced_members
                            .get(self.member.saturating_sub(status.members.len()))
                    })
                    .flatten()
                    .map(|m| m.agent_id.clone())
            })
    }
}
/// Separate-tab state and small MRU of pool buffers.
#[derive(Default)]
pub struct Pools {
    /// Whether Pools is the selected tab.
    pub visible: bool,
    /// Pool detail focus (full-screen on narrow terminals).
    pub focused: bool,
    /// Existing transcript view was opened from a roster row.
    pub member_transcript: bool,
    /// Transcript opened/restored and awaits one watcher attachment.
    pub member_watch: bool,
    /// Status request still authorized by the selected roster action.
    pub pending_member: Option<AgentId>,
    /// Goal/criteria overlay visibility.
    pub criteria: bool,
    /// Goal/criteria overlay top row.
    pub criteria_offset: usize,
    /// Open and completed discovery summaries for this page.
    pub items: Vec<PoolListView>,
    /// Exact broker-wide open count.
    pub open_total: u64,
    /// Exact broker-wide completed count.
    pub completed_total: u64,
    /// Shared offset of the two independently filtered discovery pages.
    pub offset: usize,
    /// Selected stable pool, including direct CLI targets absent from discovery.
    pub selected: Option<PoolId>,
    /// Selected buffer followed by at most three older selections.
    pub buffers: VecDeque<Buffer>,
    /// Discovery/read failure, bounded by transport and sanitized at rendering.
    pub error: Option<String>,
    /// Broker lacks list_pools on its current pool connection.
    pub fallback: bool,
    /// Stable pointer targets from the last frame.
    pub hits: std::cell::RefCell<Vec<(ratatui::layout::Rect, Target)>>,
}
/// Stable identities recorded for pool pointer actions.
#[derive(Clone)]
pub enum Target {
    /// Pool list row, independent of pending list revisions.
    Pool(PoolId),
    /// Member row in this pool.
    Member(PoolId, AgentId),
    /// Chat row in this pool.
    Entry(PoolId, u64),
}
impl Pools {
    /// Returns the selected cached pool.
    pub fn buffer(&self) -> Option<&Buffer> {
        self.selected
            .as_ref()
            .and_then(|id| self.buffers.iter().find(|b| &b.id == id))
    }
    /// Returns mutable selected pool state.
    pub fn buffer_mut(&mut self) -> Option<&mut Buffer> {
        let id = self.selected.as_ref()?;
        self.buffers.iter_mut().find(|b| &b.id == id)
    }
    /// Selects/restores a pool, preserving chat and roster state and bounding the MRU.
    pub fn select(&mut self, id: PoolId) {
        if self.selected.as_ref() == Some(&id) {
            return;
        }
        if let Some(old) = self.buffer_mut() {
            old.older = None;
        }
        if let Some(at) = self.buffers.iter().position(|b| b.id == id) {
            let buffer = self.buffers.remove(at).expect("known pool");
            self.buffers.push_front(buffer);
        } else {
            self.buffers.push_front(Buffer::new(id.clone()));
        }
        self.buffers.truncate(4);
        self.pending_member = None;
        self.selected = Some(id);
    }
    /// Applies an entire discovery round; exact totals remain independent of loaded rows.
    pub fn listing(
        &mut self,
        offset: usize,
        open: ListPoolsView,
        completed: ListPoolsView,
    ) -> bool {
        if offset != self.offset {
            return false;
        }
        let mut items = open.items;
        items.extend(completed.items);
        let cleared_error = self.error.take().is_some();
        let changed = self.items != items
            || self.open_total != open.total
            || self.completed_total != completed.total
            || cleared_error;
        self.items = items;
        self.open_total = open.total;
        self.completed_total = completed.total;
        if self.selected.is_none() {
            if let Some(item) = self.items.first() {
                self.select(item.pool_id.clone());
            }
        }
        changed
    }
    /// Builds the worker target; invisible tabs issue no pool reads.
    pub fn request(&self, sessions: &[AgentView]) -> Request {
        let b = self.buffer();
        Request {
            visible: self.visible,
            offset: self.offset,
            id: self.selected.clone(),
            after: b.map_or(0, |b| b.after),
            initialized: b.is_some_and(|b| b.status.is_some()),
            before: b.and_then(|b| b.older),
            completed: b
                .and_then(|b| b.status.as_ref())
                .is_some_and(|s| s.state == PoolState::Completed)
                && b.is_some_and(|b| b.after >= b.last_seq),
            candidates: candidates(sessions, self.selected.as_ref()),
        }
    }
}
/// Latest visible read target; changes cancel stale pool-lane requests.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Request {
    /// Poll discovery only while Pools is selected.
    pub visible: bool,
    /// Discovery offset for both state filters.
    pub offset: usize,
    /// Selected stable pool.
    pub id: Option<PoolId>,
    /// Last loaded forward entry sequence.
    pub after: u64,
    /// Whether status and an initial tail page have arrived.
    pub initialized: bool,
    /// On-demand exclusive reverse cursor.
    pub before: Option<u64>,
    /// Completed pools stop forward polling but still allow history.
    pub completed: bool,
    /// Candidate ids from loaded task summaries and explicit CLI selection.
    pub candidates: Vec<PoolId>,
}
/// One-second pool polling interval; public operator reads have no wait parameter.
const POLL: Duration = Duration::from_secs(1);
/// Reads one filtered discovery page using the exact public contract.
pub async fn listing(
    broker: &dyn Broker,
    state: PoolState,
    offset: usize,
) -> agent_run::Result<ListPoolsView> {
    Ok(serde_json::from_value(
        broker
            .call(
                "list_pools",
                json!({"state":state,"limit":50,"offset":offset}),
            )
            .await?,
    )?)
}
/// Extracts deduplicated pool ids from loaded summaries and an optional direct target.
/// Summary matches must use the exact minted ASCII id shape and have no adjacent
/// alphanumeric characters; the broker remains authoritative and must confirm each id.
pub fn candidates(sessions: &[AgentView], direct: Option<&PoolId>) -> Vec<PoolId> {
    let mut found = BTreeMap::new();
    if let Some(id) = direct {
        found.insert(id.to_string(), id.clone());
    }
    for session in sessions {
        let text = session.task_summary.as_str();
        let bytes = text.as_bytes();
        for start in 0..bytes.len().saturating_sub(4) {
            if bytes[start..].starts_with(b"pool-") && start + 31 <= bytes.len() {
                let candidate = &text[start..start + 31];
                if candidate.parse::<PoolId>().is_ok()
                    && (start == 0 || !bytes[start - 1].is_ascii_alphanumeric())
                    && bytes
                        .get(start + 31)
                        .is_none_or(|b| !b.is_ascii_alphanumeric())
                {
                    let id: PoolId = candidate.parse().expect("validated pool id");
                    found.insert(id.to_string(), id);
                }
            }
        }
    }
    found.into_values().collect()
}
/// Reads a tail, forward or older pool page with mutually exclusive signed cursors.
pub async fn read(broker: &dyn Broker, request: &Request) -> agent_run::Result<Page> {
    let mut params = json!({"pool_id":request.id,"limit":50});
    if let Some(before) = request.before {
        params["before_seq"] = json!(before);
    } else if !request.initialized {
        params["before_seq"] = json!(i64::MAX);
    } else {
        params["after_seq"] = json!(request.after);
    }
    let page: Page = serde_json::from_value(broker.call("pool", params).await?)?;
    for entry in &page.entries {
        entry.validate()?;
    }
    Ok(page)
}
/// Polls visible discovery/status on the fourth socket, cancelling target changes.
/// List refreshes and regular forward polls are throttled independently; older pages
/// and target switches are immediate. Method-not-found selects the read-confirmed,
/// session-derived compatibility path until the pool socket generation changes;
/// failed rounds keep last valid UI data.
pub async fn worker(
    broker: SharedBroker,
    mut requests: watch::Receiver<Request>,
    tx: mpsc::Sender<BrokerEvent>,
) {
    let mut list_supported = true;
    let mut connection_generation = broker.pool_connection_generation();
    let mut list_at = tokio::time::Instant::now();
    let mut pool_at = list_at;
    let mut last = Request::default();
    loop {
        let current_generation = broker.pool_connection_generation();
        if current_generation != connection_generation {
            connection_generation = current_generation;
            list_supported = true;
        }
        let target = requests.borrow_and_update().clone();
        if !target.visible {
            if requests.changed().await.is_err() {
                return;
            }
            continue;
        }
        let changed_pool = target.id != last.id || (!last.visible && target.visible);
        if changed_pool || target.before != last.before && target.before.is_some() {
            pool_at = tokio::time::Instant::now();
        }
        last = target.clone();
        let now = tokio::time::Instant::now();
        if now < list_at.min(pool_at) {
            tokio::select! {
                _ = tokio::time::sleep_until(list_at.min(pool_at)) => {},
                result = requests.changed() => { if result.is_err() { return; } }
            }
            continue;
        }
        if now >= list_at {
            list_at = tokio::time::Instant::now() + POLL;
            let round = async {
                if list_supported {
                    match listing(&*broker, PoolState::Open, target.offset).await {
                        Ok(open) => {
                            match listing(&*broker, PoolState::Completed, target.offset).await {
                                Ok(completed) => return Ok((open, completed, false)),
                                Err(error) => return Err(error),
                            }
                        }
                        Err(error)
                            if error
                                .to_string()
                                .to_ascii_lowercase()
                                .contains("method not found") =>
                        {
                            list_supported = false
                        }
                        Err(error) => return Err(error),
                    }
                }
                let mut open_items = Vec::new();
                let mut completed_items = Vec::new();
                for id in &target.candidates {
                    let Ok(value) = broker.call("pool", json!({"pool_id":id,"limit":1})).await
                    else {
                        continue;
                    };
                    let Ok(page) = serde_json::from_value::<Page>(value) else {
                        continue;
                    };
                    let s = page.status;
                    let item = PoolListView {
                        pool_id: id.clone(),
                        state: s.state,
                        goal: s.goal.chars().take(128).collect(),
                        goal_truncated: s.goal.chars().count() > 128,
                        created_at: 0.0,
                        last_seq: page.last_seq,
                        completed_at: None,
                        roster_revision: s.roster_revision,
                        members_count: s.members.len(),
                        ready: s.ready(),
                        current_proposal_seq: s.current_proposal.as_ref().map(|p| p.seq),
                        members: s
                            .members
                            .into_iter()
                            .map(|m| PoolListMemberView {
                                slot: m.slot,
                                name: m.name,
                                role: m.role,
                                agent_id: m.agent_id,
                                tip_status: m.tip_status,
                            })
                            .collect(),
                    };
                    if item.state == PoolState::Open {
                        open_items.push(item);
                    } else {
                        completed_items.push(item);
                    }
                }
                let page = |items: Vec<PoolListView>| ListPoolsView {
                    total: items.len() as u64,
                    items,
                    offset: 0,
                    limit: 50,
                    next_offset: None,
                    complete: true,
                };
                Ok((page(open_items), page(completed_items), true))
            };
            let result = tokio::select! {
                result = round => result.map_err(|e| e.to_string()),
                changed = requests.changed() => { if changed.is_err() { return; } continue; }
            };
            list_at = tokio::time::Instant::now() + POLL;
            if tx
                .send(BrokerEvent::Pools {
                    offset: target.offset,
                    fallback: result.as_ref().is_ok_and(|(_, _, fallback)| *fallback)
                        || !list_supported,
                    page: result.map(|(open, completed, _)| (open, completed)),
                })
                .await
                .is_err()
            {
                return;
            }
        }
        if tokio::time::Instant::now() >= pool_at {
            if target.id.is_some() && (!target.completed || target.before.is_some()) {
                let result = tokio::select! {
                    result = read(&*broker, &target) => result.map(Box::new).map_err(|e| e.to_string()),
                    changed = requests.changed() => { if changed.is_err() { return; } continue; }
                };
                let id = target.id.clone().expect("checked pool");
                if tx
                    .send(BrokerEvent::Pool {
                        id,
                        before: target.before,
                        page: result,
                    })
                    .await
                    .is_err()
                {
                    return;
                }
            }
            pool_at = tokio::time::Instant::now() + POLL;
        }
    }
}
/// Pool-specific actions, routed through the existing pure reducer and frame pacer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    /// Change selection/member/chat cursor by a signed delta.
    Move(i64),
    /// Scroll chat by rendered rows.
    Scroll(i64),
    /// Open detail, transcript or expanded chat entry according to focus.
    Open,
    /// Return to list or dismiss full criteria.
    Back,
    /// Toggle member/chat focus.
    Roster,
    /// Toggle replacement history.
    History,
    /// Full sanitized goal and criteria.
    Criteria,
    /// Toggle tail following.
    Follow,
    /// Load older entries and jump to current loaded top.
    Top,
    /// Follow the tail.
    Bottom,
    /// Open selected member transcript.
    Transcript,
    /// Discovery page step, 50 per state.
    Page(i64),
}
/// Maps pool keys after global quit/help/tab keys and transcript keys.
pub fn key(app: &App, code: KeyCode) -> crate::events::Action {
    use crate::events::Action as Global;
    let action = match code {
        KeyCode::Esc | KeyCode::Left | KeyCode::Backspace => Action::Back,
        KeyCode::Up | KeyCode::Char('k') => Action::Move(-1),
        KeyCode::Down | KeyCode::Char('j') => Action::Move(1),
        KeyCode::PageUp => Action::Scroll(-10),
        KeyCode::PageDown => Action::Scroll(10),
        KeyCode::Enter | KeyCode::Right | KeyCode::Char(' ') => Action::Open,
        KeyCode::Char('m') => Action::Roster,
        KeyCode::Char('h') => Action::History,
        KeyCode::Char('c') => Action::Criteria,
        KeyCode::Char('f') => Action::Follow,
        KeyCode::Home | KeyCode::Char('g') => Action::Top,
        KeyCode::End | KeyCode::Char('G') => Action::Bottom,
        KeyCode::Char('t') => Action::Transcript,
        KeyCode::Char('[') => Action::Page(-1),
        KeyCode::Char(']') => Action::Page(1),
        KeyCode::Char('q') => return Global::Quit,
        KeyCode::Char('?') => return Global::Help,
        _ => return Global::None,
    };
    if app.pools.criteria
        && !matches!(
            action,
            Action::Back | Action::Move(_) | Action::Scroll(_) | Action::Top | Action::Bottom
        )
    {
        return Global::None;
    }
    Global::Pool(action)
}
/// Returns the rendered pool body width, matching split and narrow panes.
pub fn width(app: &App) -> usize {
    if app.split_view() {
        usize::from(app.last_width.saturating_sub(crate::ui::LIST_COLS + 4))
    } else {
        usize::from(app.last_width.saturating_sub(3))
    }
}
/// Requests one older page when the scrolled viewport reaches its loaded beginning.
pub fn older(app: &mut App) {
    let width = width(app);
    if let Some(buffer) = app.pools.buffer_mut() {
        if !buffer.follow
            && buffer.offset < 10
            && !buffer.history_complete
            && buffer.older.is_none()
        {
            buffer.older = buffer.entries.first().map(|e| e.seq);
            // Keep width-dependent rows cached before prepend for anchoring.
            let _ = crate::ui::pools::rows(buffer, width);
        }
    }
}
/// Applies a pool action without broker writes; member reads dispatch through the one-shot lane.
/// Criteria navigation clamps and persists wrapped offsets at the current viewport size,
/// so End and oversized steps remain immediately reversible.
pub fn apply(app: &mut App, action: Action) -> Dispatched {
    app.dirty = true;
    if app.pools.criteria {
        let max = crate::ui::pools::criteria_max_offset(app);
        app.pools.criteria_offset = app.pools.criteria_offset.min(max);
        match action {
            Action::Back => app.pools.criteria = false,
            Action::Move(d) | Action::Scroll(d) => {
                app.pools.criteria_offset = app
                    .pools
                    .criteria_offset
                    .saturating_add_signed(d as isize)
                    .min(max)
            }
            Action::Top => app.pools.criteria_offset = 0,
            Action::Bottom => app.pools.criteria_offset = max,
            _ => {}
        }
        return Dispatched::None;
    }
    if matches!(action, Action::Transcript)
        || matches!(action, Action::Open)
            && app.pools.focused
            && app.pools.buffer().is_some_and(|b| b.roster)
    {
        if let Some(agent) = app.pools.buffer().and_then(Buffer::member_id) {
            app.pools.pending_member = Some(agent.clone());
            return Dispatched::PoolMember(agent);
        }
        return Dispatched::None;
    }
    if let Action::Page(delta) = action {
        let max = app
            .pools
            .open_total
            .max(app.pools.completed_total)
            .saturating_sub(1) as usize
            / 50
            * 50;
        app.pools.offset = app
            .pools
            .offset
            .saturating_add_signed(delta as isize * 50)
            .min(max);
        return Dispatched::None;
    }
    if !app.pools.focused {
        match action {
            Action::Move(delta) | Action::Scroll(delta) => {
                let at = app
                    .pools
                    .items
                    .iter()
                    .position(|i| Some(&i.pool_id) == app.pools.selected.as_ref())
                    .unwrap_or(0);
                let at = at
                    .saturating_add_signed(delta as isize)
                    .min(app.pools.items.len().saturating_sub(1));
                if let Some(item) = app.pools.items.get(at) {
                    app.pools.select(item.pool_id.clone());
                }
            }
            Action::Open => app.pools.focused = app.pools.selected.is_some(),
            Action::Back => {}
            _ => {
                app.pools.focused = app.pools.selected.is_some();
            }
        }
        if !app.pools.focused || matches!(action, Action::Open) {
            return Dispatched::None;
        }
    }
    let width = width(app);
    let viewport = crate::ui::pools::viewport(app);
    if let Some(buffer) = app.pools.buffer_mut() {
        let total = crate::ui::pools::rows(buffer, width).len();
        match action {
            Action::Back => {
                app.pools.focused = false;
                app.pools.pending_member = None;
            }
            Action::Roster => buffer.roster = !buffer.roster,
            Action::History => {
                buffer.history = !buffer.history;
                buffer.member = 0;
            }
            Action::Criteria => {
                app.pools.criteria = true;
                app.pools.criteria_offset = 0;
            }
            Action::Move(delta) if buffer.roster => {
                let count = buffer.status.as_ref().map_or(0, |s| {
                    s.members.len()
                        + if buffer.history {
                            s.replaced_members.len()
                        } else {
                            0
                        }
                });
                buffer.member = buffer
                    .member
                    .saturating_add_signed(delta as isize)
                    .min(count.saturating_sub(1));
            }
            Action::Move(delta) => {
                let at = buffer
                    .cursor
                    .and_then(|seq| buffer.entries.iter().position(|e| e.seq == seq))
                    .unwrap_or(0);
                let at = at
                    .saturating_add_signed(delta as isize)
                    .min(buffer.entries.len().saturating_sub(1));
                buffer.cursor = buffer.entries.get(at).map(|e| e.seq);
                let rows = crate::ui::pools::rows(buffer, width);
                if let Some(row) = rows.iter().position(|r| Some(r.seq) == buffer.cursor) {
                    let offset = if buffer.follow {
                        total.saturating_sub(viewport)
                    } else {
                        buffer.offset
                    };
                    buffer.offset = if row < offset {
                        row
                    } else if row >= offset + viewport {
                        row.saturating_sub(viewport / 2)
                    } else {
                        offset
                    };
                    buffer.follow = false;
                }
            }
            Action::Scroll(delta) => {
                let offset = if buffer.follow {
                    total.saturating_sub(viewport)
                } else {
                    buffer.offset
                };
                buffer.offset = offset
                    .saturating_add_signed(delta as isize)
                    .min(total.saturating_sub(viewport));
                buffer.follow = delta > 0 && buffer.offset == total.saturating_sub(viewport);
            }
            Action::Open => {
                if let Some(seq) = buffer.cursor {
                    if !buffer.expanded.remove(&seq) {
                        buffer.expanded.insert(seq);
                    }
                }
            }
            Action::Follow => {
                buffer.follow = !buffer.follow;
            }
            Action::Top => {
                buffer.follow = false;
                buffer.offset = 0;
            }
            Action::Bottom => {
                buffer.follow = true;
                buffer.cursor = buffer.entries.last().map(|e| e.seq);
            }
            _ => {}
        }
    }
    older(app);
    Dispatched::None
}
/// Opens a roster transcript using the existing buffer and watcher, preserving pool state.
pub fn open_member(app: &mut App, agent: AgentView) -> Dispatched {
    if !app.pools.visible
        || app.pools.pending_member.as_ref() != Some(&agent.agent_id)
        || app.pools.buffer().and_then(Buffer::member_id).as_ref() != Some(&agent.agent_id)
    {
        return Dispatched::None;
    }
    app.pools.pending_member = None;
    app.close_transcript();
    let id = agent.agent_id.clone();
    let run = agent.run_id.clone();
    let identity = app.watch_agent(agent);
    app.pools.member_watch = true;
    app.screen = Screen::Transcript;
    app.pools.member_transcript = true;
    app.dirty = true;
    identity.map_or(Dispatched::Watch(id, run, 0), |(id, run, cursor)| {
        Dispatched::Watch(id, run, cursor)
    })
}
/// Handles stable pool row/member/entry clicks; coordinates refer to the last completed frame.
pub fn mouse(app: &mut App, mouse: MouseEvent) -> Dispatched {
    if mouse.kind != MouseEventKind::Down(MouseButton::Left) {
        return Dispatched::None;
    }
    if app.pools.criteria {
        app.pools.criteria = false;
        return Dispatched::None;
    }
    let hit = app
        .pools
        .hits
        .borrow()
        .iter()
        .find(|(r, _)| r.contains((mouse.column, mouse.row).into()))
        .map(|(_, h)| h.clone());
    match hit {
        Some(Target::Pool(id)) => {
            let repeated = app.pools.selected.as_ref() == Some(&id);
            app.pools.select(id);
            app.pools.focused = repeated;
        }
        Some(Target::Member(id, agent)) if app.pools.selected.as_ref() == Some(&id) => {
            app.pools.focused = true;
            if let Some(b) = app.pools.buffer_mut() {
                if let Some(s) = &b.status {
                    if let Some(at) = s
                        .members
                        .iter()
                        .map(|m| &m.agent_id)
                        .chain(s.replaced_members.iter().map(|m| &m.agent_id))
                        .position(|id| id == &agent)
                    {
                        b.member = at;
                        b.roster = true;
                    }
                }
            }
        }
        Some(Target::Entry(id, seq)) if app.pools.selected.as_ref() == Some(&id) => {
            app.pools.focused = true;
            if let Some(b) = app.pools.buffer_mut() {
                b.roster = false;
                b.cursor = Some(seq);
                if !b.expanded.remove(&seq) {
                    b.expanded.insert(seq);
                }
            }
        }
        _ => {}
    }
    Dispatched::None
}

/// Pool contract, reducer, worker and six deterministic frame regressions.
#[cfg(test)]
#[path = "pools_tests.rs"]
mod tests;
