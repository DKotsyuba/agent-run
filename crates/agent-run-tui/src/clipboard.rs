//! Complete, bounded chat export and user-local clipboard writes, independent of rendering caches.
use crate::net::{Broker, SharedBroker};
use agent_run_domain::domain::AgentId;
use agent_run_domain::pool::{PoolId, render_entry};
use agent_run_domain::transcript::{TranscriptQuery, TranscriptView};
use agent_run_domain::views::TranscriptPage;
use serde_json::{Value, json};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch};

/// Maximum complete exported UTF-8 text; oversized histories fail without touching the clipboard.
const MAX_BYTES: usize = 32 * 1024 * 1024;
/// Bounds pathological empty/progressing page sequences independently of the output byte bound.
const MAX_PAGES: usize = 10_000;
/// Whole history fetch budget, independent of ongoing transcript/pool watchers.
const FETCH_TIMEOUT: Duration = Duration::from_secs(30);

/// Immutable viewer selection captured when the operator presses copy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Target {
    /// Exact run when present, otherwise the viewer's retained logical-agent lineage.
    Agent {
        /// Stable viewer identity.
        agent: AgentId,
        /// Viewer-selected legacy execution, absent for retained lineage history.
        run: Option<AgentId>,
    },
    /// One complete immutable cooperative pool log.
    Pool(PoolId),
}
impl Target {
    /// Short explicit identity used by copying/result feedback after the operator changes selection.
    pub fn label(&self) -> String {
        match self {
            Self::Agent { agent, .. } => {
                format!("agent {}", crate::app::id_hash(agent.as_str(), 10))
            }
            Self::Pool(id) => format!("pool {}", crate::app::id_hash(id.as_str(), 10)),
        }
    }
}
/// One copy request; its generation rejects late completions after a newer copy request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Request {
    /// Monotonic local request generation; never a broker mutation identifier.
    pub generation: u64,
    /// Frozen target; navigation never retargets an admitted copy.
    pub target: Target,
}

/// Clipboard completion future; implementations own cancellation and complete-payload writes.
pub type ClipboardFuture<'a> = Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;
/// Native writer seam; test implementations record only fully fetched synthetic payloads.
pub trait Clipboard: Send + Sync {
    /// Writes complete input once; never supplies a partial payload. Native commit races report unknown outcome.
    fn write<'a>(&'a self, text: String, cancel: watch::Receiver<bool>) -> ClipboardFuture<'a>;
}
/// macOS native clipboard writer; other platforms report unavailable without changing clipboard state.
pub struct NativeClipboard;

impl Clipboard for NativeClipboard {
    /// Gives pbcopy a complete staged file, then waits for its exit or kills/reaps on cancel/timeout.
    /// A race after native commit reports copied or unknown outcome; cancellation never claims to undo it.
    fn write<'a>(&'a self, text: String, cancel: watch::Receiver<bool>) -> ClipboardFuture<'a> {
        Box::pin(async move {
            #[cfg(not(target_os = "macos"))]
            {
                let _ = (text, cancel);
                Err("Native clipboard is unavailable on this platform".into())
            }
            #[cfg(target_os = "macos")]
            {
                let mut cancel = cancel;
                let file = tokio::task::spawn_blocking(move || staged_input(&text))
                    .await
                    .map_err(|_| "Clipboard staging failed".to_string())??;
                if *cancel.borrow() {
                    return Err("Copy cancelled; clipboard unchanged".into());
                }
                let mut child = tokio::process::Command::new("/usr/bin/pbcopy")
                    .stdin(std::process::Stdio::from(file))
                    .stdout(std::process::Stdio::null())
                    .stderr(std::process::Stdio::null())
                    .kill_on_drop(true)
                    .spawn()
                    .map_err(|_| "Clipboard is unavailable".to_string())?;
                let result = tokio::select! {
                    result = child.wait() => Some(result),
                    _ = cancel.changed() => None,
                    _ = tokio::time::sleep(Duration::from_secs(2)) => None,
                };
                if let Some(result) = result {
                    return match result {
                        Ok(status) if status.success() => Ok(()),
                        _ => Err("Clipboard command failed; native outcome unknown".into()),
                    };
                }
                let _ = child.kill().await;
                let status = child.wait().await;
                if status.is_ok_and(|status| status.success()) {
                    return Ok(());
                }
                Err(if *cancel.borrow() {
                    "Clipboard cancellation outcome unknown"
                } else {
                    "Clipboard timeout outcome unknown"
                }
                .into())
            }
        })
    }
}

/// Builds a private, already-complete, unlinked stdin file; partial writes can never become clipboard input.
/// All temporary paths are created exclusively with owner-only permissions and removed before child launch.
#[cfg(target_os = "macos")]
fn staged_input(text: &str) -> Result<std::fs::File, String> {
    use std::io::{Seek, Write};
    use std::os::unix::fs::OpenOptionsExt;
    use std::sync::atomic::{AtomicU64, Ordering};
    /// Unique names within this process; exclusive creation rejects hostile existing paths.
    static SERIAL: AtomicU64 = AtomicU64::new(0);
    let path = std::env::temp_dir().join(format!(
        ".agent-run-copy-{}-{}",
        std::process::id(),
        SERIAL.fetch_add(1, Ordering::Relaxed)
    ));
    let mut file = std::fs::OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .mode(0o600)
        .open(&path)
        .map_err(|_| "Clipboard staging failed".to_string())?;
    let written = file.write_all(text.as_bytes()).and_then(|()| file.rewind());
    std::fs::remove_file(&path).map_err(|_| "Clipboard staging cleanup failed".to_string())?;
    written.map_err(|_| "Clipboard staging failed".to_string())?;
    Ok(file)
}

/// Adds one unwrapped payload part without dropping whitespace; overflow refuses the entire copy.
fn append(out: &mut String, value: &str) -> Result<(), String> {
    if out.len().saturating_add(value.len()) > MAX_BYTES {
        return Err("History exceeds the 32 MiB copy limit; clipboard unchanged".into());
    }
    out.push_str(value);
    Ok(())
}

/// Reads raw complete history through a frozen tail watermark; never follows newly appended messages.
/// Broker errors, missing/unknown body coverage, bad cursor progress and resource ceilings refuse the copy.
pub async fn export(broker: &dyn Broker, target: &Target) -> Result<String, String> {
    match target {
        Target::Pool(id) => export_pool(broker, id).await,
        Target::Agent { agent, run } => export_agent(broker, agent, run.as_ref()).await,
    }
}

/// Fetches and validates one raw transcript page on the independent copy connection.
async fn agent_page(
    broker: &dyn Broker,
    agent: &AgentId,
    run: Option<&AgentId>,
    params: Value,
) -> Result<TranscriptPage, String> {
    let mut params = params;
    params["agent_id"] = json!(agent);
    if let Some(run) = run {
        params["run_id"] = json!(run);
    }
    let value = broker
        .copy_call("transcript", params)
        .await
        .map_err(|e| e.to_string())?;
    let page: TranscriptPage = serde_json::from_value(value)
        .map_err(|_| "Invalid transcript copy response".to_string())?;
    if &page.agent_id != agent
        || page
            .run_id
            .as_ref()
            .is_some_and(|actual| Some(actual) != run)
    {
        return Err("Transcript identity changed; clipboard unchanged".into());
    }
    Ok(page)
}

/// Captures an immutable tail and copies complete raw bodies as readable logical blocks.
/// Known native identities concatenate continuations byte-faithfully across pages; block-level error
/// evidence is reported after the body, never between fragments. No wrapping or whitespace trimming occurs.
async fn export_agent(
    broker: &dyn Broker,
    agent: &AgentId,
    run: Option<&AgentId>,
) -> Result<String, String> {
    let tail = agent_page(
        broker,
        agent,
        run,
        serde_json::to_value(TranscriptQuery {
            view: TranscriptView::Blocks,
            tail_blocks: Some(1),
            ..TranscriptQuery::default()
        })
        .map_err(|e| e.to_string())?,
    )
    .await?;
    if tail.view != Some(TranscriptView::Blocks) {
        return Err(
            "Broker cannot provide a transcript snapshot watermark; clipboard unchanged".into(),
        );
    }
    let watermark = match (tail.resume_cursor, tail.messages.is_empty()) {
        (Some(seq), false) if seq > 0 => seq,
        (None, true) => 0,
        _ => return Err("Invalid transcript snapshot watermark; clipboard unchanged".into()),
    };
    let mut out = format!(
        "agent-run transcript {agent}\nScope: {}\nSnapshot through #{watermark}\nUntrusted transcript text\n\n",
        if run.is_some() {
            "selected execution"
        } else {
            "retained lineage"
        }
    );
    let mut cursor = 0;
    let mut sanitizer = crate::ui::text::Sanitizer::default();
    let mut identity = None;
    let mut facts = std::collections::BTreeSet::new();
    for _ in 0..MAX_PAGES {
        if cursor >= watermark {
            return Ok(out);
        }
        let page = agent_page(broker, agent, run, json!({"cursor":cursor,"limit":1000})).await?;
        let previous = cursor;
        for message in &page.messages {
            if message.seq <= cursor {
                return Err("Transcript cursor did not advance; clipboard unchanged".into());
            }
            if message.seq > watermark {
                break;
            }
            match message.content_complete {
                Some(true) if message.omitted_bytes.unwrap_or(0) == 0 => {}
                Some(false) => {
                    return Err(
                        "Transcript contains spooled or incomplete text; clipboard unchanged"
                            .into(),
                    );
                }
                _ => {
                    return Err("Legacy transcript coverage is unknown; clipboard unchanged".into());
                }
            }
            let key = (
                message.role.clone(),
                message.name.clone(),
                message.raw_ref.clone(),
            );
            let continuation = message.raw_ref.is_some()
                && identity.as_ref() == Some(&key)
                && message.starts_block != Some(true);
            if !continuation {
                if identity.is_some() {
                    finish_block(&mut out, &mut facts)?;
                }
                sanitizer = crate::ui::text::Sanitizer::default();
                let name = message
                    .name
                    .as_deref()
                    .map_or(String::new(), |name| format!(" · {}", header_field(name)));
                let native = message
                    .raw_ref
                    .as_deref()
                    .map_or(String::new(), |id| format!(" · id {}", header_field(id)));
                append(
                    &mut out,
                    &format!(
                        "{}{}{} · #{}\n",
                        header_field(&message.role),
                        name,
                        native,
                        message.seq
                    ),
                )?;
                identity = Some(key);
            }
            if message.error.is_some() || message.error_source.is_some() {
                facts.insert((message.error, message.error_source.clone()));
            }
            append(&mut out, &sanitizer.push(&message.content))?;
            cursor = message.seq;
        }
        if cursor >= watermark {
            if identity.is_some() {
                finish_block(&mut out, &mut facts)?;
            }
            return Ok(out);
        }
        if cursor == previous || page.complete {
            return Err("Transcript snapshot history is unavailable; clipboard unchanged".into());
        }
        if page.next_cursor != Some(cursor) {
            return Err("Invalid transcript continuation cursor; clipboard unchanged".into());
        }
    }
    Err("Transcript exceeds the copy page limit; clipboard unchanged".into())
}

/// Sanitizes one header field and keeps native labels on one line without changing body whitespace.
fn header_field(value: &str) -> String {
    agent_run::transcript::sanitize(value).replace(['\n', '\r', '\t'], " ")
}

/// Closes a complete logical body and reports every distinct native error/source observation.
/// The added framing follows the body; no body byte is trimmed or interleaved with metadata.
fn finish_block(
    out: &mut String,
    facts: &mut std::collections::BTreeSet<(Option<bool>, Option<String>)>,
) -> Result<(), String> {
    append(out, "\n")?;
    for (error, source) in std::mem::take(facts) {
        append(
            out,
            &format!(
                "[tool status: error={} · source={}]\n",
                error.map_or("unknown".into(), |error| error.to_string()),
                source.as_deref().map_or("unreported".into(), header_field)
            ),
        )?;
    }
    append(out, "\n")
}

/// Copies every pool entry through the first page's durable watermark using the canonical shared formatter.
async fn export_pool(broker: &dyn Broker, id: &PoolId) -> Result<String, String> {
    let mut cursor = 0;
    let mut watermark = None;
    let mut out = String::new();
    for index in 0..MAX_PAGES {
        let value = broker
            .copy_call("pool", json!({"pool_id":id,"after_seq":cursor,"limit":50}))
            .await
            .map_err(|e| e.to_string())?;
        let page: crate::pools::Page =
            serde_json::from_value(value).map_err(|_| "Invalid pool copy response".to_string())?;
        if &page.pool_id != id {
            return Err("Pool identity changed; clipboard unchanged".into());
        }
        if index == 0 {
            let last = page.last_seq.unwrap_or(0);
            if last == 0 && !page.entries.is_empty() {
                return Err("Invalid empty pool watermark".into());
            }
            watermark = Some(last);
            append(
                &mut out,
                &format!("agent-run pool {id}\nSnapshot through #{last}\n\n"),
            )?;
        }
        let boundary = watermark.expect("first page supplied watermark");
        let previous = cursor;
        for entry in &page.entries {
            if entry.seq <= cursor {
                return Err("Pool cursor did not advance; clipboard unchanged".into());
            }
            if entry.seq > boundary {
                break;
            }
            let text = render_entry(entry).map_err(|e| e.to_string())?;
            append(&mut out, &agent_run::transcript::sanitize(&text))?;
            append(&mut out, "\n\n")?;
            cursor = entry.seq;
        }
        if cursor >= boundary {
            return Ok(out);
        }
        if cursor == previous || page.complete {
            return Err("Pool snapshot history is unavailable; clipboard unchanged".into());
        }
    }
    Err("Pool exceeds the copy page limit; clipboard unchanged".into())
}

/// Fetches one complete payload under a deadline/cancellation, then invokes the complete-input writer once.
async fn execute(
    broker: &dyn Broker,
    clipboard: &dyn Clipboard,
    request: &Request,
    mut cancel: watch::Receiver<bool>,
) -> Result<usize, String> {
    let text = tokio::select! {
        result = tokio::time::timeout(FETCH_TIMEOUT,export(broker,&request.target)) =>
            result.map_err(|_|"Copy timed out; clipboard unchanged".to_string())??,
        _ = cancel.changed() => return Err("Copy cancelled".into()),
    };
    if *cancel.borrow() {
        return Err("Copy cancelled".into());
    }
    let bytes = text.len();
    clipboard.write(text, cancel).await?;
    Ok(bytes)
}

/// Serializes frozen jobs and reports actual outcomes, including full copies that beat cancellation.
/// Channel closure cancels and awaits cleanup without waiting on a potentially full UI event queue.
pub async fn worker(
    broker: SharedBroker,
    clipboard: Arc<dyn Clipboard>,
    mut requests: watch::Receiver<Option<Request>>,
    tx: mpsc::Sender<crate::events::BrokerEvent>,
) {
    loop {
        let Some(request) = requests.borrow_and_update().clone() else {
            if requests.changed().await.is_err() {
                return;
            }
            continue;
        };
        let (cancel_tx, cancel_rx) = watch::channel(false);
        let job = execute(&*broker, &*clipboard, &request, cancel_rx);
        tokio::pin!(job);
        let mut changed_during_job = false;
        let result = tokio::select! {
            result = &mut job => result,
            changed = requests.changed() => {
                let _ = cancel_tx.send(true);
                let result = job.await;
                if changed.is_err() { return; }
                changed_during_job = true;
                result
            }
        };
        let event = crate::events::BrokerEvent::Copied {
            generation: request.generation,
            target: request.target.clone(),
            result,
        };
        let send = tx.send(event);
        tokio::pin!(send);
        loop {
            tokio::select! {
                result = &mut send => {if result.is_err() {return;} break;},
                changed = requests.changed() => {
                    if changed.is_err() {return;}
                    changed_during_job = true;
                }
            }
        }
        if !changed_during_job && requests.changed().await.is_err() {
            return;
        }
    }
}

#[cfg(test)]
#[path = "clipboard_tests.rs"]
mod tests;
