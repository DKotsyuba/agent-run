//! Bounded evidence that a native command returned an unfinished handle to its caller.
//!
//! A PTY processId identifies synchronous commands too. The owned rollout's
//! complete tool-output rows distinguish a returned pending session without
//! requiring an experimental raw-response subscription on native resume.
//! ponytail: one bounded read on the existing supervisor tick; use a native
//! returned-handle event instead when that event is supported on resume.
use crate::{Result, error::invalid};
use agent_run_domain::error::Error;
use agent_run_platform::fs::Dir;
use serde_json::Value;
use std::{
    collections::BTreeSet,
    fs::File,
    io::{BufRead, BufReader, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
};

/// One bounded filesystem read per supervisor pass; never scans old history.
const READ_BYTES: usize = 64 * 1024;
/// Maximum buffered native JSONL row, including a partial trailing row.
const ROW_BYTES: usize = 4 * 1024 * 1024;
/// Fail closed instead of retaining unlimited model-visible handle evidence.
const HANDLES: usize = 4096;

/// A descriptor-confined, attempt-local observer of one exact native rollout.
/// Only complete JSONL rows after admission are consumed. Thread identity is
/// anchored by session_meta; opaque stdout, instructions and old turns never
/// become wait evidence. Read/malformed/shrink errors disable this observer at
/// its caller, which records an existing coverage-gap diagnostic without waking.
pub(super) struct NativeReturns {
    /// Regular file once the native writer materializes it; absent during startup.
    file: Option<File>,
    /// Already-open admitted home; later opens cannot escape it through links.
    directory: Dir,
    /// Validated session-relative path, never scanned or created by this observer.
    relative: PathBuf,
    /// Verified native thread identity, never exposed as diagnostic text.
    thread: String,
    /// Partial bytes retained until the complete native JSONL row arrives.
    partial: Vec<u8>,
    /// Whether this file's first complete row verified the exact thread.
    anchored: bool,
    /// Current offset, used to reject truncation rather than replay old evidence.
    offset: u64,
    /// Last native turn context, for historical output rows without passthrough.
    context: Option<String>,
    /// Pending handles explicitly returned by a tool in an owned native turn.
    pending: BTreeSet<(String, String)>,
    /// Complete rollout turn boundaries; earlier output rows have been consumed.
    closed: BTreeSet<String>,
}

impl NativeReturns {
    /// Opens the native response's exact sessions path beneath its admitted home.
    /// Reads at most ROW_BYTES+1 header bytes to verify session_meta, then starts
    /// at the old EOF. A not-yet-written header is verified on its first complete
    /// row instead. Native lazy creation is retried from offset0 without losing
    /// initial returns. Foreign/nonregular/symlink paths fail without scans;
    /// old rollout content is never imported or sent to a model or persistence.
    pub(super) fn open(home: &Path, response: &Value, thread: &str) -> Result<Self> {
        let path = response
            .pointer("/thread/path")
            .or_else(|| response.get("path"))
            .and_then(Value::as_str)
            .ok_or_else(|| invalid("native return observation path missing"))?;
        let relative = Path::new(path)
            .strip_prefix(home)
            .map_err(|_| invalid("native return path is outside its home"))?;
        if !relative.starts_with("sessions") {
            return Err(invalid("native return path is not a session rollout"));
        }
        let relative = relative.to_path_buf();
        let _ = agent_run_platform::fs::relative(&relative)?;
        let directory = Dir::open(home)?;
        let mut file = optional_file(&directory, &relative)?;
        let (anchored, offset) = match file.as_mut() {
            Some(file) => Self::admission(file, thread)?,
            None => (false, 0),
        };
        Ok(Self {
            file,
            directory,
            relative,
            thread: thread.into(),
            partial: Vec::new(),
            anchored,
            offset,
            context: None,
            pending: BTreeSet::new(),
            closed: BTreeSet::new(),
        })
    }

    /// Capture a pre-existing rollout's complete admission boundary.
    /// The caller owns the regular descriptor. A partial last row is restarted,
    /// not treated as a new JSON suffix; I/O, identity and size errors propagate.
    fn admission(file: &mut File, thread: &str) -> Result<(bool, u64)> {
        let mut header = Vec::new();
        BufReader::new(&mut *file)
            .take((ROW_BYTES + 1) as u64)
            .read_until(b'\n', &mut header)?;
        let anchored = if let Some(end) = header.iter().position(|b| *b == b'\n') {
            Self::anchor(&header[..end], thread)?;
            true
        } else {
            if header.len() > ROW_BYTES {
                return Err(invalid("native return header exceeds bound"));
            }
            false
        };
        let offset = if anchored {
            let end = file.seek(SeekFrom::End(0))?;
            file.seek(SeekFrom::Start(end.saturating_sub(1)))?;
            let mut last = [0_u8; 1];
            file.read_exact(&mut last)?;
            let boundary = if last[0] == b'\n' {
                end
            } else {
                // Admission can overlap a partially appended settings row.
                // Restart that row, never a suffix that could look like JSON.
                let start = end.saturating_sub(ROW_BYTES as u64);
                file.seek(SeekFrom::Start(start))?;
                let mut tail = Vec::new();
                (&mut *file).take(end - start).read_to_end(&mut tail)?;
                start
                    + tail
                        .iter()
                        .rposition(|b| *b == b'\n')
                        .ok_or_else(|| invalid("native return admission row exceeds bound"))?
                        as u64
                    + 1
            };
            file.seek(SeekFrom::Start(boundary))?
        } else {
            file.seek(SeekFrom::Start(0))?
        };
        Ok((anchored, offset))
    }

    /// Whether this producer has materialized and anchored its session header.
    /// Not-ready observation remains retryable and cannot certify a pending return.
    pub(super) fn ready(&self) -> bool {
        self.anchored
    }

    /// Verifies a complete first row against the response's exact native thread.
    /// Malformed, missing and conflicting identities are errors; no text is logged.
    fn anchor(line: &[u8], thread: &str) -> Result<()> {
        let row: Value =
            serde_json::from_slice(line).map_err(|_| invalid("malformed native return header"))?;
        if row["type"] != "session_meta"
            || row.pointer("/payload/id").and_then(Value::as_str) != Some(thread)
            || row
                .pointer("/payload/session_id")
                .is_some_and(|id| id.as_str() != Some(thread))
        {
            return Err(invalid("native return header identity mismatch"));
        }
        Ok(())
    }

    /// Consumes up to READ_BYTES new bytes and retains only caller-owned turns.
    /// Partial rows never certify a pending return. JSON size/identity/truncation
    /// errors propagate; all stdout content stays opaque, including JSON inside
    /// a command's output string. No model turn or database mutation occurs.
    pub(super) fn refresh(&mut self, turns: &BTreeSet<String>) -> Result<()> {
        self.pending.retain(|(turn, _)| turns.contains(turn));
        self.closed.retain(|turn| turns.contains(turn));
        if self.file.is_none() {
            self.file = optional_file(&self.directory, &self.relative)?;
        }
        let Some(file) = self.file.as_mut() else {
            return Ok(());
        };
        if file.metadata()?.len() < self.offset {
            return Err(invalid("native return rollout was truncated"));
        }
        let mut chunk = [0_u8; READ_BYTES];
        let read = file.read(&mut chunk)?;
        self.offset += read as u64;
        self.partial.extend_from_slice(&chunk[..read]);
        while let Some(end) = self.partial.iter().position(|b| *b == b'\n') {
            if end > ROW_BYTES {
                return Err(invalid("native return row exceeds bound"));
            }
            let line = self.partial.drain(..=end).collect::<Vec<_>>();
            if !self.anchored {
                Self::anchor(&line[..end], &self.thread)?;
                self.anchored = true;
                continue;
            }
            let row: Value = serde_json::from_slice(&line[..end])
                .map_err(|_| invalid("malformed native return row"))?;
            let payload = &row["payload"];
            if row["type"] == "session_meta" {
                Self::anchor(&line[..end], &self.thread)?;
            } else if row["type"] == "turn_context" {
                self.context = payload["turn_id"].as_str().map(str::to_owned);
            } else if row["type"] == "response_item"
                && matches!(
                    payload["type"].as_str(),
                    Some("custom_tool_call_output" | "function_call_output")
                )
            {
                let turn =
                    match payload.pointer("/internal_chat_message_metadata_passthrough/turn_id") {
                        Some(explicit) => explicit.as_str(),
                        None => self.context.as_deref(),
                    };
                if let Some(turn) = turn.filter(|turn| turns.contains(*turn)) {
                    for handle in pending_handles(&payload["output"]) {
                        self.pending.insert((turn.to_owned(), handle));
                    }
                }
            } else if row["type"] == "event_msg"
                && payload["type"] == "task_complete"
                && let Some(turn) = payload["turn_id"]
                    .as_str()
                    .filter(|turn| turns.contains(*turn))
            {
                self.closed.insert(turn.into());
            }
            if self.pending.len() + self.closed.len() > HANDLES {
                return Err(invalid("native return evidence exceeds bound"));
            }
        }
        if self.partial.len() > ROW_BYTES {
            return Err(invalid("partial native return row exceeds bound"));
        }
        Ok(())
    }

    /// Whether this exact origin turn returned the matching unfinished handle.
    pub(super) fn pending(&self, turn: &str, process: &str) -> bool {
        self.pending
            .contains(&(turn.to_owned(), process.to_owned()))
    }

    /// Retires a pending-return witness after its one accepted owned completion.
    /// A later command reusing the PTY id needs a fresh tool-return row; the old
    /// witness cannot arm an unrelated new native item in the same origin turn.
    pub(super) fn consume(&mut self, turn: &str, process: &str) {
        self.pending.remove(&(turn.to_owned(), process.to_owned()));
    }

    /// Whether all native tool-return rows before a turn's end were consumed.
    pub(super) fn closed(&self, turn: &str) -> bool {
        self.closed.contains(turn)
    }
}

/// Open an anchored relative regular file, distinguishing expected lazy absence.
/// Only NotFound is retryable absence. Permission, type, symlink and other I/O
/// failures propagate; the path is never followed outside the admitted home.
fn optional_file(directory: &Dir, relative: &Path) -> Result<Option<File>> {
    match directory.open_file(relative) {
        Ok(file) => Ok(Some(file)),
        Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

/// Extracts complete structured native returns, including outer batches and
/// standard fulfilled Promise results. Preamble, rejected results and opaque
/// stdout remain non-evidence. The caller still verifies the exact owned native
/// thread, origin turn, process and started item before a handle can wake.
fn pending_handles(output: &Value) -> Vec<String> {
    let texts = match output {
        Value::String(text) => vec![text.as_str()],
        Value::Array(blocks) => blocks
            .iter()
            .filter_map(|block| {
                (block["type"] == "input_text")
                    .then(|| block["text"].as_str())
                    .flatten()
            })
            .collect(),
        _ => Vec::new(),
    };
    let mut handles = Vec::new();
    for text in texts {
        if let Ok(value) = serde_json::from_str::<Value>(text) {
            collect_pending_handles(&value, &mut handles);
        }
    }
    handles
}

/// Visits only outer arrays and exact `{status:"fulfilled", value:...}` wrappers
/// in one parsed native return. JSON parsing already bounds nesting and the
/// observer bounds row bytes and total retained evidence. Arbitrary map fields,
/// strings and native stdout are never recursively decoded. Terminal/malformed
/// envelopes add nothing; valid pending handles retain the original 256-byte cap.
fn collect_pending_handles(value: &Value, handles: &mut Vec<String>) {
    if let Some(items) = value.as_array() {
        for item in items {
            collect_pending_handles(item, handles);
        }
        return;
    }
    if value["status"] == "fulfilled"
        && value.as_object().is_some_and(|fields| fields.len() == 2)
        && let Some(inner) = value.get("value")
    {
        collect_pending_handles(inner, handles);
        return;
    }
    if value.get("exit_code").is_some()
        || value["chunk_id"].as_str().is_none_or(str::is_empty)
        || value["wall_time_seconds"]
            .as_f64()
            .is_none_or(|n| n < 0.0 || !n.is_finite())
        || value["original_token_count"].as_u64().is_none()
        || value["output"].as_str().is_none()
    {
        return;
    }
    let Some(handle) = value["session_id"]
        .as_u64()
        .map(|id| id.to_string())
        .or_else(|| value["session_id"].as_str().map(str::to_owned))
    else {
        return;
    };
    if !handle.is_empty() && handle.len() <= 256 {
        handles.push(handle);
    }
}

/// Regression evidence for bounded native return parsing and admission fences.
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::{fs::OpenOptions, io::Write};

    /// Build an actual structured unfinished native command return envelope.
    fn pending() -> Value {
        json!({"chunk_id":"chunk","wall_time_seconds":0.1,"original_token_count":0,"output":"","session_id":42})
    }

    /// Model-facing code-mode output block; nested stdout remains a string.
    fn output(value: &Value) -> Value {
        json!([{"type":"input_text","text":value.to_string()}])
    }

    /// Append one complete native JSONL row, retaining the supplied file identity.
    fn append(path: &Path, row: &Value) {
        writeln!(OpenOptions::new().append(true).open(path).unwrap(), "{row}").unwrap();
    }

    /// A complete owned native tool return in a particular inference turn.
    fn returned(turn: &str, value: &Value) -> Value {
        json!({"type":"response_item","payload":{"type":"custom_tool_call_output","internal_chat_message_metadata_passthrough":{"turn_id":turn},"output":output(value)}})
    }

    /// Create a private sessions file and response with the real native shape.
    fn fixture() -> (tempfile::TempDir, std::path::PathBuf, Value) {
        let home = tempfile::tempdir().unwrap();
        std::fs::create_dir(home.path().join("sessions")).unwrap();
        let path = home.path().join("sessions/owned.jsonl");
        std::fs::write(
            &path,
            "{\"type\":\"session_meta\",\"payload\":{\"id\":\"thread\"}}\n",
        )
        .unwrap();
        let response = json!({"thread":{"path":path}});
        (home, path, response)
    }

    /// Code-mode Promise.allSettled output retains a pending background handle,
    /// including a batch next to an inline foreground result. Native preamble
    /// text is not a result; grouping must not turn terminal output into a wake.
    #[test]
    fn fulfilled_and_batched_native_returns_are_evidence() {
        let background = json!({"status":"fulfilled","value":pending()});
        let mut terminal = pending();
        terminal["exit_code"] = json!(0);
        let foreground = json!({"status":"fulfilled","value":terminal});
        assert_eq!(pending_handles(&output(&pending())), ["42"]);
        assert_eq!(pending_handles(&output(&background)), ["42"]);
        assert_eq!(
            pending_handles(&output(&json!([background.clone(), foreground.clone()]))),
            ["42"]
        );
        assert_eq!(
            pending_handles(&json!([
                {"type":"input_text","text":"Script completed\nOutput:\n"},
                {"type":"input_text","text":background.to_string()},
                {"type":"input_text","text":foreground.to_string()}
            ])),
            ["42"]
        );
        assert!(pending_handles(&output(&foreground)).is_empty());
    }

    /// Grouping does not grant rejected, terminal, malformed or opaque stdout
    /// data authority, and mixed wrapper/terminal objects cannot hide exit_code.
    #[test]
    fn wrapped_returns_preserve_rejection_boundaries() {
        let mut terminal = pending();
        terminal["exit_code"] = json!(0);
        for value in [
            json!({"status":"fulfilled","value":terminal}),
            json!({"status":"rejected","reason":pending()}),
            json!({"status":"rejected","value":pending()}),
            json!({"status":"fulfilled","value":pending().to_string()}),
            json!({"status":"fulfilled","value":pending(),"exit_code":0}),
            json!({"arbitrary":[pending()]}),
            json!({"chunk_id":"c","wall_time_seconds":0.1,"original_token_count":0,"exit_code":0,"output":pending().to_string()}),
        ] {
            assert!(pending_handles(&output(&json!([value]))).is_empty());
        }
        let mut too_deep = pending();
        for _ in 0..140 {
            too_deep = json!({"status":"fulfilled","value":too_deep});
        }
        assert!(pending_handles(&output(&too_deep)).is_empty());
    }

    /// Terminal, malformed and stdout-injected handles cannot create wait proof.
    #[test]
    fn only_structured_unfinished_returns_are_evidence() {
        assert_eq!(pending_handles(&output(&pending())), ["42"]);
        let mut terminal = pending();
        terminal["exit_code"] = json!(0);
        assert!(pending_handles(&output(&terminal)).is_empty());
        terminal["exit_code"] = Value::Null;
        assert!(pending_handles(&output(&terminal)).is_empty());
        let mut forged = json!({"chunk_id":"c","wall_time_seconds":0.1,"original_token_count":0,"exit_code":0,"output":pending().to_string()});
        assert!(pending_handles(&output(&forged)).is_empty());
        forged.as_object_mut().unwrap().remove("exit_code");
        assert!(pending_handles(&output(&forged)).is_empty());
        for field in [
            "chunk_id",
            "wall_time_seconds",
            "original_token_count",
            "output",
            "session_id",
        ] {
            let mut missing = pending();
            missing.as_object_mut().unwrap().remove(field);
            assert!(pending_handles(&output(&missing)).is_empty(), "{field}");
        }
        assert!(
            pending_handles(&json!([{"type":"image","text":pending().to_string()}])).is_empty()
        );
    }

    /// Fresh and resumed observers skip old evidence, retain incomplete lines and
    /// reject foreign-turn data. Only a fully appended owned return can arm once.
    #[test]
    fn admission_boundary_partial_rows_and_exact_origin_turn() {
        let (home, path, response) = fixture();
        append(&path, &returned("old", &pending()));
        let mut history = NativeReturns::open(home.path(), &response, "thread").unwrap();
        let turns = BTreeSet::from(["turn".to_owned()]);
        history.refresh(&turns).unwrap();
        assert!(!history.pending("old", "42"));
        append(&path, &returned("foreign", &pending()));
        let row = format!("{}\n", returned("turn", &pending()));
        let split = row.len() / 2;
        let mut writer = OpenOptions::new().append(true).open(&path).unwrap();
        writer.write_all(&row.as_bytes()[..split]).unwrap();
        history.refresh(&turns).unwrap();
        assert!(!history.pending("turn", "42"));
        writer.write_all(&row.as_bytes()[split..]).unwrap();
        append(
            &path,
            &json!({"type":"event_msg","payload":{"type":"task_complete","turn_id":"turn"}}),
        );
        history.refresh(&turns).unwrap();
        assert!(history.pending("turn", "42") && history.closed("turn"));
        assert!(!history.pending("foreign", "42"));
        history.refresh(&turns).unwrap();
        assert_eq!(history.pending.len(), 1);
        history.consume("turn", "42");
        history.refresh(&turns).unwrap();
        assert!(
            !history.pending("turn", "42"),
            "old return cannot arm a reused process id"
        );
        let resumed = NativeReturns::open(home.path(), &response, "thread").unwrap();
        assert!(
            !resumed.pending("turn", "42"),
            "resume cannot replay prior evidence"
        );
    }

    /// An admission overlapping a partial old row restarts that complete row;
    /// it never parses a suffix or loses a subsequent owned pending return.
    #[test]
    fn partial_admission_row_and_deferred_first_header() {
        let (home, path, response) = fixture();
        let old = "{\"type\":\"event_msg\",\"payload\":{\"type\":\"thread_settings_applied\"}}\n";
        let split = old.len() / 2;
        let mut writer = OpenOptions::new().append(true).open(&path).unwrap();
        writer.write_all(&old.as_bytes()[..split]).unwrap();
        let mut history = NativeReturns::open(home.path(), &response, "thread").unwrap();
        writer.write_all(&old.as_bytes()[split..]).unwrap();
        append(&path, &returned("turn", &pending()));
        let turns = BTreeSet::from(["turn".to_owned()]);
        history.refresh(&turns).unwrap();
        assert!(history.pending("turn", "42"));
        drop(history);
        std::fs::write(&path, "").unwrap();
        let mut history = NativeReturns::open(home.path(), &response, "thread").unwrap();
        append(
            &path,
            &json!({"type":"session_meta","payload":{"id":"thread"}}),
        );
        append(&path, &returned("turn", &pending()));
        history.refresh(&turns).unwrap();
        assert!(history.pending("turn", "42"));
    }

    /// Native thread/start may acknowledge a path before creating its parent or
    /// file. Late creation is retried from0; late symlink substitution is refused.
    #[test]
    fn lazy_native_creation_retains_initial_pending_return() {
        let (home, path, response) = fixture();
        std::fs::remove_file(&path).unwrap();
        std::fs::remove_dir(path.parent().unwrap()).unwrap();
        let turns = BTreeSet::from(["turn".to_owned()]);
        let mut history = NativeReturns::open(home.path(), &response, "thread").unwrap();
        history.refresh(&turns).unwrap();
        assert!(!history.ready() && !history.pending("turn", "42"));
        std::fs::create_dir(path.parent().unwrap()).unwrap();
        std::fs::write(
            &path,
            "{\"type\":\"session_meta\",\"payload\":{\"id\":\"thread\"}}\n",
        )
        .unwrap();
        append(&path, &returned("turn", &pending()));
        history.refresh(&turns).unwrap();
        assert!(history.ready() && history.pending("turn", "42"));
        drop(history);
        std::fs::remove_file(&path).unwrap();
        let mut history = NativeReturns::open(home.path(), &response, "thread").unwrap();
        let foreign = home.path().join("outside.jsonl");
        std::fs::write(&foreign, "never read").unwrap();
        std::os::unix::fs::symlink(foreign, &path).unwrap();
        assert!(history.refresh(&turns).is_err());
        assert!(!history.pending("turn", "42"));
    }

    /// A producer cannot grow one incomplete row past the fixed memory bound.
    #[test]
    fn oversized_partial_row_disables_observation() {
        let (home, path, response) = fixture();
        let mut history = NativeReturns::open(home.path(), &response, "thread").unwrap();
        OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(&vec![b'x'; ROW_BYTES + 1])
            .unwrap();
        let turns = BTreeSet::from(["turn".to_owned()]);
        let mut refused = false;
        for _ in 0..=ROW_BYTES / READ_BYTES + 1 {
            if history.refresh(&turns).is_err() {
                refused = true;
                break;
            }
        }
        assert!(refused);
        assert!(!history.pending("turn", "42"));
    }

    /// A path, identity or JSONL integrity failure never certifies pending work.
    #[test]
    fn foreign_files_links_truncation_and_malformed_rows_fail_closed() {
        let (home, path, response) = fixture();
        assert!(NativeReturns::open(home.path(), &response, "foreign").is_err());
        assert!(
            NativeReturns::open(
                home.path(),
                &json!({"thread":{"path":"/etc/passwd"}}),
                "thread"
            )
            .is_err()
        );
        let alias = home.path().join("sessions/alias.jsonl");
        std::os::unix::fs::symlink(&path, &alias).unwrap();
        assert!(
            NativeReturns::open(home.path(), &json!({"thread":{"path":alias}}), "thread").is_err()
        );
        let mut history = NativeReturns::open(home.path(), &response, "thread").unwrap();
        std::fs::write(&path, "").unwrap();
        assert!(history.refresh(&BTreeSet::new()).is_err());
        std::fs::write(
            &path,
            "{\"type\":\"session_meta\",\"payload\":{\"id\":\"thread\"}}\n",
        )
        .unwrap();
        let mut history = NativeReturns::open(home.path(), &response, "thread").unwrap();
        writeln!(
            OpenOptions::new().append(true).open(&path).unwrap(),
            "invalid"
        )
        .unwrap();
        assert!(history.refresh(&BTreeSet::new()).is_err());
    }
}
