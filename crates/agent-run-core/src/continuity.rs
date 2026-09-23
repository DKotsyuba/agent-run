//! Pre-launch proof that a native conversation can be continued.
//!
//! An explicit provider resume attaches a new process to a native session
//! that a previous, cleaned-up attempt wrote. Before any child is admitted,
//! the harness's own history file for that exact session must exist inside
//! the expected storage root, be complete, name the expected session, and
//! leave no tool call without its result. A recorded session id alone is not
//! proof. Every failure is a typed `continuation_unavailable` refusal; the
//! history is only read, never repaired or rewritten.

use crate::{Error, Result};
use agent_run_domain::catalog::HarnessId;
use serde_json::Value;
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
};

/// Largest native history file read for a proof (64 MiB).
const MAX_HISTORY_BYTES: u64 = 64 * 1024 * 1024;

/// The verified native history of one session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeHistory {
    /// Canonical path of the history file inside its storage root.
    pub path: PathBuf,
    /// Number of complete JSON records it holds.
    pub records: usize,
}

/// Builds the typed refusal for an unprovable continuation.
fn unavailable(reason: impl std::fmt::Display) -> Error {
    Error::Unsupported(format!("continuation_unavailable: {reason}"))
}

/// Proves `session` continuable from `root`, the harness's session storage:
/// the run's `CODEX_HOME` for Codex (`sessions/**/rollout-*-<id>.jsonl`), the
/// Claude config directory for Claude Code (`projects/*/<id>.jsonl`).
///
/// Returns the verified history; refuses with `continuation_unavailable`
/// when the file is missing or ambiguous, a symlink or outside `root`,
/// oversized, truncated or not line-delimited JSON, names another session,
/// or has a tool call without a result.
pub fn prove(harness: HarnessId, root: &Path, session: &str) -> Result<NativeHistory> {
    if session.is_empty() || session.contains(['/', '\0']) || session.contains("..") {
        return Err(unavailable("native session id is not a plain identifier"));
    }
    let root = root
        .canonicalize()
        .map_err(|_| unavailable("native session storage is missing"))?;
    let matches = match harness {
        HarnessId::Codex => find(&root.join("sessions"), 4, &|name| {
            name.starts_with("rollout-") && name.ends_with(&format!("-{session}.jsonl"))
        }),
        HarnessId::ClaudeCode => find(&root.join("projects"), 1, &|name| {
            name == format!("{session}.jsonl")
        }),
    };
    let [path] = matches.as_slice() else {
        return Err(unavailable(if matches.is_empty() {
            "no native history for the recorded session"
        } else {
            "native history for the session is ambiguous"
        }));
    };
    let metadata =
        std::fs::symlink_metadata(path).map_err(|_| unavailable("native history is unreadable"))?;
    if !metadata.file_type().is_file() {
        return Err(unavailable("native history is not a regular file"));
    }
    let canonical = path
        .canonicalize()
        .map_err(|_| unavailable("native history is unreadable"))?;
    if !canonical.starts_with(&root) {
        return Err(unavailable("native history escapes its storage root"));
    }
    if metadata.len() == 0 || metadata.len() > MAX_HISTORY_BYTES {
        return Err(unavailable("native history is empty or oversized"));
    }
    let bytes =
        std::fs::read(&canonical).map_err(|_| unavailable("native history is unreadable"))?;
    if bytes.last() != Some(&b'\n') {
        return Err(unavailable("native history is truncated"));
    }
    let text =
        std::str::from_utf8(&bytes).map_err(|_| unavailable("native history is not UTF-8"))?;
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
    }
    Ok(NativeHistory {
        path: canonical,
        records: records.len(),
    })
}

/// Lists regular-or-linked entries under `dir` (at most `depth` directory
/// levels deep, never following directory symlinks) whose file name
/// satisfies `wanted`.
fn find(dir: &Path, depth: usize, wanted: &dyn Fn(&str) -> bool) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return found;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let Ok(kind) = entry.file_type() else {
            continue;
        };
        if kind.is_dir() && depth > 0 {
            found.extend(find(&path, depth - 1, wanted));
        } else if !kind.is_dir() && entry.file_name().to_str().is_some_and(wanted) {
            found.push(path);
        }
    }
    found
}

/// Codex rollout: a `session_meta` record must name `session`, and every
/// `*_call` response item's `call_id` must have its `*_call_output`.
fn codex(records: &[Value], session: &str) -> Result<()> {
    let metas: Vec<&Value> = records
        .iter()
        .filter(|record| record["type"] == "session_meta")
        .collect();
    if metas.is_empty() || metas.iter().any(|meta| meta["payload"]["id"] != session) {
        return Err(unavailable("native history belongs to another session"));
    }
    let (mut calls, mut outputs) = (BTreeSet::new(), BTreeSet::new());
    for payload in records
        .iter()
        .filter(|record| record["type"] == "response_item")
        .map(|record| &record["payload"])
    {
        let (Some(kind), Some(id)) = (payload["type"].as_str(), payload["call_id"].as_str()) else {
            continue;
        };
        if kind.ends_with("_call_output") {
            outputs.insert(id.to_owned());
        } else if kind.ends_with("_call") {
            calls.insert(id.to_owned());
        }
    }
    unresolved(&calls, &outputs)
}

/// Claude transcript: every record carrying `sessionId` must name `session`
/// (at least one must), and every `tool_use` must have its `tool_result`.
fn claude(records: &[Value], session: &str) -> Result<()> {
    let ids: Vec<&Value> = records
        .iter()
        .filter_map(|record| record.get("sessionId"))
        .collect();
    if ids.is_empty() || ids.iter().any(|id| *id != session) {
        return Err(unavailable("native history belongs to another session"));
    }
    let (mut calls, mut outputs) = (BTreeSet::new(), BTreeSet::new());
    for item in records
        .iter()
        .filter_map(|record| record["message"]["content"].as_array())
        .flatten()
    {
        match (
            item["type"].as_str(),
            item["id"].as_str(),
            item["tool_use_id"].as_str(),
        ) {
            (Some("tool_use"), Some(id), _) => {
                calls.insert(id.to_owned());
            }
            (Some("tool_result"), _, Some(id)) => {
                outputs.insert(id.to_owned());
            }
            _ => {}
        }
    }
    unresolved(&calls, &outputs)
}

/// Refuses when a tool call has no recorded result.
fn unresolved(calls: &BTreeSet<String>, outputs: &BTreeSet<String>) -> Result<()> {
    if calls.difference(outputs).next().is_some() {
        return Err(unavailable("native history has an unresolved tool call"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{prove, HarnessId};
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
}
