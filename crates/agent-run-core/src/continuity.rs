//! Native conversation evidence for explicit continuation.
//!
//! A resumed attempt attaches a new process to the native session a previous,
//! cleaned-up attempt wrote. The evidence is minted once, by the supervisor at
//! that attempt's cleanup boundary ([`seal`]): the harness's own history file
//! for the exact session, found inside the storage root the attempt actually
//! used, read through an anchored no-follow descriptor with a hard byte bound,
//! structurally checked, and recorded as root, relative path, length, SHA-256
//! and record count. A resume and the supervisor handoff then only [`verify`]
//! that recorded seal against the same file; they never mint a new baseline
//! from whatever history exists later. Every failure is a typed
//! `continuation_unavailable` refusal; the history is only read.
//!
//! Structural checks: every record is one complete line of JSON, the session
//! is named consistently, and native tool calls follow a chronological
//! lifecycle — each recognized call has a nonempty unique id, each result
//! answers exactly one earlier pending call, and no call is left pending.

use crate::{fs, Error, Result};
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
    /// config directory).
    pub root: PathBuf,
    /// History file path relative to `root`, without `..` or links.
    pub relative: PathBuf,
    /// Exact byte length.
    pub bytes: u64,
    /// Lowercase SHA-256 of the exact bytes.
    pub sha256: String,
    /// Number of complete JSON records.
    pub records: usize,
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
    })
}

/// Re-reads the sealed file (anchored at the recorded root, no links, bounded)
/// and refuses unless its length and digest equal the seal, it still passes
/// the structural checks, and it belongs to `harness` and `session`.
pub fn verify(seal: &HistorySeal, harness: HarnessId, session: &str) -> Result<()> {
    if seal.harness != harness || seal.session != session {
        return Err(unavailable("recorded history seal names another session"));
    }
    let bytes = read(&seal.root, &seal.relative)?;
    if bytes.len() as u64 != seal.bytes || fs::sha256(&bytes) != seal.sha256 {
        return Err(unavailable("native history changed since it was sealed"));
    }
    check(harness, &bytes, session)?;
    Ok(())
}

/// Reports whether the sealed Codex history shows the admitted turn itself:
/// a `response_item` user `message` whose
/// `internal_chat_message_metadata_passthrough.turn_id` is exactly `turn`
/// (the id `turn/start` returned for this attempt) and one of whose
/// `input_text` items hashes to `input_sha256` (the exact wire input, role
/// preamble included).
///
/// Provenance is the turn id, never text equality, so an older turn with
/// identical text (an explicit-resume parent, a repeated control text) can
/// never stand in for this one. The file is re-read and must still match
/// the seal; a changed or unreadable history is an error, not `false`.
/// `false` means the turn never reached native history (for example a
/// usage-limit rejection before the user input was recorded).
pub fn admitted_turn_recorded(seal: &HistorySeal, turn: &str, input_sha256: &str) -> Result<bool> {
    if seal.harness != HarnessId::Codex || turn.is_empty() {
        return Ok(false);
    }
    let bytes = read(&seal.root, &seal.relative)?;
    if bytes.len() as u64 != seal.bytes || fs::sha256(&bytes) != seal.sha256 {
        return Err(unavailable("native history changed since it was sealed"));
    }
    let text =
        std::str::from_utf8(&bytes).map_err(|_| unavailable("native history is not UTF-8"))?;
    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        let record: Value = serde_json::from_str(line)
            .map_err(|_| unavailable("native history has a malformed record"))?;
        let payload = &record["payload"];
        if record["type"] != "response_item"
            || payload["type"] != "message"
            || payload["role"] != "user"
            || payload["internal_chat_message_metadata_passthrough"]["turn_id"] != turn
        {
            continue;
        }
        let admitted = payload["content"].as_array().is_some_and(|items| {
            items.iter().any(|item| {
                item["type"] == "input_text"
                    && item["text"]
                        .as_str()
                        .is_some_and(|text| fs::sha256(text.as_bytes()) == input_sha256)
            })
        });
        if admitted {
            return Ok(true);
        }
    }
    Ok(false)
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
                    ))
                }
            },
            _ => {
                return Err(unavailable(
                    "native history has a tool record without an id",
                ))
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

/// Claude transcript: every record carrying `sessionId` must name `session`
/// (at least one must); `tool_use` content opens a lifecycle by `id` that a
/// later `tool_result` closes by `tool_use_id`.
fn claude(records: &[Value], session: &str) -> Result<()> {
    let ids: Vec<&Value> = records
        .iter()
        .filter_map(|record| record.get("sessionId"))
        .collect();
    if ids.is_empty() || ids.iter().any(|id| *id != session) {
        return Err(unavailable("native history belongs to another session"));
    }
    lifecycle(
        records
            .iter()
            .filter_map(|record| record["message"]["content"].as_array())
            .flatten()
            .filter_map(|item| match item["type"].as_str()? {
                "tool_use" => Some(Tool::Call(item["id"].as_str())),
                "tool_result" => Some(Tool::Result(item["tool_use_id"].as_str())),
                _ => None,
            }),
    )
}

/// Compatibility name for [`seal`] used by the structural unit tests.
#[cfg(test)]
fn prove(harness: HarnessId, root: &Path, session: &str) -> Result<HistorySeal> {
    seal(harness, root, session)
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
        let item = |kind: &str, id: Option<&str>| {
            match id {
            Some(id) => format!("{{\"type\":\"response_item\",\"payload\":{{\"type\":\"{kind}\",\"call_id\":\"{id}\"}}}}\n"),
            None => format!("{{\"type\":\"response_item\",\"payload\":{{\"type\":\"{kind}\"}}}}\n"),
        }
        };
        let codex = |body: String| {
            rollout(temp.path(), "s1", &format!("{meta}{body}"));
            prove(HarnessId::Codex, temp.path(), "s1")
        };
        assert!(codex(
            item("custom_tool_call", Some("a")) + &item("custom_tool_call_output", Some("a"))
        )
        .is_ok());
        assert!(codex(
            item("function_call", Some("a"))
                + &item("function_call", Some("a"))
                + &item("function_call_output", Some("a"))
        )
        .is_err());
        assert!(codex(
            item("function_call", Some("a"))
                + &item("function_call_output", Some("a"))
                + &item("function_call_output", Some("a"))
        )
        .is_err());
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
        for body in [
            meta.to_owned(),
            format!("{meta}{}", turn.replace("nonce-1", "nonce-2")),
            format!("{meta}{turn}{turn}"),
        ] {
            fs::write(&path, &body).unwrap();
            let error = super::verify(&sealed, HarnessId::Codex, "s1").unwrap_err();
            assert!(
                error.to_string().contains("changed since it was sealed"),
                "{error}"
            );
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
}
