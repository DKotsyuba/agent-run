//! Exact-byte answer sealing and verification. Process exit zero is insufficient.
use crate::fs::{self, Dir};
use agent_run_domain::{
    domain::{Outcome, Status},
    error::invalid,
    Error, Result,
};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
pub const MAX_ANSWER: usize = 16 * 1024 * 1024;
pub const INLINE_ANSWER: usize = 128 * 1024;
pub const LEGACY_FRAME: &[u8] = b"\n<<<agent-run:complete>>>\n";
pub const MEDIA_TYPE: &str = "text/markdown; charset=utf-8";
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Proof {
    pub path: PathBuf,
    pub bytes: u64,
    pub sha256: String,
    pub proof_version: u32,
}
#[derive(Debug, Serialize, Deserialize)]
struct Sidecar {
    kind: String,
    media_type: String,
    proof_version: u32,
    answer: String,
    bytes: u64,
    sha256: String,
}
pub fn seal(root: &Path, relative: &Path, text: &str) -> Result<Proof> {
    if text.is_empty() {
        return Err(invalid("cannot seal an empty answer"));
    }
    if text.len() > MAX_ANSWER {
        return Err(Error::Integrity("answer exceeds 16 MiB".into()));
    }
    let dir = Dir::open(root)?;
    fs::relative(relative)?;
    let name = relative
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| invalid("answer filename must be UTF-8"))?;
    let parent = relative.parent().unwrap_or_else(|| Path::new(""));
    let proof = Proof {
        path: root.join(relative),
        bytes: text.len() as u64,
        sha256: fs::sha256(text.as_bytes()),
        proof_version: 2,
    };
    let sidecar = Sidecar {
        kind: "agent_answer".into(),
        media_type: MEDIA_TYPE.into(),
        proof_version: 2,
        answer: name.into(),
        bytes: proof.bytes,
        sha256: proof.sha256.clone(),
    };
    // Commit marker first. A crash at any following stage cannot downgrade to legacy.
    dir.write(&parent.join(".answer-format"), b"2\n", 0o600)?;
    dir.write(relative, text.as_bytes(), 0o600)?;
    let mut data = serde_json::to_vec(&sidecar)?;
    data.push(b'\n');
    dir.write(&parent.join(format!("{name}.proof.json")), &data, 0o600)?;
    Ok(proof)
}
pub fn read(root: &Path, proof: &Proof, inline_limit: usize) -> Result<(u32, Option<String>)> {
    let relative = proof
        .path
        .strip_prefix(root)
        .map_err(|_| Error::Integrity("answer path escapes its agent directory".into()))?;
    fs::relative(relative)?;
    let dir = Dir::open(root)?;
    let bytes = dir.read(relative, MAX_ANSWER)?;
    if bytes.is_empty() || bytes.len() as u64 != proof.bytes || fs::sha256(&bytes) != proof.sha256 {
        return Err(Error::Integrity(
            "answer bytes do not match the recorded proof".into(),
        ));
    }
    let text = std::str::from_utf8(&bytes)
        .map_err(|_| Error::Integrity("answer is not valid UTF-8".into()))?;
    let name = relative
        .file_name()
        .and_then(|v| v.to_str())
        .ok_or_else(|| invalid("invalid answer filename"))?;
    let parent = relative.parent().unwrap_or_else(|| Path::new(""));
    let marker = dir.optional(&parent.join(".answer-format"), 4096)?;
    let sidecar = dir.optional(&parent.join(format!("{name}.proof.json")), 4096)?;
    let version = if marker.is_none() && sidecar.is_none() {
        if !bytes.ends_with(LEGACY_FRAME) {
            return Err(Error::Integrity(
                "legacy answer lacks exact terminal frame".into(),
            ));
        }
        1
    } else {
        if marker.as_deref().is_some_and(|m| m != b"2\n") {
            return Err(Error::Integrity("invalid answer format marker".into()));
        }
        let raw =
            sidecar.ok_or_else(|| Error::Integrity("answer proof sidecar is missing".into()))?;
        let p: Sidecar = serde_json::from_slice(&raw)
            .map_err(|_| Error::Integrity("malformed answer proof".into()))?;
        if p.proof_version != 2
            || p.kind != "agent_answer"
            || p.media_type != MEDIA_TYPE
            || p.answer != name
            || p.bytes != proof.bytes
            || p.sha256 != proof.sha256
        {
            return Err(Error::Integrity(
                "answer sidecar contradicts the stored proof".into(),
            ));
        }
        2
    };
    let content = if bytes.len() <= inline_limit {
        Some(if version == 1 {
            text[..text.len() - LEGACY_FRAME.len()].to_owned()
        } else {
            text.to_owned()
        })
    } else {
        None
    };
    Ok((version, content))
}
/// Only bounded, unmistakable error-only payloads are classified as provider errors.
pub fn error_only(text: &str) -> Option<&'static str> {
    let s = text.trim().to_lowercase();
    if s.len() > 2048 || s.lines().count() > 6 {
        return None;
    }
    for (prefix, kind) in [
        ("you've hit your usage limit", "quota_exhausted"),
        ("usage limit reached", "quota_exhausted"),
        ("rate limit exceeded", "rate_limited"),
        ("error: authentication failed", "auth_error"),
        ("error: invalid api key", "auth_error"),
        ("error: server overloaded", "provider_overloaded"),
    ] {
        if s.starts_with(prefix) {
            return Some(kind);
        }
    }
    None
}
pub fn completion(
    mut outcome: Outcome,
    proof: Option<&Proof>,
    group_gone: bool,
    cancelled: bool,
) -> Outcome {
    if !group_gone {
        outcome.status = Status::Failed;
        outcome.failure_kind = Some("engine_group_survived".into());
    } else if cancelled {
        outcome.status = Status::Cancelled;
    } else if outcome.status == Status::Succeeded && proof.is_none() {
        outcome.status = Status::Failed;
        outcome.failure_kind = Some("no_answer".into());
    }
    outcome
}
