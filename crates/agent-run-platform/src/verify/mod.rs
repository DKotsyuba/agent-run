//! Exact-byte answer sealing and verification. Process exit zero is insufficient.
mod completion;
pub use completion::{
    inspect_answer, silence_seconds, verify_completion, AnswerProof, StopReason, ANSWER_INCOMPLETE,
    ANSWER_PRESENT, DEFAULT_SILENCE_THRESHOLD_SECONDS, ENGINE_VANISHED, GROUP_SURVIVED, NO_ANSWER,
};

use crate::fs::{self, Dir};
use agent_run_domain::{error::invalid, Error, Result};
use serde::{Deserialize, Serialize};
use std::io::Read;
use std::path::{Path, PathBuf};
pub const MAX_ANSWER: usize = 16 * 1024 * 1024;
/// Default inline-display cutoff for returned answer text; independent of
/// `MAX_ANSWER`, matching Python's separate verification/inline bounds.
/// Mirrors `_DEFAULT_INLINE_ANSWER_BYTES` (`service.py:68`).
pub const INLINE_ANSWER: usize = 1024 * 1024;
pub const LEGACY_FRAME: &[u8] = b"\n<<<agent-run:complete>>>\n";
pub const MEDIA_TYPE: &str = "text/markdown; charset=utf-8";
/// Maximum bytes read from either metadata file (marker or proof sidecar).
/// Mirrors `_MAX_ANSWER_METADATA_BYTES` (`verify.py:56`).
const MAX_METADATA_BYTES: u64 = 4096;
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Proof {
    pub path: PathBuf,
    pub bytes: u64,
    pub sha256: String,
    pub proof_version: u32,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Sidecar {
    pub kind: String,
    pub media_type: String,
    pub proof_version: u32,
    pub answer: String,
    pub bytes: u64,
    pub sha256: String,
}
/// True when `error` reflects a no-follow open refusing a symlink or other
/// non-regular file — whether detected by the syscall itself (`ELOOP` on a
/// final-component symlink, `ENOTDIR` on a path through a non-directory) or
/// by [`Dir::open_file`]'s post-open regular-file check (special files that
/// slip past `O_NOFOLLOW`, e.g. a FIFO). Mirrors the escapes
/// `_open_regular_descriptor` raises as `PathEscapeError`
/// (`verify.py:99,125-127`).
fn is_path_escape(error: &Error) -> bool {
    match error {
        Error::Io(e) => matches!(e.raw_os_error(), Some(libc::ELOOP) | Some(libc::ENOTDIR)),
        Error::Integrity(msg) => msg.contains("regular file"),
        _ => false,
    }
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
/// Read one optional no-follow metadata file bounded to
/// [`MAX_METADATA_BYTES`]. Mirrors `_read_answer_metadata` (`verify.py:188-223`):
/// `None` for a missing file; a symlinked/special file, an oversized file, or
/// an I/O failure all raise a labeled answer-proof error naming `label`.
fn read_metadata(dir: &Dir, path: &Path, label: &str) -> Result<Option<Vec<u8>>> {
    let mut file = match dir.open_file(path) {
        Ok(f) => f,
        Err(Error::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) if is_path_escape(&e) => {
            return Err(Error::AnswerIntegrity(format!(
                "{label} must be a regular file"
            )))
        }
        Err(e) => {
            return Err(Error::AnswerIntegrity(format!(
                "{label} is unreadable: {e}"
            )))
        }
    };
    let size = file
        .metadata()
        .map_err(|e| Error::AnswerIntegrity(format!("{label} is unreadable: {e}")))?
        .len();
    if size > MAX_METADATA_BYTES {
        return Err(Error::AnswerIntegrity(format!(
            "{label} exceeds the {MAX_METADATA_BYTES}-byte bound"
        )));
    }
    let mut data = Vec::new();
    (&mut file)
        .take(MAX_METADATA_BYTES + 1)
        .read_to_end(&mut data)
        .map_err(|e| Error::AnswerIntegrity(format!("{label} is unreadable: {e}")))?;
    if data.len() as u64 > MAX_METADATA_BYTES {
        return Err(Error::AnswerIntegrity(format!(
            "{label} exceeds the {MAX_METADATA_BYTES}-byte bound"
        )));
    }
    Ok(Some(data))
}
/// Load and validate the `.answer-format`/`*.proof.json` sidecar pair against an
/// already-known payload size and hash. `Ok(None)` means neither file exists
/// (historical legacy artifact); any other mismatch is a hard error. Mirrors
/// Python's `_load_answer_proof` (`verify.py:321-351`) minus its legacy-frame
/// check, which callers apply separately (see `read` and `completion::inspect_answer`).
pub fn load_sidecar(
    dir: &Dir,
    parent: &Path,
    name: &str,
    bytes: u64,
    sha256: &str,
) -> Result<Option<Sidecar>> {
    let marker = read_metadata(dir, &parent.join(".answer-format"), "answer format marker")?;
    let sidecar = read_metadata(
        dir,
        &parent.join(format!("{name}.proof.json")),
        "answer proof sidecar",
    )?;
    if marker.is_none() && sidecar.is_none() {
        return Ok(None);
    }
    if marker.as_deref().is_some_and(|m| m != b"2\n") {
        return Err(Error::AnswerIntegrity(
            "answer format marker is malformed".into(),
        ));
    }
    let raw =
        sidecar.ok_or_else(|| Error::AnswerIntegrity("answer proof sidecar is missing".into()))?;
    let p: Sidecar = serde_json::from_slice(&raw)
        .map_err(|e| Error::AnswerIntegrity(format!("answer proof sidecar is malformed: {e}")))?;
    if p.proof_version != 2 {
        return Err(Error::AnswerIntegrity(format!(
            "answer proof version is unsupported: {}",
            p.proof_version
        )));
    }
    if p.kind != "agent_answer" {
        return Err(Error::AnswerIntegrity(
            "answer proof kind must be \"agent_answer\"".into(),
        ));
    }
    if p.media_type != MEDIA_TYPE {
        return Err(Error::AnswerIntegrity(format!(
            "answer proof media_type must be {MEDIA_TYPE:?}"
        )));
    }
    if p.answer != name {
        return Err(Error::AnswerIntegrity(
            "answer proof does not name its payload file".into(),
        ));
    }
    if p.bytes != bytes {
        return Err(Error::AnswerIntegrity(
            "answer proof byte count does not match the payload".into(),
        ));
    }
    if p.sha256 != sha256 {
        return Err(Error::AnswerIntegrity(
            "answer proof hash does not match the payload".into(),
        ));
    }
    Ok(Some(p))
}
/// Read one already-sealed answer's exact payload against its recorded
/// `proof`, then classify its format version. Composes Python's decoupled
/// `read_answer_payload` (`verify.py:435-522`, payload bytes/size/hash/UTF-8)
/// with `load_answer_proof` (`verify.py:394-413`, the sidecar check) into the
/// single verification entry point every Rust caller uses — matching the
/// effective behavior of `AgentService.answer()` (`service.py`), which calls
/// both before trusting a stored proof. `MAX_ANSWER` is always the read
/// bound (Python's `max_bytes` in every production call site); `inline_limit`
/// only decides whether the verified text is retained in the return value.
pub fn read(root: &Path, proof: &Proof, inline_limit: usize) -> Result<(u32, Option<String>)> {
    let relative = proof
        .path
        .strip_prefix(root)
        .map_err(|_| Error::PathEscape("answer path escapes its agent directory".into()))?;
    fs::relative(relative)?;
    let dir = Dir::open(root)?;
    let mut file = match dir.open_file(relative) {
        Ok(f) => f,
        Err(Error::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {
            return Err(Error::AnswerIntegrity(format!(
                "answer artifact is missing: {}",
                proof.path.display()
            )));
        }
        Err(e) if is_path_escape(&e) => {
            return Err(Error::PathEscape(format!(
                "answer artifact must be a regular file: {}",
                proof.path.display()
            )));
        }
        Err(e) => return Err(e),
    };
    let size = file.metadata()?.len();
    if size != proof.bytes {
        return Err(Error::AnswerIntegrity(
            "answer artifact size does not match its recorded proof".into(),
        ));
    }
    if size > MAX_ANSWER as u64 {
        return Err(Error::AnswerIntegrity(format!(
            "answer artifact is {size} bytes, above the {MAX_ANSWER}-byte read bound"
        )));
    }
    let mut bytes = Vec::with_capacity(size as usize);
    (&mut file).take(size + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 != size {
        return Err(Error::AnswerIntegrity(
            "answer artifact size does not match its recorded proof".into(),
        ));
    }
    if fs::sha256(&bytes) != proof.sha256 {
        return Err(Error::AnswerIntegrity(
            "answer artifact hash does not match its recorded proof".into(),
        ));
    }
    let text = std::str::from_utf8(&bytes)
        .map_err(|_| Error::AnswerIntegrity("answer artifact is not valid UTF-8".into()))?;
    let name = relative
        .file_name()
        .and_then(|v| v.to_str())
        .ok_or_else(|| invalid("invalid answer filename"))?;
    let parent = relative.parent().unwrap_or_else(|| Path::new(""));
    let version = match load_sidecar(&dir, parent, name, proof.bytes, &proof.sha256)? {
        Some(_) => 2,
        None => {
            if !bytes.ends_with(LEGACY_FRAME) {
                return Err(Error::AnswerIntegrity(
                    "legacy answer lacks its exact terminal frame".into(),
                ));
            }
            1
        }
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
