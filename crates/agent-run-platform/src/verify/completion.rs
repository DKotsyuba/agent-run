//! Terminal completion decision policy: "answers are evidence, not trust".
//!
//! Ports `src/agent_run/verify.py:134-330` (`AnswerProof`, `inspect_answer`,
//! `_inspect_proof_sidecar`) and `verify.py:525-637` (`silence_seconds`,
//! `_silence_note`, `verify_completion`). The engine's own reported status is
//! never trusted alone: a reported success without a complete answer is a
//! failure, and no terminal status is issued while the engine process group
//! is still alive.
//!
//! Rust threads a sealed answer's path/bytes/sha256 to storage separately
//! (`supervisor::launch` passes `Option<&Proof>` straight to `Store::finish`),
//! so unlike Python's `_with_answer` this module does not embed those fields
//! back onto `Outcome` — `domain::Outcome` has no `answer_*` fields to carry
//! them. The decision (status/failure_kind/failure_text) is otherwise exact.

use super::{load_sidecar, Dir, MAX_ANSWER};
use crate::fs;
use agent_run_domain::{
    domain::{Outcome, Status},
    error::invalid,
    Error, Result,
};
use std::{
    io::Read,
    path::{Path, PathBuf},
};

/// No answer file exists, or it exists but is empty. Mirrors `verify.py:21`.
pub const NO_ANSWER: &str = "no_answer";
/// An answer file exists but its completion proof failed. Mirrors `verify.py:22`.
pub const ANSWER_INCOMPLETE: &str = "answer_incomplete";
/// An answer file exists with valid completion proof. Mirrors `verify.py:23`.
pub const ANSWER_PRESENT: &str = "answer_present";
/// The runtime produced no terminal result at all. Mirrors `verify.py:24`.
pub const ENGINE_VANISHED: &str = "engine_vanished";
/// The engine's process group outlived the supervised session. Mirrors `verify.py:25`.
pub const GROUP_SURVIVED: &str = "engine_group_survived";

/// Default quiet-time cutoff labeled "silent" in `failure_text`. Mirrors the
/// `verify_completion` default (`verify.py:569`); no caller overrides it.
pub const DEFAULT_SILENCE_THRESHOLD_SECONDS: f64 = 60.0;

/// Why supervision stopped waiting for the engine. Python represents this as
/// a `stop_reason: str | None` validated against two literals
/// (`verify.py:578-579`); the string-facing wrapper below preserves the
/// validation error for callers crossing the transport boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopReason {
    Cancel,
    Timeout,
}

/// What is on disk for an agent's answer, and whether it terminated.
///
/// Mirrors Python's `AnswerProof` (`verify.py:134-172`). Unlike `super::Proof`
/// (the sealer's record of a payload this binary itself wrote), this is
/// built by reading whatever is on disk with no prior expectation of its
/// hash or size — the same distinction Python draws between `verify.py`'s
/// sealed proof and its read-side `inspect_answer` classification.
#[derive(Debug, Clone)]
pub struct AnswerProof {
    pub path: PathBuf,
    pub exists: bool,
    pub size_bytes: u64,
    pub sha256: Option<String>,
    pub sentinel_found: bool,
    /// `1` for a historical sentinel-framed artifact, `2` for a versioned
    /// sidecar proof (whether or not that proof actually validated).
    pub proof_version: u32,
    /// Set only when `proof_version == 2` and the sidecar failed to validate.
    pub proof_error: Option<String>,
}

impl AnswerProof {
    /// Classify the absence of any answer artifact at `path`.
    pub fn absent(path: PathBuf) -> Self {
        Self {
            path,
            exists: false,
            size_bytes: 0,
            sha256: None,
            sentinel_found: false,
            proof_version: 1,
            proof_error: None,
        }
    }

    /// Classify an already-sealed `Proof` as its (always-complete) evidence.
    /// `super::seal` only ever writes a non-empty, proof-version-2 payload,
    /// so a sealed answer needs no independent re-inspection to know it is
    /// complete.
    pub fn sealed(proof: &super::Proof) -> Self {
        Self {
            path: proof.path.clone(),
            exists: true,
            size_bytes: proof.bytes,
            sha256: Some(proof.sha256.clone()),
            sentinel_found: true,
            proof_version: 2,
            proof_error: None,
        }
    }

    /// Whether the artifact has valid completion evidence. Mirrors
    /// `AnswerProof.complete` (`verify.py:154-162`).
    pub fn complete(&self) -> bool {
        if !self.exists || self.size_bytes == 0 {
            return false;
        }
        if self.proof_version == 2 {
            self.proof_error.is_none()
        } else {
            self.sentinel_found
        }
    }

    /// The stable completion-evidence label. Mirrors `AnswerProof.evidence`
    /// (`verify.py:164-172`).
    pub fn evidence(&self) -> &'static str {
        if !self.exists || self.size_bytes == 0 {
            return NO_ANSWER;
        }
        let present = if self.proof_version == 2 {
            self.proof_error.is_none()
        } else {
            self.sentinel_found
        };
        if present {
            ANSWER_PRESENT
        } else {
            ANSWER_INCOMPLETE
        }
    }
}

/// Hash the answer file at `root/relative` and establish its completion proof.
///
/// Mirrors `inspect_answer` (`verify.py:226-297`) with the default sentinel.
/// [`inspect_answer_with_sentinel`] additionally supports Python's
/// `sentinel=None` contract. The read is no-follow and bounded by
/// `MAX_ANSWER`, checked from the open descriptor's metadata before content.
pub fn inspect_answer(root: &Path, relative: &Path) -> Result<AnswerProof> {
    inspect_answer_with_sentinel(root, relative, Some("<<<agent-run:complete>>>"))
}

/// Inspects one answer using Python's optional sentinel contract.
///
/// `Some` requires a nonblank UTF-8 marker in the exact terminal
/// `newline + marker + newline` frame; `None` accepts any nonempty payload.
/// The owned answer tree is read without following links and failures are
/// returned as typed validation, I/O, or integrity errors.
pub fn inspect_answer_with_sentinel(
    root: &Path,
    relative: &Path,
    sentinel: Option<&str>,
) -> Result<AnswerProof> {
    if sentinel.is_some_and(|value| value.trim().is_empty()) {
        return Err(invalid("sentinel must be a nonblank string or None"));
    }
    let full = root.join(relative);
    let dir = Dir::open(root)?;
    let file = match dir.open_file(relative) {
        Ok(f) => f,
        Err(Error::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok(AnswerProof::absent(full));
        }
        Err(e) => return Err(e),
    };
    let size = file.metadata()?.len();
    if size > MAX_ANSWER as u64 {
        return Err(Error::Integrity(format!(
            "answer artifact is {size} bytes, above the {MAX_ANSWER}-byte inspection bound"
        )));
    }
    let mut bytes = Vec::with_capacity(size as usize);
    file.take(size + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 != size {
        return Err(Error::Integrity(
            "answer artifact changed size while inspecting it".into(),
        ));
    }
    let sha256 = fs::sha256(&bytes);
    let sentinel_found = if let Some(value) = sentinel {
        !bytes.is_empty() && bytes.ends_with(format!("\n{value}\n").as_bytes())
    } else {
        !bytes.is_empty()
    };
    let name = relative
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| invalid("invalid answer filename"))?;
    let parent = relative.parent().unwrap_or_else(|| Path::new(""));
    let (proof_version, proof_error) = match load_sidecar(&dir, parent, name, size, &sha256) {
        Ok(None) => (1, None),
        Ok(Some(_)) => (2, None),
        Err(error) => (2, Some(error.to_string())),
    };
    Ok(AnswerProof {
        path: full,
        exists: true,
        size_bytes: size,
        sha256: Some(sha256),
        sentinel_found,
        proof_version,
        proof_error,
    })
}

/// Seconds since the last observed engine progress, or `None` if never any.
/// Mirrors `silence_seconds` (`verify.py:525-535`).
pub fn silence_seconds(last_progress_at: Option<f64>, now: f64) -> Result<Option<f64>> {
    let Some(last) = last_progress_at else {
        return Ok(None);
    };
    if !last.is_finite() || !now.is_finite() {
        return Err(invalid("last_progress_at and now must be finite numbers"));
    }
    Ok(Some((now - last).max(0.0)))
}

/// Mirrors `_silence_note` (`verify.py:538-543`).
fn silence_note(last_progress_at: Option<f64>, now: f64, threshold: f64) -> Result<String> {
    Ok(match silence_seconds(last_progress_at, now)? {
        None => "silence=no_progress".to_owned(),
        Some(quiet) => {
            let label = if quiet >= threshold {
                "silent"
            } else {
                "active"
            };
            format!("silence={quiet:.1}s/{label}")
        }
    })
}

/// Decide the terminal outcome from process facts plus answer evidence.
///
/// Mirrors `verify_completion` (`verify.py:561-637`) branch-for-branch. The
/// engine's own exit status is never trusted on its own: a success without a
/// complete answer is a failure, and no terminal state is issued while the
/// engine process group is still alive.
#[allow(clippy::too_many_arguments)]
pub fn verify_completion(
    session_outcome: Option<Outcome>,
    stop_reason: Option<StopReason>,
    answer: Option<&AnswerProof>,
    group_gone: bool,
    last_progress_at: Option<f64>,
    now: f64,
    silence_threshold_seconds: f64,
) -> Result<Outcome> {
    let note = silence_note(last_progress_at, now, silence_threshold_seconds)?;
    let evidence = answer.map(AnswerProof::evidence).unwrap_or(NO_ANSWER);
    let session_id = session_outcome
        .as_ref()
        .and_then(|o| o.runtime_session_id.clone());

    if !group_gone {
        return Ok(Outcome {
            status: Status::Failed,
            exit_code: None,
            failure_kind: Some(GROUP_SURVIVED.into()),
            failure_text: Some(format!("{evidence}; {note}")),
            runtime_session_id: session_id,
        });
    }
    if let Some(reason) = stop_reason {
        let status = match reason {
            StopReason::Cancel => Status::Cancelled,
            StopReason::Timeout => Status::TimedOut,
        };
        return Ok(Outcome {
            status,
            exit_code: None,
            failure_kind: Some(evidence.into()),
            failure_text: Some(note),
            runtime_session_id: session_id,
        });
    }
    let Some(outcome) = session_outcome else {
        return Ok(Outcome {
            status: Status::Failed,
            exit_code: None,
            failure_kind: Some(ENGINE_VANISHED.into()),
            failure_text: Some(format!("{evidence}; {note}")),
            runtime_session_id: None,
        });
    };
    if outcome.status != Status::Succeeded {
        return Ok(outcome);
    }
    if !answer.is_some_and(AnswerProof::complete) {
        return Ok(Outcome {
            status: Status::Failed,
            exit_code: outcome.exit_code,
            failure_kind: Some(evidence.into()),
            failure_text: Some(note),
            runtime_session_id: session_id,
        });
    }
    Ok(outcome)
}

/// Validates Python's string stop-reason boundary before applying completion policy.
///
/// `stop_reason` accepts only `None`, `"cancel"`, or `"timeout"`; unknown
/// strings are rejected before process or answer facts are evaluated. The
/// remaining arguments and returned terminal outcome have the same semantics
/// as [`verify_completion`].
pub fn verify_completion_with_stop_reason(
    session_outcome: Option<Outcome>,
    stop_reason: Option<&str>,
    answer: Option<&AnswerProof>,
    group_gone: bool,
    last_progress_at: Option<f64>,
    now: f64,
    silence_threshold_seconds: f64,
) -> Result<Outcome> {
    let parsed = match stop_reason {
        None => None,
        Some("cancel") => Some(StopReason::Cancel),
        Some("timeout") => Some(StopReason::Timeout),
        Some(_) => return Err(invalid("stop_reason must be cancel, timeout, or None")),
    };
    verify_completion(
        session_outcome,
        parsed,
        answer,
        group_gone,
        last_progress_at,
        now,
        silence_threshold_seconds,
    )
}
