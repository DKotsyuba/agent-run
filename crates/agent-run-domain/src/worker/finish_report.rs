//! Optional structured `finish` result: bounded fields, validation, the
//! human `answer.md` rendering and the compact completion-notice line.
//!
//! A finish carrying only `summary` (and optionally `status`) stays a plain
//! finish with its historical bytes, digest and answer. Any structured field
//! turns the call into a structured result whose core fields are required.

use super::{FinishRequest, FinishStatus, bounded_text};
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::{Component, Path};

/// Shared budget of every structured text field, matching the plain summary.
pub const MAX_STRUCTURED_BYTES: usize = 65536;
/// Bound of the 1–3 sentence summary that accompanies structured fields.
pub const MAX_STRUCTURED_SUMMARY: usize = 4096;
/// Final `answer.md` line binding the rendering to its `answer.json` bytes.
pub const STRUCTURED_FOOTER: &str = "Structured result: answer.json sha256 ";
/// Characters of the worker's `next` text shown in a completion notice.
const NOTICE_NEXT_CHARS: usize = 120;
/// Bounds of a single-line path or command.
const MAX_LINE: usize = 1024;
/// Bounds of one multi-line list entry or note.
const MAX_ITEM: usize = 2048;

/// Generic serde refusal; never echoes the rejected value.
const INVALID_ARGUMENTS: &str = "unknown, missing, or incorrectly typed finish argument";

/// Kind of one changed file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangeKind {
    /// The file was created.
    Added,
    /// The file existed and was changed.
    Modified,
    /// The file was removed.
    Deleted,
}

impl ChangeKind {
    /// Stable wire spelling.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Added => "added",
            Self::Modified => "modified",
            Self::Deleted => "deleted",
        }
    }
}

/// One worker-reported file change; untrusted and never checked on disk.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileChange {
    /// Changed path as the worker reports it, one line of at most 1024 bytes.
    pub path: String,
    /// Added, modified or deleted.
    pub kind: ChangeKind,
}

/// Literal declaration that the run changed nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NoChanges {
    /// The task could change files but did not.
    None,
    /// The task was read-only by design.
    ReadOnly,
}

/// Either a file list or a literal `"none"` / `"read_only"`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum FinishChanges {
    /// Worker-reported changed files.
    Files(Vec<FileChange>),
    /// Nothing changed.
    Literal(NoChanges),
}

/// Outcome of one reported check.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckResult {
    /// The check ran and passed.
    Passed,
    /// The check ran and failed.
    Failed,
    /// The check was not run.
    NotRun,
}

impl CheckResult {
    /// Stable wire spelling.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Passed => "passed",
            Self::Failed => "failed",
            Self::NotRun => "not_run",
        }
    }
}

/// One worker-reported check; Run neither reruns it nor reads its evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinishCheck {
    /// The command or check name, one line of at most 1024 bytes.
    pub command: String,
    /// Passed, failed or not_run.
    pub result: CheckResult,
    /// Optional explanation, at most 2048 bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    /// Optional log or test-report path inside the workdir.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence: Option<String>,
}

impl FinishStatus {
    /// Stable wire spelling.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Done => "done",
            Self::Partial => "partial",
            Self::Blocked => "blocked",
            Self::Failed => "failed",
        }
    }

    /// Public failure category of a non-done verdict; `None` means success
    /// remains subject to answer and cleanup verification.
    pub const fn failure_kind(self) -> Option<&'static str> {
        match self {
            Self::Done => None,
            Self::Partial => Some("worker_partial"),
            Self::Blocked => Some("worker_blocked"),
            Self::Failed => Some("worker_failed"),
        }
    }
}

/// Nonblank single-line text: no control characters at all.
fn line(label: &str, text: &str, max: usize) -> Result<()> {
    if text.trim().is_empty() || text.len() > max || text.chars().any(char::is_control) {
        return Err(Error::Validation(format!(
            "{label} must be one line of 1–{max} UTF-8 bytes"
        )));
    }
    Ok(())
}

/// Bounds a list's length and every entry with `check`.
fn list<T>(label: &str, items: &[T], max: usize, check: impl Fn(&T) -> Result<()>) -> Result<()> {
    if items.len() > max {
        return Err(Error::Validation(format!(
            "{label} accepts at most {max} entries"
        )));
    }
    items.iter().try_for_each(check)
}

impl FinishRequest {
    /// Decode raw tool arguments. Unlike plain deserialization this knows
    /// whether `status` was supplied, which a structured result requires.
    /// Serde refusals use one fixed message and never echo argument values.
    pub fn decode(arguments: Value) -> Result<Self> {
        let explicit_status = arguments
            .get("status")
            .is_some_and(|value| !value.is_null());
        let input: Self = serde_json::from_value(arguments)
            .map_err(|_| Error::Validation(INVALID_ARGUMENTS.into()))?;
        if input.is_structured() && !explicit_status {
            return Err(Error::Validation(
                "structured finish requires status: done, partial, blocked or failed".into(),
            ));
        }
        input.validate()?;
        Ok(input)
    }

    /// True when any field beyond `summary` and `status` is present.
    pub fn is_structured(&self) -> bool {
        self.changes.is_some()
            || self.commits.is_some()
            || self.checks.is_some()
            || self.unverified.is_some()
            || self.next.is_some()
            || self.baseline_failures.is_some()
            || self.blocked_on.is_some()
            || self.questions.is_some()
            || self.artifacts.is_some()
            || self.report.is_some()
    }

    /// Every worker-authored string, for secret screening and the shared budget.
    fn texts(&self) -> Vec<&str> {
        let mut texts = vec![self.summary.as_str()];
        if let Some(FinishChanges::Files(files)) = &self.changes {
            texts.extend(files.iter().map(|file| file.path.as_str()));
        }
        for check in self.checks.iter().flatten() {
            texts.push(&check.command);
            texts.extend(check.note.as_deref());
            texts.extend(check.evidence.as_deref());
        }
        for items in [
            &self.commits,
            &self.unverified,
            &self.baseline_failures,
            &self.questions,
            &self.artifacts,
        ] {
            texts.extend(items.iter().flatten().map(String::as_str));
        }
        texts.extend(self.next.as_deref());
        texts.extend(self.blocked_on.as_deref());
        texts.extend(self.report.as_deref());
        texts
    }

    /// True when `found` matches any worker-authored text or, for a structured
    /// result, its complete rendering. Callers pass a secret detector, so a
    /// secret split by JSON escaping or hidden in any field is still refused.
    pub fn screen(&self, mut found: impl FnMut(&str) -> bool) -> bool {
        self.texts().into_iter().any(&mut found)
            || (self.is_structured() && self.answer_text().is_ok_and(|text| found(&text)))
    }

    /// Bounds a structured result. Required: status (checked by `decode`),
    /// a short summary, changes or commits, checks, unverified and next;
    /// blocked_on when blocked. All text shares the 64 KiB budget.
    pub(super) fn validate_structured(&self) -> Result<()> {
        bounded_text("finish summary", &self.summary, MAX_STRUCTURED_SUMMARY)?;
        let mut missing = Vec::new();
        if self.changes.is_none() && self.commits.is_none() {
            missing.push("changes (or commits)");
        }
        if self.checks.is_none() {
            missing.push("checks");
        }
        if self.unverified.is_none() {
            missing.push("unverified");
        }
        if self.next.is_none() {
            missing.push("next");
        }
        if self.status == FinishStatus::Blocked && self.blocked_on.is_none() {
            missing.push("blocked_on");
        }
        if !missing.is_empty() {
            return Err(Error::Validation(format!(
                "structured finish is missing {}; use changes \"none\" or \"read_only\" and empty lists when nothing applies",
                missing.join(", ")
            )));
        }
        match &self.changes {
            Some(FinishChanges::Files(files)) => list("changes", files, 1000, |file| {
                line("change path", &file.path, MAX_LINE)
            })?,
            Some(FinishChanges::Literal(_))
                if self.commits.as_ref().is_some_and(|c| !c.is_empty()) =>
            {
                return Err(Error::Validation(
                    "changes \"none\" or \"read_only\" cannot be combined with commits".into(),
                ));
            }
            _ => {}
        }
        list(
            "commits",
            self.commits.as_deref().unwrap_or(&[]),
            200,
            |sha| {
                if (7..=64).contains(&sha.len()) && sha.bytes().all(|b| b.is_ascii_hexdigit()) {
                    Ok(())
                } else {
                    Err(Error::Validation(
                        "each commit must be a 7–64 character hexadecimal SHA".into(),
                    ))
                }
            },
        )?;
        list(
            "checks",
            self.checks.as_deref().unwrap_or(&[]),
            200,
            |check| {
                line("check command", &check.command, MAX_LINE)?;
                if let Some(note) = &check.note {
                    bounded_text("check note", note, MAX_ITEM)?;
                }
                if let Some(evidence) = &check.evidence {
                    line("check evidence", evidence, MAX_LINE)?;
                }
                Ok(())
            },
        )?;
        let item = |label: &'static str| move |text: &String| bounded_text(label, text, MAX_ITEM);
        list(
            "unverified",
            self.unverified.as_deref().unwrap_or(&[]),
            100,
            item("unverified entry"),
        )?;
        list(
            "baseline_failures",
            self.baseline_failures.as_deref().unwrap_or(&[]),
            100,
            item("baseline failure"),
        )?;
        list(
            "questions",
            self.questions.as_deref().unwrap_or(&[]),
            50,
            item("question"),
        )?;
        list(
            "artifacts",
            self.artifacts.as_deref().unwrap_or(&[]),
            200,
            |path| line("artifact path", path, MAX_LINE),
        )?;
        if let Some(next) = &self.next {
            bounded_text("next", next, MAX_LINE)?;
        }
        if let Some(blocked_on) = &self.blocked_on {
            bounded_text("blocked_on", blocked_on, MAX_STRUCTURED_SUMMARY)?;
        }
        if let Some(report) = &self.report {
            bounded_text("report", report, MAX_STRUCTURED_BYTES)?;
        }
        let total: usize = self.texts().iter().map(|text| text.len()).sum();
        if total > MAX_STRUCTURED_BYTES {
            return Err(Error::Validation(format!(
                "structured finish text must total at most {MAX_STRUCTURED_BYTES} UTF-8 bytes"
            )));
        }
        Ok(())
    }

    /// Check evidence must name a path inside `workdir`: relative without
    /// `..`, or absolute under it. Lexical only; files are never opened.
    pub fn validate_workdir(&self, workdir: &Path) -> Result<()> {
        for evidence in self
            .checks
            .iter()
            .flatten()
            .filter_map(|c| c.evidence.as_deref())
        {
            let path = Path::new(evidence);
            let inside = !path.components().any(|c| c == Component::ParentDir)
                && (path.is_relative() || path.starts_with(workdir));
            if !inside {
                return Err(Error::Validation(
                    "check evidence must be a path inside the workdir".into(),
                ));
            }
        }
        Ok(())
    }

    /// The exact canonical JSON persisted as the finish intent and published
    /// as `answer.json`; its SHA-256 is the finish receipt digest.
    pub fn canonical_payload(&self) -> Result<String> {
        String::from_utf8(crate::canonical::dumps(&serde_json::to_value(self)?, true))
            .map_err(|_| Error::Validation("finish serialization failed".into()))
    }

    /// The public `answer.md` text: the exact summary of a plain finish, or the
    /// deterministic Markdown rendering of a structured result ending with
    /// the digest of its `answer.json`.
    pub fn answer_text(&self) -> Result<String> {
        if !self.is_structured() {
            return Ok(self.summary.clone());
        }
        let digest = crate::canonical::sha256_hex(&serde_json::to_value(self)?, true);
        let mut out = format!(
            "Status: {}\n\n{}\n",
            self.status.as_str(),
            self.summary.trim_end()
        );
        let mut section = |title: &str, body: String| {
            out.push_str(&format!("\n## {title}\n\n{}\n", body.trim_end()));
        };
        let mut changes = Vec::new();
        match &self.changes {
            Some(FinishChanges::Literal(NoChanges::None)) => changes.push("- none".to_owned()),
            Some(FinishChanges::Literal(NoChanges::ReadOnly)) => {
                changes.push("- none (read-only task)".to_owned());
            }
            Some(FinishChanges::Files(files)) if files.is_empty() => {
                changes.push("- no files listed".to_owned());
            }
            Some(FinishChanges::Files(files)) => changes.extend(
                files
                    .iter()
                    .map(|file| format!("- {} {}", file.kind.as_str(), code(&file.path))),
            ),
            None => {}
        }
        changes.extend(
            self.commits
                .iter()
                .flatten()
                .map(|sha| format!("- commit {}", code(sha))),
        );
        section("Changes", changes.join("\n"));
        let checks = self.checks.as_deref().unwrap_or(&[]);
        section(
            "Checks",
            if checks.is_empty() {
                "- none reported".into()
            } else {
                checks
                    .iter()
                    .map(|check| {
                        let mut entry =
                            format!("- {}: {}", check.result.as_str(), code(&check.command));
                        if let Some(note) = &check.note {
                            entry.push_str(&format!(" — {}", indent(note)));
                        }
                        if let Some(evidence) = &check.evidence {
                            entry.push_str(&format!("\n  evidence: {}", code(evidence)));
                        }
                        entry
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
            },
        );
        if let Some(items) = self.baseline_failures.as_deref().filter(|i| !i.is_empty()) {
            section("Baseline failures", bullets(items));
        }
        section(
            "Unverified",
            match self.unverified.as_deref() {
                Some(items) if !items.is_empty() => bullets(items),
                _ => "- none".into(),
            },
        );
        if let Some(blocked_on) = &self.blocked_on {
            section("Blocked on", blocked_on.clone());
        }
        if let Some(items) = self.questions.as_deref().filter(|i| !i.is_empty()) {
            section("Questions", bullets(items));
        }
        if let Some(items) = self.artifacts.as_deref().filter(|i| !i.is_empty()) {
            section(
                "Artifacts",
                items
                    .iter()
                    .map(|path| format!("- {}", code(path)))
                    .collect::<Vec<_>>()
                    .join("\n"),
            );
        }
        if let Some(next) = &self.next {
            section("Next", next.clone());
        }
        if let Some(report) = &self.report {
            section("Report", report.clone());
        }
        out.push_str(&format!("\n---\n{STRUCTURED_FOOTER}{digest}\n"));
        Ok(out)
    }

    /// The `answer.json` digest named by a verified structured `answer.md`,
    /// or `None` for a plain answer.
    pub fn structured_digest(answer: &str) -> Option<&str> {
        let digest = answer
            .strip_suffix('\n')?
            .rsplit('\n')
            .next()?
            .strip_prefix(STRUCTURED_FOOTER)?;
        (digest.len() == 64
            && digest
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()))
        .then_some(digest)
    }

    /// One compact, injection-safe completion-notice line for a structured
    /// result, e.g. `done · 3 files · checks 5 passed, 1 not_run · 2
    /// unverified · next: "commit and push"`. Counts are derived from the
    /// validated result; only `next` is worker text, JSON-quoted and bounded.
    pub fn notice_line(&self) -> Option<String> {
        if !self.is_structured() {
            return None;
        }
        let plural =
            |n: usize, one: &str, many: &str| format!("{n} {}", if n == 1 { one } else { many });
        let mut parts = vec![self.status.as_str().to_owned()];
        let mut changes = Vec::new();
        match &self.changes {
            Some(FinishChanges::Files(files)) => changes.push(plural(files.len(), "file", "files")),
            Some(FinishChanges::Literal(NoChanges::None)) => changes.push("no changes".into()),
            Some(FinishChanges::Literal(NoChanges::ReadOnly)) => changes.push("read-only".into()),
            None => {}
        }
        if let Some(commits) = self.commits.as_ref().filter(|c| !c.is_empty()) {
            changes.push(plural(commits.len(), "commit", "commits"));
        }
        if !changes.is_empty() {
            parts.push(changes.join(", "));
        }
        let checks = self.checks.as_deref().unwrap_or(&[]);
        if checks.is_empty() {
            parts.push("no checks".into());
        } else {
            let counts: Vec<String> = [
                CheckResult::Passed,
                CheckResult::Failed,
                CheckResult::NotRun,
            ]
            .into_iter()
            .filter_map(|result| {
                let n = checks.iter().filter(|check| check.result == result).count();
                (n > 0).then(|| format!("{n} {}", result.as_str()))
            })
            .collect();
            parts.push(format!("checks {}", counts.join(", ")));
        }
        let count = |items: &Option<Vec<String>>| items.as_ref().map_or(0, Vec::len);
        let baseline = count(&self.baseline_failures);
        if baseline > 0 {
            parts.push(plural(baseline, "baseline failure", "baseline failures"));
        }
        let unverified = count(&self.unverified);
        if unverified > 0 {
            parts.push(format!("{unverified} unverified"));
        }
        let questions = count(&self.questions);
        if questions > 0 {
            parts.push(plural(questions, "question", "questions"));
        }
        if let Some(next) = &self.next {
            let mut shown: String = next.chars().take(NOTICE_NEXT_CHARS).collect();
            if shown.len() < next.len() {
                shown.push('…');
            }
            parts.push(format!("next: {}", quote(&shown)));
        }
        Some(parts.join(" · "))
    }
}

/// JSON-quotes untrusted text so newlines, separators and markup stay inert.
pub fn quote(text: &str) -> String {
    serde_json::to_string(text)
        .unwrap_or_else(|_| "\"\"".into())
        .replace('\u{2028}', "\\u2028")
        .replace('\u{2029}', "\\u2029")
        .replace('<', "\\u003c")
        .replace('>', "\\u003e")
}

/// Inline code when the text carries no backtick; otherwise the raw text.
fn code(text: &str) -> String {
    if text.contains('`') {
        text.to_owned()
    } else {
        format!("`{text}`")
    }
}

/// Indents continuation lines so multi-line text stays in its list item.
fn indent(text: &str) -> String {
    text.trim_end().replace('\n', "\n  ")
}

/// A Markdown bullet list of multi-line entries.
fn bullets(items: &[String]) -> String {
    items
        .iter()
        .map(|item| format!("- {}", indent(item)))
        .collect::<Vec<_>>()
        .join("\n")
}

#[cfg(test)]
/// Structured finish contract: decoding, bounds, rendering and notice line.
mod tests {
    use super::*;
    use serde_json::json;

    /// A complete structured result used by several checks.
    fn structured() -> Value {
        json!({
            "status":"done",
            "summary":"Added structured finish. Focused tests pass.",
            "changes":[
                {"path":"src/a.rs","kind":"modified"},
                {"path":"src/b.rs","kind":"added"},
                {"path":"old.txt","kind":"deleted"}
            ],
            "commits":["0a1b2c3d"],
            "checks":[
                {"command":"cargo test -p x","result":"passed","evidence":"logs/test.txt"},
                {"command":"cargo fmt --check","result":"passed"},
                {"command":"cargo clippy","result":"passed"},
                {"command":"cargo xtask contract check","result":"passed"},
                {"command":"node --test","result":"passed"},
                {"command":"cargo xtask check","result":"not_run","note":"host-only gate\nfails on main"}
            ],
            "unverified":["macOS launch","Codex anyOf rendering"],
            "next":"commit and push"
        })
    }

    /// A plain finish keeps its historical canonical bytes and exact answer.
    #[test]
    fn plain_finish_keeps_historical_payload_and_answer() {
        for (arguments, status) in [
            (json!({"summary":"All done."}), FinishStatus::Done),
            (
                json!({"summary":"Half.","status":"partial"}),
                FinishStatus::Partial,
            ),
        ] {
            let input = FinishRequest::decode(arguments).unwrap();
            assert!(!input.is_structured());
            assert_eq!(input.status, status);
            assert_eq!(input.answer_text().unwrap(), input.summary);
            assert_eq!(input.notice_line(), None);
        }
        let input = FinishRequest::decode(json!({"summary":"All done."})).unwrap();
        assert_eq!(
            input.canonical_payload().unwrap(),
            r#"{"status":"done","summary":"All done."}"#
        );
        assert_eq!(FinishStatus::Partial.failure_kind(), Some("worker_partial"));
        assert_eq!(FinishStatus::Done.failure_kind(), None);
    }

    /// Structured fields require an explicit status and the core sections.
    #[test]
    fn structured_finish_requires_status_and_core_fields() {
        let mut missing_status = structured();
        missing_status.as_object_mut().unwrap().remove("status");
        let error = FinishRequest::decode(missing_status).unwrap_err();
        assert!(error.to_string().contains("requires status"), "{error}");
        let error = FinishRequest::decode(json!({"status":"done","summary":"x","next":"none"}))
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("changes (or commits), checks, unverified"),
            "{error}"
        );
        let mut blocked = structured();
        blocked["status"] = json!("blocked");
        let error = FinishRequest::decode(blocked.clone())
            .unwrap_err()
            .to_string();
        assert!(error.contains("blocked_on"), "{error}");
        blocked["blocked_on"] = json!("Needs the deploy key from the owner.");
        assert!(FinishRequest::decode(blocked).is_ok());
        let minimal = json!({"status":"done","summary":"Reviewed.","changes":"read_only",
            "checks":[],"unverified":[],"next":"none"});
        assert!(FinishRequest::decode(minimal).unwrap().is_structured());
    }

    /// Bounds, literal/commit conflicts and typing errors refuse without echoing values.
    #[test]
    fn structured_finish_bounds_are_enforced() {
        let refuse = |patch: Value| {
            let mut value = structured();
            for (key, field) in patch.as_object().unwrap() {
                value[key] = field.clone();
            }
            FinishRequest::decode(value).unwrap_err().to_string()
        };
        assert!(refuse(json!({"summary":"s".repeat(4097)})).contains("finish summary"));
        assert!(refuse(json!({"changes":"none"})).contains("cannot be combined"));
        assert!(refuse(json!({"commits":["xyz"]})).contains("hexadecimal"));
        assert!(refuse(json!({"changes":[{"path":"a\nb","kind":"added"}]})).contains("one line"));
        assert!(
            refuse(json!({"checks":[{"command":"x","result":"skipped-SECRET"}]}))
                == INVALID_ARGUMENTS
        );
        assert!(refuse(json!({"unverified":vec!["u"; 101]})).contains("at most 100"));
        assert!(refuse(json!({"report":"r".repeat(65400)})).contains("total at most 65536"));
        assert!(refuse(json!({"extra":1})) == INVALID_ARGUMENTS);
        assert!(refuse(json!({"next":"   "})).contains("next"));
    }

    /// Evidence must stay lexically inside the workdir.
    #[test]
    fn check_evidence_must_be_inside_workdir() {
        let workdir = Path::new("/work/repo");
        let with = |evidence: &str| {
            let mut value = structured();
            value["checks"][0]["evidence"] = json!(evidence);
            FinishRequest::decode(value)
                .unwrap()
                .validate_workdir(workdir)
        };
        assert!(with("logs/test.txt").is_ok());
        assert!(with("/work/repo/target/report.xml").is_ok());
        assert!(with("../other/log.txt").is_err());
        assert!(with("/tmp/log.txt").is_err());
        assert!(with("/work/repo/../x").is_err());
    }

    /// The rendering lists every section and ends with the answer.json digest.
    #[test]
    fn structured_answer_renders_sections_and_digest() {
        let mut value = structured();
        value["baseline_failures"] = json!(["cargo xtask check"]);
        value["questions"] = json!(["Ship now?"]);
        value["artifacts"] = json!(["target/report.html"]);
        value["report"] = json!("### Details\nAll good.");
        let input = FinishRequest::decode(value).unwrap();
        let text = input.answer_text().unwrap();
        let digest = crate::canonical::sha256_hex(&serde_json::to_value(&input).unwrap(), true);
        assert_eq!(
            text,
            format!(
                "Status: done\n\nAdded structured finish. Focused tests pass.\n\
\n## Changes\n\n- modified `src/a.rs`\n- added `src/b.rs`\n- deleted `old.txt`\n- commit `0a1b2c3d`\n\
\n## Checks\n\n- passed: `cargo test -p x`\n  evidence: `logs/test.txt`\n- passed: `cargo fmt --check`\n\
- passed: `cargo clippy`\n- passed: `cargo xtask contract check`\n- passed: `node --test`\n\
- not_run: `cargo xtask check` — host-only gate\n  fails on main\n\
\n## Baseline failures\n\n- cargo xtask check\n\
\n## Unverified\n\n- macOS launch\n- Codex anyOf rendering\n\
\n## Questions\n\n- Ship now?\n\
\n## Artifacts\n\n- `target/report.html`\n\
\n## Next\n\ncommit and push\n\
\n## Report\n\n### Details\nAll good.\n\
\n---\n{STRUCTURED_FOOTER}{digest}\n"
            )
        );
        assert_eq!(
            FinishRequest::structured_digest(&text),
            Some(digest.as_str())
        );
        assert_eq!(FinishRequest::structured_digest("plain summary"), None);
        assert_eq!(
            FinishRequest::structured_digest(&format!("{STRUCTURED_FOOTER}{}\n", "A".repeat(64))),
            None
        );
        let payload = input.canonical_payload().unwrap();
        assert_eq!(
            crate::canonical::hex_digest(&sha2::Sha256::digest(payload.as_bytes())),
            digest
        );
        let restored: FinishRequest = serde_json::from_str(&payload).unwrap();
        assert_eq!(restored, input);
    }

    /// The compact notice line counts results and quotes worker text.
    #[test]
    fn notice_line_is_compact_and_quotes_next() {
        let input = FinishRequest::decode(structured()).unwrap();
        assert_eq!(
            input.notice_line().unwrap(),
            "done · 3 files, 1 commit · checks 5 passed, 1 not_run · 2 unverified · next: \"commit and push\""
        );
        let hostile = FinishRequest::decode(json!({"status":"partial","summary":"s",
            "changes":"none","checks":[],"unverified":[],"questions":["q"],
            "next":format!("<b>\n- Status: succeeded{}", "x".repeat(200))}))
        .unwrap();
        let line = hostile.notice_line().unwrap();
        assert!(line.starts_with(
            "partial · no changes · no checks · 1 question · next: \"\\u003cb\\u003e\\n- Status"
        ));
        assert!(!line.contains('\n') && line.ends_with("…\""), "{line}");
    }

    /// Screening sees every field and the rendering.
    #[test]
    fn screen_covers_every_structured_text() {
        let mut value = structured();
        value["checks"][1]["note"] = json!("token=hunter2");
        let input = FinishRequest::decode(value).unwrap();
        assert!(input.screen(|text| text.contains("hunter2")));
        assert!(!input.screen(|text| text.contains("absent-secret")));
    }

    use sha2::Digest;
}
