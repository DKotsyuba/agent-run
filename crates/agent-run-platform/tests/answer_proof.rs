//! Answer sealing, proof sidecars, and bounded verified reads (M24). Each
//! test names the Python behavior it mirrors so a regression here also flags
//! the Python test to re-check. `tests/fixtures/baseline/answers/` was
//! written by the Python release (`cases.json` records its verdicts); it is
//! read here but never mutated — tests that need to corrupt an artifact do
//! so in a private tempdir seeded by `verify::seal`.
use agent_run_domain::Error;
use agent_run_platform::{
    fs::Dir,
    verify::{self, Proof},
};
use std::path::{Path, PathBuf};

fn tempdir() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("agent-run-answer-proof-")
        .tempdir_in(std::env::var_os("AGENT_RUN_TEST_TMP").unwrap_or_else(|| "/tmp".into()))
        .expect("tempdir")
}

fn corpus_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/baseline/answers")
}

fn corpus_cases() -> Vec<serde_json::Value> {
    let raw = std::fs::read(corpus_root().join("cases.json")).expect("golden corpus is present");
    let doc: serde_json::Value = serde_json::from_slice(&raw).expect("corpus JSON parses");
    doc["cases"].as_array().expect("cases array").clone()
}

fn exception_of(case: &serde_json::Value, field: &str) -> Option<String> {
    case.get(field)?
        .get("exception")?
        .as_str()
        .map(str::to_owned)
}

fn message_of(case: &serde_json::Value, field: &str) -> Option<String> {
    case.get(field)?.get("message")?.as_str().map(str::to_owned)
}

/// Python's exception classes collapse onto two Rust variants: a symlinked
/// or special-file escape is always `PathEscapeError`, and every other
/// answer-evidence failure (`AnswerMissingError`, `AnswerTamperedError`,
/// `AnswerOversizedError`, `AnswerEncodingError`, `AnswerProofError`) is
/// `Error::AnswerIntegrity`. `None` means Python accepted the case.
fn expected_kind(exception: Option<&str>) -> Option<&'static str> {
    match exception {
        None => None,
        Some("PathEscapeError") => Some("PathEscapeError"),
        Some(_) => Some("AnswerIntegrityError"),
    }
}

fn actual_kind(error: &Error) -> &'static str {
    match error {
        Error::PathEscape(_) => "PathEscapeError",
        Error::AnswerIntegrity(_) => "AnswerIntegrityError",
        _ => "Other",
    }
}

/// Mirrors the golden corpus itself (inventory `CX-2`, plan tests T37-T44)
/// and `test_answer_payload_proof.py::test_read_answer_payload_raises_distinct_typed_errors`:
/// `verify::read` composes payload verification with the sidecar check —
/// matching `AgentService.answer()` in Python, which checks both before
/// trusting a stored proof — so a case is accepted only when *both* the
/// corpus's `read` and `proof` fields are, and otherwise fails with
/// whichever of the two names an exception.
#[test]
fn golden_corpus_composed_read_matches_python_verdicts() {
    for case in corpus_cases() {
        let name = case["case"].as_str().expect("case name");
        let root = corpus_root().join("agents").join(name);
        let proof = Proof {
            path: root.join("answer.md"),
            bytes: case["expected_bytes"].as_u64().expect("expected_bytes"),
            sha256: case["expected_sha256"]
                .as_str()
                .expect("expected_sha256")
                .to_owned(),
            proof_version: case["format"].as_u64().expect("format") as u32,
        };
        let exception = exception_of(&case, "read").or_else(|| exception_of(&case, "proof"));
        let want = expected_kind(exception.as_deref());
        let message = message_of(&case, "read").or_else(|| message_of(&case, "proof"));
        match verify::read(&root, &proof, verify::INLINE_ANSWER) {
            Ok(_) => assert!(want.is_none(), "case {name}: expected {want:?}, got Ok"),
            Err(e) => {
                assert_eq!(Some(actual_kind(&e)), want, "case {name}: {e}");
                if let Some(expected_message) = message {
                    // The symlink case's Python message embeds an absolute
                    // path from wherever the corpus was generated, which
                    // never matches this worktree's own path; every other
                    // message is path-free and must match exactly.
                    if want != Some("PathEscapeError") {
                        assert_eq!(e.to_string(), expected_message, "case {name}");
                    } else {
                        assert!(
                            e.to_string()
                                .starts_with("answer artifact must be a regular file:"),
                            "case {name}: {e}"
                        );
                    }
                }
            }
        }
    }
}

/// Mirrors the corpus's isolated `proof` field against `load_answer_proof`
/// (`verify.py:394-413`), decoupled from the live payload bytes — Rust's
/// `load_sidecar` is that same decoupled check.
#[test]
fn golden_corpus_proof_sidecar_matches_python() {
    for case in corpus_cases() {
        let name = case["case"].as_str().expect("case name");
        let root = corpus_root().join("agents").join(name);
        let dir = Dir::open(&root).expect("corpus agent directory opens");
        let bytes = case["expected_bytes"].as_u64().expect("expected_bytes");
        let sha256 = case["expected_sha256"].as_str().expect("expected_sha256");
        let result = verify::load_sidecar(&dir, Path::new(""), "answer.md", bytes, sha256);
        match exception_of(&case, "proof") {
            None => assert!(result.is_ok(), "case {name}: {result:?}"),
            Some(_) => {
                let err = result.expect_err("case should reject");
                if let Some(expected_message) = message_of(&case, "proof") {
                    assert_eq!(err.to_string(), expected_message, "case {name}");
                }
            }
        }
    }
}

/// Mirrors `test_seal_writes_clean_payload_and_versioned_proof`: sealing
/// writes the exact payload bytes, a `.answer-format` marker of `"2\n"`, and
/// a `<name>.proof.json` sidecar binding kind/media_type/answer/bytes/sha256
/// — and the round trip through `verify::read` returns the same text.
#[test]
fn seal_round_trip_matches_python_layout() {
    let dir = tempdir();
    let root = dir.path();
    let text = "body text";
    let proof = verify::seal(root, Path::new("answer.md"), text).unwrap();
    assert_eq!(
        std::fs::read(root.join("answer.md")).unwrap(),
        text.as_bytes()
    );
    assert_eq!(std::fs::read(root.join(".answer-format")).unwrap(), b"2\n");
    let sidecar: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("answer.md.proof.json")).unwrap()).unwrap();
    assert_eq!(sidecar["kind"], "agent_answer");
    assert_eq!(sidecar["media_type"], "text/markdown; charset=utf-8");
    assert_eq!(sidecar["proof_version"], 2);
    assert_eq!(sidecar["answer"], "answer.md");
    assert_eq!(sidecar["bytes"], text.len());
    assert_eq!(sidecar["sha256"], proof.sha256);
    let (version, content) = verify::read(root, &proof, verify::INLINE_ANSWER).unwrap();
    assert_eq!(version, 2);
    assert_eq!(content.as_deref(), Some(text));
}

/// Mirrors the golden corpus's `proof-v2-valid` and `legacy-v1-valid`
/// cases: an artifact the Python release wrote must still verify
/// byte-for-byte in Rust, for both proof formats, with the legacy frame
/// stripped only for format 1.
#[test]
fn python_written_artifacts_verify_for_both_formats() {
    let cases = corpus_cases();
    for (name, version, frame_stripped) in [
        ("proof-v2-valid", 2u32, false),
        ("legacy-v1-valid", 1u32, true),
    ] {
        let entry = cases
            .iter()
            .find(|c| c["case"] == name)
            .unwrap_or_else(|| panic!("corpus case {name} present"));
        let root = corpus_root().join("agents").join(name);
        let proof = Proof {
            path: root.join("answer.md"),
            bytes: entry["expected_bytes"].as_u64().unwrap(),
            sha256: entry["expected_sha256"].as_str().unwrap().to_owned(),
            proof_version: version,
        };
        let (got_version, content) = verify::read(&root, &proof, verify::INLINE_ANSWER)
            .unwrap_or_else(|e| panic!("case {name} should verify: {e}"));
        assert_eq!(got_version, version, "case {name}");
        let content = content.expect("small fixture reads back inline");
        if frame_stripped {
            assert!(
                !content.contains("<<<agent-run:complete>>>"),
                "case {name}: legacy frame should be stripped"
            );
        }
    }
}

/// Mirrors `test_metadata_reads_are_bounded_and_reject_symlinks`: an
/// oversized proof sidecar and a symlinked proof sidecar are both refused
/// with a labeled error, never silently accepted or downgraded to legacy.
#[test]
fn proof_sidecar_bound_and_symlink_are_enforced() {
    let dir = tempdir();
    let root = dir.path();
    let proof = verify::seal(root, Path::new("answer.md"), "bounded").unwrap();
    let handle = Dir::open(root).unwrap();
    let sidecar_path = root.join("answer.md.proof.json");

    std::fs::write(&sidecar_path, vec![b'x'; 4097]).unwrap();
    let err = verify::load_sidecar(
        &handle,
        Path::new(""),
        "answer.md",
        proof.bytes,
        &proof.sha256,
    )
    .unwrap_err();
    assert!(err.to_string().contains("4096-byte bound"), "{err}");

    std::fs::remove_file(&sidecar_path).unwrap();
    let outside = tempdir();
    std::fs::write(outside.path().join("elsewhere.json"), b"{}").unwrap();
    std::os::unix::fs::symlink(outside.path().join("elsewhere.json"), &sidecar_path).unwrap();
    let err = verify::load_sidecar(
        &handle,
        Path::new(""),
        "answer.md",
        proof.bytes,
        &proof.sha256,
    )
    .unwrap_err();
    assert!(err.to_string().contains("regular file"), "{err}");
}

/// Mirrors `test_read_answer_payload_raises_distinct_typed_errors`: a
/// missing payload and a symlinked payload are each refused with their own
/// distinct, labeled error, never conflated with a hash/size mismatch.
#[test]
fn read_classifies_missing_and_symlinked_payloads() {
    let dir = tempdir();
    let root = dir.path();
    let proof = verify::seal(root, Path::new("answer.md"), "typed errors").unwrap();

    let missing = Proof {
        path: root.join("absent.md"),
        ..proof.clone()
    };
    let err = verify::read(root, &missing, verify::INLINE_ANSWER).unwrap_err();
    assert!(
        matches!(err, Error::AnswerIntegrity(ref m) if m.contains("is missing")),
        "{err}"
    );

    std::fs::remove_file(root.join("answer.md")).unwrap();
    std::os::unix::fs::symlink(root.join("answer.md.proof.json"), root.join("answer.md")).unwrap();
    let err = verify::read(root, &proof, verify::INLINE_ANSWER).unwrap_err();
    assert!(
        matches!(err, Error::PathEscape(ref m) if m.contains("must be a regular file")),
        "{err}"
    );
}

/// Mirrors `_DEFAULT_INLINE_ANSWER_BYTES` (`service.py:68`): the inline
/// display cutoff is 1 MiB, independent of the 16 MiB verification bound
/// (`MAX_ANSWER_PAYLOAD_BYTES`, `verify.py:53`).
#[test]
fn inline_bound_matches_python_default_and_stays_under_the_verification_bound() {
    assert_eq!(verify::INLINE_ANSWER, 1024 * 1024);
    assert_eq!(verify::MAX_ANSWER, 16 * 1024 * 1024);
}
