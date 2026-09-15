mod common;
use agent_run::{
    domain::{Outcome, Status},
    fs::{self, Dir},
    verify::{self, Proof},
};
use std::{os::unix::fs::symlink, path::Path};

#[test]
fn answer_roundtrip_preserves_exact_utf8_and_terminal_whitespace() {
    let h = common::Home::new();
    let text = "Привет 🦀\n  result\n\n";
    let proof = verify::seal(&h.path, Path::new("answer.md"), text).unwrap();
    assert_eq!(
        verify::read(&h.path, &proof, 1024).unwrap(),
        (2, Some(text.into()))
    );
    assert_eq!(proof.bytes, text.len() as u64);
}
#[test]
fn missing_sidecar_never_downgrades_to_legacy() {
    let h = common::Home::new();
    let text = format!("body{}", std::str::from_utf8(verify::LEGACY_FRAME).unwrap());
    let proof = verify::seal(&h.path, Path::new("answer.md"), &text).unwrap();
    std::fs::remove_file(h.path.join("answer.md.proof.json")).unwrap();
    assert!(verify::read(&h.path, &proof, 1024).is_err());
}
#[test]
fn corrupt_marker_and_sidecar_are_rejected() {
    let h = common::Home::new();
    let proof = verify::seal(&h.path, Path::new("answer.md"), "answer").unwrap();
    std::fs::write(h.path.join(".answer-format"), b"1\n").unwrap();
    assert!(verify::read(&h.path, &proof, 1024).is_err());
    std::fs::write(h.path.join(".answer-format"), b"2\n").unwrap();
    std::fs::write(h.path.join("answer.md.proof.json"), b"{}").unwrap();
    assert!(verify::read(&h.path, &proof, 1024).is_err());
}
#[test]
fn same_length_tampering_is_detected_by_hash() {
    let h = common::Home::new();
    let proof = verify::seal(&h.path, Path::new("answer.md"), "alpha").unwrap();
    std::fs::write(&proof.path, "omega").unwrap();
    assert!(verify::read(&h.path, &proof, 1024).is_err());
}
#[test]
fn legacy_frame_must_be_exactly_terminal_and_is_stripped_once() {
    let h = common::Home::new();
    let text = "before\n<<<agent-run:complete>>>\ninside\n<<<agent-run:complete>>>\n";
    let path = h.path.join("old.md");
    std::fs::write(&path, text).unwrap();
    let proof = Proof {
        path,
        bytes: text.len() as u64,
        sha256: fs::sha256(text.as_bytes()),
        proof_version: 1,
    };
    let (version, content) = verify::read(&h.path, &proof, 1024).unwrap();
    assert_eq!(version, 1);
    assert_eq!(content.unwrap(), "before\n<<<agent-run:complete>>>\ninside");
    let bytes = b"body\n<<<agent-run:complete>>>\nnot terminal";
    std::fs::write(&proof.path, bytes).unwrap();
    let proof = Proof {
        bytes: bytes.len() as u64,
        sha256: fs::sha256(bytes),
        ..proof
    };
    assert!(verify::read(&h.path, &proof, 1024).is_err());
}
#[test]
fn non_inline_answer_is_still_verified() {
    let h = common::Home::new();
    let text = "a".repeat(4096);
    let proof = verify::seal(&h.path, Path::new("answer.md"), &text).unwrap();
    assert_eq!(verify::read(&h.path, &proof, 1).unwrap(), (2, None));
    std::fs::write(&proof.path, "b".repeat(4096)).unwrap();
    assert!(verify::read(&h.path, &proof, 1).is_err());
}
#[test]
fn answer_limit_is_independent_of_inline_limit() {
    let h = common::Home::new();
    assert!(verify::seal(
        &h.path,
        Path::new("big.md"),
        &"a".repeat(verify::MAX_ANSWER + 1)
    )
    .is_err());
}
#[test]
fn payload_and_parent_symlinks_are_not_followed() {
    let h = common::Home::new();
    let external = tempfile::tempdir().unwrap();
    std::fs::write(external.path().join("payload"), b"private").unwrap();
    symlink(external.path(), h.path.join("alias")).unwrap();
    assert!(Dir::open(&h.path)
        .unwrap()
        .read(Path::new("alias/payload"), 100)
        .is_err());
    symlink(external.path().join("payload"), h.path.join("payload")).unwrap();
    assert!(Dir::open(&h.path)
        .unwrap()
        .read(Path::new("payload"), 100)
        .is_err());
}
#[test]
fn traversal_and_absolute_owned_paths_are_refused() {
    for p in ["../secret", "/etc/passwd", "a/../../b", ""] {
        assert!(fs::relative(Path::new(p)).is_err());
    }
}
#[test]
fn invalid_utf8_is_rejected_even_with_matching_legacy_hash() {
    let h = common::Home::new();
    let mut bytes = vec![0xff, 0xfe];
    bytes.extend_from_slice(verify::LEGACY_FRAME);
    let path = h.path.join("old.md");
    std::fs::write(&path, &bytes).unwrap();
    let proof = Proof {
        path,
        bytes: bytes.len() as u64,
        sha256: fs::sha256(&bytes),
        proof_version: 1,
    };
    assert!(verify::read(&h.path, &proof, 100).is_err());
}
#[test]
fn error_only_detection_does_not_classify_long_explanations() {
    assert_eq!(
        verify::error_only("You've hit your usage limit"),
        Some("quota_exhausted")
    );
    assert_eq!(
        verify::error_only(&format!(
            "Usage limit reached {}",
            "discussion ".repeat(500)
        )),
        None
    );
    assert_eq!(
        verify::error_only("The program handles the string 'rate limit exceeded'."),
        None
    );
}
#[test]
fn completion_requires_answer_and_process_cleanup() {
    let success = Outcome::success(None);
    assert_eq!(
        verify::completion(success.clone(), None, true, false).status,
        Status::Failed
    );
    assert_eq!(
        verify::completion(success.clone(), None, false, false)
            .failure_kind
            .as_deref(),
        Some("engine_group_survived")
    );
    assert_eq!(
        verify::completion(success, None, true, true).status,
        Status::Cancelled
    );
}
#[test]
fn atomic_write_replaces_a_link_without_following_it() {
    let h = common::Home::new();
    let external = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(external.path(), b"keep").unwrap();
    symlink(external.path(), h.path.join("output")).unwrap();
    Dir::open(&h.path)
        .unwrap()
        .write(Path::new("output"), b"new", 0o600)
        .unwrap();
    assert_eq!(std::fs::read(external.path()).unwrap(), b"keep");
    assert_eq!(std::fs::read(h.path.join("output")).unwrap(), b"new");
}
#[test]
fn canonical_json_sorts_nested_maps_independently_of_insertion_order() {
    let a: serde_json::Value = serde_json::from_str(r#"{"z":1,"a":{"y":2,"x":3}}"#).unwrap();
    let b: serde_json::Value = serde_json::from_str(r#"{"a":{"x":3,"y":2},"z":1}"#).unwrap();
    assert_eq!(
        fs::canonical_json(&a).unwrap(),
        fs::canonical_json(&b).unwrap()
    );
}
