//! Migration evidence inventory verification coverage for M56.

use sha2::Digest;
use std::fs;
use tempfile::tempdir;
use xtask::evidence;

/// Verifies a complete small inventory and ignores its self-referential index.
#[test]
fn evidence_index_round_trip() {
    // Rust-only tooling has no Python counterpart and therefore no Mirrors citation.
    let temporary = tempdir().expect("temporary root");
    let evidence_directory = temporary.path().join("migration/evidence");
    let adr_directory = temporary.path().join("migration/adr");
    fs::create_dir_all(&evidence_directory).expect("evidence directory");
    fs::create_dir_all(&adr_directory).expect("ADR directory");
    let evidence = b"proof\n";
    let decision = b"decision\n";
    fs::write(evidence_directory.join("proof.md"), evidence).expect("evidence file");
    fs::write(adr_directory.join("A1.md"), decision).expect("ADR file");
    let index = serde_json::json!({
        "index_version": 1,
        "generated_for_commit": "a".repeat(40),
        "generated_at": "2026-09-16T00:00:00Z",
        "entry_count": 2,
        "entries": [
            {
                "path": "migration/evidence/proof.md",
                "kind": "evidence",
                "bytes": evidence.len(),
                "sha256": format!("{:x}", sha2::Sha256::digest(evidence))
            },
            {
                "path": "migration/adr/A1.md",
                "kind": "decision",
                "bytes": decision.len(),
                "sha256": format!("{:x}", sha2::Sha256::digest(decision))
            }
        ]
    });
    fs::write(
        evidence_directory.join("index.json"),
        serde_json::to_vec(&index).expect("index JSON"),
    )
    .expect("index file");
    assert_eq!(evidence::verify(temporary.path()).expect("valid index"), 2);
}
