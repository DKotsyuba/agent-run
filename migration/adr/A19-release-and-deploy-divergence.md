# A19 — Native release publication and deployment boundaries

Status: accepted and implemented for the Rust-primary release.

## Decision

Release publication and installation are separate boundaries:

1. GitHub Actions builds locked native artifacts for the declared target,
   verifies the sealed directory, packages it, publishes checksums and
   provenance/attestation, and creates the GitHub Release only after all gates
   pass. GitHub is the public distribution channel.
2. `cargo xtask release build-native` and `verify` create and validate immutable
   local release directories containing the binary, metadata, `SHA256SUMS`, and
   `COMPLETE`.
3. `cargo xtask release install`, `update`, `recover`, `roll-forward`, and
   `rollback` operate on an explicit local prefix/home. They verify the candidate,
   schema compatibility, quiescence, backup, journal, and atomic current-pointer
   switch. They do not publish to GitHub.

Python wheel/venv installation and the old monolithic publication script are not
part of the Rust product. Their historical behavior remains on
`archive/python-legacy` and is not a parity requirement for 0.12.x.

## Consequences

- A green GitHub publication workflow does not prove a production cutover.
- A successful local deployment does not authorize or create a GitHub release.
- Linux publication must remain labelled unqualified until the evidence required
  by [A15](A15-platforms.md) exists.
- Operator commands and public artifact policy live in
  [docs/releasing.md](../../docs/releasing.md); migration acceptance remains in
  [review-checklist.md](../review-checklist.md).
