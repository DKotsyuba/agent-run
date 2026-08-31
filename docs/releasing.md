# Releasing agent-run

The canonical package version is `[project].version` in `pyproject.toml`.
Releases use annotated Semantic Versioning tags (`vX.Y.Z`); the release workflow
refuses a tag that does not match the package version.

## Prepare a version

1. Update `pyproject.toml` and `CHANGELOG.md` in the same pull request.
2. Run the full test suite and build checks locally.
3. Merge only after the `CI` workflow passes.
4. Create and push an annotated tag from the accepted `main` commit:

   ```bash
   git tag -a v0.1.0 -m "agent-run 0.1.0"
   git push origin v0.1.0
   ```

The tag-triggered `Release` workflow repeats the tests, builds wheel and sdist,
runs package/install smokes, writes `SHA256SUMS`, creates GitHub provenance
attestations, creates a draft GitHub Release, and publishes it only after every
gate succeeds. A failed run leaves no public partial release.

Verify downloaded artifacts with:

```bash
# Linux
sha256sum -c SHA256SUMS
# macOS
shasum -a 256 -c SHA256SUMS
gh attestation verify agent_run-0.1.0-py3-none-any.whl \
  --repo DKotsyuba/agent-run
```

## Distribution boundary

GitHub Releases is the only public distribution channel. Each release carries
the Python wheel, source archive, checksums, and GitHub provenance attestation.
The project does not publish to PyPI, GitHub Packages, Docker, or GHCR.

## Repository settings

Require the `CI` checks on `main`, require pull requests, enable private
vulnerability reporting, retain read-only default workflow-token permissions,
and require immutable action references. These settings live on GitHub and are
reviewed separately from repository code.
