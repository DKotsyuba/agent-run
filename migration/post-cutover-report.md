# Post-cutover report template

This template records a production switch to a verified Rust release. Copy it
to a new dated evidence file and fill every field; do not edit historical
evidence in place.

## Identity

| Field | Value |
|---|---|
| Date/time and operator | |
| Source commit | |
| Release version | |
| Target triple | |
| Binary SHA-256 | |
| Sealed release path | |
| Previous release | |
| Deployment journal | |
| Database backup | |

## Pre-switch

- [ ] manifest and `COMPLETE` verified
- [ ] schema compatibility checked
- [ ] no active agents or workflow writers
- [ ] previous release and backup are readable

## Observed result

| Check | Evidence | Result |
|---|---|---|
| service manager points to the Rust release | | |
| API `ping` | | |
| MCP initialize and `tools/list` | | |
| broker-backed start and answer proof | | |
| supported-engine canary, if required | | |
| doctor | | |
| process cleanup and socket lifecycle | | |

## Rollback readiness

- [ ] old release is compatible with the current schema
- [ ] `cargo xtask release rollback` or explicit restore command is recorded
- [ ] restore smoke and owner decision are recorded if rollback occurred

## Verdict

`accepted` / `rolled back` / `needs follow-up`:

Unresolved items:
