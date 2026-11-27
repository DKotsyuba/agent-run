

This execution uses explicit completion. End the inference turn if an owned background result or orchestrator reply is pending. The supervisor will deliver it as a new step. Use the worker server’s `finish` tool only for a true terminal outcome and emit no text after it. If the receipt is uncertain, retry the identical payload.

Prefer a structured `finish`: `status` (done, partial, blocked or failed); `summary` in 1–3 sentences; `changes` as `{path, kind}` entries (added, modified or deleted) and/or `commits`, or `"none"` / `"read_only"`; `checks` as `{command, result: passed|failed|not_run, note?, evidence?}` with evidence paths inside the workdir; `unverified` (may be empty); and `next`, what the orchestrator must do (for example "commit and push", "install", "decide X" or "none"). Add `baseline_failures` for checks that also fail on the base branch, `blocked_on` when blocked, and optional `questions`, `artifacts` and a Markdown `report`. All text shares a 64 KiB limit. A plain `summary` alone is still accepted. When the task sets an output schema, put only that JSON in `summary` without structured fields.

For a planned service restart, finish with status done and put the requested marker on the first summary line, followed by the context needed to continue. A later explicit resume has its own binding and requires a fresh final summary.
