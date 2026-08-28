# Qwen Code 0.22.2 probe

- Headless output: `qwen -p PROMPT --output-format stream-json --approval-mode MODE`. Output is JSONL: `system/init`, optional `stream_event`, `assistant` (text at `message.content[].text`, usage at `message.usage`), then `result` (final text at `result`, totals at `usage.input_tokens`, `usage.output_tokens`, and cached input at `usage.cache_read_input_tokens`).
- Permissions: `plan` exposes read-only tools and prevents edits; `auto-edit` permits edits. An explicit mode is mandatory for every adapter launch. `yolo` is deliberately not used.
- Provider: `OPENAI_BASE_URL`, `OPENAI_API_KEY`, and `OPENAI_MODEL` selected an arbitrary OpenAI-compatible endpoint and model in `-p` mode without `/auth` (verified against an intentionally unreachable loopback endpoint; init reported the supplied model).
- MCP/context: Qwen settings use `mcpServers` for stdio servers and `context.fileName` for the context contract. The adapter gives the context file a per-run absolute name, so ambient `QWEN.md` is not selected.
- Sandbox: `--sandbox` invokes macOS Seatbelt. With a throwaway `HOME`, 0.22.2 failed to relaunch because `getconf DARWIN_USER_CACHE_DIR` failed in this managed sandbox. The adapter still always passes `--sandbox`; no write mode disables it.
- Hooks: 0.22.2 exposes the hook manager, but its CLI did not document a stable settings schema for command hook executors. TODO: wire `lsp_guard` after the hook settings schema is confirmed rather than emitting guessed configuration.
- Exit/cancel: provider connection failure still emitted a terminal `result` and exited 0. Agent-run therefore classifies the JSON result, not only the exit code; cancellation uses the existing process-group SIGINT/SIGKILL one-shot session behavior.

