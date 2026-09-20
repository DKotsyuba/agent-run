# Python MCP stdio baseline

Captured on 2026-09-16 from the Python MCP CLI at reference commit
`c9904f9843ba4a0772bdfa8bac5259f18fad9dc3`, using Python 3.14.3 and isolated
temporary homes.

These fixtures are immutable historical inputs on the Rust-primary branch.
Regenerate them only from the frozen `archive/python-legacy` branch using its
documented Python environment, then import an accepted result through a
separately reviewed change.

`handshake.json` contains the four protocol revisions accepted by the Python
SDK's initialize handshake: 2024-11-05, 2025-03-26, 2025-06-18, and
2025-11-25. Each exchange records initialize, `notifications/initialized`,
`tools/list`, successful `models`, validation and unknown-tool errors, a
cancellation notification, and clean EOF. `broker-unavailable.json` records
the equivalent call with no resident broker. The real Python `api serve`
broker backed successful calls; no user home, credentials, agent task, or
answer content was read.

Dynamic capacity timestamps are retained as provenance and excluded from the
Rust parity assertions; all tested wire values are otherwise exact JSON.
