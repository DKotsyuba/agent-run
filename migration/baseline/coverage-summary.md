# Python-to-Rust coverage summary

Generated from Python baseline `c9904f9843ba4a0772bdfa8bac5259f18fad9dc3`; no live engine evidence is implied.

## Baseline files by kind

| kind | count |
| --- | ---: |
| asset | 3 |
| ci | 3 |
| doc | 8 |
| operator-doc | 9 |
| other | 9 |
| packaging | 5 |
| python-src | 111 |
| python-test | 84 |
| script | 2 |
| sql | 16 |
| test-fixture | 3 |

## Test status

| status | count |
| --- | ---: |
| ported | 73 |
| planned | 1074 |
| unassigned | 0 |

## Test coverage by Python file

| Python file | total | ported | planned | unassigned |
| --- | ---: | ---: | ---: | ---: |
| tests/test_adapter_environment.py | 3 | 0 | 3 | 0 |
| tests/test_adapter_home.py | 10 | 0 | 10 | 0 |
| tests/test_adapter_versions.py | 7 | 0 | 7 | 0 |
| tests/test_adapters_base.py | 7 | 0 | 7 | 0 |
| tests/test_answer_payload_proof.py | 25 | 18 | 7 | 0 |
| tests/test_api_socket.py | 24 | 2 | 22 | 0 |
| tests/test_bind_hook.py | 6 | 0 | 6 | 0 |
| tests/test_broker_client.py | 8 | 0 | 8 | 0 |
| tests/test_capacity_advice.py | 6 | 0 | 6 | 0 |
| tests/test_capacity_cli.py | 4 | 0 | 4 | 0 |
| tests/test_capacity_codex_appserver.py | 20 | 3 | 17 | 0 |
| tests/test_capacity_collect.py | 5 | 0 | 5 | 0 |
| tests/test_capacity_diagnostics.py | 1 | 0 | 1 | 0 |
| tests/test_capacity_forecast.py | 15 | 4 | 11 | 0 |
| tests/test_capacity_identity.py | 5 | 2 | 3 | 0 |
| tests/test_capacity_launchd.py | 4 | 0 | 4 | 0 |
| tests/test_capacity_oauth_refresh.py | 3 | 0 | 3 | 0 |
| tests/test_capacity_order.py | 2 | 0 | 2 | 0 |
| tests/test_capacity_outcomes.py | 7 | 1 | 6 | 0 |
| tests/test_capacity_ranking.py | 12 | 3 | 9 | 0 |
| tests/test_capacity_reset_identity.py | 10 | 3 | 7 | 0 |
| tests/test_capacity_service.py | 2 | 0 | 2 | 0 |
| tests/test_capacity_snapshot.py | 6 | 2 | 4 | 0 |
| tests/test_capacity_sources.py | 24 | 1 | 23 | 0 |
| tests/test_capacity_topology.py | 14 | 1 | 13 | 0 |
| tests/test_ci.py | 2 | 0 | 2 | 0 |
| tests/test_claude_adapter.py | 53 | 0 | 53 | 0 |
| tests/test_claude_developer_environment.py | 4 | 0 | 4 | 0 |
| tests/test_claude_session.py | 23 | 0 | 23 | 0 |
| tests/test_claude_stream.py | 15 | 0 | 15 | 0 |
| tests/test_claude_uds.py | 13 | 0 | 13 | 0 |
| tests/test_cli.py | 35 | 1 | 34 | 0 |
| tests/test_codex_adapter.py | 74 | 0 | 74 | 0 |
| tests/test_codex_app_server.py | 64 | 0 | 64 | 0 |
| tests/test_codex_desktop_relay.py | 19 | 0 | 19 | 0 |
| tests/test_codex_environment.py | 10 | 0 | 10 | 0 |
| tests/test_codex_permission_request.py | 3 | 0 | 3 | 0 |
| tests/test_codex_queue.py | 16 | 0 | 16 | 0 |
| tests/test_command_policy.py | 7 | 0 | 7 | 0 |
| tests/test_config.py | 24 | 6 | 18 | 0 |
| tests/test_context_hook.py | 5 | 0 | 5 | 0 |
| tests/test_delivery_base.py | 13 | 0 | 13 | 0 |
| tests/test_delivery_dispatch.py | 22 | 0 | 22 | 0 |
| tests/test_dispatch.py | 8 | 1 | 7 | 0 |
| tests/test_doc.py | 12 | 0 | 12 | 0 |
| tests/test_doctor.py | 25 | 0 | 25 | 0 |
| tests/test_domain.py | 5 | 2 | 3 | 0 |
| tests/test_effective_policy.py | 7 | 0 | 7 | 0 |
| tests/test_glm_adapter.py | 17 | 0 | 17 | 0 |
| tests/test_launch.py | 20 | 1 | 19 | 0 |
| tests/test_launch_reaper.py | 1 | 0 | 1 | 0 |
| tests/test_lifecycle.py | 18 | 0 | 18 | 0 |
| tests/test_logging_setup.py | 8 | 0 | 8 | 0 |
| tests/test_m008_integration.py | 3 | 0 | 3 | 0 |
| tests/test_mcp.py | 8 | 0 | 8 | 0 |
| tests/test_native_settings.py | 28 | 4 | 24 | 0 |
| tests/test_omniroute_current_cache.py | 13 | 0 | 13 | 0 |
| tests/test_paths.py | 4 | 0 | 4 | 0 |
| tests/test_plugin_integration.py | 13 | 0 | 13 | 0 |
| tests/test_preparation.py | 2 | 0 | 2 | 0 |
| tests/test_priority_context_regressions.py | 12 | 0 | 12 | 0 |
| tests/test_process_identity.py | 2 | 0 | 2 | 0 |
| tests/test_profiles.py | 5 | 4 | 1 | 0 |
| tests/test_qwen_adapter.py | 25 | 0 | 25 | 0 |
| tests/test_reconciliation.py | 10 | 0 | 10 | 0 |
| tests/test_release_script.py | 35 | 0 | 35 | 0 |
| tests/test_resume.py | 33 | 0 | 33 | 0 |
| tests/test_resume_adapters.py | 5 | 0 | 5 | 0 |
| tests/test_role_plan.py | 4 | 1 | 3 | 0 |
| tests/test_run_stats.py | 10 | 0 | 10 | 0 |
| tests/test_service.py | 36 | 0 | 36 | 0 |
| tests/test_snapshots.py | 10 | 0 | 10 | 0 |
| tests/test_state_db.py | 6 | 5 | 1 | 0 |
| tests/test_state_migrations.py | 19 | 2 | 17 | 0 |
| tests/test_state_outbox.py | 14 | 1 | 13 | 0 |
| tests/test_state_store.py | 22 | 2 | 20 | 0 |
| tests/test_supervisor.py | 42 | 0 | 42 | 0 |
| tests/test_supervisor_main.py | 6 | 0 | 6 | 0 |
| tests/test_verify.py | 21 | 3 | 18 | 0 |
| tests/test_wait.py | 6 | 0 | 6 | 0 |

## Test coverage by lane

| lane | status | rows |
| --- | --- | ---: |
| baseline/control | ported | 1 |
| baseline/control | planned | 67 |
| baseline/control | unassigned | 0 |
| capacity | ported | 100 |
| capacity | planned | 600 |
| capacity | unassigned | 0 |
| claude/glm/qwen adapters | ported | 0 |
| claude/glm/qwen adapters | planned | 389 |
| claude/glm/qwen adapters | unassigned | 0 |
| codex adapter | ported | 0 |
| codex adapter | planned | 1174 |
| codex adapter | unassigned | 0 |
| config/roles | ported | 29 |
| config/roles | planned | 166 |
| config/roles | unassigned | 0 |
| core lifecycle | ported | 23 |
| core lifecycle | planned | 339 |
| core lifecycle | unassigned | 0 |
| delivery/hooks | ported | 2 |
| delivery/hooks | planned | 328 |
| delivery/hooks | unassigned | 0 |
| operations/release | ported | 0 |
| operations/release | planned | 117 |
| operations/release | unassigned | 0 |
| platform/artifacts | ported | 22 |
| platform/artifacts | planned | 108 |
| platform/artifacts | unassigned | 0 |
| store | ported | 67 |
| store | planned | 520 |
| store | unassigned | 0 |
| transports/CLI | ported | 4 |
| transports/CLI | planned | 76 |
| transports/CLI | unassigned | 0 |

## Unassigned files and tests

- Files: none.
- Tests: none.

## Proposed board additions

No additions proposed; every baseline source/test row has an owner task.
