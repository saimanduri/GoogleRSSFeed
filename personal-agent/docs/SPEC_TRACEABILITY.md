# Spec traceability

Spec: `docs/spec/personal_desktop_agent_spec_v1_1.txt`. Status per item: see PROGRESS.md. Deviations: DEVIATIONS.md.

| Spec | Requirement (short) | Code | Tests |
|---|---|---|---|
| 0 P1-P8 | LLM proposes, gateway decides, default deny, data-flow, re-check, audit-or-refuse, secrets never exposed | `tools/gateway.py`, `policy/engine.py`, `audit/writer.py` | `test_policy.py`, `test_agent_security.py`, red team |
| 0 P9 | Security floor | `settings_schema.py` (floor tags, no floor settings), `policy/engine.py` | `test_settings_rules.py` |
| 0 P10 | Missing isolation disables capability | `sandbox.py` (UNAVAILABLE), posture | `test_policy.py::test_python_needs_sandbox` |
| 0 P11 | No admin at runtime | installer only elevated step; `posture_fix repair_firewall` re-elevates only the script | laptop B |
| 2.1-2.3 | Process split | `app.py::CoreSupervisor`, `workers.py`, `pa_workers/*` | `test_selftest_over_named_pipes_with_core_process` |
| 2.4 | IPC security | `ipc/pipe_server.py`, `pa_common/pipeclient.py`, `ipc/dispatch.py` | `tests/windows/*pipe*` |
| 2.5 | Install layout, ACL, cloud-folder refusal, BitLocker warning | `app.py::secure_data_folder`, `paths.looks_cloud_synced`, `posture.py` | `test_data_folder_acl` |
| 4.1-4.8 | Identity, keys, flows, protections | `vault/*`, `auth/*`, `app.py`, `winsession.py` | `test_vault.py`, `test_auth_flows.py` |
| 5.1-5.3 | Single UI + Settings | `app/ui/src/**`, `settings_schema.py` | UI build (CI), screenshots |
| 5.4-5.5 | Floor + tighten/loosen rules | `settings.py`, `LoosenDialog.tsx` | `test_settings_rules.py` |
| 6 | Secrets vault | `secrets_store.py`, `dlp/dlp.py`, `Secrets.tsx` | `test_step_up_required_for_secret_reveal`, `test_secret_never_reaches_model_context`, `test_secret_export_encrypted`, clipboard test |
| 7.1-7.3 | Tool definitions, invocation sequence, policy | `policy/tools_registry.py`, `tools/gateway.py`, `policy/engine.py` | `test_policy.py`, `test_agent_security.py` |
| 8 | Connector framework | `connectors/service.py` | `test_connector_off_denies_and_no_alternate_path`, `test_connector_toggle_and_mission_suspended` |
| 9 | Local Outlook | `connectors/outlook_local.py`, `pa_workers/outlook` | laptop D1-D3 |
| 10 | Microsoft 365 | `connectors/m365.py` | laptop D4-D6 |
| 11 | Web search/fetch | `connectors/web.py`, `egress/http.py`, `pa_common/html_text.py` | `test_egress.py` |
| 12 | Prompt-injection defence | `tools/injection.py`, `pa_core/prompts.py`, labelling | `test_injection_in_tool_output_*`, red team |
| 13 | Sensitivity, propagation, high-water mark | `pa_common/sensitivity.py`, `TaskService.raise_hwm`, runs/chats hwm | `test_confidential_context_egress_*`, policy table tests |
| 14 | Egress + DLP | `egress/http.py`, `dlp/dlp.py`, `tools/gateway.py::_dlp` | `test_dlp.py`, `test_egress.py` |
| 15 | File pipeline | `files/checks.py`, `files/store.py`, `pa_workers/parser` | `test_files_backup_misc.py` |
| 16 | Missions, triggers, scheduling | `agentdata/missions.py`, `agentdata/schedule.py`, `app.py::_scheduler_loop` | `test_cron_and_plain_words`, `test_missed_run_policy`, `test_external_event_task_cannot_egress` |
| 17 | Tasks, durability, idempotency | `agentdata/tasks.py`, outbox | `test_outcome_unknown_never_retried`, recovery |
| 18 | Budgets | `budgets.py` | `test_budget_stops_runaway` |
| 19 | Sandbox | `sandbox.py`, `pa_workers/sandbox/appcontainer.py` | `test_appcontainer_has_no_network`, laptop B5 |
| 20 | Local LLM runtime | `llm/*` | selftest, laptop B7/C1-C3 |
| 21 | Memory | `agentdata/memory.py` | `test_memory_trust`, `test_injection_*` |
| 22 | Skills | `agentdata/skills.py` | `test_skill_rules` |
| 23 | Approvals | `approvals.py`, `ApprovalCard.tsx` | `test_approval_payload_binding_and_single_use`, `test_edit_and_repropose` |
| 24 | Kill switch | `killswitch.py`, `app.py::_on_killswitch`, `src-tauri/src/main.rs` | `test_kill_switch_blocks_within_2s` |
| 25 | Unified log | `audit/*` | `test_audit.py` |
| 26 | Notifications | `agentdata/home.py`, Tauri toasts | `test_reminder_fires`, laptop C5 |
| 27 | Backup & restore | `backup.py` | `test_backup_restore_roundtrip`, `test_backup_tamper_detected` |
| 28 | Retention & deletion | `files/store.py::delete`, `app.py::_daily`, `delete_everything` | `test_history_search_excludes_deleted`, `test_delete_everything` |
| 29 | Updates & supply chain | `backup.snapshot_for_update` | pending (P1) |
| 30 | Resilience & diagnostics | `CoreSupervisor` backoff, M365 circuit breaker, `diagnostics.*` | selftest |
| 31/32 | Security tests, red team | `tests/**`, `tests/redteam/corpus.json` | CI (N=20) |
| 39.1 | Listener hardening | `pipe_server.py`, `llm/runtime.py`, `llm/service.py::exposure_check`, m365 loopback | Windows pipe tests, laptop B7 |
| 39.2 | No configuration via links | `api_ui.py::ui_open_link` | `test_link_cannot_change_anything` |
| 39.3 | Per-client method allowlists | `ipc/dispatch.py`, `api_core.py` | `test_core_method_allowlist`, red team |
| 39.4 | Extension security | `extensions.py`, `agentdata/skills.py` | `test_skill_rules` |
| 39.5 | Session event log | `agentdata/sessionlog.py`, `pa_core/context.py`, `llm/service.py` | `test_unlogged_context_is_rejected`, `test_session_log_chain` |
| 39.6 | Governed skill evolution | `agentdata/skills.py` | `test_skill_rules` |
| 39.7 | Safe summarising | `pa_core/context.py`, `api_core.session_append` | unit-level via selftest; laptop |
| 39.8 | Sub-agents | `tools/builtin.py::subtask` | policy/hwm tests |
| 39.9 | Scripts that call tools | setting only | pending (P3) |
| 39.10 | Security Posture page | `posture.py`, `AccountSettings.tsx` | `test_posture_runs` |
| 39.11 | Safe updates | snapshot helper | pending (P1) |
| 39.12 | History search | `agentdata/history.py` | `test_history_search_excludes_deleted` |
| 39.13 | Memory review / About me | `agentdata/memory.py`, `Memory.tsx` | `test_memory_trust` |
| 39.14 | Approval cards in chat | `ApprovalCard.tsx`, `Chat.tsx` | selftest, screenshots |
| 39.15 | Missions in plain words | `schedule.parse_plain`, `MissionService.describe_to_form` | `test_cron_and_plain_words` |
| User | Step timeline of last 100 requests, archive + search | `agentdata/runs.py`, `RunTimeline.tsx`, `Activity.tsx` | `test_chat_round_trip_and_steps` |
| User | Voice (STT via Ollama/vLLM/Run:ai-compatible endpoint) + reminders after confirmation | `llm/service.py::transcribe`, `VoiceButton.tsx`, `agentdata/reminders.py` | selftest (reminder), laptop C4/C5 |
| User | Any LLM (Ollama / vLLM / Run:ai / built-in), add/modify any time | `llm/service.py`, `ModelSettings.tsx` | laptop C1-C3 |
| User | Routines like Claude Routines | `agentdata/missions.py` (kind=routine), `Missions.tsx` | mission tests |
