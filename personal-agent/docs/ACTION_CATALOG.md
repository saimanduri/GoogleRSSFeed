# Action catalogue (generated - do not edit; run `python scripts/gen_action_catalog.py`)

161 backend actions (RPC methods): **150 covered by automated scenarios**, 11 manual-only (reason given), **0 gaps**. UI calls to unknown RPCs: none.

Columns: state = required session state, step-up = re-auth category, UI = where the UI calls it, Scenarios = ids in tests/e2e/scenarios.json.

| RPC | State | Step-up | Params (* = required) | UI callers | Scenarios / manual reason |
|---|---|---|---|---|---|
| `about` | any | - | - | - | meta.about |
| `account.change_password` | unlocked | - | current*, new* | screens/settings/AccountSettings.tsx:67 | account.change_password |
| `account.change_username` | unlocked | - | password*, username* | screens/settings/AccountSettings.tsx:66 | account.username |
| `account.new_recovery_key` | unlocked | - | password*, pin* | screens/settings/AccountSettings.tsx:70 | account.new_recovery_key |
| `account.set_pin` | unlocked | - | password*, pin*, recovery_key | screens/settings/AccountSettings.tsx:68 | account.set_pin |
| `account.set_profile` | unlocked | - | display_name*, assistant_name* | screens/settings/AccountSettings.tsx:29 | account.profile |
| `account.signin_history` | unlocked | - | - | screens/settings/AccountSettings.tsx:99 | account.signin_history |
| `activity.events` | unlocked | - | filters*, limit, since, until | screens/Activity.tsx:164 | activity.events |
| `activity.what_did_agent_do` | unlocked | - | since*, until* | screens/Activity.tsx:204 | activity.what_did_agent_do |
| `approvals.decide` | unlocked | - | approval_id*, approve, edited_payload*, payload_hash*, opened_at_ms | components/ApprovalCard.tsx:23 | approvals.bad_hash, approvals.approve, approvals.single_use |
| `approvals.list` | unlocked | - | status | screens/Approvals.tsx:13 | approvals.pending, approvals.history |
| `auth.forgot_password` | any | - | pin*, recovery_key*, new_password* | screens/SignIn.tsx:105 | auth.forgot_password |
| `auth.lock` | any | - | reason | screens/Chat.tsx:213, Shell.tsx:41, Shell.tsx:103 … | auth.lock |
| `auth.quick_unlock` | any | - | pin* | screens/SignIn.tsx:34 | auth.quick_unlock |
| `auth.sign_in` | any | - | username*, password* | screens/SignIn.tsx:35 | auth.sign_in_wrong, auth.sign_in, auth.sign_in_after_reset |
| `auth.sign_out` | any | - | reason | - | auth.sign_out |
| `auth.step_up` | unlocked | - | category*, method*, secret* | app.tsx:229, screens/Onboarding.tsx:299 | auth.stepup_password, auth.stepup_pin |
| `backup.enable_schedule` | unlocked | - | password* | screens/Onboarding.tsx:301, screens/settings/Misc.tsx:25 | backup.enable_schedule |
| `backup.restore` | unlocked | backup_restore | path*, password* | screens/settings/Misc.tsx:31, screens/settings/Misc.tsx:35 | manual: destructive: replaces the signed-in vault; covered by tests/integration/test_files_backup_misc.py (restore to a new PC) and checklist F1 |
| `backup.restore_signed_out` | any | - | path*, password* | screens/SignIn.tsx:147 | manual: needs an empty data folder (new PC); covered by test_files_backup_misc.py::test_backup_restore_new_pc and checklist F1 |
| `backup.run_now` | unlocked | - | folder, password* | screens/settings/Misc.tsx:24 | backup.run_now, backup.wrong_password |
| `backup.status` | unlocked | - | - | screens/settings/Misc.tsx:12 | backup.status |
| `backup.verify` | unlocked | - | path*, password* | screens/settings/Misc.tsx:30 | backup.verify |
| `chat.create` | unlocked | - | title, allow_tools | screens/Chat.tsx:137, screens/Chat.tsx:233 | chat.create, approvals.create_via_chat |
| `chat.delete` | unlocked | - | chat_id* | screens/Chat.tsx:310 | chat.delete |
| `chat.get` | unlocked | - | chat_id* | screens/Chat.tsx:79 | chat.reply_arrives, chat.canary_never_returned |
| `chat.list` | unlocked | - | archived | screens/Chat.tsx:78, screens/History.tsx:29 | auth.locked_blocks, chat.list_pinned, chat.list_archived, chat.deleted_gone |
| `chat.send` | unlocked | - | chat_id*, text*, voice | screens/Chat.tsx:238 | chat.send, approvals.ask, killswitch.chat_blocked |
| `chat.update` | unlocked | - | chat_id*, title, allow_tools, archived, pinned, folder | screens/Chat.tsx:83, screens/Chat.tsx:215, screens/Chat.tsx:277 … | chat.update, chat.update_bad_folder, chat.archive |
| `connectors.disconnect` | unlocked | connectors | connector*, delete_data | screens/settings/ConnectorSettings.tsx:35 | connectors.disconnect_web |
| `connectors.list` | unlocked | - | - | screens/Missions.tsx:82, screens/Onboarding.tsx:243, screens/settings/ConnectorSettings.tsx:9 | connectors.list |
| `connectors.m365_sign_in` | unlocked | connectors | custom_scheme | - | manual: opens the browser and needs a Microsoft tenant: checklist D4 |
| `connectors.pause_all` | unlocked | - | paused | screens/settings/ConnectorSettings.tsx:17 | connectors.pause_all, connectors.resume_all |
| `connectors.set` | unlocked | - | connector* | screens/Onboarding.tsx:253, screens/settings/ConnectorSettings.tsx:11 | connectors.enable_web, connectors.disable_web |
| `diagnostics.bundle` | unlocked | - | path | screens/settings/Misc.tsx:106, screens/settings/Misc.tsx:110 | diagnostics.bundle |
| `diagnostics.health` | unlocked | - | - | screens/settings/Misc.tsx:93 | diagnostics.health |
| `diagnostics.resources` | unlocked | - | - | components/ResourceMeters.tsx:14 | diagnostics.resources |
| `emailskills.list` | unlocked | - | mark_seen | components/EmailSkillList.tsx:18 | emailskills.list |
| `emailskills.run` | unlocked | - | skill* | components/EmailSkillList.tsx:46 | emailskills.run_needs_setup |
| `emailskills.set` | unlocked | - | skill*, enabled | components/EmailSkillList.tsx:41, screens/settings/EmailMonitoring.tsx:21 | emailskills.set_needs_setup, emailskills.set_off |
| `files.analyse` | unlocked | - | file_id* | components/FileMeta.tsx:19 | files.analyse |
| `files.delete` | unlocked | - | file_id* | screens/Files.tsx:114 | files.delete |
| `files.list` | unlocked | - | folder, query | screens/Files.tsx:17 | files.ready |
| `files.meta_update` | unlocked | - | file_id*, title*, doc_type, summary, keywords* | components/FileMeta.tsx:17 | files.meta_update, files.meta_update_rejects_ids |
| `files.preview` | unlocked | - | file_id* | components/FileMeta.tsx:13, screens/Files.tsx:68 | files.preview |
| `files.release_unscanned` | unlocked | security_settings | file_id* | screens/Files.tsx:41 | files.release_unscanned |
| `files.reread` | unlocked | - | file_id* | screens/Files.tsx:112 | files.reread |
| `files.save_copy` | unlocked | - | file_id*, path* | screens/Files.tsx:113 | files.save_copy |
| `files.set_label` | unlocked | - | file_id*, level* | screens/Files.tsx:102 | files.label |
| `files.update` | unlocked | - | file_id*, folder, tags*, in_knowledge, name | screens/Files.tsx:105, screens/Files.tsx:107, screens/Files.tsx:109 | files.update |
| `files.upload` | unlocked | - | path, name*, sensitivity, folder, tags* | screens/Chat.tsx:184, screens/Files.tsx:22, screens/Files.tsx:33 | files.upload, files.missing_upload |
| `history.search` | unlocked | - | query*, limit, kinds* | screens/Activity.tsx:221, screens/History.tsx:41, Shell.tsx:167 | history.search |
| `home.dismiss` | unlocked | - | id* | screens/Home.tsx:75 | home.dismiss |
| `home.summary` | unlocked | - | since | components/UsageMeter.tsx:9, screens/Activity.tsx:14, screens/Home.tsx:88 | home.summary |
| `home.widgets` | unlocked | - | refresh | screens/Home.tsx:87 | home.widgets |
| `home.widgets_set` | unlocked | - | enabled* | screens/Home.tsx:97 | home.widgets_set |
| `killswitch.activate` | keys | - | level*, source | screens/settings/Misc.tsx:48, screens/SignIn.tsx:79, Shell.tsx:62 | killswitch.activate, killswitch.partial, killswitch.bad_level |
| `killswitch.release` | unlocked | - | level | screens/settings/Misc.tsx:49 | killswitch.release_needs_password, killswitch.release, killswitch.partial_release |
| `killswitch.state` | unlocked | - | - | - | killswitch.state |
| `llm.add` | unlocked | - | model*, make_default, auto_test | screens/settings/ModelSettings.tsx:158 | llm.add_mock, llm.add_second, llm.remote_needs_password |
| `llm.complete` | keys | - | task_id*, messages*, role, json_mode, action_schema, max_tokens, stream, purpose | - | manual: core-role RPC (pa-core only): exercised by every chat.send; UI role is refused (tests/integration/test_agent_security.py) |
| `llm.discover` | unlocked | - | provider*, endpoint*, api_key | screens/settings/ModelSettings.tsx:183 | llm.discover_ollama |
| `llm.inspect` | unlocked | - | provider*, endpoint*, model_name*, api_key | screens/settings/ModelSettings.tsx:148 | llm.inspect_ollama |
| `llm.models` | unlocked | - | - | screens/settings/ModelSettings.tsx:29 | llm.models_empty |
| `llm.remove` | unlocked | - | model_id* | screens/settings/ModelSettings.tsx:61 | llm.remove |
| `llm.set_role` | unlocked | - | role*, model_id | screens/settings/ModelSettings.tsx:65 | llm.set_role |
| `llm.test` | unlocked | - | model_id* | screens/settings/ModelSettings.tsx:42 | llm.test_mock |
| `llm.update` | unlocked | - | model_id* | screens/settings/ModelSettings.tsx:104 | llm.update_params, llm.update_params_reset |
| `localfiles.allow_subfolders` | unlocked | - | grant_id*, confirm | components/LocalShare.tsx:88 | localfiles.allow_subfolders_unconfirmed, localfiles.allow_subfolders |
| `localfiles.deny_request` | unlocked | - | request_id* | components/LocalShare.tsx:89 | localfiles.deny_request |
| `localfiles.folder_info` | unlocked | - | path* | components/LocalShare.tsx:17 | localfiles.folder_info |
| `localfiles.grant` | unlocked | - | chat_id*, path*, sensitivity | screens/Chat.tsx:163 | localfiles.grant, localfiles.grant_blocked |
| `localfiles.grant_folder` | unlocked | - | chat_id*, path*, include_subfolders, confirm_subfolders, sensitivity | components/LocalShare.tsx:22 | localfiles.grant_folder_subfolders_unconfirmed, localfiles.grant_folder, localfiles.grant_folder_drive_root |
| `localfiles.list` | unlocked | - | chat_id* | screens/Chat.tsx:156 | localfiles.list |
| `localfiles.reapprove` | unlocked | - | grant_id*, include_subfolders, confirm_subfolders | components/LocalShare.tsx:21, components/LocalShare.tsx:54 | localfiles.reapprove |
| `localfiles.requests` | unlocked | - | chat_id* | components/LocalShare.tsx:78 | localfiles.requests |
| `localfiles.revoke` | unlocked | - | grant_id* | components/LocalShare.tsx:66 | localfiles.revoke |
| `logs.export` | unlocked | export | path* | screens/settings/Misc.tsx:130 | logs.export |
| `logs.rotate` | unlocked | - | - | - | logs.rotate |
| `logs.siem_test` | unlocked | security_settings | - | screens/settings/Misc.tsx:131 | logs.siem_not_configured |
| `logs.status` | unlocked | - | - | screens/settings/Misc.tsx:126 | logs.status |
| `logs.verify` | unlocked | - | - | screens/Activity.tsx:177, screens/settings/Misc.tsx:129 | logs.verify, logs.verify_after_rotate |
| `memory.about_me` | unlocked | - | - | screens/Memory.tsx:20 | memory.about_me |
| `memory.action` | unlocked | - | id*, action*, content | screens/Home.tsx:61, screens/Memory.tsx:25, screens/Memory.tsx:34 | memory.edit, memory.disable, memory.enable, memory.bad_action … |
| `memory.add` | unlocked | - | content*, type | screens/Memory.tsx:44, screens/Memory.tsx:45 | memory.add |
| `memory.delete_all` | unlocked | - | - | screens/Memory.tsx:64 | memory.delete_all |
| `memory.list` | unlocked | - | status | screens/Memory.tsx:22 | memory.list |
| `memory.review` | unlocked | - | - | screens/Memory.tsx:21 | memory.review |
| `missions.activate` | unlocked | - | mission_id* | screens/Approvals.tsx:28, screens/Missions.tsx:43 | missions.activate |
| `missions.create` | unlocked | - | mission* | screens/Missions.tsx:100 | missions.create |
| `missions.describe` | unlocked | - | text* | screens/Missions.tsx:65 | missions.describe |
| `missions.list` | unlocked | - | - | screens/Approvals.tsx:10, screens/Missions.tsx:15 | missions.list |
| `missions.parse_schedule` | unlocked | - | text*, timezone | screens/Missions.tsx:90 | missions.parse_schedule |
| `missions.run_now` | unlocked | - | mission_id* | screens/Missions.tsx:45 | missions.run_now |
| `missions.set_status` | unlocked | - | status*, mission_id* | screens/Approvals.tsx:30, screens/Missions.tsx:44, screens/Missions.tsx:47 | missions.pause, missions.bad_status, missions.cancel |
| `missions.update` | unlocked | - | mission_id*, mission* | screens/Missions.tsx:100 | missions.update |
| `network.logs` | unlocked | - | days, host, component, outcome, limit, offset, include_local | screens/NetworkLogs.tsx:40 | network.logs, network.logs_filtered |
| `notifications.list` | unlocked | - | - | - | notifications.list |
| `notifications.mark_read` | unlocked | - | id | - | notifications.mark_read |
| `posture.fix` | unlocked | security_settings | action* | screens/settings/AccountSettings.tsx:89 | posture.fix_unknown, posture.fix_verify_log |
| `posture.run` | unlocked | - | - | screens/settings/AccountSettings.tsx:79 | posture.run |
| `privacy.data_map` | unlocked | - | - | screens/settings/Misc.tsx:62 | privacy.data_map |
| `privacy.delete_everything` | unlocked | - | password*, confirmation* | screens/settings/Misc.tsx:75 | privacy.delete_wrong_confirm, privacy.delete_everything |
| `privacy.export` | unlocked | export | password*, folder* | screens/settings/Misc.tsx:70 | privacy.export |
| `reminders.action` | unlocked | - | id*, action*, minutes | screens/Reminders.tsx:14 | reminders.snooze, reminders.cancel, reminders.bad_action |
| `reminders.create` | unlocked | - | text*, due_at*, timezone | screens/Reminders.tsx:24 | reminders.create |
| `reminders.list` | unlocked | - | include_done | screens/Reminders.tsx:12 | reminders.list, reminders.show_done |
| `run.step` | keys | - | task_id*, type*, title*, text | - | manual: core-role RPC: written by pa-core during chat.send; see runs.get steps |
| `runs.get` | unlocked | - | run_id* | components/RunTimeline.tsx:17, components/RunTimeline.tsx:30 | runs.get |
| `runs.list` | unlocked | - | limit, archived, chat_id | screens/Activity.tsx:106, screens/History.tsx:30 | runs.list |
| `runs.replay` | unlocked | - | event_id*, model_id | screens/Activity.tsx:154 | runs.replay |
| `runs.transcript` | unlocked | - | run_id* | screens/Activity.tsx:131 | runs.transcript, runs.transcript_event |
| `secrets.binding_targets` | unlocked | - | - | screens/Secrets.tsx:72 | secrets.binding_targets |
| `secrets.copy` | unlocked | secrets | id*, clear_after | screens/Secrets.tsx:28 | secrets.copy |
| `secrets.create` | unlocked | - | item*, bindings* | screens/Secrets.tsx:77 | secrets.create |
| `secrets.delete` | unlocked | secrets | id* | screens/Secrets.tsx:83 | secrets.delete |
| `secrets.export` | unlocked | export | password*, path* | screens/Secrets.tsx:36 | secrets.export_encrypted |
| `secrets.generate` | unlocked | - | length, lower, upper, digits, symbols | screens/Secrets.tsx:95 | secrets.generate |
| `secrets.health` | unlocked | - | - | screens/Secrets.tsx:19 | secrets.health |
| `secrets.import_csv` | unlocked | secrets | path*, secure_delete | screens/Secrets.tsx:35 | secrets.import_csv |
| `secrets.list` | unlocked | - | - | screens/Secrets.tsx:18 | secrets.list_hides_values |
| `secrets.restore_version` | unlocked | secrets | id*, version_id* | - | secrets.restore_version |
| `secrets.reveal` | unlocked | secrets | id* | screens/Secrets.tsx:22, screens/Secrets.tsx:52 | secrets.reveal |
| `secrets.set_bindings` | unlocked | secrets | id*, bindings* | screens/Secrets.tsx:76 | secrets.set_bindings |
| `secrets.update` | unlocked | secrets | id*, item* | screens/Secrets.tsx:76 | secrets.update |
| `secrets.versions` | unlocked | - | id* | - | secrets.versions |
| `session.append` | keys | - | task_id*, kind*, content*, meta* | - | manual: core-role RPC: pa-core logs every message before a model call (rule 14); see runs.transcript |
| `session.status` | any | - | - | app.tsx:59 | meta.status_fresh, meta.status_unlocked, account.profile_check, meta.after_delete |
| `session.touch` | unlocked | - | - | app.tsx:150 | meta.touch |
| `settings.apply` | unlocked | - | changes*, loosen_token | components/IconUpload.tsx:27, components/LoosenDialog.tsx:18, components/ThemePicker.tsx:29 … | settings.apply_theme, settings.apply_bad, settings.apply_unknown, settings.loosen_needs_password … |
| `settings.begin_loosen` | unlocked | - | changes* | components/LoosenDialog.tsx:14 | settings.begin_loosen |
| `settings.classify` | unlocked | - | changes* | screens/settings/GenericGroup.tsx:16 | settings.classify_tighten |
| `settings.describe` | unlocked | - | - | screens/settings/Settings.tsx:20 | settings.describe |
| `settings.history` | unlocked | - | - | screens/settings/Misc.tsx:186 | settings.history |
| `settings.profile_preview` | unlocked | - | profile* | screens/Onboarding.tsx:266 | settings.profile_preview |
| `settings.rules_plain` | unlocked | - | - | screens/settings/Misc.tsx:187 | settings.rules_plain |
| `setup.check_password` | any | - | password*, username | screens/Onboarding.tsx:36, screens/SignIn.tsx:99 | setup.weak_password, setup.strong_password |
| `setup.check_pin` | any | - | pin*, allow_letters, password | screens/Onboarding.tsx:41 | setup.trivial_pin, setup.good_pin |
| `setup.confirm_recovery` | unlocked | - | answers* | screens/Onboarding.tsx:211 | setup.confirm_wrong, setup.confirm_recovery |
| `setup.create` | any | - | username*, password*, pin*, allow_letters, display_name, assistant_name | screens/Onboarding.tsx:49 | setup.create |
| `setup.preflight` | any | - | - | screens/Onboarding.tsx:33 | setup.preflight |
| `setup.recovery_pdf` | unlocked | - | path* | screens/Onboarding.tsx:206 | setup.recovery_pdf |
| `skills.activate` | unlocked | skills | skill_id* | screens/settings/Misc.tsx:169 | skills.activate |
| `skills.import` | unlocked | - | path* | screens/settings/Misc.tsx:155 | skills.import |
| `skills.list` | unlocked | - | - | screens/settings/Misc.tsx:144 | skills.list |
| `skills.set_status` | unlocked | - | skill_id*, status* | screens/settings/Misc.tsx:167, screens/settings/Misc.tsx:168 | skills.disable |
| `system.usage` | unlocked | - | - | components/GpuMeter.tsx:25 | system.usage |
| `task.context` | keys | - | task_id* | - | manual: core-role RPC: pa-core reads it at task start (chat.send) |
| `task.finish` | keys | - | task_id*, status*, result, reason | - | manual: core-role RPC: pa-core ends each task (chat.send) |
| `task.heartbeat` | keys | - | task_id* | - | manual: core-role RPC: pa-core lease heartbeat during long tasks |
| `tasks.get` | unlocked | - | task_id* | screens/Activity.tsx:77 | tasks.get_missing |
| `tasks.list` | unlocked | - | states*, limit | screens/Activity.tsx:107 | tasks.list |
| `tasks.resume` | unlocked | - | task_id* | - | tasks.resume_invalid |
| `tasks.stop` | unlocked | - | task_id* | - | tasks.stop |
| `tools.catalog` | unlocked | - | - | screens/Missions.tsx:87, screens/settings/Misc.tsx:144 | tools.catalog |
| `tools.invoke` | keys | - | task_id*, tool*, args* | - | manual: core-role RPC: the only way tools run; policy/red-team tests: tests/unit/test_policy.py, tests/redteam |
| `ui.open_link` | any | - | url* | - | ui.open_link_screen, ui.open_link_cannot_change_settings, ui.open_link_unknown |
| `updates.status` | unlocked | - | - | screens/settings/Misc.tsx:119 | updates.status |
| `voice.transcribe` | unlocked | - | mime | components/VoiceButton.tsx:48 | voice.no_stt |
| `web.test_search` | unlocked | - | - | screens/settings/ConnectorSettings.tsx:41 | web.test_search_not_set_up |
| `work.next` | keys | - | wait | - | manual: core-role RPC: pa-core long-poll for tasks (chat.send) |
