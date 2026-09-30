# Unified security log (spec 25)

**File:** `%LOCALAPPDATA%\PersonalAgent\logs\agent-security.jsonl` (active; path never changes).
Rotated segments: `agent-security-<UTC timestamp>-<sequence>.jsonl`, each ending with a `log.seal` record.
Rotation: 100 MB or 7 days (Settings > Logs & SIEM). Retention of rotated segments: 365 days.

## Record (JSON Lines, OCSF-aligned names)
```json
{"schema_version":"1.0","event_id":"evt_01…","sequence":20417,"prev_hash":"b41e…","timestamp":"2026-10-01T02:14:07.512Z",
 "category":"authorization","event_type":"policy.decision","severity":"info","component":"pa-gateway",
 "component_version":"0.1.0","user":"local-user","tool":"web.search","task_id":"task_…","mission_id":"msn_…",
 "trigger_type":"SCHEDULE","context_high_water_mark":"CONFIDENTIAL","policy_decision":"REQUIRE_APPROVAL",
 "policy_version":"local-1.0.0+user-7","decision_reason":"flow.confidential_egress","payload_hash":"sha256:5e88…",
 "connector":"web","chain_mode":"hmac","chain_hmac":"9c07…"}
```
Envelope fields are never overwritten by event fields (a colliding detail key becomes `x_<key>`).

## Integrity chain
`chain_i = HMAC-SHA256(K_log, prev_chain || canonical(event_i without chain_hmac))`; `prev_hash` = previous
`chain_hmac`; genesis = 64 zeros. Events written before unlock (e.g. failed sign-ins) use
`chain_mode:"sha256"` (unkeyed) and are anchored by the next HMAC'd event, which covers their chain value.
`Verify integrity` (Settings, Activity, every sign-in) recomputes the chain across all retained segments and
detects edits, deletions and re-ordering (tested). Without K_log the chain cannot be recomputed.

## Fail closed
`AuditWriter.write()` raises `AuditFailure` if neither the log nor the bounded spool (10 MB,
`state\audit.spool`) accepted the event. The tool gateway writes `tool.execute` **before** executing and
refuses the action on failure (tested).

## Content minimisation
Metadata only. Keys such as `password, pin, recovery_key, token, secret, value, body, content, text, prompt,
messages, payload, code, cookie, authorization` are replaced by `[REDACTED]`; strings are truncated to 300
chars; any string containing a stored secret value (keyed-hash scan) becomes `[REDACTED:SECRET]`. Full prompts,
completions and tool payloads live only in the encrypted Transcript Store (`session_events`), referenced by id.

## Event types (main)
| Category | event_type |
|---|---|
| lifecycle | gateway.started/stopping, core.started/crashed, scheduler.error, tasks.recovered |
| authentication | setup.completed, auth.signin / signin_failed / quick_unlock / pin_failed / pin_tpm_lockout / locked / unlocked / signout / stepup / stepup_failed / recovery_failed / password_reset / password_changed / pin_changed / recovery_key_rotated / username_changed, recovery_key.confirmed / saved_to_file |
| authorization | policy.decision, tool.denied, tool.unknown, tool.caller_rejected |
| execution | tool.execute (about to execute), tool.result, tool.result_discarded |
| dlp / egress | dlp.blocked, dlp.secret_in_response, egress.fetch, egress.denied |
| approval | approval.requested / approved / denied / expired / voided / used / replaced / voided_all / fatigue_warning |
| configuration | settings.changed (before/after, direction) |
| connector | connector.changed / pause_all / sign_in_started / connected / disconnected / callback_rejected |
| killswitch | killswitch.activated / released |
| task / mission | task.created / transition, mission.created / updated / status / catch_up, trigger.rate_limited |
| file | file.ingested / ready / quarantined / deleted / label_changed / saved_copy |
| secrets | secret.created / updated / deleted / revealed / copied / used_by_tool / bindings_changed / imported / exported |
| memory / skill | memory.*, skill.proposed / activated / status / signature_invalid |
| model | model.added / removed / verified / verify_failed / loaded / tested, llm.request / response / replay, stt.request, llm.key_bruteforce |
| sandbox | sandbox.run / result |
| security | ipc.method_denied / handshake_failed / bad_frame, injection.suspected, ui.link_rejected, log.verify_failed, posture.checked |
| backup / privacy | backup.created / verified / restore_staged / scheduled_enabled, update.snapshot, privacy.exported / delete_everything |
| audit | log.seal, log.verified, logs.exported |

## SIEM (optional, off by default)
Settings > Logs & SIEM: HTTPS (JSON array POST) or syslog over TLS (RFC 5425 octet counting), server
certificate pinned by SHA-256 fingerprint, JSON or CEF, background queue with backoff, lag shown in the UI.
Any shipper (Splunk UF, Elastic Agent, Fluent Bit, Wazuh, Sentinel AMA) can also tail the file.
