# Database

- Engine: **SQLCipher 4** (AES-256, SQLite 3.51) via `sqlcipher3` (`sqlcipher3-wheels` on Windows).
- File: `%LOCALAPPDATA%\PersonalAgent\db\agent.db` (+ `-wal`, `-shm`). Key: `K_db` (raw 256-bit, from the VMK).
- Opened only by pa-gateway, one connection guarded by a re-entrant lock; WAL, `foreign_keys=ON`, `secure_delete=ON`.
- Schema: `app/pa_gateway/db/schema.py`. Migrations are append-only `(version, sql)` pairs; the current
  version is in `meta.schema_version`. **Never edit a released migration** - add a new one and document it here.
- Timestamps are UTC ISO-8601 strings with milliseconds and `Z` (sortable). JSON columns end in `_json`.
- Sensitivity columns hold 0 PUBLIC, 1 INTERNAL, 2 CONFIDENTIAL, 3 RESTRICTED.
- Embeddings are float32 blobs inside the encrypted DB; search is cosine similarity in memory (numpy).
  Chosen over a vector extension to keep everything inside SQLCipher with no extra native code.

## Tables (schema v1)

| Table | Purpose / key columns |
|---|---|
| `meta` | key/value: `schema_version`, `killswitch` (JSON), `connectors.pause_all`, `backup.last`, `backup.kek` |
| `settings` | user values of settings (`key`, `value_json`, `updated_at`). Defaults live in code. |
| `settings_history` | every change: before/after JSON, `direction` = tighten / loosen / neutral |
| `signin_history` | sign-in / unlock attempts (kind, success) - last 50 shown in Settings |
| `secrets` | vault items: `wrapped_key` (item key under K_secret), `blob` (AES-GCM item JSON), `version` |
| `secret_versions` | previous encrypted versions |
| `secret_fingerprints` | `(secret_id, length, fp=HMAC(K_dlp, value))` for DLP scanning without plaintext |
| `secret_bindings` | which tool/connector may use a secret (`web.search`, `llm:<id>`, `m365.refresh_token`) |
| `connectors` | per connector: `enabled`, `use_chat`, `use_missions`, `connected`, `scopes_json`, `last_used_at` |
| `models` | model registry: provider, endpoint, model_name, path + `sha256` (GGUF), kind (chat/stt/embedding), `tested`, `test_report_json` |
| `chats` | chat list; `hwm` = highest sensitivity seen; `sources_json`; `allow_tools`; `archived`; `pinned`, `folder` (migration 2, UI organisation only) |
| `chat_messages` | user/assistant messages with sources and sensitivity |
| `runs` | one per request (chat turn, mission run, sub-task): status, hwm, tokens, tool calls, `archived` (beyond last 100) |
| `run_steps` | the live step timeline: type (input/llm/thought/plan/tool/policy/approval/summary/…), status, detail JSON, duration |
| `session_events` | **session log / Transcript Store** (39.5, 25.5): everything that entered or left the model, per session (chat id or run id), hash-chained (`prev_hash`, `hash`), with source, trust, sensitivity |
| `tasks` | durable queue: `session_id`, `state`, `priority` (chat 1 < sub-task 3 < mission 5), `trigger_type`, `allowed_tools_json` (mission scope), `budget_json` (narrowing only), `usage_json`, `hwm`, lease owner/expiry |
| `task_transitions` | every state change with reason |
| `missions` | missions/routines: `schedule_json`, `timezone` (IANA), allowed tools/connectors, budgets, missed-run policy, `status`, next/last run |
| `approvals` | exact `payload_json` + `payload_hash` (sha256 of tool+payload+destination), risk, `requires_password`, status (PENDING/APPROVED/DENIED/EXPIRED/VOIDED/USED/REPLACED), `decision_ms` (fatigue detection) |
| `outbox` | idempotency for external side effects: `idem_key = sha256(task|tool|payload_hash)`, state INTENT/DONE/FAILED/UNKNOWN |
| `files` | My Files metadata: sha256, sniffed type, source, sensitivity, status (QUARANTINED/PROCESSING/READY/REJECTED), `scan_json`, `wrapped_key`, extracted `text_content`, `hidden_json` (labelled hidden content), `derived_from` (cascade) |
| `memories` | type, content, source/source_ref/provenance, `trust` (TRUSTED/VERIFIED/INFERRED/UNTRUSTED), status (ACTIVE/PROPOSED/DISABLED/REJECTED), embedding |
| `skills` | declarative definition JSON, version, status, lineage, `tainted`, review highlights, test results, `mac` (HMAC K_skill), denials |
| `reminders` | text, `due_at` (UTC), timezone, status (SCHEDULED/FIRED/DISMISSED/CANCELLED), source |
| `notifications` | in-app notification centre (body only if the content level allows) |
| `budget_usage` | per local day totals (`tokens`, `web_requests`, `egress_bytes`, `runtime_seconds`, …) |
| `home_events` | "While you were away" items (severity, dismissed) |
| `policy_history` | reserved for policy overlay versions |
| `history_fts` | FTS5 index (porter/unicode61) over chat messages, run results and file text; rows removed on delete |

## Retention (Settings > Files & Storage)
- Transcripts (`session_events`): default 30 days (daily purge).
- Runs beyond the newest 100 (configurable) are `archived=1` (hidden from the timeline, still searchable).
- Memories: optional retention; expired memories deleted.
- Deleting a file cascades to derived files, memories (`source_ref`) and the search index; disconnecting a
  connector with "also delete data" deletes its files and memories.
- "Delete everything" crypto-erases (destroys the TPM key and overwrites the vault header) before deleting files.

## Backups / copies
`Database.backup_to()` uses `sqlcipher_export` into a new file with the same key (used by backups and
pre-update snapshots). A copied data folder cannot be opened without the password (tested).

## Migration 3 (0.1.7)
- `missions.template_id` - set for email-monitoring skills (see agentdata/email_skills.py); NULL for normal routines.
- `net_log` - one row per outbound request (ts, component web|m365|llm, method, scheme, host, port, path WITHOUT query, status, outcome ok|error|blocked, reason,
  bytes_out, bytes_in, duration_ms, ip, loopback, purpose, tool, task_id, run_id). Kept 14 days (purged hourly). No bodies, headers, tokens or queries.
- `local_grants` - files the user attached to a chat from disk (id, chat_id, path, name, ext, size, mtime, sensitivity, created_at, last_used_at). The model only ever sees id + name.
  Private SQLite index files live in `tmp/localfiles/<grant>.sqlite` (deleted on removal, chat deletion, or after 7 days).


## Migration 4 (0.1.11): local_grants
`local_grants.scope` ('file' | 'folder'), `recursive` (0/1: subfolders approved), `session_nonce` (the app session in which the user approved; an approval is valid only while it equals the gateway's current session nonce, which changes at every start and sign-out). Pre-existing rows get '' and therefore need re-approval.


## Migration 5 (0.1.13): file_meta, memory_learn_state, memory_forgotten
`file_meta` (one row per file: title, doc_type, summary, keywords_json, pii_json = KINDS of personal data only, status PENDING/ANALYSING/READY/FAILED, edited flag, model, note, memory_id); `memory_learn_state` (per chat: newest user message already looked at); `memory_forgotten` (SHA-256 of normalised text of memories the user deleted or rejected - automatic learning never recreates them).

## Migration 6 (0.1.14): models.context_length, models.temperature
Per-model context length (tokens) and temperature (Settings > AI Model > Adjust, RPC `llm.update`). NULL = use the defaults
`llm.context_tokens` / `llm.temperature`. Ollama receives them through its native `/api/chat` (`options.num_ctx`,
`options.temperature`); OpenAI-style servers receive the temperature only; the built-in runtime starts with `--ctx-size`.
