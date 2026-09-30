# Version history

Semantic versioning. Newest first. Every entry lists user-visible changes, security-relevant changes and
DB migrations.

## 0.1.1 - 2026-09-30 - personal names
**Added**
- Name your assistant and tell it your name during setup; both editable any time in
  Settings → Account & Security → Profile (`account.set_profile`, audited as `profile.changed`).
- Sign-in / lock screens greet you by name ("Welcome back, Sai", "Jarvis is locked"); sidebar, window title,
  Home and the chat box use the assistant's name; the system prompt tells the model its name and yours.

**Security**
- Names live in the vault header (not secret, MAC-protected, validated: 1-40 chars, no control characters or
  markup). No DB migration.

**Fixed**
- Named-pipe client retries while the gateway's pipe instance does not exist yet (startup race seen in CI).

## 0.1.0 - 2026-09-30 - first complete build
Built from `docs/spec/personal_desktop_agent_spec_v1_1.txt` (spec v1.1 incl. section 39).

**Added**
- Security core `pa-gateway` (Python 3.12): vault (Argon2id + TPM PIN protector + recovery key), SQLCipher DB,
  unified HMAC-chained security log with SIEM forwarding, deterministic policy engine, tool gateway (full 7.2
  sequence), DLP with keyed secret fingerprints, SSRF-safe egress filter, approvals, budgets, kill switch,
  named-pipe IPC with per-role method allowlists, posture checks, backups.
- Agent runtime `pa-core`: JSON-action agent loop, context built only from the session log, safe summaries,
  skills, sub-agents.
- Connectors: Web (Brave / SearXNG / Exa / Tavily + fetch), Microsoft 365 (Graph, PKCE), classic Outlook (COM worker).
- Models: built-in llama.cpp manager, Ollama, any OpenAI-compatible server (vLLM, LM Studio, Run:ai...),
  speech-to-text, embeddings, "Test model".
- Missions & Routines (cron / interval / once / event, plain-words), Reminders with confirmation cards and
  voice input, Memory with trust levels, My Files pipeline, Secrets vault, History search.
- Desktop UI (Tauri 2 + React): wizard, sign-in/lock/forgot, Home, Chat with live step timeline and streaming,
  Missions, Reminders, Tasks, Approvals, My Files, Memory, Secrets, Activity, full Settings, STOP ALL, tray,
  hotkey, command palette (Ctrl+K), light/dark themes.
- Tests: 144 automated (unit, integration, Windows platform, red-team gate N=20 in CI); laptop checklist.
- Windows CI: tests, UI + Tauri build, PyInstaller bundle artifact.

**Security notes**
- SQLCipher `cipher_memory_security` disabled on all platforms (Windows stack overflow) - see DEVIATIONS D5.

**DB**: schema v1.
