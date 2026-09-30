# Progress

Status legend: ✅ implemented + automated tests · 🟢 implemented (tested manually / partially automated) ·
🟡 partial (see PENDING_WORK.md) · ⬜ not started. "Laptop" = needs the laptop checklist in TESTS.md.

_Last updated: 2026-09-30 (v0.1.0)._

## Phase 1 - Security spine
| Spec | Item | Status |
|---|---|---|
| 4.3 | Key hierarchy (VMK, HKDF sub-keys), vault header with MAC | ✅ |
| 4.3/4.4 | Argon2id password wrap (calibrated ≥ 1 s, 256 MiB) | ✅ |
| 4.4 | TPM PIN protector via CNG Platform Crypto Provider | 🟢 code complete; laptop test A3/A5/A6 |
| 4.4 | Software PIN protector (dev mode only) | ✅ |
| 4.5 | Cold start / quick unlock / step-up / auto-lock / Windows lock hooks | ✅ (session hooks: laptop) |
| 4.6 | Forgot password (PIN + recovery, rotation), change password, set PIN, new recovery key | ✅ |
| 4.7 | Brute-force delays, PIN/recovery lockouts, key wipe on sign-out/sleep, WER exclusion, VirtualLock | ✅ / 🟢 (sleep: laptop) |
| 2.4/39.1/39.3 | Named-pipe IPC: DACL, reject remote, first-instance, token, server PID, client image (+Authenticode when signed), per-role allowlists | ✅ (Windows CI) |
| 25 | Unified HMAC-chained log, rotation + seal, verify, redaction, spool, fail-closed | ✅ |
| 25.6 | SIEM forwarding (HTTPS / syslog-TLS, pinned cert, CEF/JSON) | 🟢 (laptop F3) |
| 7.3 | Policy engine (default deny, conflict order, plain-language rules) | ✅ |
| 24 | Kill switch levels, effects, password release, tray + hotkey | ✅ / 🟢 (tray/hotkey: laptop) |
| 5 | Single UI shell, left nav, Settings button, first-run wizard, Account & Security, Emergency Stop, Logs | ✅ (UI build in CI) |
| 2.5 | Data folder ACL, cloud-folder refusal | ✅ |
| 2.5 | Signed MSI installer | 🟡 dev install script only |

## Phase 2 - Secrets and files
| Spec | Item | Status |
|---|---|---|
| 6 | Secrets vault: per-item keys, versions, bindings, reveal/copy with step-up, capture exclusion, secure clipboard, health, generator, CSV import, encrypted export | ✅ |
| 15 | Quarantine → validate (sniffing, archive limits) → Defender + EICAR → macro/active-content detection → pa-parser (Job Object) → READY | ✅ (Defender: CI best-effort) |
| 15 | pa-parser in AppContainer | 🟡 Job Object only (D8) |
| 5.2 | My Files UI (upload, drag-drop, folders, tags, labels, preview text, save copy, knowledge, quarantine view) | ✅ |

## Phase 3 - Agent core
| Spec | Item | Status |
|---|---|---|
| 20 | Model registry, providers (built-in llama.cpp, Ollama, OpenAI-compatible: vLLM/LM Studio/Run:ai), roles, remote-endpoint gating, exposure check, Test model (+ red-team subset), STT, embeddings | ✅ / 🟢 (real servers: laptop C1-C4) |
| 20.1 | llama-server manager (loopback, random port/key, SHA-256 verify, 401 rotation) | 🟢 (laptop B7) |
| 39.5 | Session log, context only from log, verification before every model call, replay | ✅ |
| 7.2 | Tool gateway full sequence | ✅ |
| 12/13 | Injection heuristics, labelled data sections, sensitivity high-water marks | ✅ |
| 17 | Durable tasks, leases, recovery after restart, outbox idempotency, OUTCOME_UNKNOWN | ✅ |
| 18 | Budgets per task/day, priority for chat | ✅ |
| 19 | Sandbox: Windows Sandbox (strong), AppContainer (standard), unavailable | ✅ AppContainer (CI) / 🟢 Windows Sandbox (laptop B5) |
| 21 | Memory: trust levels, proposals, About me, review, embeddings, cascade delete | ✅ |
| 23/39.14 | Approvals: payload hash, single use, expiry, step-up/password, edit & re-propose, fatigue, cards in chat | ✅ |
| 39.7 | Safe summarising | ✅ (unit-level; long real chats: laptop) |
| 39.8 | Sub-agents (no more rights, sensitivity both ways, max 3) | ✅ |
| 39.9 | Scripts that call tools | 🟡 setting only (D16) |

## Phase 4 - Connectors
| Spec | Item | Status |
|---|---|---|
| 8 | Framework: effective state, pause all, off → suspend missions, disconnect (+delete data), manifests | ✅ |
| 11 | Web search (Brave, SearXNG, Exa, Tavily) + fetch through SSRF-safe egress | ✅ (live providers: laptop D7) |
| 10 | Microsoft 365: PKCE loopback sign-in, scopes, search/read/attachments/calendar, drafts, send with approval + idempotency header | 🟢 (laptop D4-D6) |
| 9 | Local Outlook worker (COM, read-only, AI drafts, folder/PST/label controls, no OMG bypass) | 🟢 (laptop D1-D3) |
| 39.4 | Signed connector manifests (Ed25519, pinned keys, revocation) | 🟡 verifier implemented; no external packages yet |

## Phase 5 - Autonomy and learning
| Spec | Item | Status |
|---|---|---|
| 16 | Missions & Routines, cron/interval/once/event, IANA TZ (DST-safe), missed-run policies, rate-limited triggers | ✅ |
| 39.15 | Describe a mission in plain words | ✅ |
| User req. | Reminders by chat/voice with confirmation card, fire as toast + banner + Home | ✅ / 🟢 (voice: laptop C4) |
| 22/39.6 | Skills: declarative, static checks, HMAC signature, lineage/taint, review diff + highlights, activation step-up, auto-retire/suspend | ✅ |
| 26 | Notifications (content level, quiet hours, private toasts, no actions) | ✅ / 🟢 toasts |
| 5.2 | Home "While you were away" | ✅ |
| 39.12 | History search (FTS5) | ✅ |
| 39.13 | Memory review + About me | ✅ |
| User req. | Step timeline for every request (last 100 visible, older archived + searchable), streaming answers | ✅ |

## Phase 6 - Hardening
| Spec | Item | Status |
|---|---|---|
| 31/32 | Security tests + red-team suite (N=20 in CI) | ✅ |
| 39.10 | Security Posture page with Fix actions | ✅ |
| 27 | Backup/restore (.pabk), scheduled backups, verify, restore to new PC | ✅ |
| 39.11 | Safe updates with rollback | 🟡 snapshot helper only (D13) |
| 29 | Signed builds, SBOM, dependency scanning | 🟡 CI builds unsigned bundle |
| - | Accessibility review | 🟡 labels/keyboard/themes done; formal review pending |

## Numbers
~13 k lines Python, ~3 k lines TypeScript/React, ~360 lines Rust; 144 automated tests (131 run on any OS,
13 Windows-only), red-team corpus 13 cases.
