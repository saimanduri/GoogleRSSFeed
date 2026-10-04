# Progress

Status legend: ✅ implemented + automated tests · 🟢 implemented (tested manually / partially automated) ·
🟡 partial (see PENDING_WORK.md) · ⬜ not started. "Laptop" = needs the laptop checklist in TESTS.md.

_Last updated: 2026-10-01 (v0.1.4; manifests still say 0.1.1 - bump at release)._

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

## Phase 7 - Laptop hardening, portable mode, UI polish (2026-09-30 / 10-01)
| Item | Status |
|---|---|
| Automated tests never use the real TPM (regression test) | ✅ |
| Dev workers from a venv: exact-pid IPC check kept (base interpreter start) | ✅ |
| Release firewall enforcement (pa-core/workers refuse to start) | ✅ |
| Supply chain: hash-pinned locks, audits, SBOM, CodeQL, Dependabot (CI not yet run on GitHub) | 🟢 |
| Packaged bundle shipped without password/PIN lists (wizard blocked) - fixed + test | ✅ |
| `install-dev.ps1` source-folder bug fixed | ✅ |
| Portable copy-and-run folder, single `pa-ui.exe` start, exact-path IPC checks | ✅ (no OS network isolation yet: PENDING_WORK 0) |
| `scripts/laptop_smoke.py` (TPM, PIN, forgot password, secrets, backup, STOP ALL, Ollama chat, log verify, delete) | ✅ 21/21 on the real TPM (once); later runs hit the TPM throttle, 18/18 in `--software` mode |
| UI: 8 themes, 5 animated backgrounds, Day & night auto theme, local-time labels | ✅ (browser preview + build) |
| UI: History screen, pinned chats + folders (DB v2), slash commands, floating scroll buttons, token meter | ✅ |
| UI: plan preview before long tasks, diff view for agent-written files, quick permission-mode switch | ⬜ see PENDING_WORK |

## Numbers
~13 k lines Python, ~3.6 k lines TypeScript/React/CSS, ~380 lines Rust; 162 automated tests (13 Windows-only),
red-team corpus 13 cases.

## Laptop session log (Windows 11 Home, real Intel PTT TPM 2.0)
- 2026-09-30 Phase 1: Python 3.12.10, Rust 1.98 (stable-msvc), Node 22, WebView2 present. VS Build Tools install pending (needs UAC). `check_env.py` OK.
- Phase 2: ruff clean; `npm install` + `npm run build` OK; selftest OK (with `PA_FORCE_SOFTWARE_PROTECTOR=1`);
  pytest 148 passed (K4 venv-launcher pid issue fixed in workers.py, exact-pid check untouched).
- FIXED: automated tests used the REAL TPM (default_protector prefers it), so wrong-PIN tests exhausted the TPM's
  per-user authorization budget (System event 23; NCryptFinalizeKey 0x80290409 for every key). Tests now set
  `PA_FORCE_SOFTWARE_PROTECTOR=1` (dev mode only; ignored in release builds); regression test
  `test_tests_never_use_real_tpm`.
- Phase 4: release bundle built at `dist\PersonalAgent`; release gateway refuses dev flags (verified). Install (admin) not yet run.
- Phase 6: item 4 (firewall enforcement) and item 6 (supply chain) done with tests: 153 passed.
- 2026-10-01: portable mode built and verified (gateway + UI from dist\PersonalAgent-Portable, single pa-ui.exe start, exact-path UI check, release flags). 159 tests pass.
- 2026-10-01: end-to-end action catalogue built (175/175 steps pass, 126/137 RPCs covered, 11 manual-only with reasons);
  it found and fixed the pipe accept-loop lock-out (K11). Apple-design UI review written (docs/UI_REVIEW_APPLE_DESIGN.md),
  improvements pending (PENDING_WORK UI-A).
- 2026-10-01: Apple-design improvements implemented (0.1.6): contrast fixed in all themes (test_theme_contrast), spring
  motion + exit animations, press states, soft edges, type scale, Undo for deletes, breadcrumbs, OS accessibility prefs,
  background default Off. Tests: 174 passed (clipboard test passes with an unlocked screen), e2e 175/175, ruff + tsc clean.
- 2026-10-01: could NOT rebuild pa-ui.exe with the new UI on this PC: after `cargo clean --release` (the old build cache was
  corrupt) Windows Smart App Control blocks every freshly compiled Rust build script (os error 4551). The portable folder
  therefore still contains the previous UI. Options: build in GitHub Actions (windows-ci), on another PC, or with Smart App
  Control off. Frontend was verified in the browser preview (`?demo`) and by tsc/vite build only.
- 2026-10-01: Smart App Control turned off by the user; pa-ui.exe rebuilt and portable folder repackaged. Found by the user's
  first real launch: a stale `run\gateway.json` (gateway killed/crashed) left the UI on "Waiting for the Personal Agent
  service" forever (K13). Fixed in main.rs (`pid_alive` via exit code; stale rendezvous -> start a new gateway), verified by
  force-killing the gateway and relaunching. Earlier note about the build blocker is resolved.
- 2026-10-01: user's first wizard run: Continue was (correctly) disabled for an 11-character password, but the screen said
  "strong" in green and gave no reason. Wizard now lists what is still needed and marks the password "Not accepted yet".
  Product name changed to "ChiRAG Agent" (window title, UI texts, default assistant name; folders/exe names unchanged).
  Found: Windows Defender is OFF on this PC (McAfee is the active antivirus) -> uploaded files cannot be scanned and are
  rejected (fail closed). Message now says "not scanned: Microsoft Defender is turned off", tests no longer depend on
  Defender (conftest, PA_TEST_REAL_DEFENDER=1 to use it). Decision needed from the user: see PENDING_WORK K14.
- 2026-10-01: TPM throttle (0x80290409) blocked account creation on the user's PC: friendly error 'tpm_throttled' + no half-created vault (test), probe script waits for it to clear. e2e runner/tests skip Defender in dev mode (PA_SKIP_DEFENDER). 175 tests + 175/175 e2e pass. Portable folder repackaged.
- 2026-10-01: user dev-mode feedback fixed: reminder times (model now told local time + zone; card shows local weekday/date/time), chat list and steps panel closed by default (menu button in chat header opens the list; + = new chat), Guide screen with word-start search (56 entries) after Activity log, accent colour choice (Blue/Teal/Emerald/Violet/Graphite + theme default, AA-tested in light+dark), scripts/run-dev.ps1 (developer mode without TPM). 189 tests + 175/175 e2e pass.
- 2026-10-01: user feedback round 3: model replies like {action:'reminders.propose',tool:...} are now accepted as tool calls (pa_core/actions.py, tests); routine form: schedule picker (once a day / several times a day with interval + optional hours + days), 8 output formats (OUTPUT_FORMATS), plain-language schedule text (schedule.describe_cron); routines using web.search without a search provider show a warning + Fix button and are not run (Home notice / needs_setup error) instead of failing silently. 207 tests + 175 e2e pass.
- 2026-10-01: reliability of tool calls: agent loop requests schema-constrained output (json_schema ACTION_SCHEMA) on local runtimes with fallback to json_object/none; lenient parser; raw JSON never shown (agent._readable). Warnings orange / errors red (Notice, Button tone, tinted toasts). Tests 209 pass; clipboard test + e2e secrets.copy fail only because Windows denies OpenClipboard right now (environment, passed before).

- 2026-10-01 (evening): large feature round after the first real use on the user's PC (all with tests; 256 pytest + 187/187 e2e):
  * Mission proposals made by the assistant now appear in Approvals > Proposed routines (Activate / Review / Dismiss), counted in the menu badge.
  * Outlook: found and fixed a locale bug (Outlook reads `10/01/2026` day-first on this PC, so every date-limited search was wrong); worker now uses
    the Windows short-date pattern + the table API. New tools outlook_local.digest / mail_stats / awaiting_reply with deterministic flags
    (APPROVAL / DEADLINE / URGENT / QUESTION, in To vs CC, replied or not). Measured on the user's mailbox: 727 mails/day, stats 6 s, 7-day stats 12 s.
  * Email monitoring skills (10 read-only routines) with on/off switches in Settings > Email monitoring and an Outlook screen (nav button above Guide).
  * Picture upload (assistant icon, user picture), Alt+letter shortcuts for the left menu, live GPU meter above Settings, Network Logs tab
    (central net_log table; web, Microsoft 365 and model calls incl. localhost), local files read in place (paperclip / drag-drop; 99 MB xlsx indexed in 62 s,
    queries ~1 s), dev gateways get their own single-instance lock (so e2e/dev run next to the installed app).
  * Found: the user was chatting in the OLD installed build (logon tasks) - new bundle must be installed (admin) to get any of the fixes.
- 2026-10-01 (late): live check of all email skills on the user's real mailbox + local model (see TESTS.md); fixes: reasoning_effort none for Ollama, placeholder answers rejected,
  prefetch of data-gathering tool calls, deadline_within_days. 257 pytest (clipboard test fails only when Windows denies OpenClipboard) + 187/187 e2e. New bundle built (needs admin install).

## Consolidated status (end of the long session, 2026-10-01 night)
All phases 1-6 of the first brief were worked through (environment, tests, dev run (user skipped as mandatory), release bundle, laptop checklist subset, pending items); after that the user drove a long feedback loop
(portable mode, UI redesign + themes, Apple-design pass, e2e catalogue, wizard/TPM issues, ChiRAG naming, routines, Outlook, observability, local files). Everything is committed on `claude/elegant-gates-cyfi1y` (not pushed).
Single best entry point for a new session: `docs/HANDOFF.md`. Open work: `PENDING_WORK.md` ("Next session start here"). The user's real installed app still had to be updated to the new bundle (admin install; DLL-lock fix documented in docs/BUILD_AND_RELEASE.md).

- 2026-10-02: user requested (planning only, no code): R1 rename/pin any chat, R2 Windows notifications named after the task/reminder/session (study how Claude does it), R3 SAST + DAST + hacker-style security audit. Recorded in PENDING_WORK.md.
- 2026-10-02: studied voice input: existing STT code calls an OpenAI-style /audio/transcriptions endpoint that Ollama does not have; frozenlab/qwen3-asr:1.7b needs /api/chat with base64 16 kHz mono WAV in "images". Also studied the ChatGPT right-edge position rail. Both, plus a real-window Settings test pass, recorded as R4-R6 in PENDING_WORK.md (no code).

- 2026-10-02: voice input with Ollama speech models (Qwen3-ASR proven live on this PC through the gateway), auto-test on 'Add model' + auto default, model type detection (llm.inspect), WAV conversion in the window; 269 pytest (only the OpenClipboard environment failure) + e2e 187/188 (same clipboard cause). Details VERSION_HISTORY 0.1.8, TESTS H17/H19.

- 2026-10-02: Web search with Exa: provider already supported; added `web.test_search` + "Test web search" button (Settings > Connectors > Web) and Guide steps; tests in test_web_search_setup.py (key never returned/logged). User stores the key themselves in Secrets (bound to web.search).
- 2026-10-02: security audit round (SAST with ruff-bandit rules + manual triage, DAST with the 217-file corpus and 4060 hostile RPC calls + pipe abuse): 11 findings fixed with tests, none open; report docs/SECURITY_AUDIT_2026-10-02.md. 306 pytest + e2e 188/189 (clipboard = environment).
- 2026-10-02: R1 rename/pin any chat (list, header, History, /rename, F2), R2 named Windows notifications (+ focus-aware chat toasts, new setting show_names), R6 generated settings round-trip test; 359 pytest (clipboard = environment), e2e 188/189 same cause; cargo check OK.
- 2026-10-02: R5 chat position rail built and verified in the browser preview (ticks, hover list, jump, scroll tracking).
- 2026-10-02 (later): model kinds split (chat/voice/vision/embedding cards + server-side enforcement), vision pipeline (images, scanned PDFs, paste) proven live with qwen2.5vl:7b, folder sharing with per-chat + per-session + subfolder approvals (25 tests), live 30 s voice pieces (10-minute test), file sanity round 2, 20 fonts + 3 sizes with a layout sweep. 424 pytest + e2e 198/199 (clipboard = environment). Details VERSION_HISTORY 0.1.11.
- 2026-10-02 (review round): empty-reply rescue, history search fixed, antivirus AMSI + explicit release, per-chat web approval, folder digest, header token ring, Home cleanup, ChiRAG icon + shortcuts. 452 pytest, e2e 199/200. Details VERSION_HISTORY 0.1.12.

- 2026-10-02: docs/HOW_IT_WORKS.html (flowcharts, tech stack, SBOM from the lock files, every button) generated by scripts/make_howitworks.py - re-run after dependency changes.
- 2026-10-03: automatic memory learning + forget, automatic file summaries (editable, PII-safe, files.find), Home widgets with a Widgets panel, update-app.ps1 one-command install. Details VERSION_HISTORY 0.1.13.
- 2026-10-04 (0.1.14 round, cloud session, Linux container; branch claude/v0.1.14 from update-0.1.13): fixed the 3 failures of the 0.1.13 Windows CI run (deleted-chat search leak race, proposal badge vs Home notice, voice model default while testing) with regression tests; Windows-dependent tests marked; catalogue generator OS-independent. Linux: 447 pytest pass, 30 skipped (Windows-only), ruff clean.
- 2026-10-04: top-left logo cache-proof + title-bar icon, Rules & Safety sections/readable options/history, Diagnostics resource meters (resmon.py, red >= 90 %), preview uses the real settings schema, e2e runner added to Windows CI. Linux: 460 pytest pass.
- 2026-10-04: per-model context length + temperature (migration 6, llm.update, Adjust dialog); found + fixed: Ollama's OpenAI endpoint ignored num_ctx (prompts cut to 4096) -> native /api/chat streaming. Linux: 474 pytest pass.
- 2026-10-04: web research: web.read (Exa contents, live crawl, subpages), web.answer, web.research (Exa research agent), search filters; check_target domain rules before any provider crawl; research method in the prompt; 21 + 3 red-team tests.
