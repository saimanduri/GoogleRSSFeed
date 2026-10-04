# Tests

Three layers:
1. **Automated, any OS** (unit + integration + red-team) - run locally and in CI.
2. **Automated, Windows-only** (`tests/windows`) - run on GitHub `windows-latest` runners and on your laptop.
3. **Laptop checklist** (below) - things a CI runner cannot do: a real TPM with a PIN, Windows Sandbox,
   classic Outlook, your Microsoft 365 tenant, WebView2 UI, microphone, real firewall rules, sleep/hibernate.

## How to run
```powershell
cd personal-agent
.\.venv\Scripts\Activate.ps1
python scripts\check_env.py            # native libraries (SQLCipher, crypto) smoke test
python -m pytest -q                    # everything that applies to this machine
python -m pytest -q -m windows         # only Windows platform tests
$env:PA_REDTEAM_N = "20"; python -m pytest -q tests\redteam     # release gate (N=20 per case)
$env:PA_LAPTOP_TESTS = "1"; python -m pytest -q -m laptop       # hardware tests (see checklist)
cd app; python -m pa_gateway --selftest   # end-to-end: real gateway + core + mock model
cd app\ui; npm run build                  # UI type-check + production build
```

## Automated suites
| Suite | File | Covers (spec) |
|---|---|---|
| Vault & keys | `tests/unit/test_vault.py` | 4.3-4.6: unlock, wrong password, reset needs PIN + recovery key, recovery rotation, header tamper (crypto + MAC), set-PIN needs current recovery key, no plaintext VMK |
| Security log | `tests/unit/test_audit.py` | 25: HMAC chain, edit/delete/reorder detection, key required, fail-closed, redaction (forbidden keys + vault secret values), pre-unlock anchoring, rotation seal, restart continuity |
| Policy | `tests/unit/test_policy.py` | 7.3, 13.3 table (web + external writes, password for CONFIDENTIAL writes), external-event read-only, mission allowlist, disabled tools, connector state, sandbox required, tighten-to-deny, injection flag, engine failure → deny, reminders need confirmation, definition hash |
| DLP | `tests/unit/test_dlp.py` | 6.4, 14: cards (Luhn), IBAN, key formats, keyed-hash secret detection without plaintext, custom patterns/keywords |
| Egress / SSRF | `tests/unit/test_egress.py` | 11.2, 31 Web: private/loopback/link-local/CGNAT/multicast/IPv6-mapped/6to4, schemes, ports, userinfo, metadata, DNS rebinding (mixed answers), connector domains when off, redirect re-check, size & content-type limits, IP pinning + Host header, cookies stripped |
| Settings rules | `tests/unit/test_settings_rules.py` | 5.4/5.5: tighten immediately, loosen needs password + 10 s token, floor cannot be breached, unknown settings rejected |
| Auth flows | `tests/integration/test_auth_flows.py` | 34.1-34.4: setup, cold start, weak password/trivial PIN, password delay, quick unlock + PIN lockout, pause-missions-when-locked wipes keys, forgot password rotates key, recovery lockout, step-up for secret reveal, **copied folder needs the password**, delete everything |
| Agent security | `tests/integration/test_agent_security.py` | 39.3 pa-core method allowlist, UI cannot call core methods, chat round trip + steps, **unlogged context rejected** (39.5), leased-task ownership, **canary secret never reaches model/log/egress**, **kill switch < 2 s** + password release, connector-off + no alternate path, **approval payload binding + single use**, edit-and-re-propose, RESTRICTED egress denied, budgets stop runaway, external-event task cannot egress, injection flag + tainted memory blocked, history excludes deleted, **links cannot change settings** (39.2), mission widening pauses + write missions need password |
| Files, backup, misc | `tests/integration/test_files_backup_misc.py` | 15: EICAR quarantined, masquerading .exe, zip bomb, docx hidden text + macro flag, PDF parse, XXE rejected, blobs encrypted at rest; 27: backup verify, wrong password, **restore to a new PC** (needs new PIN), tamper detection; 22/39.6 skill checks + signature; 21 memory trust; 16 cron/DST/plain words, missed-run policy; reminders fire; session-log chain; connector toggle suspends missions; secret export encrypted |
| Red team | `tests/redteam/test_redteam.py` + `corpus.json` | 32: 13 attack cases × N runs with a fully malicious "model": forward mail, exfil via URL/search, SSRF (metadata, localhost), memory poisoning, planted mission/skill, unknown tool, settings change, self-approval, kill-switch release, external draft. Gate: no unauthorised action, no protected data out, no state change from untrusted content |
| Windows platform | `tests/windows/test_windows_platform.py` | 2.4 named pipe handshake (good/bad token, unexpected core), **pipe squatting detection**, **pipe DACL** (NETWORK denied, no Everyone/Users), Job Object memory limit, parser under Job Object, **AppContainer has no network**, clipboard excluded from history, data-folder ACL, posture runs, TPM probe, Defender detects EICAR, firewall script parses, **real gateway + pa-core processes over named pipes** |
| E2E action catalogue | `scripts/e2e_runner.py` + `tests/e2e/scenarios.json` (175 steps), `tests/e2e/ui_actions.json` (80 UI controls), `tests/e2e/test_action_catalog.py` | every UI action -> backend RPC -> response + security-log events; coverage report in docs/ACTION_CATALOG.md |
| Chat organisation / themes | `tests/integration/test_chat_organise.py` | pinned + folder (DB v2), folder validation, theme/background settings |
| Bundle data | `tests/unit/test_bundle_data.py` | password/PIN lists load; the PyInstaller spec ships them |
| Portable mode | `tests/unit/test_portable_mode.py` | exact-path checks for UI/core in the portable folder; a source checkout is never portable |
| Firewall gate | `tests/unit/test_firewall_gate.py` | 14.2/35.8: missing rule or failed lookup blocks pa-core/worker start in release builds |
| End-to-end | `python -m pa_gateway --selftest` | setup → chat → reminder approval → scheduled → log verifies |
| UI | `npm run build` (tsc strict) + Playwright script used during development | type safety; screenshots in `docs/screenshots` |

### CI (GitHub Actions `windows-latest`)
`python` job: env check, ruff, full pytest incl. Windows tests and red-team N=20, selftest.
`ui` job: `npm run build`, `tauri build --no-bundle` (Rust compile of pa-ui.exe).
`bundle` job: PyInstaller executables + pa-ui.exe → artifact `PersonalAgent-windows-bundle`.

Last verified (v0.1.1, 2026-09-30): `python` 147 tests - all passed on Windows (incl. AppContainer no-network
and real gateway + pa-core over named pipes); `ui` passed (pa-ui.exe compiled); `bundle` built all
executables. pa-core's firewall self-test reports "outbound NOT blocked" on CI (no firewall rules there) -
that check is laptop item B1 after `install-dev.ps1`.

Laptop session 2026-09-30/10-01 (Windows 11 Home, Intel PTT TPM): 162 automated tests pass locally; ruff clean;
`npm run build` and `tauri build --no-bundle` OK; selftest OK; release bundle built (`dist\PersonalAgent`).
Laptop checklist A-G/P: NOT yet run - waiting for the user (TPM auth throttle must clear first; B5 impossible on Home).

### UI features (v0.1.4) - manual
- [ ] H1 Settings > Appearance & Voice: each theme card applies at once; light/dark text stays readable in all eight.
- [ ] H2 Each animated background runs smoothly (Aurora, Bubbles, Waves, Starfield); "Reduce motion" stops it.
- [ ] H3 "Day & night" switches at the times you set (change the day/night times to 2 minutes from now to see it).
- [ ] H4 Long chat: floating up/down buttons appear; down shows the count of new messages while scrolled up.
- [ ] H5 Type `/` in chat: menu opens; `/theme sunset`, `/remind call mum tomorrow 9`, `/history` work.
- [ ] H6 History: groups Today/Yesterday/…; search finds an old chat; click opens it.
- [ ] H7 Pin a chat (stays on top) and move one to a folder; both survive restart.
- [ ] H8 Times in chat/history use your PC's 12/24-hour setting.
- [ ] H9 Portable: copy `dist\PersonalAgent-Portable` to another folder, double-click `pa-ui.exe` only; the window opens and
      connects; Posture shows the "Portable mode" High finding.

### Profile names (v0.1.1) - laptop
- [ ] P1 Setup: enter "Your name" + "Name your assistant" → Home says "Good …, <your name>", sidebar and window
      title show the assistant name.
- [ ] P2 Lock (Win+L or Lock button) → lock screen says "<assistant> is locked"; cold start says
      "Welcome back, <your name>".
- [ ] P3 Settings → Account & Security → Profile: change both names → Save → UI updates at once; ask in chat
      "what is your name?" → the model answers with the new name.

## Laptop test checklist (manual / `-m laptop`)
`python scripts/laptop_smoke.py` runs the automated subset (throwaway dev vault, real TPM + Ollama, 21 checks, all passed
once on 2026-10-01; options `--software` (no TPM), `--wrong-pin` (one wrong PIN - never repeat runs: TPM budget). Items ticked below say how they were verified; unticked items still need a person.
Run these on your Windows 11 laptop after installing the bundle (or from source with `PA_DEV_MODE` **off**).
Tick them off in this file or in an issue.

### A. First run & keys
- [ ] A1 Wizard step 1 shows TPM **ready**, BitLocker status, sandbox strength, GPU.
- [x] A2 (automated, scripts/laptop_smoke.py 2026-10-01) Create account; weak passwords and 123456-style PINs are rejected.
- [x] A3 (automated, scripts/laptop_smoke.py 2026-10-01) Security Posture: "TPM 2.0 … PIN protector: tpm" is **OK** (not software).
- [ ] A4 Recovery key: Save as PDF works; Copy clears after 30 s; typing back 2 groups is enforced.
- [x] A5 (PIN unlock + 1 wrong PIN automated; Ctrl+Shift+L/Win+L manual, lockout A6 NOT tested to protect the TPM) Lock (button, Ctrl+Shift+L, Windows+L) → PIN unlock works; 5 wrong PINs disable the PIN until password.
- [ ] A6 TPM lockout: after several wrong PINs Windows TPM lockout message is shown (no crash).
- [x] A7 (password path only, scripts/laptop_smoke.py 2026-10-01) Reboot → password required (cold start). Idle auto-lock after the configured minutes.
- [x] A8 (automated, scripts/laptop_smoke.py 2026-10-01) Forgot password with PIN + recovery key → new password works → **new** recovery key shown, old one fails.
- [ ] A9 Copy `%LOCALAPPDATA%\PersonalAgent` to another PC/user → cannot be opened with PIN + recovery key.
- [ ] A10 Sleep with BitLocker off → keys wiped (password needed); with BitLocker on and default setting → UI lock only.

### B. Isolation (installed bundle, firewall rules present)
- [ ] B1 Posture: firewall rules present; **live outbound test** from pa-core.exe is blocked.
- [ ] B2 `Test-NetConnection 1.1.1.1 -Port 443` from a PowerShell started as pa-parser.exe context is not possible; verify with Resource Monitor that only pa-gateway.exe has outbound connections.
- [ ] B3 Another local Windows user cannot open the data folder (ACL) nor connect to the pipe.
- [ ] B4 A second copy of pa-ui.exe from a different folder is rejected by the gateway (image path check).
- [ ] B5 Windows Sandbox enabled → python.run shows **Strong isolation**; a script trying `socket.create_connection` fails; files written to `OUTPUT_DIR` appear in My Files.
- [ ] B6 Without Windows Sandbox (Home edition or feature off) → Standard isolation (AppContainer) works.
- [ ] B7 Built-in runtime: put `llama-server.exe` + a GGUF model; the model loads only if its SHA-256 matches; `curl http://127.0.0.1:<port>/v1/models` without the key → 401; five bad keys → restart on a new port + Home event.

### C. Models & voice
- [x] C1 (automated, scripts/laptop_smoke.py 2026-10-01) Ollama: add, Discover, Test model passes; exposure check says not reachable from the network.
- [ ] C2 vLLM / LM Studio / Run:ai (OpenAI-compatible) endpoint: add, test, chat.
- [ ] C3 Remote endpoint requires the password and shows "REMOTE" on Posture.
- [ ] C4 Speech-to-text (e.g. faster-whisper-server / vLLM Whisper, OpenAI-compatible `/v1/audio/transcriptions`): mic button records, text is sent, "remind me …" produces a confirmation card.
- [ ] C5 Reminder fires at the time: Windows toast + in-app banner + Home entry (also after the window was closed to tray).

### D. Connectors
- [ ] D1 Classic Outlook open: list folders, search, read message, save attachment (quarantined → READY).
- [ ] D2 Outlook closed + "Start Outlook when needed" off → task waits with "Outlook is closed".
- [ ] D3 Outlook security prompt (Object Model Guard) is shown by Outlook and never auto-clicked.
- [ ] D4 Microsoft 365: enter Client/Tenant ID, sign in via the browser, search mail, read calendar.
- [ ] D5 Turn M365 off during a mission → mission SUSPENDED within 2 s; web.fetch to graph.microsoft.com denied.
- [ ] D6 Enable Mail.Send → a send request shows an approval card with the exact email; CONFIDENTIAL context requires the password.
- [ ] D7 Web: configure Brave/SearXNG with the API key bound to `web.search`; search works; fetch of a non-allowlisted domain is denied.

### E. Autonomy
- [ ] E1 Routine "every weekday at 7:30 …" runs with the window closed and Windows locked; output appears in My Files and a toast.
- [ ] E2 PC off overnight → at next sign-in the missed run policy (RUN_ONCE) catches up once.
- [x] E3 (automated (API), scripts/laptop_smoke.py 2026-10-01) STOP ALL (button, tray, Ctrl+Alt+Shift+S) stops everything within 2 s; release needs the password.
- [ ] E4 Approvals accepted in < 2 s three times → fatigue warning on Home.

### F. Data
- [x] F1 (automated (backup+verify; restore on 2nd PC still manual), scripts/laptop_smoke.py 2026-10-01) Back up now to an external drive; Verify; restore on a second PC (new PIN + recovery key required).
- [x] F2 (automated, scripts/laptop_smoke.py 2026-10-01) Delete everything → app returns to the first-run wizard; old data unreadable.
- [ ] F3 SIEM: HTTPS endpoint with pinned fingerprint receives events; wrong fingerprint → error shown.

### G. UI
- [ ] G1 Secrets reveal: screenshot (Win+Shift+S) shows a black window while the value is visible.
- [ ] G2 Copy secret → Windows clipboard history (Win+V) does not contain it.
- [ ] G3 Keyboard-only navigation, dark/light theme, text size 140 %.
- [ ] G4 External link in an answer asks before opening the browser; remote images are never loaded.

### Button audit (2026-10-01, browser preview with the mock gateway)
Every button on Home, Chat, History, Missions, Reminders, Tasks, Approvals, My Files, Memory, Secrets, Activity and all
18 Settings sections, the theme cards / background chips, the top bar (search palette, STOP ALL menu, Lock), the lock
screen and wizard steps 1-3 was clicked by script: no JavaScript errors, every control produced a visible change, dialog,
toast or navigation (the only "no effect" cases were already-active tabs and empty lists). Not testable in the mock:
wizard steps 4-9 (recovery key etc.), microphone, native file dialogs, tray/hotkey, anything needing the real gateway.

### End-to-end action catalogue (2026-10-01)
`python scripts/e2e_runner.py` runs 175 scenario steps against a throwaway developer gateway (software protector, mock
model; Ollama steps run when it is reachable; `--tpm` uses the real TPM) and checks for every UI action the response
(`expect`), values that must never be returned (`absent`) and the security-log events it must write (`audit`). Result
2026-10-01: **175 passed, 0 failed**. It found two real bugs (K11 pipe accept loop, K12 low-memory error text).
`tests/e2e/ui_actions.json` maps every UI control to its RPC and expected visible result (walk it with the browser
tools in the `?demo` preview or the real app). `python scripts/gen_action_catalog.py` regenerates
`docs/ACTION_CATALOG.md` (137 RPCs: 126 covered, 11 manual-only with reasons, 0 gaps) and `test_action_catalog.py`
fails CI if a new RPC has neither a scenario nor a manual-only reason.

### H10 Motion and accessibility review (manual, in the real window)
- [ ] Open/close Emergency stop, Ctrl+K palette, `/` menu in chat: each fades/springs in AND out; reopening mid-close is fine.
- [ ] Dialog grows from the button that opened it (e.g. STOP ALL, a file row).
- [ ] Delete a chat / file / memory / cancel a mission: item disappears, "Undo" toast for 8 s; Undo brings it back;
      without Undo it is really gone (also after Lock and after closing the window within 8 s).
- [ ] Windows Settings > Accessibility > Visual effects > Animation off (and Contrast themes): UI uses short fades only, no pulsing.
- [ ] Every theme: read faint text, buttons, badges - nothing hard to read. Settings > Appearance: background Off by default.
- [ ] Breadcrumb in the top bar and window title follow the screen; restart keeps the last screen.

### H11 Dev-mode walkthrough (scripts\run-dev.ps1, no TPM)
- [ ] Chats list closed by default; menu button opens it; picking a chat closes it; + starts a new chat.
- [ ] Steps panel closed by default; Steps button opens it.
- [ ] Guide (below Activity log): search 'lock', 'undo', 'theme colour', 'STOP ALL' show relevant entries; Open buttons navigate.
- [ ] Settings > Appearance: 5 accent colours + theme default recolour buttons in every theme (light and dark).
- [ ] 'Remind me in an hour' shows the local time (compare with the taskbar clock).

### H12-H16 Real-window checks for the 2026-10-01 evening features
- [ ] H12 Outlook: Settings > Connectors > Local Outlook on (use in missions) > Settings > Email monitoring: switch on "Hourly inbox check" and
      "Emails waiting for my approval", press Run now on each: results appear on the Outlook screen within ~1-2 minutes; badge counts unseen results;
      switch off -> they stop; "Show the Outlook button" off hides the menu button.
- [ ] H13 Chat: "how many emails did I receive today and in how many am I in To?" gives numbers that match Outlook (mail_stats), "emails waiting for my approval" lists real ones.
- [ ] H14 Attach a big Excel file with the paperclip (or drag it from the Desktop): chip appears; ask "total of <column> per <column>"; first answer may take 1-2 minutes
      (indexing), later answers seconds; Remove on the chip stops access; delete the chat -> access gone. File on disk unchanged.
- [ ] H15 Activity log > Network Logs: web fetches, blocked sites (red), model calls (this PC) with times in local time; Copy as CSV works.
- [ ] H16 GPU meter above Settings moves when the model answers; Alt+C/H/I/M/R/T/A/F/E/S/L/O/G/comma and F1 navigate; picture upload shows in the top bar / next to your name.
- [ ] H17 Voice with the real microphone (needs the speech model added: Settings > AI Model > Discover > frozenlab/qwen3-asr:1.7b > Add model, wait for "tested"): Chat > mic, speak 5 s, stop -> text appears; speak 2 minutes -> text appears (pieces joined); Windows microphone permission prompt handled.
- [ ] H19 Add model: Discover > click a chat model > Add model: row shows "testing..." then "tested" without pressing Test model; it can be used in chat at once. A model that fails stays "not tested" and gets a warning toast.
Automated: `tests/unit/test_audio.py`, `tests/integration/test_stt_models.py` (fake Ollama: auto-test, default only if unset, chunking, WAV-only, inspect), `scripts/live_asr_check.py` (REAL Ollama + Qwen3-ASR + a clip spoken by Windows: passed 2026-10-02, 6 s test, 0.4 s transcript).
- [ ] H21 Network Logs > Copy as CSV, paste into Excel: no cell is evaluated as a formula (cells that start with = + - @ show a leading apostrophe).
Security tests (2026-10-02, details docs/SECURITY_AUDIT_2026-10-02.md): `scripts/corpus_dast.py <corpus>` (217 files, expect 0 FAIL, 0 benign false positives), `scripts/pentest_rpc.py` (expect FINDINGS: none),
`tests/integration/test_{upload_hardening,ipc_robustness,role_matrix}.py`, `tests/unit/test_{winpaths,egress_exotic,audio}.py`.
- [ ] H22 Rename/pin: hover a chat > pencil (or double-click the title / F2 / `/rename Budget`): name changes in list, header and History; Esc cancels; empty name is refused; pin in header moves the chat to "Pinned".
- [ ] H23 Notifications (needs rebuilt pa-ui.exe): create a reminder "Call the dentist" for 1 minute ahead and minimise the window: the Windows toast title is "Call the dentist", text "Reminder"; a routine "Run now" toast is titled with the routine name; with the chat window in front no toast appears for answers, with another app in front a toast titled with the chat name appears; Settings > Notifications > Show names off -> generic titles.
- [ ] H24 Position rail: open a chat with 5+ questions: thin lines at the right edge, dark one follows the scrolling; hover opens the list, click jumps, Up/Down move; hidden when the window is narrow; works in every theme. (Checked in the browser preview 2026-10-02: 20 ticks, hover list, click-jump, scroll tracking matched the visible question.)
- [ ] H25 Settings > AI Model: four cards. Add `frozenlab/qwen3-asr:1.7b` under Voice models (Discover shows only voice models there; trying it under Chat models is refused), add `qwen2.5vl:7b` or `qwen3-vl` under Vision models; each becomes "tested" by itself and the default of its own kind only.
- [ ] H26 Pictures/scans: attach a photo or screenshot of text (paperclip, drag, Ctrl+V) and a scanned PDF: the text is read; without a vision model you get the hint and "Read with vision model" in My Files after adding one.
- [ ] H27 Folder sharing: folder button > dialog shows file/subfolder counts, subfolders unticked; ask "what is in the folder?" works; ask for a file in a subfolder: card "Allow subfolders" appears; sign out and in: chip says "Allow again"; a new chat has no access; AppData / C:\ / your user folder are refused.
- [ ] H28 Live voice: dictate for 2-10 minutes (the counter shows pieces transcribed); text appears while speaking; press Stop: only the last piece is awaited; close the window mid-way and reopen: unsent text is restored.
- [ ] H29 Fonts: Settings > Appearance & Voice > pick several fonts and Small/Medium/Large: every screen stays tidy (no cut-off or overlapping text); Aptos shows "not installed" unless Office/Windows has it.
Automated 2026-10-02 (second part): `test_model_kinds.py`, `test_vision_pipeline.py`, `test_local_folders.py` (25: traversal, junction, session expiry, subfolder approval ...), `test_upload_hardening.py` (37), `test_font_settings.py`; live: `scripts/live_vision_check.py` (qwen2.5vl:7b: image and scanned PDF 3/3 key words in 4 s), `scripts/live_asr_long.py 10` (see VERSION_HISTORY 0.1.11); preview: Segmenter/Resampler tests with synthetic 10-minute audio, font x size x screen sweep.
- [ ] H30 After installing the new bundle: upload a few files; if your antivirus cannot be asked they wait in My Files > Quarantine with the banner: "Allow without antivirus scan" asks for your password and moves the file to Files marked "NOT antivirus-scanned".
- [ ] H31 Chat: ask for a web search: an approval card "Allow web access for this chat" appears once; the second search in the same chat does not ask; a new chat asks again.
- [ ] H32 Share the `corpus` folder and ask "analyse the contents": the assistant uses localfile.digest and answers; no "model_empty" (if it still happens, tell me the model name).
- [ ] H33 Search History for part of a word ("expl"): finds your question; the x in the search box clears it. Token ring in the chat header; Activity > Usage & budgets; no "gateway" badge; ChiRAG icon in the title bar, taskbar, tray and on the desktop shortcut.
Automated 2026-10-02 (third part): `test_empty_rescue.py`, `test_history_search.py`, `test_unscanned_files.py`, `test_web_per_chat.py`, digest test in `test_local_folders.py`, `test_icons.py`, `test_all_modules_compile.py`; live: `scripts/live_chat_check.py` (real qwen3-coder + real Outlook: no empty replies in 4 runs).
- [ ] H34 Memory learns: chat "I work in finance and prefer short bullet-point reports" -> within a minute Memory shows "learned from your chats" items; Forget one -> say it again -> it does not come back; toggle Learn automatically off -> nothing new appears.
- [ ] H35 My Files: upload a PAN card scan/PDF: after a few seconds the row shows "PAN card" + summary; the file is CONFIDENTIAL; File details > Summary is editable (typing the PAN number there is refused); ask the assistant "show me my PAN card" -> it finds and shows the file; Memory has "My Files has 'PAN card...' - values are in the file".
- [ ] H36 Home: Widgets button on the right: switch widgets on/off and reorder; mail widgets fill in after a few seconds (Local Outlook must be on); Refresh re-reads Outlook; reload the app: the choice is remembered.
- [ ] H37 Update with ONE command (see the instructions in the chat / installer\windows\update-app.ps1).
Automated (fourth part): `test_memory_learning.py`, `test_file_insights.py`, `test_home_widgets.py`; live: `scripts/live_memory_check.py` (real qwen3-coder).
Performance (scripts/perf_tables.py, this PC): 99 MB .xlsx, 1.5 million rows x 12 columns: first index 62 s; group-by 0.7 s; filter 0.5 s; cache 234 MB.

### Live check of the email skills (scripts/live_outlook_check.py, 2026-10-01)
Real classic Outlook (~700 mails/day, 112 folders) + real Ollama model (gemma4:latest), throwaway data folder, software key, read-only. All ten skills
produced structured, correct-looking results (inbox_hourly 64-76 s, approvals 72 s, deadlines 48 s, morning_brief 24-36 s, meeting_prep 36 s (found today's meeting),
followups 20 s, reply_needed 20 s, cleanup_report 24 s, weekly_summary 56 s, vip_alert 12 s). Problems found and fixed on the way:
(1) Outlook read `10/01/2026` day-first -> locale-safe dates; (2) thinking models returned EMPTY replies (answer budget spent on hidden reasoning) -> `reasoning_effort:none`
for Ollama + "model_empty" error; (3) the model answered "..." -> placeholder answers rejected; (4) the model skipped tool calls / date logic -> the first data-gathering
calls now run in code before the model starts (`prefetch`), deadline filtering is done by the tool.

### Status at the end of the 2026-10-01 session
pytest 257 passed / 1 environmental failure (clipboard: Windows denies OpenClipboard when the screen is locked or another app holds it) / 1 skipped; e2e `scripts\e2e_runner.py` 187/187; ruff and `tsc` clean;
UI behaviour checked in the browser preview (`/?demo`) with scripted DOM checks; real Outlook + real Ollama via `scripts/live_outlook_check.py`; 99 MB workbook via `scripts/perf_tables.py`.
NOT yet verified in the real window (needs the new bundle installed): H11-H16 above. Never test against the real TPM (CLAUDE.md).

- [ ] H20 Exa web search: Secrets > Add (type API key, value = your Exa key, Used by: web.search) > Settings > Web Access > Search provider exa (password) > Settings > Connectors > Web on > Test web search shows "works"; then ask in chat "search the web for ...".

### 0.1.14 - pending on the laptop (real window; built in the cloud, checked in the browser preview + Windows CI only)
- [ ] H40 App logo: the top-left logo in the window, the title bar icon (top-left of the window frame) and the taskbar all show the new ChiRAG icon. If the top bar still shows an old picture: Settings > Appearance & Voice > Assistant icon > Remove (a custom picture you uploaded replaces the logo).
- [ ] H41 Settings > Rules & Safety: four headed sections (What new data counts as / Sending data to the internet / Text that must never leave this PC / Routines started by new mail); choices read "Allowed / Ask me first / Never"; change one, see it in Change history with readable names, press Revert.
- [ ] H42 Settings > Diagnostics & About > Resource use: CPU, Memory, GPU, Storage bars move every 3 s; the darker part is this app; open a heavy program and see a bar turn orange (75 %) / red (90 %); "Show this app's processes" lists pa-gateway, pa-core, pa-ui (and workers when running).
- [ ] H43 Settings > AI Model: under qwen3-coder press Adjust > 32k + temperature 0.1 > Save; the row shows "Context 32,768 tokens · Temperature 0.1". Ask in chat about a long document: in Activity log > Network Logs the model call says "context 32768". In a PowerShell window `ollama ps` shows the model with the larger context while it runs. "Use defaults" brings back "(default)".
