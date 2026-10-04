# Handoff - read this first in a new session (written 2026-10-01, end of a very long session)

Everything a new Claude Code / Codex session needs to continue without the old chat. Read, in this order: this file, `CLAUDE.md` (rules),
`PROGRESS.md` (log), `PENDING_WORK.md` (what is open), `TESTS.md` (how it is verified), `docs/DEVIATIONS.md`, then the spec
`docs/spec/personal_desktop_agent_spec_v1_1.txt` for any security question.

## 1. What this project is
"Personal Agent" (the user renamed the product text to **ChiRAG Agent**; folders, exe names, data folders keep the old names): a single-user, local-first, security-first
AI agent for Windows 11. React + TypeScript + Tauri 2 window `pa-ui.exe` -> named pipe -> Python `pa-gateway` (vault, TPM PIN protector, policy engine, tool gateway,
DLP, egress filter, approvals, budgets, kill switch, HMAC-chained audit log, SQLCipher DB) -> `pa-core` (agent loop, no keys, no network) and workers
(`pa-parser`, `pa-outlook-worker`, sandbox). The model proposes, the gateway decides. Spec rules are in `CLAUDE.md` (never log/print/send secrets, PINs, passwords, recovery keys, tokens).
Project folder: `F:\Personal_Agent\personal-agent` (git branch `claude/elegant-gates-cyfi1y`, ~25 commits, **nothing pushed - push only when the user says**).

## 2. The user and their PC (facts that matter)
- Windows 11 Home, user `sendm`, India locale (Outlook/Windows read dates day-first), laptop with NVIDIA RTX 5080 (16 GB) + Intel graphics, TPM 2.0.
- **Ollama** at 127.0.0.1:11434 with gemma4:latest (thinking model, used for tests), qwen3-coder, qwen2.5vl:7b, llama-guard3, granite3-guardian.
- **Classic Outlook** (Office 16) installed and in use, ~700 mails/day, 112 inbox sub-folders (rules file mail into sub-folders); new Outlook also installed (cannot be read).
- Antivirus: **McAfee active, Microsoft Defender OFF** -> the app's file scan (Defender MpCmdRun) cannot run, uploads are rejected as "not scanned" (K14, decision pending).
- **Smart App Control was turned OFF by the user** (it blocked unsigned exes/Rust build scripts; cannot be re-enabled without resetting Windows). Code signing is still P1 in PENDING_WORK.
- **TPM per-user throttle** (0x80290409) was caused by earlier tests with the real TPM; creating NEW keys was still blocked at 14:43 on 2026-10-01 (heals in 10-30 min .. 24 h). Never run tests against the real TPM, never try wrong PINs on purpose.
- The user's **real, installed app** (C:\Program Files\PersonalAgent, started by two logon tasks "PersonalAgent Gateway"/"PersonalAgent Tray") has a real account in `%LOCALAPPDATA%\PersonalAgent`. For a long time it was the OLD build (0.1.1 code): none of the later fixes reached it. The new bundle `dist\PersonalAgent` must be installed with **admin** (`installer\install-dev.ps1`; stop the two scheduled tasks and all pa-* processes first, otherwise `libcrypto-3.dll` is locked and the copy fails - see section 6). Last known state: the install was being retried after that DLL-lock error.
- The user works in dev mode for testing: `scripts\run-dev.ps1 [-Fresh]` (software key, data in `%LOCALAPPDATA%\PersonalAgent-dev`; an older dev account was moved to `PersonalAgent-dev-old-20261001-143152`). Use throwaway credentials there.
- The user is a beginner: explain in simple steps, ask before admin/firewall/delete/install actions (rules from the first brief: never elevate yourself, never delete data folders without asking, keep dev data separate, commit after each finished step, push only on request, log in PROGRESS.md, update md files with every change).

## 3. State at the end of this session (all committed, last commit bf61680 + docs commit)
Versions are still 0.1.1 in manifests; changelog is 0.1.2 .. 0.1.7 in `VERSION_HISTORY.md`. Tests: **257 pytest pass** (1 environmental failure: `test_secure_clipboard_excluded_from_history` when Windows denies
OpenClipboard), **e2e runner 187/187** (`python scripts\e2e_runner.py`), ruff clean, `tsc` clean. Built artifacts: `dist\PersonalAgent` (PyInstaller bundle, current), `dist\PersonalAgent-Portable` (current), `app\ui\src-tauri\target\release\pa-ui.exe`.

Delivered in this session (details in VERSION_HISTORY / PROGRESS):
1. Environment, test suite, release bundle, installer fixes, firewall enforcement, supply chain (locks, audits, SBOM, CodeQL), portable copy-and-run build, pipe accept-loop lock-out fix (K11), laptop smoke script.
2. UI: 8 themes + 5 accent colours (WCAG AA tested), animated backgrounds (off by default), History, slash commands, pinned/folder chats, usage meter, local-time labels, scroll buttons,
   Apple-design pass (spring motion with exit animations, press states, soft edges, reduced motion/transparency/contrast, Undo for deletes), Guide screen with search (60+ entries),
   collapsed chat list + steps by default, breadcrumbs, picture upload (assistant icon / user picture), Alt+letter shortcuts, GPU meter, orange warnings / red errors, wizard hints.
3. Chat/agent reliability: lenient tool-call parsing, constrained JSON reply schema for Ollama, `reasoning_effort:none` (thinking models returned empty replies), placeholder answers rejected, never show raw JSON, local time + zone in the prompt, proposals from the agent visible in Approvals.
4. Routines: schedule picker (once a day / several times a day / days / hours), 8 output formats, web-search setup checks (no run without a provider), readable schedule text.
5. Outlook: locale-safe dates (Jet filter with the Windows short-date pattern), table-API based fast listing, tools `outlook_local.digest/mail_stats/awaiting_reply` with deterministic flags
   (APPROVAL/DEADLINE/URGENT/QUESTION, To vs CC, replied or not), **10 email-monitoring skills** (Settings > Email monitoring, Outlook screen) whose data-gathering tool calls run in code before the model (`prefetch`);
   verified live on the user's real mailbox with gemma4 (`scripts/live_outlook_check.py`).
6. Observability/files: central **network log** (Activity log > Network Logs), **local files read in place** (paperclip / `/attach` / drag-drop; xlsx/csv/docx/pdf/txt; SQLite index; 99 MB xlsx = 62 s first, ~1 s later; `scripts/perf_tables.py`).
7. Docs: all md files kept current; `docs/ACTION_CATALOG.md` generated (142 RPCs, 0 gaps).

## 3b. Session 2026-10-02 (everything committed, last commit see `git log`)
Voice with Ollama speech models (Qwen3-ASR, proven live: `scripts/live_asr_check.py`), 'Add model' tests itself in the background + becomes default if none, `llm.inspect` model-type detection,
Exa web search ("Test web search" button), security audit round (`docs/SECURITY_AUDIT_2026-10-02.md`, scripts `corpus_dast.py` / `pentest_rpc.py`, 11 findings fixed incl. non-English uploads rejected),
rename/pin any chat (F2, /rename), named Windows notifications (`notifications.show_names`, focus-aware chat toasts), position rail (`ChatRail.tsx`), generated settings round-trip test.
Tests: 359 pytest (only the OpenClipboard environment failure), e2e 188/189 (same cause). New bundle: `dist\PersonalAgent-0.1.10` (pa-ui.exe included); `dist\PersonalAgent` is the OLD one; the portable folder was not rebuilt.
Tooling gotcha repeated: heredoc patches corrupt backslash escapes - write patch scripts with the Write tool and check for control bytes.

## 3c. Second part of 2026-10-02 (also committed)
Model kinds split into four cards with server-side enforcement; vision pipeline (`vision.py`, qwen2.5vl:7b proven live: `scripts/live_vision_check.py`); folder sharing with per-chat + per-session + subfolder approvals (`localfiles.py`, 25 tests, DB migration 4);
live voice in 30 s pieces (`voiceStream.ts`, `VoiceButton.tsx`; 10-minute proof `scripts/live_asr_long.py`); file sanity round 2 (`files/checks.py`); 20 fonts + Small/Medium/Large (`fonts.ts`). Build the bundle again before installing (`scripts\build.ps1 -OutDir dist\PersonalAgent-0.1.11`). Tests: 424 pytest, e2e 198/199 (clipboard).

## 3d. Review round (VERSION_HISTORY 0.1.12)
Empty-reply rescue (`_run_with_rescue`), history search prefix + fallback, AMSI + `files.release_unscanned` + `files.accept_unscanned`, per-chat web approval (`web.ask_per_chat`), `localfile.digest`, usage ring, Home cleanup, ChiRAG icon (`scripts/make_icons.py`) and installer shortcuts. Live scripts: `live_chat_check.py`. Build: `scripts\build.ps1 -OutDir dist\PersonalAgent-0.1.12` after `npx tauri build --no-bundle`.

## 3e. Memory / summaries / widgets round (VERSION_HISTORY 0.1.13)
`memory_learn.py`, `files/insights.py`, `pii.py`, `home_widgets.py`, `Home.tsx`, `FileMeta.tsx`, `installer\windows\update-app.ps1`. Live scripts: `live_memory_check.py`. Build: `npx tauri build --no-bundle` then `scripts\build.ps1 -OutDir dist\PersonalAgent-0.1.13` and copy pa-ui.exe in.

## 4. Open items, most important first (full list in PENDING_WORK.md)
1. **Install the new bundle on the user's PC (admin) and have them verify H11-H16 in `TESTS.md` in the real window** (new screens were only seen in the browser preview).
2. TPM: confirm key creation works again (`scripts` probe snippet in this file's section 6) before any real first-time setup; do not hammer it.
3. Decide K14 (Defender off / McAfee): keep Defender active, add an adapter for the other AV, or an explicit "scan unavailable" hold mode.
4. Web search needs a keyed provider (Exa/Brave/Tavily/SearXNG); decide whether a keyless option is acceptable (egress/privacy review).
5. Code signing / MSI / signed updates (P1); AppContainer for pa-core in portable mode (item 0); P2/P3 spec gaps.
6. Push to GitHub only when asked, then check CI (`windows-ci.yml`, `security.yml` have never run).
7. Possible next features: save local-file query results as CSV/XLSX, charts, more languages for approval/deadline wording, user-configurable shortcuts, per-skill schedule editor in Email monitoring, resizable panes.

## 5. How to work here (commands)
```powershell
cd F:\Personal_Agent\personal-agent
.\.venv\Scripts\python.exe -m pytest -q -p no:cacheprovider                 # backend tests (set PA_DEV_MODE etc. by conftest; software key only)
.\.venv\Scripts\python.exe scripts\e2e_runner.py                             # 187 end-to-end steps against a throwaway gateway (must stay all PASS)
.\.venv\Scripts\python.exe -m ruff check app tests scripts --select E,F,W,B --ignore E501,B905,B007,B904
cd app\ui; npx tsc --noEmit; npx vite --port 5199 --strictPort            # then browse http://127.0.0.1:5199/?demo (mock gateway; preview pane is hidden -> visibilityState trick, see section 6)
cd app\ui; $env:Path += ";$env:USERPROFILE\.cargo\bin"; npx tauri build --no-bundle   # pa-ui.exe (needs Smart App Control OFF or a signed build)
powershell -File scripts\build.ps1 -OutDir dist\PersonalAgent               # PyInstaller bundle (release flags are set during the build and restored in buildinfo.py)
powershell -File scripts\build-portable.ps1                                  # portable folder (no admin)
powershell -File scripts\run-dev.ps1 [-Fresh]                                # dev mode window, no TPM, separate data
.\.venv\Scripts\python.exe scripts\live_outlook_check.py gemma4:latest approvals   # REAL Outlook + Ollama; LIVE_SHOW=400 shows result text (contains mail!), LIVE_VIPS="Name;Name"
.\.venv\Scripts\python.exe scripts\perf_tables.py 1500000                    # 99 MB xlsx timing
```
Adding an RPC requires an e2e scenario (or manual-only reason) AND a `ui_actions.json` control: `python scripts\gen_action_catalog.py` (CI test `test_action_catalog.py`). New Guide-worthy features need a Guide entry (`test_guide_coverage.py`).
Security-sensitive changes need tests in the same change; never weaken a control to make a test pass (ask the user).

## 6. Gotchas learned the hard way
- **Editing files from this assistant**: backslash sequences inside tool-written Python/heredoc scripts get unescaped (`\\n` became a real newline, `\\server` broke); use the Write tool for scripts, build strings with `chr(92)` or the Edit tool for lines containing backslashes. Files are CRLF in git; helper scripts preserve the original newline style. Never pass a heredoc to a bare `python -` (it hangs waiting for stdin).
- Commit identity: `git -c user.name=dev -c user.email=backupsendmemail9030@gmail.com commit ...`; commit message ends with the `Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>` line.
- `app/pa_common/buildinfo.py` must stay at `RELEASE_BUILD = False`, `BUILD_HASH = "dev"` in git (the build script overwrites and restores it; an old commit had release flags by mistake - fixed in the docs commit).
- Single-instance lock: the installed gateway holds `Local\PersonalAgent-Gateway`; dev/test gateways started with `--data-dir` use their own lock (hash of the data dir). The UI starts the gateway itself if `run\gateway.json` is stale/missing.
- Installing over a running app: stop scheduled tasks first (`Stop-ScheduledTask`/`Disable-ScheduledTask` for "PersonalAgent Gateway" and "PersonalAgent Tray"), kill `pa-ui pa-gateway pa-core pa-outlook-worker pa-parser`, wait, run `installer\install-dev.ps1`, start the gateway task, then `pa-ui.exe`. Killing a task's process makes Task Scheduler restart it after 1 minute.
- Browser preview pane is "hidden" for visibility: `document.visibilityState` is 'hidden', requestAnimationFrame is paused and transitions do not run; override with `Object.defineProperty(document,'visibilityState',{get:()=>'visible'})` to test polling code.
- Outlook COM: date literals follow the Windows regional format (`mailops.ol_date`); `ConversationID` is not a table column (use ConversationIndex first 44 hex chars); body text searches via DASL LIKE take minutes (filter in Python); `GetTable` is ~100x faster than item loops.
- Ollama thinking models need `reasoning_effort: none` for the JSON agent loop; constrained `json_schema` is requested for ollama/builtin with automatic fallback to json_object/none.
- TPM probe (read-only, creates and deletes a test key; blocked = still throttled):
  `from pa_gateway.vault.protector import TpmProtector; p=TpmProtector(); d=p.create("483920", b"x"*32); p.destroy(d)` (run with `.venv\Scripts\python.exe` from `app`).
- Defender off -> uploads rejected (K14); tests/e2e set `PA_SKIP_DEFENDER=1` (dev mode only) / conftest monkeypatches the scanner; `PA_TEST_REAL_DEFENDER=1` uses the real one.

## 7. New code map (this session)
`pa_gateway/netlog.py` (network log) · `pa_gateway/sysmon.py` (GPU meter) · `pa_gateway/localfiles.py` + `pa_workers/parser/tables.py` (local files) ·
`pa_gateway/agentdata/email_skills.py` + `pa_workers/outlook/mailops.py|mailscan.py` (Outlook skills) · `pa_core/actions.py` (lenient parser) ·
UI: `screens/Outlook.tsx`, `NetworkLogs.tsx`, `Guide.tsx`+`guideData.ts`, `settings/EmailMonitoring.tsx`, `components/{GpuMeter,IconUpload,EmailSkillList,SchedulePicker,motion}.tsx`, `shortcuts.ts`, `polish.css`, `accents.css`, `themes.css`.
Tests added: `tests/unit/test_{mailscan,tables,sysmon,theme_contrast,guide_coverage,schedule_describe,actions_parse}.py`, `tests/integration/test_{email_skills,local_files,network_log,missions_setup,sloppy_tool_call}.py`, `tests/e2e/*`.
