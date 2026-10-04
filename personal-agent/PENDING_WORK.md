# Pending work

## Next session start here (2026-10-02)
1. Install the NEW bundle `dist\PersonalAgent-0.1.10` (admin; docs/BUILD_AND_RELEASE.md "Installing a new bundle over a running install"; it has the voice, security, rename, notification and rail changes and a rebuilt pa-ui.exe) and run the real-window checks H11-H24 in TESTS.md; fix what they find. Ask the user how the last install attempt (libcrypto-3.dll in use) ended.
2. Voice: add the speech model in the app (Settings > AI Model > Discover > frozenlab/qwen3-asr:1.7b > Add model; it tests itself) and test the real microphone (H17). Exa: user stores the key themselves (Secrets, used by web.search), provider exa, "Test web search" (H20).
3. Decisions still needed from the user: K14 (Defender off / McAfee), permission for the TPM re-check (rule: never test against the real TPM), code-signing certificate, permission to install external scanners (bandit, semgrep, pip-audit, gitleaks) for the next SAST round.
4. Then: MSI, signed updates, AppContainer for portable mode, P2/P3 items below. Do not push to GitHub until the user says; then check CI.

## Open after the 2026-10-03 round (memory, summaries, widgets)
- Real-window checks H34-H37 (TESTS.md).
- Memory learning only looks at what you TYPE; learning from the assistant's work on your mail (e.g. frequent contacts) is deliberately not done (mail text is untrusted). Could add counted facts (top senders) if wanted.
- Home widgets for calendar/meetings today are not built (needs the Outlook calendar op in the widget refresh); widgets cannot be resized, only ordered.
- File summaries of very large files use the first 6000 characters; OCR text of pictures is used when a vision model exists.

## Open after the 2026-10-02 review round
- AMSI works only if the installed antivirus registers with it; on this PC (McAfee) uploads need the explicit release or the setting. A command-line scanner adapter for McAfee does not exist; enabling Defender in passive mode is not possible while McAfee is active.
- Model empty replies could not be reproduced with qwen3-coder (4 live runs); the rescue path is tested with fakes. If it still happens, send the model name and the Steps of the failed run.

## Done 2026-10-02 (second request): model kinds, voice pieces, fonts, vision, folder approvals, file sanity - see VERSION_HISTORY 0.1.11. Still open from it:
- Real-window checks H25-H29 (TESTS.md); the microphone itself (WebView2 permission) is untested outside the preview.
- Scanned PDFs work when the pages are JPEG scans; other page encodings (CCITT/Flate/JBIG2) need a PDF renderer (pypdfium2) - needs the user's approval to add the dependency.
- Vision for TIFF/BMP/HEIC and OCR of pictures embedded in DOCX/PPTX are not done.
- Folder sharing: approvals could also be time-limited (e.g. 8 h) if wanted; no per-folder file-type filter.
- Audio piece boundaries use a fixed quiet-moment rule; a real speech VAD would cut more precisely.

## Requested by the user 2026-10-02 (planning only - NOT started, do not code until the user says)
R1. (DONE 2026-10-02; real-window check H22) **Rename and pin any chat**: every chat (not only some) can be renamed and pinned/unpinned from the chat list, the chat header and the History screen
    (check what exists today: pins and folders exist; confirm rename works for every chat type, incl. routine/mission chats). Needs GUI control + Guide entry + mock + e2e + `ui_actions.json` entry.
R2. (DONE 2026-10-02; needs pa-ui.exe rebuild + check H23; click-to-open is not possible with the Windows toast plugin, so the toast carries names only) **Meaningful Windows notifications**: a Windows toast must show the name of the task / reminder / routine / chat session it is about (e.g. "Reminder: Call the bank",
    "Routine: Hourly inbox check", "Chat: <chat name>") instead of a generic "Windows alert". Study how Claude (desktop/Code) does it first: title = session/task name,
    body = short status line, click opens that exact screen/chat. Must obey CLAUDE.md rule 12 (notifications only open a screen via `ui.open_link`, never carry secrets/tokens)
    and the DLP/sensitivity rules (no confidential mail content in a toast; title is the name only, quiet-hours respected).
R3. (FIRST ROUND DONE 2026-10-02, see docs/SECURITY_AUDIT_2026-10-02.md; remaining: external scanners need install approval, installed-app checks, web-view XSS browser test, model-port/OAuth/sandbox fuzzing) **Security assessment "like a hacker"** (authorized, own app, own PC only; use dev data dir `PersonalAgent-dev`, never the real TPM or the real account):
    a. **SAST**: ruff + bandit, semgrep (Python/TS/Rust rules), CodeQL results (workflow exists, never run), `cargo clippy`/`cargo audit`, `npm audit`, `pip-audit`, secret scanning (gitleaks/trufflehog) over the full git history.
    b. **DAST**: run a throwaway gateway + UI in dev mode and attack it: named-pipe fuzzing (malformed frames, oversize, role confusion, pa-core role calling UI-only RPCs, replay), the 127.0.0.1 model-runtime port,
       the M365 OAuth loopback callback, prompt-injection red-team (mail/web/file content trying to call tools, exfiltrate via URLs, change settings), egress/SSRF bypass (redirects, DNS rebinding, IPv6/decimal IPs),
       DLP bypass, path traversal/symlink/junction/ADS/UNC in local files and My Files, zip/xlsx/docx/pdf bombs and malformed files in pa-parser, sandbox escape attempts, approval/step-up bypass, kill-switch bypass,
       audit-log tampering (HMAC chain), DB at rest (SQLCipher file without key), DLL search-order/side-loading and install-folder ACLs (C:\Program Files\PersonalAgent, logon tasks), Tauri webview (CSP, IPC allowlist, XSS via markdown/HTML in chat, `ui.open_link`).
    c. **Threat-model + manual audit** against the spec (docs/spec) and CLAUDE.md rules 1-16; write findings to a new `docs/SECURITY_AUDIT_<date>.md` (severity, proof, fix, test) and add each fix with a regression test.
    Needs from the user first: scope confirmation and permission before installing any scanner tool (rule: ask before installing software); no admin/firewall changes without asking.

## Requested by the user 2026-10-02 (2): voice model + chat position rail (planning only - NOT started)
R4. (DONE 2026-10-02 in code + live test; only the real-microphone check H17 and the Exa key remain) **Use the local ASR model `frozenlab/qwen3-asr:1.7b` (Ollama, 2.5 GB, Apache-2.0) for voice input.** Findings from the study:
    - Today's `LlmService.transcribe()` (`app/pa_gateway/llm/service.py`) posts to `<base>/audio/transcriptions` (OpenAI/Whisper style). **Ollama has no such endpoint**, so this model cannot work with it as is.
    - This model (per its Ollama page) takes audio through **`POST /api/chat`** with `{"model": ..., "stream": false, "messages":[{"role":"user","content":"","images":["<base64 16 kHz mono WAV>"]}]}`;
      limits: ~60 s per request (quality drops after ~2 min), 52 languages with auto-detect; the reply starts with `language <Name><asr_text>` which must be stripped. It needs an Ollama build with the `qwen3a` audio projector
      (this PC reports Ollama 0.35.0; the model's metadata says architecture qwen3vl + a second blob = audio encoder). **Not yet proven to work on this PC** - first step is a 5-second read-only probe with a WAV (localhost only).
    - Plan: (1) add a provider/"protocol" choice for STT models: `openai_audio` (existing) and `ollama_chat_audio` (new; Settings > AI Model > add model: kind Speech-to-text, runtime Ollama, name `frozenlab/qwen3-asr:1.7b`, location this PC);
      (2) the window records `audio/webm` (VoiceButton) -> convert to 16 kHz mono WAV **before** sending: in the UI with the Web Audio API (`AudioContext` decode + resample + WAV encoder) or in the gateway; chunk recordings > 55 s and join the text
      (the 120 s recording cap in VoiceButton stays); (3) strip the `language X<asr_text>` prefix, keep detected language; honour `voice.language` only if the model accepts it; (4) "Test model" for stt must really transcribe a built-in 2 s test clip (today it only pings the server);
      (5) audit `stt.request` (bytes, model, location - never the audio or text), network log entry for the localhost call, no audio stored; (6) friendly errors: model missing / Ollama too old ("update Ollama") / clip too long;
      (7) tests: unit (prefix stripping, WAV conversion, chunking), integration with a fake Ollama `/api/chat`, e2e scenario `voice.transcribe_ollama`, Guide entry, `mock.ts`, `ui_actions.json`; manual test H17 with the real mic.
    - Security notes: audio stays on this PC (location "this PC" only; refuse non-loopback unless the user explicitly allows, as for other models); model passes the same "Test model" gate as other models; a model with extra capability must not widen policy (rule 16).
R5. (DONE 2026-10-02; real-window check H24) **Chat position rail (like ChatGPT's thin lines at the right edge)** - study result: ChatGPT shows a column of short horizontal ticks fixed to the right edge of the conversation, one tick per
    question/answer turn; the tick of the turn currently on screen is dark and bold, the others are light grey; the rail stays visible while scrolling, so you can see where you are in a long chat. Hover/focus on the rail
    expands it into a small list showing the first words of each question; click (or Enter) jumps (smooth scroll) to that turn; the active tick follows the scroll position.
    How to build it here (`app/ui/src/screens/Chat.tsx`, new `components/ChatRail.tsx`, styles in `polish.css` using theme tokens only):
    (1) build the list of turns from the loaded messages (one entry per user message, label = first ~60 chars, plain text, no markdown); hide the rail if fewer than 4 turns;
    (2) give each turn an `id`/ref and track the active one with an `IntersectionObserver` (root = the scroll container, rootMargin about `-40% 0px -55% 0px`) instead of scroll-event maths - cheap for 500+ turns;
    (3) ticks = `button`s inside a `nav aria-label="Conversation outline"`, `aria-current="true"` on the active one, keyboard: Tab to the rail, Up/Down move, Enter jumps; tooltip/popover on hover and focus;
    (4) jump = `scrollIntoView({behavior: reduced-motion ? "auto" : "smooth", block: "start"})` and the existing "scroll to bottom/top" buttons keep working; after a jump do not auto-scroll to bottom when new tokens stream in until the user returns to the end;
    (5) for very long chats show at most ~40 ticks (group turns) and keep the active one visible; (6) respect Reduce motion/transparency/contrast, all 8 themes (contrast test), text size 140 %, narrow window (hide below ~700 px);
    (7) Guide entry ("Moving around a long chat"), `mock.ts` demo chat with many turns, `ui_actions.json` entries, scripted DOM test in the browser preview, manual check H18 in the real window. No backend change, no new RPC.
R6. (automated half DONE 2026-10-02: test_settings_roundtrip.py; real-window pass still open) **Settings buttons - real-window test still pending** (see TESTS.md "Button audit"): the 2026-10-01 audit clicked all 18 Settings sections only in the browser preview with the MOCK gateway (no JS errors, visible reaction); the e2e runner (187 steps) tests the
    settings RPCs against a real throwaway gateway (apply, tighten/loosen rules, floor, history, themes) but not every individual Settings control; real-window checks H1, H11, H12 and P3 are still unticked. Do a full pass in the Tauri window after the new bundle is installed
    (every Settings section, each toggle/field saved and still there after Lock + sign-in), plus an automated per-setting round-trip test generated from `settings_schema.py` (131 settings: set -> get -> reset) so no control is untested.

## P1 - before daily use with real data
1. **Run the laptop checklist** in TESTS.md (sections A-G) and fix what fails. Especially A3 (TPM PIN
   protector on real hardware), B1-B4 (firewall + pipe isolation of the installed bundle), B5/B6 (sandbox).
2. **Code signing** (D7): obtain a code-signing certificate; sign pa-ui.exe, pa-gateway.exe, pa-core.exe,
   pa-parser.exe, pa-outlook-worker.exe in CI; build with `-Signed` so the gateway requires Authenticode.
3. **MSI installer** (D14): Tauri's WiX bundler + a fragment with custom actions for
   `installer/windows/firewall-rules.ps1`, the per-user logon tasks and uninstall cleanup; per-machine install.
4. ~~**Enforce firewall rules at start in release builds** (D15)~~ DONE 0.1.2: if Posture finds missing rules, refuse to
   start pa-core/workers and show "Repair" (the Repair action already exists).
5. **Signed update channel** (D13, 39.11): signed manifest (Ed25519 key pinned), download via the gateway,
   snapshot (`backup.snapshot_for_update`), health check, automatic rollback, checkpoint running tasks.
6. ~~**Supply chain** (29)~~ DONE 0.1.2 (hash-pinned locks, pip-audit/npm audit/cargo audit, CycloneDX SBOMs, CodeQL, Dependabot; enable GitHub secret scanning in repo settings). Original note: (29): pin exact dependency versions (use the CI `requirements.lock`), SBOM
   (e.g. `cyclonedx-py`, `cargo cyclonedx`), `pip-audit` / `npm audit` / `cargo audit`, CodeQL, secret scanning.

## P2 - spec items still partial
0. **AppContainer for pa-core and workers in portable mode**: replaces the firewall rules (needs no admin). Reuse
   `pa_workers/sandbox/appcontainer.py`; grant read access to `python\` and `app\`, no network capability; the pipe to
   the gateway needs an ACL for the AppContainer SID. Until then portable mode has NO OS-level network isolation.
7. pa-parser inside an **AppContainer** (D8): reuse `pa_workers/sandbox/appcontainer.py`, grant the runtime
   folder, pass bytes via a pipe handle instead of stdin inheritance if needed.
8. **Pinned portable Python for the sandbox** (D9): download the embeddable zip in the build, verify SHA-256,
   ship as `sandbox-python\`, pre-install approved packages (numpy, pandas, matplotlib) offline.
9. **Bundle llama.cpp** (D10) with a pinned hash; curated model catalogue with published SHA-256 and
   "download via the gateway" (allowlisted hosts, resumable, verify before use).
10. **Custom-scheme M365 redirect** (D12): register `personalagent://` (tauri-plugin-deep-link) in the MSI.
11. **Microsoft Purview sensitivity labels** for M365 items (read `msip_labels` / label APIs) and Outlook
    (`PR_...` properties) - today private/confidential flags map to CONFIDENTIAL.
12. **New-mail triggers** (EXTERNAL_EVENT): a poller for M365 (delta query) / Outlook (NewMailEx event in the
    worker) that calls `MissionService.on_event("new_mail", ...)`; the policy side is done and tested.
13. **Wake timers / keep awake** during mission windows (`SetWaitableTimer`, `SetThreadExecutionState`).
14. **Update-time task checkpointing** (39.11) and "never run twice" across updates.
15. Model download progress UI; per-mission model pin re-test enforcement in the UI.
16. PDF page images in the file preview (D17) - render in pa-parser (pypdfium2) with no scripts.
17. A **Rust rewrite of the vault/crypto core** (optional, D4) for stronger memory hygiene.

## UI-A. Apple-design review (user request 2026-10-01) - DONE 2026-10-01 (0.1.6), see docs/UI_REVIEW_APPLE_DESIGN.md
Implemented: contrast tokens for all 8 themes + CI test; OS reduced-motion/transparency/contrast; background default Off;
spring motion with exit animations (modal, palette, slash menu, toasts), anchored dialog origin; shared press states;
soft scroll edges instead of hard dividers; type-scale tokens and scale-aware layout units; Undo (8 s) for deleting
chats/files/memory/missions; breadcrumb + window title, last screen remembered, "Activity" -> "Activity log".
Still open (needs the user's decision or real-device review): nav vocabulary (Home/Tasks/Missions & Routines overlap -
needs user wording), dialog stacking depth effect, resizable panes/drag-to-pin, optional sounds, frame-by-frame motion
review in the Tauri window (TESTS.md H10), backend soft-delete if deletes must survive an app crash within the 8 s window.

## UI follow-ups (from the Claude Code UI comparison)
U1. **Plan preview**: show the agent's plan before a long multi-step task with Run / Edit (needs a pa-core
    planning step and a new approval kind; rules 8/16 - may only narrow).
U2. **Diff view** for files the agent writes (My Files versions + before/after, Undo) and for edited approvals.
U3. **Quick permission-mode switch** in chat (Manual / Ask more): a UI shortcut that can only tighten policy.
U4. Tray badge for items needing attention; slash-command argument hints; read-only file viewer.
U5. Browser-preview coverage of History/pins in `mock.ts` is demo-only (`?demo`).

## P3 - later / nice to have
18. Scripts that call tools (39.9, D16): file-based RPC bridge from Windows Sandbox to the gateway.
19. Email-to-self summaries (D19). Quiet-hours aware batching of notifications.
20. Isolated browser (39.16) - only with Windows Sandbox.
21. Messaging apps (39.17) - deferred by the spec.
22. ChiRAG and other connectors via signed connector packages (39.4 verifier is ready).
23. Formal accessibility review (screen reader pass, high-contrast theme).
24. OCR for images/scanned PDFs inside pa-parser (e.g. Windows.Media.Ocr via WinRT).
25. i18n of the UI.

## Known issues
- K11 FIXED 2026-10-01 (found by scripts/e2e_runner.py): the gateway pipe accept loop could end up waiting on a
  pipe instance that was already connected ("all pipe instances busy" for every later client): UI and pa-core could not
  connect and the agent stopped answering (~50 % of sign-out/sign-in sequences in dev runs). `pipe_server._accept_loop`
  now handles ConnectNamedPipe return codes explicitly; regression test `test_pipe_keeps_accepting_after_connect_disconnect_storm`.
- K12 FIXED: an Argon2 `HashingError` (low free memory) surfaced as "internal error (HashingError)"; it now returns the
  friendly `low_memory` error. Seen once during e2e (together with an unexplained UI lock, probably Windows lock).
- K7 Smart App Control (Windows 11, Home) blocks the unsigned PyInstaller executables; the portable build works
  around it via signed Python. Third-party `.pyd` wheels are unsigned too and may be blocked on other PCs.
- K8 TPM per-user throttle: any run that makes wrong-PIN attempts can block TPM key creation for hours (seen
  twice). The user's FIRST real setup fails with "cannot finalize TPM key (0x80290409)" while throttled - wait.
- K9 `laptop_smoke.py` "chat answered" once timed out after 300 s on the real-TPM run (passes with `--software`);
  re-check after the TPM throttle clears.
- K10 `scripts/build-portable.ps1` embeds `pa-ui.exe` built from the current web UI: rebuild both after UI changes.
- K1 SQLCipher `cipher_memory_security` must stay off on Windows (D5).
- K2 The Defender scan writes the plaintext file briefly to the ACL-protected `tmp\` folder; AMSI buffer
  scanning would avoid that.
- K3 Browser-preview mock (`app/ui/src/api/mock.ts`) covers the main flows only.
- K4 FIXED 2026-09-30 (worker_command starts the base interpreter directly from a venv; pid check unchanged). Was: (laptop, dev runs from a venv) `test_selftest_over_named_pipes_with_core_process` fails: `.venv\Scripts\python.exe`
  is a launcher, so the pid that connects to the pipe is a child of the pid the gateway spawned and the
  handshake is rejected (passes on CI without a venv, and in release where pa-core.exe is exact). Open decision:
  launch the core with the base interpreter + venv site-packages, or accept a direct child in dev mode only.
- K5 On a TPM laptop never run wrong-PIN experiments against the real TPM: it throttles all TPM key creation
  for the user (event 23) until the TPM auth-failure counter decays. A3 must be re-tried after the throttle clears.
- K6 FIXED 2026-10-01: v0.1.1 bundles lacked `pa_gateway/data/*.txt` (spec's collect_data_files found nothing), so
  `setup.check_password` raised ModuleNotFoundError and the wizard could not continue. The spec now ships them
  explicitly and fails the build if missing. CI should also run a frozen-gateway smoke test of the setup RPCs.

## Build blocker (2026-10-01) - RESOLVED (Smart App Control turned off by the user; rebuilt)
Rebuild `pa-ui.exe` (UI 0.1.6) and re-run `scripts/build-portable.ps1`: blocked on this PC by Smart App Control (Rust build
scripts, os error 4551). Do it in CI or on a PC without that block; then re-test H1-H10 in the real window.

- K14 (2026-10-01): file scanning uses Microsoft Defender (MpCmdRun). With another antivirus active (McAfee here) Defender is
  off and every upload is rejected as "not scanned" (fail closed, correct but unusable). Options: keep Defender active /
  run it passive-mode compatible, add an optional McAfee/other-AV command-line scanner adapter, or an explicit user-approved
  "scan unavailable" quarantine-hold mode with parsing in the isolated pa-parser only. Needs the user's decision.

- Web search has no keyless provider: routines/chat that search the web need Exa/Brave/Tavily/SearXNG + key (Secrets). Decide whether to add a documented keyless option (privacy/egress review needed).

## Round 2026-10-01 evening - open items
- Install the new bundle (admin) and re-run H11-H16 in the real window; GPU meter, shortcuts, picture upload, Outlook screen, local files were only seen in the browser preview.
- Local files: .xls (old Excel), password-protected workbooks, formulas (cached values are used), UNC paths are not supported by design; Excel dates are detected by cell style.
  Possible next: save a query result as CSV/XLSX in My Files; chart rendering in chat; more file types (pptx).
- Email skills use deterministic heuristics for approval/deadline wording (English). Add Hindi/other-language patterns if needed; per-skill schedule editing is via Missions & Routines.
- Network log: DNS lookups and the loopback model-health pings are not recorded; update checks do not exist yet.
- Shortcuts are fixed (not user-configurable). GPU meter reads Windows performance counters (all vendors); no per-process view by design.
- Web search still needs a keyed provider (Exa/Brave/Tavily/SearXNG) - decision pending.
