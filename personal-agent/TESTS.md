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
| End-to-end | `python -m pa_gateway --selftest` | setup → chat → reminder approval → scheduled → log verifies |
| UI | `npm run build` (tsc strict) + Playwright script used during development | type safety; screenshots in `docs/screenshots` |

### CI (GitHub Actions `windows-latest`)
`python` job: env check, ruff, full pytest incl. Windows tests and red-team N=20, selftest.
`ui` job: `npm run build`, `tauri build --no-bundle` (Rust compile of pa-ui.exe).
`bundle` job: PyInstaller executables + pa-ui.exe → artifact `PersonalAgent-windows-bundle`.

## Laptop test checklist (manual / `-m laptop`)
Run these on your Windows 11 laptop after installing the bundle (or from source with `PA_DEV_MODE` **off**).
Tick them off in this file or in an issue.

### A. First run & keys
- [ ] A1 Wizard step 1 shows TPM **ready**, BitLocker status, sandbox strength, GPU.
- [ ] A2 Create account; weak passwords and 123456-style PINs are rejected.
- [ ] A3 Security Posture: "TPM 2.0 … PIN protector: tpm" is **OK** (not software).
- [ ] A4 Recovery key: Save as PDF works; Copy clears after 30 s; typing back 2 groups is enforced.
- [ ] A5 Lock (button, Ctrl+Shift+L, Windows+L) → PIN unlock works; 5 wrong PINs disable the PIN until password.
- [ ] A6 TPM lockout: after several wrong PINs Windows TPM lockout message is shown (no crash).
- [ ] A7 Reboot → password required (cold start). Idle auto-lock after the configured minutes.
- [ ] A8 Forgot password with PIN + recovery key → new password works → **new** recovery key shown, old one fails.
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
- [ ] C1 Ollama: add, Discover, Test model passes; exposure check says not reachable from the network.
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
- [ ] E3 STOP ALL (button, tray, Ctrl+Alt+Shift+S) stops everything within 2 s; release needs the password.
- [ ] E4 Approvals accepted in < 2 s three times → fatigue warning on Home.

### F. Data
- [ ] F1 Back up now to an external drive; Verify; restore on a second PC (new PIN + recovery key required).
- [ ] F2 Delete everything → app returns to the first-run wizard; old data unreadable.
- [ ] F3 SIEM: HTTPS endpoint with pinned fingerprint receives events; wrong fingerprint → error shown.

### G. UI
- [ ] G1 Secrets reveal: screenshot (Win+Shift+S) shows a black window while the value is visible.
- [ ] G2 Copy secret → Windows clipboard history (Win+V) does not contain it.
- [ ] G3 Keyboard-only navigation, dark/light theme, text size 140 %.
- [ ] G4 External link in an answer asks before opening the browser; remote images are never loaded.
