# Threat model

## Assets
Vault master key and sub-keys; stored secrets; mail/calendar content; My Files; memories; chat history and
transcripts; M365 tokens; the ability to act (send mail, fetch URLs, run code); the integrity of the security log.

## Attackers and what stops them
| # | Attacker / scenario | Controls |
|---|---|---|
| T1 | **Prompt injection** in mail, web pages, files, calendar invites, tool output | Untrusted data only in labelled `<data>` sections; model can only *propose*; deterministic policy + default deny; data-flow table (CONFIDENTIAL → approval, RESTRICTED → deny); approvals bound to the exact payload; injection heuristics lower rights; external-event tasks read-only; memory/skill proposals from tainted runs blocked; red-team gate in CI |
| T2 | Exfiltration via allowed channels (search query, URL, draft recipients, notifications, file names) | DLP on every egress (keyed secret fingerprints, card/IBAN/key formats, custom patterns), egress budgets, allowlists, SSRF-safe fetch, connector domains blocked when off, notification content levels |
| T3 | **Malicious website** talking to local services (ClawJacked) | No localhost web server; UI over a named pipe; model runtime on random port + 256-bit key known only to the gateway; external runtimes checked for browser-origin exposure; OAuth loopback is one-shot, state + PKCE, 120 s |
| T4 | **Links / URIs** changing configuration (CVE-2026-25253-style) | `ui.open_link` can only open a screen; parameters ignored and logged; endpoints only via Settings with step-up |
| T5 | Other local user / process on the PC | Data folder ACL (you + SYSTEM); pipe DACL (user only, NETWORK denied); client image + PID checks; per-launch tokens; Program Files write-protected |
| T6 | Malware running **as you** while unlocked | *Cannot be fully prevented* (spec 4.8). Reduced by: process separation (keys only in pa-gateway), short unlock windows, auto-lock, capture-excluded secret screens, clipboard hygiene, STOP ALL |
| T7 | Stolen/copied disk or data folder | Everything encrypted with keys derived from the password (Argon2id ≥ 1 s, 256 MiB) or TPM + PIN + recovery key; copied folder cannot be opened with PIN + recovery key on another PC; BitLocker recommended |
| T8 | Brute force of password / PIN / recovery key | Exponential delays, TPM dictionary-attack lockout, app PIN disable after 5, recovery disabled 1 h after 5, 200-bit recovery key |
| T9 | Malicious documents (macros, exploits, bombs, masquerading executables, XXE) | Quarantine; type sniffing; archive limits; Defender + EICAR; active content flagged and never executed; parsing in a separate Job-Object process with no network; XML DTDs refused |
| T10 | Model-written code | Only in Windows Sandbox (Hyper-V) or AppContainer + Job Object; no network; only granted files; outputs quarantined |
| T11 | Malicious skills / extensions (ClawHavoc) | Declarative skills only; static checks for code/commands/downloads/encoded text; HMAC-signed; review diff; step-up activation; no marketplace; connector manifests Ed25519-signed with pinned keys |
| T12 | Runaway agent / loops / sub-task explosion / trigger flooding | Budgets per task/day, steps, depth, sub-agent limit, per-task tool rate limits, trigger rate limits, kill switch |
| T13 | Log tampering / hiding actions | HMAC chain with vault key, seals, verify on every sign-in, fail-closed "about to execute", optional SIEM |
| T14 | Supply chain | Hash-pinned `requirements*.lock` (`--require-hashes`), pip-audit / npm audit / cargo audit, CycloneDX SBOMs, CodeQL, Dependabot, no runtime code download, model SHA-256 verification, embeddable Python hash + PSF signature checked at build; code signing pending (PENDING_WORK P1) |
| T15 | Approval fatigue | Rate limit per task, fast-accept warning, batching of low-risk items only |
| T16 | Agent core / workers reaching the network in **portable mode** | NOT mitigated by the OS without admin (no firewall rules). Only the gateway has network code paths and egress is filtered; Posture shows a High finding. Planned: AppContainer without network capability (PENDING_WORK 0) |
| T17 | Tests or tools exhausting the real TPM's PIN-guess budget (self-inflicted lockout) | Automated tests force the software protector (`PA_FORCE_SOFTWARE_PROTECTOR`, dev mode only); `scripts/laptop_smoke.py` makes wrong-PIN attempts opt-in |
| T18 | Smart App Control blocks unsigned executables | Portable build runs through signed Python; unsigned third-party `.pyd` wheels may still be blocked on some PCs; signing everything is PENDING_WORK 2 |
| T19 | Pipe lock-out (availability): gateway stops accepting UI/core connections | Accept loop handles every ConnectNamedPipe code; storm regression test; e2e runner exercises sign-out/in cycles |

## Out of scope / honest limits
- A compromised Windows kernel or an administrator on the PC.
- Physical access to an unlocked, signed-in PC equals access to the agent.
- Python memory copies of key material (best-effort zeroisation; see SECRETS_HANDLING.md).
- The model's own reliability: the gateway limits *actions*, not the quality of answers.

## Added 2026-10-01 (0.1.7)
| ID | Threat | Control |
|---|---|---|
| T20 | Prompt injection makes the agent read arbitrary files on the PC | Local files are *grants* created only by the user in the window (RPC role ui), per chat, read-only; the model gets an id and a name, never a path; tools refuse other chats' grants; path re-validated at use (links resolved; Windows, Program Files, the app's own data and network paths blocked; allow-listed extensions); reader runs in pa-parser (no network, Job Object limits, 3 GB) |
| T21 | Mail skills exfiltrate or alter mail | Skills are routines limited to read-only Outlook tools (test asserts no write tools); no send/move/delete exists in the worker; results stay local; approvals/deadline flags come from deterministic code, not the model |
| T22 | Network log leaks secrets or is used to infer content | Stores host, path without query, sizes and timing only; no bodies/headers/tokens; encrypted in the vault DB; 14-day retention |
| T23 | Uploaded picture used as a script vector | Only PNG/JPEG/WebP data URLs (magic bytes checked, <= 64 KB); SVG refused; shown via <img> only |
| T24 | Proposed routine silently starts | Agent-proposed routines stay DRAFT until the user presses Activate in Approvals/Missions (write-tool routines also need the password) |
