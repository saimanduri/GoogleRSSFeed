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
| T14 | Supply chain | Pinned dependency versions (lock from CI), no runtime code download, model SHA-256 verification; signing/SBOM pending (PENDING_WORK P1) |
| T15 | Approval fatigue | Rate limit per task, fast-accept warning, batching of low-risk items only |

## Out of scope / honest limits
- A compromised Windows kernel or an administrator on the PC.
- Physical access to an unlocked, signed-in PC equals access to the agent.
- Python memory copies of key material (best-effort zeroisation; see SECRETS_HANDLING.md).
- The model's own reliability: the gateway limits *actions*, not the quality of answers.
