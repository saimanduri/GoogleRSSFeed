# Security audit 2026-10-02 (SAST + DAST, "attacker's view")

Scope: the app's own code on the developer PC, throwaway gateways in developer mode (own data folder, throwaway account, software key).
The real TPM, the real account, the installed app and real mail were **not** touched. Antivirus scanning (Defender is off on this PC) was
not part of the file tests. Corpus used: `C:\Users\<user>\Downloads\Sample file generator\corpus` (217 files, 42 benign + 175 hostile; kept outside git).

## Method
| Part | What was done | Repeat with |
|---|---|---|
| SAST | `ruff` (E,F,W,B + bandit rules S) over `app` and `scripts`; manual triage of every S607 (partial program path), S314 (XML), S608 (SQL) hit; secret scan of all git history (API-key / private-key patterns); Tauri CSP + capability review; Markdown/DOMPurify review | `ruff check app scripts --select S,B` |
| DAST 1 - files | `scripts/corpus_dast.py`: every corpus file + 17 hostile file *names* through the real upload pipeline of a throwaway gateway; checks crash/hang/gateway death, stored-name safety, outbound requests (network log), benign false positives | `python scripts\corpus_dast.py "<corpus folder>"` |
| DAST 2 - IPC | `scripts/pentest_rpc.py`: 103 UI RPCs x 29 hostile values over the real named pipe (4060 calls) + 12 pipe-abuse cases + connection storms | `python scripts\pentest_rpc.py` |
| Roles | `tests/integration/test_role_matrix.py`: every UI-only RPC refused for pa-core, every core-only RPC refused for the UI | pytest |
| SSRF | `tests/unit/test_egress_exotic.py`: 35 address-notation tricks (decimal/hex/octal IPs, IPv6 forms, userinfo, odd schemes) with "fetch any site" on | pytest |
| Red team | `PA_REDTEAM_N=20 pytest tests/redteam` (malicious "model": exfiltration, SSRF, planted missions/skills, self-approval ...) | pytest |

## Findings and fixes
| ID | Severity | Finding (proof) | Fix | Regression test |
|---|---|---|---|---|
| F-01 | **High (availability / data loss for users)** | Every upload whose text was not plain ASCII (Hindi, Arabic, Chinese, emoji, even plain UTF-8 `.txt`) was rejected with "parser returned invalid output": the parser wrote JSON through the Windows console code page. 23 of 217 corpus files, all the benign multilingual ones. | `pa_workers/parser/__main__.py` writes ASCII-only JSON | `test_upload_hardening::test_non_english_text_survives_the_parser` |
| F-02 | Medium | Lone UTF-16 surrogate in any request text (`account.set_profile`, chat title...) made the gateway **never answer** (reply could not be encoded) -> UI hang; a stored value would break every later reply that contained it (persistent denial of service; model output can contain such text). | `encode_frame(errors="replace")`; all request strings scrubbed (`ipc/dispatch.py::_scrub`); nesting depth limit 24; decoder turns `RecursionError` into `FrameError` | `test_ipc_robustness` (5 tests) |
| F-03 | Medium | `icacls`, `powershell`, `taskkill` started by bare name: Windows searches the current directory first, so a planted `icacls.exe` in a user-writable working folder would run with the gateway's rights; the firewall-repair button even elevated (`-Verb RunAs`). | `pa_common/winpaths.py` absolute System32 paths; firewall repair refuses scripts outside Program Files (a user-writable script could be swapped before the UAC click) | `tests/unit/test_winpaths.py` (also bans bare names in `app/`) |
| F-04 | Medium | File names were stored with NUL, RTL-override (U+202E), zero-width characters, `:` (NTFS alternate data stream), reserved device names (`CON`, `LPT1`), trailing dot/space. Dangerous when a file is saved/exported later or displayed (spoofed extension). | `checks.safe_filename()` used on upload and rename | `test_upload_hardening::test_safe_filename` (13 cases) |
| F-05 | Medium | Image bombs were accepted (100000x100000 PNG, GIF with thousands of frames, animated WebP, 625 MP PNG, oversized ICC profile, truncated JPEG, PNG chunk length overflow). Nothing decodes them today, but a viewer or vision model later would. | `checks.check_image()` header-level validation (side <= 30000, <= 400 MP, <= 1000 frames, ICC <= 1 MB, structure/EOI checks) | `test_upload_hardening` (image tests) |
| F-06 | Low-Medium | The large-spreadsheet reader (`tables.py`, used by "local files") parsed Office XML without the DTD/entity guard that the document parser has (billion laughs / XXE). Python's expat 2.7.1 already limits amplification, so no exploit was shown. | DTD/ENTITY refused before parsing (`_open_xml/_read_xml`) | `test_table_reader_refuses_dtd_xml` |
| F-07 | Low-Medium | CSV formula injection: "Copy as CSV" in Network Logs wrote cells beginning with `= + - @` unescaped (a destination/reason string controlled by a website could run as a formula in Excel). | Cells starting with `= + - @ tab CR` get a leading apostrophe | manual H21 (UI) |
| F-08 | Low | Internal errors for user input: `backup.verify` with a missing/forbidden path, `files.set_label` / `files.upload` with an unknown label (KeyError), `files.update` with odd tags returned `internal_error`. | OS/value errors mapped to clear codes (`not_found`, `access_denied`, `invalid_request`) in the dispatcher; `_level()` helper | `test_ipc_robustness` |
| F-09 | Low | Legitimate Windows-1252 text files were refused ("binary"), arbitrary binary with an even length sniffed as UTF-16 text. | UTF-16 only with a byte-order mark; no-NUL / few-control-codes rule for legacy text | `test_legacy_windows_text_is_text_but_binary_is_not` |
| F-10 | Low | DDE fields (`DDEAUTO`) and external relationships in Office files were not flagged; more auto-run file types (`.url .iqy .slk .scf .settingcontent-ms .library-ms .rdp .xll ...`) were not blocked. | Flagged as active content / blocked | `test_dde_and_external_reference_are_flagged`, `test_more_script_like_extensions_are_blocked` |
| F-11 | Info (test hygiene) | One email-skill test queried the user's real Outlook mailbox (read-only, no content stored) because it did not fake the connector; it timed out. | Test now fakes the connector | `test_email_skills.py` |

## Results with nothing to fix
* 4060 hostile RPC calls over the real pipe: after the fixes **0 internal errors, 0 hangs, 0 gateway deaths**; bad token, role confusion (core role with a UI token), unknown role, request before hello, 16 MiB+ length headers, random bytes, invalid / non-object / deeply nested JSON, half-open connections, 40 idle connections and 300 connect/close cycles: always rejected and the gateway kept serving.
* Corpus: 0 crashes, 0 hangs (slowest file 0.8 s), gateway alive at the end, **0 network requests while processing hostile files** (SSRF/OOB canary files such as SVG external images, PDF GoToR, OOXML external relationships, XXE are never fetched). Path-traversal / device-name / RTLO / ADS names are stored harmlessly. EICAR is caught by the built-in signature check.
* All 35 SSRF notation tricks denied; red-team suite (13 tests x 20 repetitions) passes; no secrets in git history (only fake keys in DLP tests); Tauri CSP forbids inline script, remote content, frames and forms; the web view has no filesystem/shell/HTTP permissions; chat Markdown is sanitised (DOMPurify, https links only, click-through).

## Judged acceptable (human triage of the 42 "REJECT_OR_..." corpus files that end up READY)
The pipeline *parses* instead of rejecting these, in the isolated parser, and returns text only; nothing is executed or fetched: malformed PDFs that pypdf can still read, polyglots (zip inside JPEG/PDF), SVG/HTML/RTF with script (flagged "active content removed"), SVG XXE / external image / billion-laughs XML (not expanded, not fetched), EXIF-XSS JPEGs (metadata is never rendered), prompt-injection documents (stay data: hidden text is listed separately, the gateway blocks any action the model is talked into). The compound campaign documents show 7 hidden items each. `docs/corpus_dast_results.json` lists every case.

## Not covered / follow-ups
1. **Real antivirus step**: Defender is off on this PC, so malware scanning of uploads is untested end to end (decision K14).
2. **Installed-app checks that need the installed bundle**: install-folder ACLs, DLL side-loading, logon-task rights, firewall rules (checklist B1-B4).
3. Web view XSS was reviewed statically; an automated browser test with malicious Markdown is still open.
4. Homoglyph file names (Cyrillic `a`) are stored as typed; a mixed-script warning could be added.
5. SAST with external engines (bandit, semgrep, CodeQL, pip-audit, npm audit, cargo audit, gitleaks) needs tool installs: ask first; the CI workflows `security.yml` / CodeQL exist but have never run (nothing is pushed).
6. Fuzzing of the model-runtime port, M365 OAuth loopback and the sandbox escape paths needs the full stack (see PENDING_WORK R3).


## Addendum (same day): local file access and file sanity, round 2
Best practice studied: [OWASP File Upload Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/File_Upload_Cheat_Sheet.html) (allow-list, magic bytes + extension, reject colons/null bytes/double extensions, size limits, random storage names, controlled processing) and how [Claude Code](https://code.claude.com/docs/en/security) scopes access (read only inside an approved working directory, extra directories only by explicit approval, prompts for anything outside).
Implemented: see VERSION_HISTORY 0.1.11 (zip-slip, duplicate/overlapping/encrypted archives, OOXML structure, PDF features, trailing data, lnk/cab) and the folder model (explicit per-chat, per-session grants; separate subfolder approval; resolved paths confined to the root; junction/symlink/UNC/AppData/credential/system folders refused).
Tests: `test_local_folders.py` (25), `test_upload_hardening.py` (37), corpus re-run (0 FAIL, 0 benign false positives), RPC fuzz re-run with the new RPCs (FINDINGS: none).

## Addendum 2 (review round)
New controls: web access is asked per chat and per session; files that no antivirus could scan are held and released only with the user's password (never malware detections); AMSI is trusted only after it proves itself on EICAR; empty-reply rescue never widens rights. Tests: `test_web_per_chat.py`, `test_unscanned_files.py`; RPC fuzz re-run: no findings.
