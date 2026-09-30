# Pending work

Prioritised. P1 = needed before using it with real, sensitive data daily. P2 = spec items still partial.
P3 = nice to have / later phases. See also docs/DEVIATIONS.md (D-numbers).

## P1 - before daily use with real data
1. **Run the laptop checklist** in TESTS.md (sections A-G) and fix what fails. Especially A3 (TPM PIN
   protector on real hardware), B1-B4 (firewall + pipe isolation of the installed bundle), B5/B6 (sandbox).
2. **Code signing** (D7): obtain a code-signing certificate; sign pa-ui.exe, pa-gateway.exe, pa-core.exe,
   pa-parser.exe, pa-outlook-worker.exe in CI; build with `-Signed` so the gateway requires Authenticode.
3. **MSI installer** (D14): Tauri's WiX bundler + a fragment with custom actions for
   `installer/windows/firewall-rules.ps1`, the per-user logon tasks and uninstall cleanup; per-machine install.
4. **Enforce firewall rules at start in release builds** (D15): if Posture finds missing rules, refuse to
   start pa-core/workers and show "Repair" (the Repair action already exists).
5. **Signed update channel** (D13, 39.11): signed manifest (Ed25519 key pinned), download via the gateway,
   snapshot (`backup.snapshot_for_update`), health check, automatic rollback, checkpoint running tasks.
6. **Supply chain** (29): pin exact dependency versions (use the CI `requirements.lock`), SBOM
   (e.g. `cyclonedx-py`, `cargo cyclonedx`), `pip-audit` / `npm audit` / `cargo audit`, CodeQL, secret scanning.

## P2 - spec items still partial
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
- K1 SQLCipher `cipher_memory_security` must stay off on Windows (D5).
- K2 The Defender scan writes the plaintext file briefly to the ACL-protected `tmp\` folder; AMSI buffer
  scanning would avoid that.
- K3 Browser-preview mock (`app/ui/src/api/mock.ts`) covers the main flows only.
