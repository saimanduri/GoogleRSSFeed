# Deviations from the specification (and why)

The spec (`docs/spec/personal_desktop_agent_spec_v1_1.txt`) is the source of truth. Where the implementation
differs, it is listed here with the reason and whether it is temporary.

| # | Spec | Implementation | Why | Status |
|---|---|---|---|---|
| D1 | 2.1: pa-core calls pa-llm directly (pa-llm key known to gateway and core) | pa-core calls `llm.complete` on the gateway; only the gateway knows the runtime key | Enforces 39.5 ("nothing reaches the model unless logged first") in one place; fewer holders of the key; external runtimes (Ollama/vLLM/Run:ai) and remote endpoints need the gateway anyway | Permanent (stricter) |
| D2 | 16.x: scheduler lives in pa-core | Scheduler (missions, reminders, catch-up) lives in pa-gateway; pa-core only executes tasks | Deterministic component belongs with the keys/DB; a compromised pa-core cannot create schedules | Permanent (stricter) |
| D3 | 2.2: tray icon in pa-gateway | Tray icon, global STOP ALL hotkey and toasts live in pa-ui (Tauri), which keeps running in the tray when the window is closed | pa-gateway is a headless Python process; Tauri has native tray/notification/hotkey support. pa-ui holds no secrets, and the gateway continues missions if pa-ui is closed | Permanent |
| D4 | 3: Gateway in Rust recommended | Gateway in Python 3.12 (spec allows Python with vetted crypto) | Your choice (easier to read/modify). Consequence: best-effort memory zeroisation (see SECRETS_HANDLING.md) | Permanent |
| D5 | 4.7: SQLCipher memory security (implied) | `cipher_memory_security` OFF | Crashes on Windows (stack overflow from VirtualLock on every allocation) | Until SQLCipher fixes it |
| D6 | 4.2 step 4: "160-bit key ... 8 groups of 5 characters" | 8 × 5 Crockford Base32 = **200-bit** key | The display format implies 200 bits; stronger | Permanent |
| D7 | 2.4: pipe client must carry a valid Authenticode signature | Enforced only in `SIGNED_BUILD`; unsigned builds verify the exact executable path inside the install folder; source runs only in dev mode | No code-signing certificate yet | Until signing is set up (PENDING_WORK P1) |
| D8 | 15: pa-parser runs in an AppContainer | pa-parser runs in a Job Object (memory/CPU/1 process, UI restrictions, scrubbed env, firewall-blocked) | AppContainer + Python needs ACL grants on the runtime; implemented for the sandbox first | Temporary (PENDING_WORK P2) |
| D9 | 19.1: pinned signed portable Python in Windows Sandbox | Uses `<install>\sandbox-python\python.exe` if present, otherwise the base Python | Embeddable Python download + hash pinning not in the build yet | Temporary (P2) |
| D10 | 20.1: bundled llama.cpp | llama-server.exe is not bundled; drop it into the install folder or `llm-runtime\bin` (hash of the model is still verified) | Binary licence/size; you may prefer Ollama/vLLM | Temporary (P2) |
| D11 | 39.1: pa-llm rejects Origin / Sec-Fetch / wrong Host | llama.cpp cannot be configured to do this; defences are loopback + random port + 256-bit key known only to the gateway + 401-watcher rotation | Third-party binary | Documented limit |
| D12 | 10.1: custom-scheme redirect preferred for M365 | Loopback redirect (random port, one request, 120 s, state + PKCE) is the default; custom scheme accepted via `ui.open_link` but not registered by the installer yet | Deep-link registration needs the MSI | Temporary (P2) |
| D13 | 29/39.11: signed updates with automatic rollback | Snapshot helper exists (`backup.snapshot_for_update`); update channel not implemented | No update server / signing key yet | Temporary (P1) |
| D14 | 2.5: signed MSI installer | `installer/windows/install-dev.ps1` + CI bundle; Tauri can produce an MSI for the UI, but the combined MSI with firewall custom actions is not done | Needs WiX fragment + signing | Temporary (P1) |
| D15 | 14.2 layer 1: firewall rules verified at every start, runtime blocked if missing | Checked by the Posture page (start/daily/on demand) and shown as High; the agent runtime is not blocked when rules are missing | Dev/source runs cannot have per-program rules (all python.exe) | Temporary: enforce in release builds (P1) |
| D16 | 39.9: scripts that call tools | Setting exists; the in-sandbox RPC stub is not implemented, so `python.run` cannot call tools | Needs a file-based RPC bridge for Windows Sandbox | Temporary (P3) |
| D17 | 5.2: My Files preview renders PDFs as images | Preview shows extracted text only | Needs a PDF rasteriser in pa-parser | Temporary (P3) |
| D18 | 26: toasts from the gateway when no UI is running | Toasts are shown by pa-ui (tray process, started at logon); in-app notification centre always records them | See D3 | Permanent |
| D19 | 5.3 Notifications: optional email-to-self via M365 | Setting exists; sending not wired | Low priority | Temporary (P3) |
