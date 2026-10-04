"""Generates docs/HOW_IT_WORKS.html: a self-contained page (no internet needed) that explains how ChiRAG Agent works with flowcharts, the
tech stack, a granular SBOM read from the real lock files (Python, npm, Rust), and what every screen/button does.
    python scripts\\make_howitworks.py
"""
import html
import json
import re
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
E = html.escape


def py_lock(path: Path) -> list[tuple[str, str, str]]:
    out = []
    lines = path.read_text(encoding="utf-8").splitlines()
    for i, ln in enumerate(lines):
        m = re.match(r"^([A-Za-z0-9_.\-]+)==([^\s\\]+)", ln)
        if m:
            via = ""
            for nxt in lines[i + 1:i + 6]:
                if nxt.strip().startswith("# via"):
                    via = nxt.strip()[5:].strip() or "(see next lines)"
                    break
            out.append((m.group(1), m.group(2), via))
    return out


def npm_lock() -> list[tuple[str, str, str, bool]]:
    lock = json.loads((ROOT / "app/ui/package-lock.json").read_text(encoding="utf-8"))
    return sorted((n.replace("node_modules/", ""), v.get("version", ""), v.get("license", "?") if isinstance(v.get("license"), str) else "?", bool(v.get("dev")))
                  for n, v in lock["packages"].items() if n)


def cargo_lock() -> list[tuple[str, str]]:
    txt = (ROOT / "app/ui/src-tauri/Cargo.lock").read_text(encoding="utf-8")
    return sorted((m.group(1), m.group(2)) for m in re.finditer(r'\[\[package\]\]\nname = "([^"]+)"\nversion = "([^"]+)"', txt))


PY_PURPOSE = {
    "cryptography": "AES-GCM encryption, key wrapping, random numbers (vetted crypto - rule 1)", "argon2-cffi": "Argon2id password hashing", "argon2-cffi-bindings": "C core of Argon2",
    "pydantic": "Checks every tool's arguments and RPC data", "pydantic-core": "Fast engine of pydantic", "httpx": "All HTTP calls (models, web search, Microsoft 365) - only inside the gateway",
    "httpcore": "Low-level part of httpx", "h11": "HTTP/1.1 protocol", "anyio": "Async helpers used by httpx", "idna": "International domain names", "certifi": "Trusted HTTPS certificate list",
    "numpy": "Audio resampling, memory-search vectors", "pypdf": "Reads PDFs (only inside the isolated parser)", "tzlocal": "Finds your Windows time zone", "tzdata": "Time zone database",
    "sqlcipher3-wheels": "SQLite with whole-database encryption (the app's database)", "pywin32": "Windows named pipes, job objects, COM (Outlook), counters", "cffi": "Calls C code (used by crypto)",
    "pycparser": "Helper of cffi", "typing-extensions": "Type hints backport", "typing-inspection": "Type hints helper", "annotated-types": "Type hints helper (pydantic)",
}

BOX = lambda t, cls="": f'<div class="box {cls}">{t}</div>'          # noqa: E731
ARROW = '<div class="arrow">&#8595;</div>'


def flow(*steps: str) -> str:
    out = []
    for i, s in enumerate(steps):
        out.append(s if s.startswith("<div") else BOX(s))
        if i < len(steps) - 1:
            out.append(ARROW)
    return '<div class="flow">' + "".join(out) + "</div>"


def row(*cols: str, cls: str = "") -> str:
    return f'<div class="hrow {cls}">' + "".join(f'<div class="hcol">{c}</div>' for c in cols) + "</div>"


def table(head: list[str], rows: list[list[str]], cls: str = "") -> str:
    h = "".join(f"<th>{E(x)}</th>" for x in head)
    b = "".join("<tr>" + "".join(f"<td>{c}</td>" for c in r) + "</tr>" for r in rows)
    return f'<table class="{cls}"><thead><tr>{h}</tr></thead><tbody>{b}</tbody></table>'


def section(id_: str, title: str, body: str, lead: str = "") -> str:
    return f'<section id="{id_}"><h2>{title}</h2>{f"<p class=lead>{lead}</p>" if lead else ""}{body}</section>'


py_rt, py_dev = py_lock(ROOT / "requirements.lock"), py_lock(ROOT / "requirements-dev.lock")
npm, crates = npm_lock(), cargo_lock()
rt_names = {n for n, _, _ in py_rt}
npm_rt = [x for x in npm if not x[3]]
licenses: dict[str, int] = {}
for _, _, lic, _ in npm:
    licenses[lic] = licenses.get(lic, 0) + 1

# ------------------------------------------------------------------------------------------------ content
S = []

S.append(section("what", "1. What is ChiRAG Agent?", """
<p>ChiRAG Agent (folder name <code>Personal Agent</code>) is a <b>private AI assistant that lives on your Windows PC</b>. You talk to it in a window; it can answer, remind, read your Outlook mail,
read files and folders you allow, look at pictures and scans, listen to your voice, search the web (only if you allow) and run routines on a schedule.</p>
<p>The AI model itself (for example Qwen or Gemma running in <b>Ollama</b>) <b>only suggests</b> what to do. A strict program called the <b>gateway</b> decides what is really allowed, writes everything to a tamper-evident log, and keeps your
passwords and keys. That idea is repeated everywhere in the design: <i>"the model proposes, the gateway decides"</i>.</p>
<div class="cards">
<div class="card"><b>Local first</b><br>Your data stays on the PC. Nothing is sent anywhere unless you set up a web provider or a remote model, and the app tells you when it does.</div>
<div class="card"><b>Security first</b><br>Encrypted database, key protected by the PC's TPM chip, no shell access for the AI, approvals for risky actions, kill switch (STOP ALL).</div>
<div class="card"><b>Plain controls</b><br>Every setting has a button or switch in the window. The AI cannot change settings.</div>
</div>"""))

S.append(section("big", "2. The big picture (who talks to whom)", """
<svg viewBox="0 0 980 470" class="diagram" role="img" aria-label="Architecture">
 <defs><marker id="a" markerWidth="10" markerHeight="8" refX="9" refY="4" orient="auto"><path d="M0,0 L10,4 L0,8 z" fill="#475569"/></marker></defs>
 <g font-family="Segoe UI, Arial" font-size="14">
  <rect x="20" y="20" width="200" height="90" rx="12" fill="#e0e7ff" stroke="#6366f1"/><text x="120" y="50" text-anchor="middle" font-weight="bold">You</text><text x="120" y="72" text-anchor="middle">keyboard, mouse, voice</text>
  <rect x="270" y="20" width="220" height="90" rx="12" fill="#dbeafe" stroke="#3b82f6"/><text x="380" y="48" text-anchor="middle" font-weight="bold">pa-ui.exe (the window)</text><text x="380" y="70" text-anchor="middle">React + TypeScript in a</text><text x="380" y="88" text-anchor="middle">Tauri 2 (Rust) shell</text>
  <rect x="270" y="170" width="220" height="130" rx="12" fill="#fee2e2" stroke="#ef4444"/><text x="380" y="198" text-anchor="middle" font-weight="bold">pa-gateway.exe (the guard)</text><text x="380" y="220" text-anchor="middle">vault + keys, TPM, policy,</text><text x="380" y="238" text-anchor="middle">approvals, DLP, egress filter,</text><text x="380" y="256" text-anchor="middle">audit log, encrypted DB,</text><text x="380" y="274" text-anchor="middle">scheduler, kill switch</text>
  <rect x="560" y="170" width="190" height="90" rx="12" fill="#dcfce7" stroke="#22c55e"/><text x="655" y="198" text-anchor="middle" font-weight="bold">pa-core.exe (the brain loop)</text><text x="655" y="220" text-anchor="middle">asks the model, plans steps,</text><text x="655" y="238" text-anchor="middle">NO keys, NO network</text>
  <rect x="790" y="20" width="170" height="70" rx="12" fill="#fef9c3" stroke="#eab308"/><text x="875" y="48" text-anchor="middle" font-weight="bold">pa-parser.exe</text><text x="875" y="68" text-anchor="middle">reads files safely</text>
  <rect x="790" y="110" width="170" height="70" rx="12" fill="#fef9c3" stroke="#eab308"/><text x="875" y="138" text-anchor="middle" font-weight="bold">pa-outlook-worker</text><text x="875" y="158" text-anchor="middle">talks to Outlook (COM)</text>
  <rect x="790" y="200" width="170" height="70" rx="12" fill="#fef9c3" stroke="#eab308"/><text x="875" y="228" text-anchor="middle" font-weight="bold">Python sandbox</text><text x="875" y="248" text-anchor="middle">runs AI-written code</text>
  <rect x="270" y="360" width="220" height="90" rx="12" fill="#f3e8ff" stroke="#a855f7"/><text x="380" y="388" text-anchor="middle" font-weight="bold">Encrypted data (SQLCipher)</text><text x="380" y="410" text-anchor="middle">chats, files, settings,</text><text x="380" y="428" text-anchor="middle">memory, secrets</text>
  <rect x="560" y="330" width="190" height="120" rx="12" fill="#ffedd5" stroke="#f97316"/><text x="655" y="358" text-anchor="middle" font-weight="bold">AI models (Ollama)</text><text x="655" y="380" text-anchor="middle">chat, voice, vision,</text><text x="655" y="398" text-anchor="middle">embedding</text><text x="655" y="420" text-anchor="middle" font-size="12">127.0.0.1 only (this PC)</text>
  <rect x="790" y="330" width="170" height="120" rx="12" fill="#e2e8f0" stroke="#64748b"/><text x="875" y="358" text-anchor="middle" font-weight="bold">Internet (optional)</text><text x="875" y="380" text-anchor="middle">web search (Exa...),</text><text x="875" y="398" text-anchor="middle">Microsoft 365, remote</text><text x="875" y="416" text-anchor="middle">models - via gateway only</text>
  <rect x="20" y="170" width="200" height="130" rx="12" fill="#f1f5f9" stroke="#94a3b8"/><text x="120" y="198" text-anchor="middle" font-weight="bold">Windows</text><text x="120" y="220" text-anchor="middle">TPM chip, firewall rules,</text><text x="120" y="238" text-anchor="middle">Task Scheduler (starts</text><text x="120" y="256" text-anchor="middle">the app at logon), Outlook,</text><text x="120" y="274" text-anchor="middle">antivirus</text>
  <g stroke="#475569" stroke-width="2" fill="none" marker-end="url(#a)">
   <path d="M220 65 H268"/><path d="M380 110 V168"/><path d="M490 235 H558"/><path d="M750 215 H788"/><path d="M750 150 H788"/><path d="M750 195 L788 120" stroke-dasharray="4"/><path d="M380 300 V358"/><path d="M490 270 L560 350" stroke-dasharray="4"/><path d="M490 285 L788 360" stroke-dasharray="4"/><path d="M220 235 H268"/>
  </g>
 </g></svg>
<ul class="small">
<li><b>Solid arrows</b> = normal talking. The window and the gateway talk through a <b>Windows named pipe</b> (a private local channel, not a network port).</li>
<li><b>Dotted arrows</b> = the gateway reaching out to models and the internet. <b>Only the gateway may do that</b>; pa-core has no network and no keys.</li>
<li>Helpers (parser, Outlook worker, sandbox) are separate small programs started for one job so a bad file or a bug cannot touch the rest.</li>
</ul>"""))

S.append(section("procs", "3. The programs and what each is allowed to do", table(
    ["Program", "Language", "Job", "Has keys?", "Has network?"],
    [["<b>pa-ui</b>", "Rust + React", "Shows screens, sends your clicks to the gateway, shows notifications, tray icon", "No", "No"],
     ["<b>pa-gateway</b>", "Python 3.12", "The guard: login, vault, TPM, policy, approvals, DLP, egress filter, audit, DB, scheduler, model calls, web calls", "Yes", "Yes (the only one)"],
     ["<b>pa-core</b>", "Python", "The agent loop: builds the prompt, asks the model, parses its answer, asks the gateway to run tools", "No", "No"],
     ["<b>pa-parser</b>", "Python", "Opens PDFs, Word, Excel, big sheets, pictures' metadata - one file per process, memory/CPU limited", "No", "No"],
     ["<b>pa-outlook-worker</b>", "Python + COM", "Reads classic Outlook through its object model (never bypasses Outlook's security guard)", "No", "No"],
     ["<b>Python sandbox</b>", "Python (AppContainer)", "Runs code the AI wrote, no network, read-only copies of files you granted", "No", "No"]]),
    "Windows firewall rules block everything except the gateway from the internet, and the installer creates them."))

S.append(section("chat", "4. Life of one chat message", flow(
    "<b>1. You type</b> a message (or speak) and press Send in the window.",
    "<b>2. pa-ui</b> sends <code>chat.send</code> through the named pipe to the gateway.",
    "<b>3. Gateway</b> checks you are signed in, saves the message to the encrypted database and the <b>session log</b> (rule: nothing reaches a model unless it is logged first), indexes it for History search, starts a <i>task</i>.",
    "<b>4. pa-core</b> picks the task up, builds the prompt: system rules + your memory + shared files list + the conversation.",
    "<b>5. pa-core asks the gateway</b> to call the model. The gateway sends it to Ollama (<code>/v1</code>, JSON-schema answer) and streams the words back to your window. Empty answer? It retries without the JSON constraint, then through Ollama's native API.",
    BOX("<b>6. The model answers with JSON</b>: either <code>final</code> (the answer) or <code>tool</code> (what it wants to use, with arguments)", "choice"),
    "<b>7. If a tool</b>: pa-core asks the gateway <code>tools.invoke</code>. The gateway runs its safety sequence (next section), runs the tool, returns the result (marked <i>untrusted</i> data). Back to step 5 until the model says <code>final</code>.",
    "<b>8. Final answer</b> is saved, shown with its sources (web, Outlook, files...), a Windows notification is sent if the window is not in front, and the run's steps are kept for the <b>Steps</b> panel."
) + "<p class=small>Small note: every model call, tool call and result is also written to the HMAC-chained audit log (each record contains the hash of the previous one, so edits are detectable).</p>"))

S.append(section("tools", "5. How a tool call is checked (the gateway's safety sequence)", flow(
    "Kill switch on? &rarr; refuse",
    "Is it a known tool? Are the arguments valid (pydantic schema)?",
    "Who is asking? (the gateway checks the task and the worker identity itself)",
    "Is the connector (Outlook, web, files...) switched on for this chat/routine?",
    "Scope rules: allowed website? file inside the shared folder? subfolder approved?",
    BOX("Policy engine decides: <b>ALLOW</b> / <b>ASK YOU</b> / <b>DENY</b> (data sensitivity, injection suspicion, risk of the tool, autonomy profile)", "choice"),
    "Budgets (tokens, web requests, data sent out) and rate limits",
    "DLP: is a secret or private data about to leave? &rarr; block",
    "If it needs approval: an <b>approval card</b> appears; you see the exact payload; approval is bound to a hash of it and used once",
    "Re-check everything again right before running (things may have changed while you decided)",
    "Write &laquo;about to execute&raquo; to the audit log (if logging fails, nothing runs)",
    "Run the tool. Late results are thrown away if you pressed STOP meanwhile."
), "Web access has one extra rule: the <b>first</b> web search or page fetch in a chat asks you once, and the answer lasts for that chat until you sign out or restart."))

S.append(section("files", "6. Files: My Files, quarantine and the safe reader", flow(
    "<b>Upload</b> (button, drag, or paste a picture). The name is cleaned (no <code>..\\</code>, no hidden characters, no <code>:</code>).",
    "<b>Quarantine</b>: stored encrypted (AES-GCM, one key per file) - nothing can read it yet.",
    "<b>Checks</b>: real type from the first bytes vs the extension, size, programs/scripts/shortcuts blocked, zip tricks (zip-slip, bombs, passwords, duplicates), Office structure matches the extension, PDF features flagged, pictures checked without decoding (size, frames, hidden data after the end).",
    BOX("<b>Antivirus</b>: Microsoft Defender if on &rarr; otherwise Windows AMSI (only if it proves itself on the harmless EICAR test) &rarr; otherwise the file <b>waits in Quarantine</b> and you may release it with your password (never if malware was found)", "choice"),
    "<b>Parse</b> in <code>pa-parser</code> (separate process, limits, no network): text, hidden text is listed separately, active content (macros, scripts) is flagged and ignored.",
    BOX("<b>Pictures and scanned PDFs</b>: if a <b>vision model</b> is set up, it reads the text and describes the picture; a PDF with no text layer is treated as a scan", "choice"),
    "<b>Ready</b>: text is indexed for search, can be added to <i>My Knowledge</i>, and the AI can read it with <code>files.read</code>. Everything it reads is labelled <i>untrusted</i>."
), "<b>What My Files is for:</b> a private, encrypted shelf of documents the assistant may use (and files it creates for you, like routine reports). It does not summarise uploads on its own - it extracts text, labels sensitivity, flags risks and makes them searchable."))

S.append(section("local", "7. Your own files and folders (read in place)", row(
    flow("You press the <b>paperclip</b> (file) or <b>folder</b> button", "A dialog shows what the assistant would get (file counts; <b>subfolders are a separate unticked box</b>)", "You press Share &rarr; a <b>grant</b> is saved for <b>this chat</b> and <b>this app session</b>",
         "The model sees a name and an id - <b>never a path</b>", "It calls <code>localfile.digest / browse / text / query</code>; each path is resolved and must stay inside the folder", "Needs a subfolder? A <b>card</b> asks you: Allow subfolders / Not now"),
    '<div class="note"><b>Rules that cannot be switched off</b><ul><li>Read-only: nothing is changed, deleted or copied</li><li>New chat = nothing shared</li><li>Sign out or restart = approvals expire (<i>Allow again</i>)</li><li>Never shared: whole drives, your user folder, Windows, Program Files, AppData, .ssh/.aws..., network paths, programs, shortcuts/junctions that lead outside</li><li>Only you (the UI) can create or widen a grant; the AI cannot</li></ul></div>')))

S.append(section("models", "8. Models: four kinds that never mix", table(
    ["Kind", "Examples", "Used for", "How it is tested when you press Add"],
    [["<b>Chat</b>", "Qwen3-Coder, Gemma 4, Llama", "Answering, tool use, routines", "Follows an instruction, JSON action format, 3 prompt-injection probes"],
     ["<b>Voice</b> (speech-to-text)", "Qwen3-ASR, Whisper", "Microphone button", "Transcribes a short test clip"],
     ["<b>Vision</b>", "Qwen3-VL, Qwen2.5-VL", "Pictures and scanned PDFs", "Looks at a red square and must say it is red"],
     ["<b>Embedding</b>", "nomic-embed", "Memory search", "Makes a vector"]]) + """
<p>Each kind has its own card, list, default and Add button in <b>Settings &rarr; AI Model</b>. The gateway refuses a voice model added as a chat model, refuses wrong role assignments, and never uses a wrongly assigned model.
After <b>Add</b> the test runs by itself in the background; when it passes the model becomes the default <i>for its own kind</i> if none was set.</p>"""))

S.append(section("voice", "9. Voice: long dictation in 30-second pieces", flow(
    "Press the microphone: the window records 16 kHz mono audio in memory (nothing is stored as a file).",
    "Every ~30 s, at a quiet moment (max 40 s) a piece is cut off and sent to <code>voice.transcribe</code> while you keep talking.",
    "The gateway converts/checks the WAV and asks the voice model (Ollama <code>/api/chat</code> with the audio); the answer's language tag is removed.",
    "The text appears in the message box piece by piece; a failed piece is retried; unsent text is kept as a draft.",
    "Press Stop: only the last piece is awaited, then the message is sent (a 9-minute test took 16 s in total)."
)))

S.append(section("memory", "10a. Memory and file summaries: how the assistant learns about you", row(
    flow("<b>You chat</b> / switch on a routine / add a file", "A <b>local model</b> reads ONLY the lines you typed (never mail, web or tool output) and proposes up to 3 lasting facts",
         BOX("Code filters: <b>no secrets, no ID/PAN/Aadhaar/card/phone/e-mail values</b>, no duplicates, nothing you already forgot, daily cap", "choice"),
         "Saved in <b>Memory</b> with its source - visible, editable, <b>Forget</b>-able", "Next questions: the assistant is told your learned facts and the ones relevant to what you just asked"),
    flow("<b>You add a file</b> (e.g. PAN card)", "After scan + parse, a model writes: title, kind, summary, keywords, <i>kinds</i> of personal data", "Code finds ID values itself and removes them from anything the model wrote; the file is raised to CONFIDENTIAL",
         "A memory records <i>where the file is and what it is</i>", "Later: &laquo;show me my PAN card&raquo; &rarr; <code>files.find</code> &rarr; <code>files.read</code> shows the real file"))
, "Everything is switchable: Settings &rarr; Memory (Learn about me automatically) and Settings &rarr; Files &amp; Storage (Summarise new files automatically)."))

S.append(section("mail", "10. Outlook and routines", flow(
    "Switch on <b>Local Outlook</b> in Settings &rarr; Connectors (classic Outlook only; new Outlook cannot be read).",
    "<b>Email monitoring skills</b> (hourly check, waiting for my approval, VIP mail, deadlines...) are <i>routines</i>: saved read-only instructions with a schedule.",
    "When one runs, the app first <b>fetches the data itself</b> (code, not the model): counts, To vs CC, replied or not, flags like APPROVAL / DEADLINE / URGENT / QUESTION.",
    "The model only turns that data into a short answer. If nothing is new it must answer <code>NOTHING_NEW</code> and you are not disturbed.",
    "Results show on the <b>Outlook</b> screen; a Windows notification is titled with the routine's name."
), "<b>Routine vs Mission:</b> same engine, two names. A <i>routine</i> repeats on a schedule (daily mail digest); a <i>mission</i> is usually a one-off goal (research and report). Both only get the tools you allow and anything risky still asks you."))

S.append(section("sec", "11. How your data is protected", table(
    ["Layer", "What it does"],
    [["<b>Password + PIN</b>", "Password is stretched with Argon2id. PIN is wrapped by the PC's TPM chip (or a software key in developer mode only)."],
     ["<b>Key hierarchy</b>", "Master key (random) &rarr; wraps the database key, file keys, secrets key. Recovery key (printed once) can rebuild access."],
     ["<b>Database</b>", "SQLCipher: the whole database file is AES-encrypted; settings, chats, memory, secrets are inside."],
     ["<b>Audit log</b>", "Append-only, each line carries the hash of the previous one (HMAC chain); <i>Verify</i> in Activity shows if anything was changed."],
     ["<b>Named pipe + tokens</b>", "Window and gateway talk over a local pipe; each side must show a random token and its process id is checked (no pipe squatting)."],
     ["<b>Roles</b>", "Each connection has a role (ui / core / worker); each method is allowed for specific roles only. The AI role cannot touch settings, secrets, approvals or the kill switch."],
     ["<b>Egress filter</b>", "Gateway-only web access with allow-list, SSRF protection (no private/loopback/metadata addresses, DNS pinning, redirect re-checks)."],
     ["<b>DLP</b>", "Blocks secrets, keys, card/ID numbers from leaving; confidential data to remote models needs approval, restricted data never leaves."],
     ["<b>STOP ALL</b>", "Kill switch stops the agent, tasks, connectors, web and sandbox within ~2 seconds; releasing needs your password."],
     ["<b>Safe display</b>", "Chat text is sanitised (DOMPurify), links need a click-through, remote images are never loaded, strict content security policy in the window."]]),
    "More detail: docs/THREAT_MODEL.md, docs/SECURITY_AUDIT_2026-10-02.md."))

# ---- buttons
def btn(name: str, what: str) -> list[str]:
    return [f"<b>{name}</b>", what]

BUTTONS = {
    "Left menu": [btn("Home (Alt+H)", "A board of widgets (mail unread / in To / waiting for approval / deadlines, reminders, next routine runs, approvals, needs attention, recent files, storage, what I learned, model health, activity, updates). The Widgets button on the right switches each one on/off and orders them."),
                  btn("Chat (Alt+C)", "Talk to the assistant. Chat list on the left (menu button), pin/rename, folders."),
                  btn("History (Alt+I)", "Past chats and work in one timeline; search by word beginnings; rename and pin from here."),
                  btn("Missions & Routines (Alt+M)", "Create/run/pause scheduled jobs with a schedule picker and output format."),
                  btn("Reminders (Alt+R)", "Reminders the assistant created for you; snooze/done."),
                  btn("Tasks (Alt+T)", "Everything running or waiting in the background; stop or resume."),
                  btn("Approvals (Alt+A)", "Cards waiting for your Yes/No: actions, reminders, proposed routines."),
                  btn("My Files (Alt+F)", "Upload, preview, label, add to My Knowledge; each file gets an automatic, editable summary (kind, summary, keywords, kinds of personal data); Quarantine tab with release of unscanned files."),
                  btn("Memory (Alt+E)", "What the assistant knows about you. It learns automatically from your chats, routines and files; you can edit, disable or Forget anything (forgotten facts never come back) and switch learning off."),
                  btn("Secrets (Alt+S)", "Password vault: reveal/copy with step-up, bind a secret to one tool (e.g. the web search key)."),
                  btn("Activity log (Alt+L)", "Requests &amp; steps, security log (verify chain), what did the agent do, search history, network logs, usage &amp; budgets."),
                  btn("Outlook (Alt+O)", "Results of the email monitoring routines."),
                  btn("Guide (Alt+G, F1)", "Searchable help for every feature."),
                  btn("Settings (Alt+,)", "All settings (below). The GPU meter above it shows the model working.")],
    "Top bar": [btn("Search / commands (Ctrl+K)", "Jump to any screen, start a chat, search your history."),
                btn("Lock (Ctrl+Shift+L)", "Locks the window; keys leave memory per your settings."),
                btn("STOP ALL (Ctrl+Alt+Shift+S)", "Emergency stop menu: pause the agent, stop tasks, disable connectors / web / sandbox.")],
    "Chat screen": [btn("Paperclip", "Share one file (spreadsheet, document, picture, scan) read in place."),
                    btn("Folder button (/folder)", "Share a folder; tick subfolders only if you want them."),
                    btn("Microphone", "Live voice input in 30-second pieces."),
                    btn("Send / Enter", "Sends. Shift+Enter = new line. Type / for commands: /new /remind /mission /search /attach /folder /pin /rename /theme /lock /help."),
                    btn("Ctrl+V", "Paste a screenshot: saved to My Files and read by the vision model."),
                    btn("Pencil / F2", "Rename this chat. Pin icon pins it."),
                    btn("Tools switch", "Lets the assistant use tools in this chat (off = it can only talk)."),
                    btn("Steps", "Shows every step: model calls, tools, policy decisions, approvals."),
                    btn("Ring in the header", "Today's model tokens; click for Usage &amp; budgets."),
                    btn("Right-edge ticks", "Position rail: one tick per question; hover for the list, click to jump."),
                    btn("Archive / Trash", "Archive hides the chat; Delete has an 8-second Undo and removes it from search.")],
    "Settings": [btn("Account &amp; Security", "Password/PIN/recovery key, auto-lock, sign-in history, security posture check, profile names."),
                 btn("Connectors", "Switch Outlook, Microsoft 365, Web on/off for chat and for routines; Test web search."),
                 btn("AI Model", "Four cards (Chat / Voice / Vision / Embedding): Discover, Add, Test, remove, defaults."),
                 btn("Autonomy &amp; Budgets", "How much the assistant may do alone; daily limits."),
                 btn("Rules &amp; Safety, Approvals, Tools &amp; Skills", "What needs approval, which tools/skills exist, plain-language rules; loosening needs password + 10-second read delay."),
                 btn("Web Access", "Provider (Exa/Brave/Tavily/SearXNG), allowed sites, ask per chat."),
                 btn("Files &amp; Storage", "Quota (10 GB), max file size, retention, accept unscanned files."),
                 btn("Notifications", "Windows toasts, show names, content level, quiet hours."),
                 btn("Appearance &amp; Voice", "8 themes, accent colours, background, 20 fonts, text size, pictures, voice on/off."),
                 btn("Logs, Backup, Updates, Privacy, Diagnostics", "SIEM export, encrypted backups/restore, update check, data map and delete-everything, health report."),
                 btn("Emergency Stop", "Same kill switches as STOP ALL, with release.")],
}
S.append(section("buttons", "12. What each screen and button does", "".join(f"<h3>{k}</h3>" + table(["Control", "What happens"], v) for k, v in BUTTONS.items()),
                 "Every button calls one named request (RPC) on the gateway. There are 155 of them; each has a test scenario (docs/ACTION_CATALOG.md)."))

S.append(section("settings", "13. How a setting change works", flow(
    "You flip a switch in Settings.", "The window asks the gateway to <b>classify</b> it: is it <i>tightening</i> (safer), <i>loosening</i> (less safe), or neutral?",
    BOX("Tightening / neutral: applied immediately. <b>Loosening</b>: a warning with the risk in plain words, a 10-second read delay and your password", "choice"),
    "Saved in the encrypted database, logged with before/after values, shown at once. Some settings (like the search provider) also need a fresh re-authentication."
), "Defaults live in code (<code>settings_schema.py</code>). A test changes, reads back and resets every setting automatically."))

# ---- stack
STACK = table(["Area", "Technology", "Why"],
              [["Desktop shell", "Tauri 2 (Rust), WebView2", "Small native window with a very small permission list; tray icon, notifications, file dialogs, single instance"],
               ["User interface", "React 19, TypeScript 5.9, Vite 6, plain CSS tokens", "Fast UI; themes, fonts and sizes are CSS variables"],
               ["UI safety", "DOMPurify, marked, strict CSP", "Safe rendering of the assistant's text"],
               ["Backend", "Python 3.12 (gateway, core, workers)", "Rich libraries, easy to audit"],
               ["IPC", "Windows named pipes, 4-byte length + JSON frames", "No network port"],
               ["Database", "SQLite + SQLCipher (FTS5 for search)", "Encrypted at rest, full-text search inside the encrypted file"],
               ["Crypto", "cryptography (AES-GCM, HKDF), Argon2id, Windows CNG/TPM 2.0", "Only vetted libraries"],
               ["Models", "Ollama (local) or any OpenAI-compatible server; optional built-in llama.cpp (GGUF)", "Local-first"],
               ["Files", "pypdf, own OOXML/CSV reader with SQLite index, numpy", "Isolated parsing; 100 MB sheets in about 1 s after the first index"],
               ["Outlook", "pywin32 COM (classic Outlook)", "Uses Outlook's own object model"],
               ["Packaging", "PyInstaller bundle + PowerShell installer (logon tasks, firewall rules, shortcuts)", "No admin needed to run, admin once to install"],
               ["Quality", "pytest (475+ tests), end-to-end runner (200 steps), ruff, tsc, corpus attack runs, RPC fuzzing, CI workflows (CodeQL, audits)", "Everything security-relevant has tests"]])
S.append(section("stack", "14. Tech stack", STACK))

# ---- SBOM
def py_rows(items):
    return [[f"<b>{E(n)}</b>", E(v), E(PY_PURPOSE.get(n, "dependency of: " + (via or "?"))), "direct" if n in {"cryptography", "argon2-cffi", "pydantic", "httpx", "numpy", "pypdf", "tzlocal", "tzdata", "sqlcipher3-wheels", "pywin32"} else "transitive"]
            for n, v, via in items]


sbom = f"""
<p>Generated from the real lock files on {date.today().isoformat()}. Python versions are exact and hash-pinned (<code>requirements.lock</code>); npm from <code>package-lock.json</code>; Rust from <code>Cargo.lock</code>.
Machine-readable CycloneDX files are produced in CI (<code>security.yml</code>). Licences for Python and Rust crates are not recorded in the lock files - the CI SBOM adds them.</p>
<div class="cards">
<div class="card"><b>{len(py_rt)}</b><br>Python runtime packages</div><div class="card"><b>{len(py_dev) - len(py_rt)}</b><br>extra dev/test/build packages</div>
<div class="card"><b>{len(npm_rt)}</b> / {len(npm)}<br>npm runtime / total packages</div><div class="card"><b>{len(crates)}</b><br>Rust crates (incl. Windows/Tauri internals)</div></div>
<h3>Python - runtime (shipped inside the app)</h3>{table(["Package", "Version", "What it is used for", "Kind"], py_rows(py_rt))}
<h3>Python - development, test and build only (not shipped)</h3>
<details><summary>Show {len([x for x in py_dev if x[0] not in rt_names])} packages</summary>{table(["Package", "Version", "Pulled in by", ""], [[f"<b>{E(n)}</b>", E(v), E(via or "direct dev tool"), ""] for n, v, via in py_dev if n not in rt_names])}</details>
<h3>npm - user interface</h3>
<p class=small>Direct runtime: react, react-dom, marked, dompurify, @tauri-apps/api, plugin-dialog, plugin-notification, plugin-opener. Direct build tools: vite, @vitejs/plugin-react, typescript, @tauri-apps/cli, @types/react*.</p>
<p class=small>Licences: {", ".join(f"{E(k)}: {v}" for k, v in sorted(licenses.items(), key=lambda x: -x[1]))}.</p>
<details><summary>Runtime packages in the app ({len(npm_rt)})</summary>{table(["Package", "Version", "Licence"], [[f"<b>{E(n)}</b>", E(v), E(lic)] for n, v, lic, d in npm_rt])}</details>
<details><summary>Build-time only packages ({len(npm) - len(npm_rt)})</summary>{table(["Package", "Version", "Licence"], [[E(n), E(v), E(lic)] for n, v, lic, d in npm if d])}</details>
<h3>Rust - the window shell</h3>
<p class=small>Direct: tauri 2 (tray-icon), tauri-plugin-dialog, -notification, -opener, -single-instance, -global-shortcut, serde, serde_json, tokio. Release build: LTO, size-optimised, panic=abort, stripped.</p>
<details><summary>All {len(crates)} crates with versions</summary>{table(["Crate", "Version"], [[E(n), E(v)] for n, v in crates])}</details>
<h3>Other things that are part of the product but are not packages</h3>
{table(["Item", "Role"], [["Python 3.12 runtime (inside the PyInstaller bundle)", "Runs gateway/core/workers; bundled so you do not need Python"], ["PyInstaller", "Builds the .exe files (build time only)"], ["WebView2 runtime", "Windows component that draws the window (installed with Windows 11)"],
 ["Ollama + the models you pulled", "Separate programs you installed; the app only talks to them on 127.0.0.1"], ["Exa / Brave / Tavily / SearXNG", "Optional web search providers, only if you add a key; allowed to contact only their own host"],
 ["Microsoft Graph", "Optional Microsoft 365 connector (needs your own Entra app registration)"], ["Windows features", "TPM 2.0 (CNG), Task Scheduler, Firewall, AMSI, Defender or your antivirus, classic Outlook COM"], ["Fonts", "None are bundled: the 20 fonts you can choose come from Windows/Office"], ["Icons", "ChiRAG icon supplied by you"]])}
"""
S.append(section("sbom", "15. SBOM (software bill of materials)", sbom))

S.append(section("tests", "16. How we know it works", table(["Check", "What it covers"], [
    ["475+ automatic tests", "crypto, vault, policy, approvals, tools, files, Outlook skills, model kinds, vision, folder approvals, history, web approval, settings (every one round-trips), icons"],
    ["205-step end-to-end run", "A throwaway gateway is started and every screen's backend request is called like the window would"],
    ["Hostile file corpus (217 files)", "Malformed, polyglot, Office tricks, image bombs, prompt-injection documents: no crash, no hang, no network call"],
    ["RPC fuzzing (4000+ hostile calls) and pipe abuse", "Wrong types, huge/odd text, bad frames, role confusion: the guard keeps serving"],
    ["Red-team suite", "A fully malicious 'model' tries to leak data, widen rights, approve itself: nothing happens"],
    ["Live checks", "Real Ollama (voice, vision, chat) and real Outlook on this PC, with throwaway accounts"],
    ["Visual checks", "Browser preview of every screen with 21 fonts x 3 sizes"]])))

NAV = "".join(f'<a href="#{i}">{t}</a>' for i, t in [("what", "What it is"), ("big", "Big picture"), ("procs", "Programs"), ("chat", "A chat message"), ("tools", "Tool safety"), ("files", "Files"), ("local", "Folders"),
                                                       ("models", "Models"), ("voice", "Voice"), ("memory", "Memory"), ("mail", "Outlook"), ("sec", "Protection"), ("buttons", "Buttons"), ("settings", "Settings"),
                                                       ("stack", "Tech stack"), ("sbom", "SBOM"), ("tests", "Testing")])
CSS = """
:root{--bg:#f8fafc;--fg:#0f172a;--mut:#475569;--card:#fff;--bd:#e2e8f0;--acc:#4f46e5}
@media (prefers-color-scheme:dark){:root{--bg:#0b1020;--fg:#e5e7eb;--mut:#94a3b8;--card:#131a2e;--bd:#26304d;--acc:#818cf8}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--fg);font:16px/1.6 "Segoe UI",system-ui,Arial,sans-serif}
header{padding:32px 24px 12px;max-width:1100px;margin:auto}h1{margin:0;font-size:2em}.sub{color:var(--mut)}
nav{position:sticky;top:0;background:var(--bg);border-bottom:1px solid var(--bd);padding:8px 24px;z-index:5;display:flex;flex-wrap:wrap;gap:6px}
nav a{padding:3px 10px;border:1px solid var(--bd);border-radius:999px;color:var(--fg);text-decoration:none;font-size:.85em;background:var(--card)}nav a:hover{border-color:var(--acc)}
main{max-width:1100px;margin:auto;padding:0 24px 80px}section{margin:34px 0}h2{border-left:5px solid var(--acc);padding-left:12px}h3{margin-top:26px}
.lead{color:var(--mut)}code{background:var(--card);border:1px solid var(--bd);padding:1px 6px;border-radius:6px;font-size:.88em}
.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(210px,1fr));gap:12px;margin:14px 0}.card{background:var(--card);border:1px solid var(--bd);border-radius:12px;padding:12px 14px}
.flow{display:flex;flex-direction:column;align-items:center;gap:0;margin:14px 0}.box{background:var(--card);border:1px solid var(--bd);border-radius:12px;padding:10px 14px;width:100%;max-width:760px;box-shadow:0 1px 2px #0002}
.box.choice{border:2px solid var(--acc);background:color-mix(in srgb,var(--acc) 8%,var(--card))}.arrow{color:var(--acc);font-size:1.4em;line-height:1.1}
.hrow{display:flex;gap:18px;flex-wrap:wrap;align-items:flex-start}.hcol{flex:1 1 340px;min-width:0}.note{background:var(--card);border:1px solid var(--bd);border-radius:12px;padding:10px 16px}
table{width:100%;border-collapse:collapse;background:var(--card);border:1px solid var(--bd);border-radius:10px;overflow:hidden;font-size:.92em;margin:10px 0}th,td{padding:7px 10px;border-bottom:1px solid var(--bd);text-align:left;vertical-align:top}
th{background:color-mix(in srgb,var(--acc) 10%,var(--card))}details{margin:10px 0}summary{cursor:pointer;font-weight:600;color:var(--acc)}.small{font-size:.9em;color:var(--mut)}
.diagram{width:100%;height:auto;background:var(--card);border:1px solid var(--bd);border-radius:12px}.diagram text{fill:#0f172a}
footer{max-width:1100px;margin:auto;padding:0 24px 40px;color:var(--mut);font-size:.85em}
"""
page = f"""<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>How ChiRAG Agent works</title><style>{CSS}</style></head><body>
<header><h1>How ChiRAG Agent works</h1><div class="sub">Architecture, flows, tech stack, SBOM and what every button does - in plain technical language. Generated {date.today().isoformat()} from the real project files.</div></header>
<nav>{NAV}</nav><main>{"".join(S)}</main>
<footer>Generated by scripts/make_howitworks.py. Re-run it after changes to refresh the SBOM tables. Related: docs/ARCHITECTURE.md, docs/THREAT_MODEL.md, docs/USER_GUIDE.md, docs/ACTION_CATALOG.md.</footer></body></html>"""
out = ROOT / "docs" / "HOW_IT_WORKS.html"
out.write_text(page, encoding="utf-8")
print("wrote", out, f"({len(page) // 1024} KB)")
