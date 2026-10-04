# Plan and gap analysis for 0.1.14 (written 2026-10-04)

This answers the "review and tell me" parts of the 0.1.14 request and is the order of work on branch `claude/v0.1.14`.
Status is kept current in VERSION_HISTORY.md (what is done), PENDING_WORK.md (what is left) and TESTS.md (laptop checks).

Legend: **Done** = on the branch with tests · **Next** = being built in this round · **Decision** = needs your answer first · **Later** = planned, not in this round.

---

## 1. Already done on the branch
| Item | Status |
|---|---|
| 3 Windows CI failures of 0.1.13 (deleted chat back in search, proposal badge race, voice model "default while testing") | Done |
| Top-left logo stale / title-bar icon | Done (hashed logo import + explicit window icon) |
| Settings > Rules & Safety "not in order" | Done (4 headed sections, readable choices, readable history) |
| Settings > Diagnostics: CPU / memory / GPU / storage, red at 90 % | Done (`resmon.py`, `diagnostics.resources`) |
| Context length + temperature per model | Done, **and a real bug fixed**: Ollama ignored the context length (prompts were cut to ~4096 tokens) |

---

## 2. Outlook: your requirements vs what exists
Source: `docs/requirements/OUTLOOK_REQUIREMENTS.md` (sections 1-40). What the app has today: a read-only COM worker (`pa_workers/outlook`) with
search / digest / mail_stats / awaiting_reply / get_message / attachments / calendar / AI drafts, deterministic flags
(APPROVAL / DEADLINE / URGENT / QUESTION, To vs CC, replied), 10 email-monitoring routines and widgets. It works **on demand**: every routine
asks Outlook again; nothing is remembered between runs.

| Req. | Requirement | Today | Gap / plan |
|---|---|---|---|
| 2, 12 | No mailbox copy; metadata + derived intelligence only | Nothing stored (only routine results) | **Next**: "Work memory" tables store metadata, gist, tasks, deadlines - never bodies/HTML/attachments (test asserts it) |
| 3 | Local Outlook only, later Exchange with minimal change | COM worker + separate Graph connector | **Next**: a `MailSource` interface (`detect_changes`, `fetch`, `open_original`) with an Outlook-COM implementation; the Microsoft 365 connector can implement the same interface later |
| 4, 26-28 | Incremental monitoring every ~60 s, durable checkpoint, Catch up since last run, startup prompt | None (routines re-scan a time window) | **Next**: monitor with checkpoint per folder (last ReceivedTime/SentOn + EntryIDs seen at that second), "Catch up since last run" with counts, startup banner "last monitored at ..." |
| 5, 35 | Never block Outlook; detection light; AI async | Worker is separate, but routines run the model inside the request | **Next**: detect (table API, 1 call per folder) -> queue -> separate processing thread with concurrency 1, rate limit, back-off when Outlook is busy |
| 6, 32 | Size limits, timeouts, retries, dead-letter, attachments metadata-only | Per-call limits only | **Next**: `processing_queue` with attempts, next_try, dead-letter state, size caps; attachments: name/size/type only, "process this attachment" on request |
| 7, 9-11 | Understand mail; logical topics separate from threads; topic gist updated incrementally; event timeline | Digest per call only | **Next**: topics + topic_threads + topic_events; model extracts a small JSON (topic match, decisions, actions, deadlines, commitments, confidence) and only the changed fields are merged |
| 13 | 6-month retention, configurable | n/a | **Next**: setting `workmem.retention_days` (183), daily cleanup that never touches Outlook |
| 14-15 | FTS5 + relational indexes + local semantic search, incremental | FTS exists for chats/files | **Next**: FTS rows and embeddings updated per changed topic/item only |
| 16, 37 | Open the original only when needed; say clearly when it is gone | get_message by EntryID | **Next**: references keep EntryID + StoreID + conversation key; "original not available" is reported, never invented |
| 17-19, 21-22 | Tasks (My actions / Waiting for / Delegated / Commitments / Informational), completion detection by confidence, manual completion with channel/note | Flags only | **Next**: tasks table with type, owner, due, status, confidence, source; outgoing mail matched to open tasks (high = auto-close when allowed, medium = "Likely done - confirm?", low = nothing); "Mark completed" with date / channel / note |
| 20, 25, 29 | Deadline buckets (overdue / today / 3 / 7 days, configurable), dashboard, meaningful notifications | Deadline flag + widget | **Next**: Work dashboard (Today, Upcoming, Overdue, Waiting for, My commitments, Recent changes, Topics, Reminders) - this is also the new Outlook screen (section 4) |
| 23-24 | Natural-language notes on a topic ("Sharma called ... remind me Friday"); reminders linked to topic/task | Reminders exist, not linked | **Next**: "Add update" box on a topic -> model proposes event + task + deadline + reminder -> you confirm (approval card) |
| 30-31 | Confidence on every AI-derived state; audit of each change (before / after / reason / confidence / source) | Audit log is security-oriented | **Next**: `workmem_changes` table + timeline entries; nothing is invented: unknown = empty, "I believe X - confirm?" for medium confidence |
| 33 | Idempotent: same mail never creates duplicates | n/a | **Next**: unique key = StoreID + EntryID (+ InternetMessageId when present); processing is insert-or-ignore |
| 34 | Privacy: local only, remote AI only by explicit choice | Same rule already (remote model needs password; RESTRICTED never leaves) | Keep; work memory is CONFIDENTIAL by default |

**Not possible / limited with local Outlook** (honest notes): the new Outlook (Monarch) has no COM API - only classic Outlook can be monitored; "changes to existing items" can only be detected for items still in monitored folders (moved items are found again by EntryID only within the same store); COM calls must run on one thread, so the monitor and on-demand tools share the worker (calls are short and queued).

---

## 3. Web research: Exa crawling and complex tasks (flights example, AI-news email)
Today: `web.search` (Exa / Brave / Tavily / SearXNG) + `web.fetch` (one page, SSRF-safe, allowlist). Missing for tasks like your flight
example: reading many pages, following a site's subpages, fresh (live) content, comparing and tabulating, and a planner that keeps going "until satisfied".

| Capability | Plan |
|---|---|
| Exa contents / crawl | **Next**: tool `web.read` -> Exa `POST /contents` (`urls`, `text.maxCharacters`, `highlights`, `summary`, `maxAgeHours` 0 = always live, `subpages` + `subpageTarget`, `livecrawlTimeout`); per-URL status errors (`CRAWL_NOT_FOUND`, `CRAWL_TIMEOUT`, ...) are shown; cost (`costDollars`) is logged |
| Better search | **Next**: `web.search` gains `category` (news, company, financial report, pdf, research paper), date range (`startPublishedDate`), include/exclude domains, `type` (auto / fast / deep), `userLocation: IN` |
| Answer with citations | **Next**: optional `web.answer` (Exa `/answer`) for quick factual questions with citations |
| Deep research | **Next**: tool `web.research` = Exa `/research/v0/tasks` (create, poll, citations) for long investigations; and a local **research loop** in the agent: plan -> search -> read -> notes -> check gaps -> more searches (budget-limited, all steps visible) -> final table with sources |
| Tables + URLs in answers | **Next**: answer format rules (Markdown table with columns + a "Sources" list); routines can choose Word / Excel output (section 7) |
| Security | Exa receives only the query/URLs (DLP + sensitivity rules as for web.search); crawled text is untrusted (labelled, injection heuristics); domains in results are not auto-allowlisted; per-chat web approval stays |

Honest limits: live flight prices and delay history come from airline / OTA pages (MakeMyTrip, Ixigo, Yatra are JavaScript-heavy; Exa's
crawler returns what it can render); prices change by the minute and some sites block crawlers, so the answer will cite timestamps and say
when a site could not be read. Booking stays manual (the app never buys anything).

---

## 4. Screens: Home, Outlook, Tasks vs Activity, Routine vs Mission
* **Tasks vs Activity log > Requests & steps**: the same work seen twice (a task is the durable record, a "request" is its step timeline;
  every task has one run). **Plan (Next)**: merge into Activity log > Requests & steps (state badge, budget bars, Stop / Resume, state
  history inside the request's detail); remove "Tasks" from the left menu; old links to Tasks open Activity log.
* **Routine vs Mission**: in the code they are identical - `kind` is only a label (same scheduler, same rules, same tools). **Plan (Next)**:
  one name, **Routines**, everywhere; the menu says "Routines"; the type selector and the Routines/Missions tabs go away (existing
  missions simply show as routines; nothing changes in how they run).
* **Home (cluttered)**: **Next**: three zones - (1) a short "Needs you now" strip (approvals, overdue, failures; hidden when empty),
  (2) "Today" (meetings, reminders, deadlines, routine runs) as one timeline, (3) widgets you chose, in a calm grid with equal heights;
  the "last 24 hours" events become a collapsible list; no duplicated counters.
* **Outlook screen (cluttered)**: **Next**: rebuilt around the work memory: left = Attention (overdue / today / 3 days / 7 days, waiting
  for you, waiting for others, commitments), middle = Topics (search, status filter), right = the selected topic (gist, tasks, timeline,
  "Add update", "+ Reminder", "Open original in Outlook"). Email-monitoring skills move to a "Routines" tab of that screen; Catch-up
  button + "last monitored" in the header. Progressive disclosure: details only when you open a topic.

---

## 5. Meeting minutes, live transcript, diarization
* **Live transcript with crash safety**: the voice button already transcribes ~30 s pieces. **Next**: a **Meeting** mode (Chat > mic menu,
  or `/meeting`): every finished piece is saved to the encrypted database immediately (worst case after a crash: the last piece,
  < 2 minutes), the transcript is shown live, "Resume" after a restart, "Make minutes" at the end (summary, decisions, action items,
  owners, due dates -> can become reminders / work-memory tasks). Audio is not stored unless diarization needs it (then only encrypted,
  deleted after processing).
* **Diarization (who spoke when)**: Ollama cannot run diarizers. Open-source options that run on this PC:
  | Option | Licence | Needs | Notes |
  |---|---|---|---|
  | sherpa-onnx + pyannote segmentation-3.0 (ONNX) + 3D-Speaker / TitaNet embedding (ONNX) | Apache-2.0 / MIT | 1 pip package (`sherpa-onnx`), ~40 MB model files | CPU, no PyTorch, no account; recommended |
  | pyannote.audio 3.1 / community-1 | MIT (code), model terms on Hugging Face | PyTorch (~2 GB), HF token for gated weights | best known quality, heavy |
  | NVIDIA Sortformer / NeMo | NVIDIA open model licence | NeMo + PyTorch + GPU | streaming, heavy |
  | A local diarization server (WhisperX / pyannote) on 127.0.0.1 | - | you run it | no new dependency in the app |
  All of them answer in the same shape: a list of segments `{start, end, speaker}` (RTTM). Qwen3-ASR via Ollama returns text **without
  timestamps**, so the app must **diarize first, then transcribe each speaker turn** (cut the audio at speaker changes) and join
  "Speaker 1: ..." lines; speakers are matched across 30-s pieces by their voice embedding (cosine similarity), and you can rename
  "Speaker 1" to a name. GUI: a fifth model card "Diarization models" next to Chat / Voice / Vision / Embedding (add / test / default),
  tested with a built-in two-voice clip. **Decision**: which option (recommendation: sherpa-onnx).
* **Vision + audio testing**: **Next**: more automated tests (WAV conversion edge cases, piece joining, language prefix, empty / silent
  audio, wrong kinds, huge images, multi-page scans with fake vision server) - see TESTS.md when done; the real microphone and real
  models remain laptop checks.

---

## 6. Finance (financial statements, local models, sandbox) - compared with alirezarezvani/claude-skills/finance (MIT)
| Skill there | What it does | In this app today | Plan |
|---|---|---|---|
| financial-analyst | ratios (profitability, liquidity, leverage, efficiency, valuation), DCF, budget variance, rolling forecast | none (the model does arithmetic itself - unreliable) | **Next**: deterministic tools `finance.ratios`, `finance.dcf`, `finance.variance`, `finance.forecast` adapted from the MIT scripts (stdlib only, attribution kept) |
| stock-analysis | sector-relative fundamental analysis (India NSE/BSE + US): earnings quality, DuPont, valuation, forensic red flags, governance, scoring rubric, report template, data verification | none | **Next**: `finance.stock_scorecard` (ratios + red flags + score) and a "Company analysis" skill = report template + challenge pass; company research via `web.search` (category financial report / news) + `web.read` |
| saas-metrics-coach | ARR/MRR, churn, CAC/LTV, NRR, quick ratio | none | **Later** (`finance.saas_metrics`) |
| business-investment-advisor | NPV, IRR, payback, build-vs-buy, lease-vs-buy | none | **Next**: `finance.investment` (NPV / IRR / payback) |
| (statement extraction) | - | local files: xlsx/csv/pdf/docx reader (`tables.py`), large tables, vision for scans | **Next**: `finance.extract_statements` - find income statement / balance sheet / cash flow tables in a file or folder and normalise line items (code + model, every number traced to file / sheet / cell) |
| Sandbox | - | Python sandbox exists, but no bundled Python (D9) | **Decision**: bundle a pinned embeddable Python + numpy / pandas / openpyxl for free-form analysis (recommended), or calculators only |
Recommended model: qwen3-coder (Ollama) with context 32k-64k (now really applied) and temperature 0-0.2 for analysis.

---

## 7. Routine outputs as Word / Excel / PowerPoint
Today: Markdown / plain / bullets / table / JSON ... saved as text. **Next**: output formats "Word document", "Excel workbook",
"PowerPoint slides": the routine's answer (Markdown with headings, tables, bullet lists) is converted by code - headings -> styles,
tables -> real tables / sheets (numbers as numbers), bullets -> slide bullets; files land in My Files (and the routine result links them).
**Decision**: vetted libraries (python-docx, openpyxl, python-pptx; recommended) or a small built-in writer (no new dependency, plainer).

---

## 8. Local files and folders: are malicious files stopped? (honest review)
Good today: per-chat + per-session approval, subfolders need their own approval, paths resolved and kept inside the root, links / junctions
ignored, system / AppData / credential / network paths refused, programs never listed, the reader runs in a separate process with memory
and time limits, nothing is written back.

Gaps found (genuine):
1. **Files read in place skip the checks that uploads get** (content sniffing vs extension, archive / zip-bomb limits, OOXML structure,
   PDF active content, Defender / AMSI scan): `localfiles._run` hands the path straight to pa-parser. **Next**: run the same
   `files/checks.py` validation (and the antivirus scan when available) before the first read of each file version; refuse or warn.
2. **pa-parser has no AppContainer** (D8, Job Object only): a parser exploit in a crafted PDF/XLSX could read other files of the user.
   **Later (P2)**: AppContainer for pa-parser; meanwhile it never has network in the installed build (firewall rule).
3. **Mark-of-the-Web**: files downloaded from the internet carry `Zone.Identifier`; **Next**: show "downloaded from the internet" and
   treat macros / active content in such files as blocked.
4. **Cloud placeholders** (OneDrive "online-only" files): reading one triggers a download; **Next**: detect the offline / recall attributes
   and skip with a message instead of downloading silently.
5. **Spoofed names**: right-to-left override and look-alike characters in file names; **Next**: refuse names with bidi control characters.
6. **Very large folders**: listing is bounded, but repeated browse calls are not rate-limited per chat; **Next**: per-chat read budget.

---

## 9. Ideas from Claude Code "mods" and OpenAI "Dots"
* Claude Code **mods** (Oct 2026): plugins of function hooks that add live panes, bands above the prompt, status lines and toasts, and that
  can **block, rewrite or react to any tool call before it runs**; they hot-reload. Useful ideas here:
  (a) **User guard rules** - your own "before this tool runs" rules in plain words ("never send mail to outside my company", "ask me before
  any web read of a .gov.in site") compiled into the existing deterministic policy engine (can only tighten - rule 16);
  (b) a **live status band** above the chat box (running routine, queue length, model, context use) - partly exists (usage ring);
  (c) a **session pane** "what happened in this chat" (exists as Steps) with a one-click summary.
* OpenAI **Dots** (DevDay 29 Sep 2026): always-on personal agents that pursue a goal in the background across days, with memory, apps and
  voice calls. Useful ideas here (all local, approval-gated):
  (a) **Goals**: a standing objective ("keep me ahead of vendor deadlines", "watch RBI circulars on AI") that plans its own routines,
  checks progress daily and reports - built on Routines + work memory, every new action still needs your approval;
  (b) **hands-free voice conversation** (push-to-talk loop with the local ASR model + spoken answer via Windows speech) - later;
  (c) **one inbox of "things I did for you"** - the Home "Needs you now" strip + Activity.
  Not copied: cloud computers, 4,000 app connectors, remote browsing (against the local-first and security rules of this app).

---

## 10. Other items in this round
* **Code review of the last 4 days (0.1.12-0.1.13)**: **Next** - findings fixed with tests, listed in VERSION_HISTORY. First finding
  already: background file-summary threads can write after the database closed at sign-out ("Cannot operate on a closed database").
* **Guide accuracy**: **Next** - every Guide entry checked against the code and the screens; wrong entries fixed.
* **Security assessment** (after all coding): an independent pass (SAST, DAST against a throwaway gateway, manual review vs spec
  rules 1-16) by a separate agent; only genuine, reproducible findings, each with proof, severity and fix; report in
  `docs/SECURITY_AUDIT_<date>.md`; fixes with regression tests. No padding to reach a number.

## Decisions needed from you
1. Office files: libraries (python-docx / openpyxl / python-pptx) or built-in writer?
2. Diarization: sherpa-onnx (recommended), pyannote, or local server only?
3. Sandbox Python for finance: bundle it (recommended) or calculators only?
