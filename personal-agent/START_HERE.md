# START HERE - continuing Personal Agent (ChiRAG Agent) 0.1.14 in a new repository

This folder is the complete source of Personal Agent at the end of 2026-10-05 (branch `claude/v0.1.14`, based on 0.1.13):
code (Python gateway/core/workers, React + Tauri UI), tests, CI workflows (`.github/workflows/windows-ci.yml`), installer scripts and
all documentation. Nothing else is needed from the old repository.

## 1. Put it into the new GitHub repository
```powershell
# unzip so that this file is at the top of the folder, e.g. F:\ChiRAG\personal-agent\START_HERE.md
cd F:\ChiRAG\personal-agent
git init -b main
git add -A
git commit -m "Personal Agent 0.1.14 work in progress (imported)"
git remote add origin https://github.com/<new-account>/<new-repo>.git
git push -u origin main
```
CI: `.github/workflows/windows-ci.yml` runs on GitHub Actions `windows-latest` (python tests incl. e2e runner, UI build, bundle).

## 2. What to read (in this order)
1. `CLAUDE.md` - rules that must never be broken (security floor, no secrets in logs, tests with every change).
2. `docs/HANDOFF.md` - state of the project, the user's PC, gotchas (section 3f = latest round).
3. `docs/CHANGES_2026-10-04.md` - what was done on 2026-10-04/05.
4. `PENDING_WORK.md` - section **"0.1.14 - remaining work"**: items W1-W7 with step-by-step instructions and the user's decisions.
5. `docs/PLAN_0.1.14.md` - design behind every remaining item; `docs/requirements/OUTLOOK_REQUIREMENTS.md` - Outlook requirements.
6. `TESTS.md` - how everything is tested; laptop checks H40-H47 are still open.

## 3. Status in one paragraph
Done: CI fixes, logo/title-bar icon, Rules & Safety layout, resource meters, per-model context length + temperature (Ollama via native
API), Tasks merged into Activity log, "Routines" as the only name, Home redesign, web research (Exa crawl / answer / research).
Linux: 498 tests pass; Windows CI green. **Not done**: W1 Outlook work memory + new Outlook screen, W2 code review of 0.1.12-0.1.13 +
Guide check, W3 meeting minutes + pyannote Community-1 diarization, W4 Word/Excel/PowerPoint output, W5 finance tools + bundled sandbox
Python (numpy, pandas, openpyxl), W6 local-file safety, W7 version bump + independent security assessment. **The security / bug review
has NOT been done - do not use the app with real data before W2 and W7.**

## 4. Prompt for the new Claude Code session (copy everything between the lines)
---
You are continuing development of "Personal Agent" (ChiRAG Agent), a local-first, security-first AI agent for Windows 11 (never WSL).
This repository is the project root. First read, in this order: START_HERE.md, CLAUDE.md, docs/HANDOFF.md, docs/CHANGES_2026-10-04.md,
PENDING_WORK.md (section "0.1.14 - remaining work"), docs/PLAN_0.1.14.md, docs/requirements/OUTLOOK_REQUIREMENTS.md, TESTS.md.
Then give me a short summary of the state and your plan, and wait for my OK.

Work rules:
- Do the remaining items W1-W7 from PENDING_WORK.md in this order: W4, W2, W6, W5, W3, W1, W7, using the user decisions recorded there
  (pyannote Community-1; python-docx / openpyxl / python-pptx; bundled pinned sandbox Python with numpy, pandas, openpyxl).
- Follow CLAUDE.md strictly. Never log, print or send secrets, PINs, passwords, recovery keys or tokens. Never test against the real TPM.
- Every change gets tests (unit / integration; red-team case for anything with side effects), a Guide entry for UI changes
  (app/ui/src/screens/guideData.ts), mock support (app/ui/src/api/mock.ts), e2e catalogue entries for new RPCs
  (tests/e2e/scenarios.json + ui_actions.json, then `python scripts/gen_action_catalog.py`), and `python scripts/gen_mock_schema.py`
  after settings changes.
- After each finished item: run `python -m pytest -q -p no:cacheprovider`, `ruff check app tests scripts --select E,F,W,B --ignore
  E501,B905,B007,B904`, `cd app/ui && npx tsc --noEmit && npm run build`; update VERSION_HISTORY.md, PROGRESS.md, TESTS.md,
  docs/CHANGES_<date>.md; remove the item from PENDING_WORK.md; commit with a clear message; push; check that Windows CI is green.
- Ask me before installing software, admin/firewall changes, deleting data folders or adding dependencies not listed above.
- W2 (code review, fix real bugs with regression tests) and W7 (independent security assessment: SAST + DAST against a throwaway dev
  gateway + manual review against CLAUDE.md rules 1-16; genuine, reproducible findings only, each with proof, severity and fix; report in
  docs/SECURITY_AUDIT_<date>.md; fix with tests) are mandatory before the app is used with real data.
- At the end: bump the version to 0.1.14 everywhere (pa_common/version.py, pyproject.toml, app/ui/package.json,
  app/ui/src-tauri/tauri.conf.json, app/ui/src-tauri/Cargo.toml, mock "about"), build the zip with `python scripts/make_zip.py`, and give
  me a prompt to deploy and test on my laptop (TESTS.md H40 onwards).
---

## 5. Deploying on the laptop (after the work is done)
```powershell
py -3.12 -m venv .venv; .\.venv\Scripts\Activate.ps1; pip install -r requirements-dev.txt
python -m pytest -q -p no:cacheprovider
cd app\ui; npm install; npx tauri build --no-bundle; cd ..\..
powershell -File scripts\build.ps1 -OutDir dist\PersonalAgent-0.1.14
copy app\ui\src-tauri\target\release\pa-ui.exe dist\PersonalAgent-0.1.14\
# as administrator (stops the running app, installs, starts it again):
powershell -File installer\windows\update-app.ps1
```
Then go through TESTS.md checks H40-H47 in the real window.
