"""Deterministic mail heuristics for the Outlook worker: approval requests, deadlines, urgency, questions.

Pure functions (no COM, no model) so they are unit-testable and give the same answer every time. The language model
then only has to summarise the flagged mails; it does not have to guess which ones matter.
"""
from __future__ import annotations

import re
from datetime import date, datetime, timedelta

# "for your approval", "please approve", "approval is sought", "may kindly approve", "concurrence" ... but NOT "was approved".
_APPROVAL = re.compile(
    r"(for\s+(your\s+|the\s+)?(kind\s+|necessary\s+|further\s+)?(approval|concurrence|sanction|consideration\s+and\s+approval)"
    r"|(please|kindly|may\s+kindly|request\s+you\s+to|requested\s+to)\s+(\w+\s+){0,2}(approve|accord|sanction|concur|clear)\b"
    r"|approval\s+(is\s+|are\s+)?(sought|requested|required|awaited|needed|solicited)"
    r"|(seek|seeking|sought|solicit\w*)\s+(your\s+|the\s+)?(kind\s+)?(approval|concurrence|sanction|nod)"
    r"|put\s+up\s+for\s+approval|submitted\s+for\s+approval|pending\s+(your\s+)?approval|await\w*\s+(your\s+)?approval"
    r"|approval\s+of\s+the\s+(competent|appropriate)\s+authority|need\w*\s+your\s+(approval|sign[- ]?off))",
    re.I,
)
_URGENT = re.compile(r"\b(urgent|asap|immediately|at the earliest|top priority|high priority|critical|time[- ]sensitive)\b", re.I)
_QUESTION = re.compile(r"(\?|\b(please|kindly)\s+(let me know|advise|confirm|revert|respond|reply|share your (views|comments|inputs))\b)", re.I)
_CUE = r"(?:deadline|due(?:\s+date)?|due\s+by|by|before|on\s+or\s+before|latest\s+by|last\s+date(?:\s+is)?|no\s+later\s+than|till|until)"
_MONTHS = {m: i for i, m in enumerate(["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"], 1)}
_DAYS = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]

_ISO = re.compile(rf"{_CUE}\s*[:\-]?\s*(\d{{4}})-(\d{{2}})-(\d{{2}})", re.I)
_DMY = re.compile(rf"{_CUE}\s*[:\-]?\s*(\d{{1,2}})[/.\-](\d{{1,2}})[/.\-](\d{{2,4}})", re.I)
_DMON = re.compile(rf"{_CUE}\s*[:\-]?\s*(?:the\s+)?(\d{{1,2}})(?:st|nd|rd|th)?\s+(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\.?(?:,?\s+(\d{{4}}))?", re.I)
_MOND = re.compile(rf"{_CUE}\s*[:\-]?\s*(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\.?\s+(\d{{1,2}})(?:st|nd|rd|th)?(?:,?\s+(\d{{4}}))?", re.I)
_REL = re.compile(rf"{_CUE}\s*[:\-]?\s*(today|tonight|tomorrow|eod|cob|end of (?:the )?day|close of business|(?:this\s+|next\s+)?({'|'.join(_DAYS)}))", re.I)


def _safe(y: int, m: int, d: int) -> date | None:
    try:
        return date(y if y > 99 else 2000 + y, m, d)
    except ValueError:
        return None


def find_deadline(text: str, today: date) -> date | None:
    """Earliest date that follows a deadline cue ('by', 'before', 'due', 'deadline'...). Dates in the past are ignored
    unless they are within the last day (a deadline 'today' is still relevant)."""
    found: list[date] = []
    for m in _ISO.finditer(text):
        d = _safe(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        if d:
            found.append(d)
    for m in _DMY.finditer(text):
        d = _safe(int(m.group(3)), int(m.group(2)), int(m.group(1)))  # day-month-year (India / UK style)
        if d:
            found.append(d)
    for m in _DMON.finditer(text):
        d = _safe(int(m.group(3) or today.year), _MONTHS[m.group(2).lower()[:3]], int(m.group(1)))
        if d:
            found.append(d)
    for m in _MOND.finditer(text):
        d = _safe(int(m.group(3) or today.year), _MONTHS[m.group(1).lower()[:3]], int(m.group(2)))
        if d:
            found.append(d)
    for m in _REL.finditer(text):
        word = m.group(1).lower()
        if word in ("today", "tonight", "eod", "cob") or word.startswith("end of") or word.startswith("close of"):
            found.append(today)
        elif word == "tomorrow":
            found.append(today + timedelta(days=1))
        elif m.group(2):
            target = _DAYS.index(m.group(2).lower())
            delta = (target - today.weekday()) % 7
            if word.startswith("next") and delta == 0:
                delta = 7
            found.append(today + timedelta(days=delta))
    found = [d for d in found if d >= today - timedelta(days=1)]
    return min(found) if found else None


def scan(subject: str, body: str, importance: int = 1, today: date | None = None) -> dict:
    """Flags for one message. importance: Outlook 0 low, 1 normal, 2 high."""
    today = today or datetime.now().date()
    head = f"{subject}\n{body[:4000]}"
    dl = find_deadline(head, today)
    return {
        "approval": bool(_APPROVAL.search(head)),
        "deadline": dl.isoformat() if dl else None,
        "urgent": importance >= 2 or bool(_URGENT.search(head)),
        "question": bool(_QUESTION.search(body[:800])),
    }


def flag_text(f: dict) -> str:
    out = []
    if f["approval"]:
        out.append("APPROVAL")
    if f["deadline"]:
        out.append(f"DEADLINE:{f['deadline']}")
    if f["urgent"]:
        out.append("URGENT")
    if f["question"]:
        out.append("QUESTION")
    return ",".join(out)
