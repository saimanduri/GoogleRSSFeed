"""ID-like personal data detection shared by memory learning and file summaries (code, independent of any model)."""
from __future__ import annotations

import re

PII = {
    "PAN number": re.compile(r"\b[A-Z]{5}[0-9]{4}[A-Z]\b"),
    "Aadhaar number": re.compile(r"\b[2-9]\d{3}\s?\d{4}\s?\d{4}\b"),
    "bank IFSC code": re.compile(r"\b[A-Z]{4}0[A-Z0-9]{6}\b"),
    "phone number": re.compile(r"(?<!\d)(?:\+91[\s-]?)?[6-9]\d{4}[\s-]?\d{5}(?!\d)"),
    "email address": re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+"),
    "passport number": re.compile(r"\b[A-PR-WY][1-9]\d{6,7}\b"),
}
CARD = re.compile(r"(?<!\d)(?:\d[ -]?){13,19}(?!\d)")
DOB = re.compile(r"(date of birth|\bdob\b|जन्म)", re.I)
def luhn(digits: str) -> bool:
    s, alt = 0, False
    for ch in reversed(digits):
        d = int(ch)
        if alt:
            d = d * 2 - 9 if d * 2 > 9 else d * 2
        s += d
        alt = not alt
    return s % 10 == 0


def detect_pii(text: str) -> list[str]:
    found = [kind for kind, rx in PII.items() if rx.search(text)]
    if any(luhn(re.sub(r"\D", "", m.group())) for m in CARD.finditer(text) if 13 <= len(re.sub(r"\D", "", m.group())) <= 19):
        found.append("card number")
    if DOB.search(text):
        found.append("date of birth")
    return found


def redact(text: str) -> str:
    for rx in list(PII.values()) + [CARD]:
        text = rx.sub("[hidden]", text)
    return text




def detect_values(text: str) -> list[str]:
    """Only real ID-like VALUES (not the mere mention of a kind such as 'date of birth'): used to keep values out of summaries and memories."""
    found = [kind for kind, rx in PII.items() if rx.search(text)]
    if any(luhn(re.sub(r"\D", "", m.group())) for m in CARD.finditer(text) if 13 <= len(re.sub(r"\D", "", m.group())) <= 19):
        found.append("card number")
    return found
