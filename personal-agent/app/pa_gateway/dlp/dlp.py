"""Data-loss prevention for every egress channel (spec 6.4, 14).

Detects:
  - values of secrets stored in the vault, WITHOUT keeping plaintext: each secret value is
    fingerprinted as HMAC(K_dlp, value); outbound text is scanned with a sliding window of each
    fingerprinted length and the windows are HMAC'd and compared.
  - API-key/token formats, private keys, JWTs
  - payment card numbers (Luhn-checked), IBANs (mod-97 checked)
  - user-defined regular expressions and keywords (Settings > Rules & Safety)
"""
from __future__ import annotations

import hashlib
import hmac
import re
import threading
from dataclasses import dataclass
from typing import Callable, Iterable

MIN_SECRET_LEN = 8
MAX_SCAN_CHARS = 2_000_000
MAX_SECRET_SCAN_CHARS = 262_144  # sliding-window HMAC cost; responses beyond this are truncated for the secret scan

BUILTIN_PATTERNS: list[tuple[str, str]] = [
    ("aws_access_key", r"\b(AKIA|ASIA)[0-9A-Z]{16}\b"),
    ("github_token", r"\bgh[pousr]_[A-Za-z0-9]{36,}\b"),
    ("openai_style_key", r"\bsk-(?:proj-|ant-)?[A-Za-z0-9_\-]{20,}\b"),
    ("slack_token", r"\bxox[abprs]-[A-Za-z0-9-]{10,}\b"),
    ("google_api_key", r"\bAIza[0-9A-Za-z_\-]{35}\b"),
    ("azure_storage_key", r"AccountKey=[A-Za-z0-9+/=]{40,}"),
    ("private_key", r"-----BEGIN (?:RSA |EC |OPENSSH |DSA |PGP )?PRIVATE KEY-----"),
    ("jwt", r"\beyJ[A-Za-z0-9_\-]{10,}\.[A-Za-z0-9_\-]{10,}\.[A-Za-z0-9_\-]{10,}\b"),
    ("bearer_token", r"(?i)\bbearer\s+[A-Za-z0-9\-._~+/]{20,}=*"),
    ("password_assignment", r"(?i)\b(pass(word)?|pwd|secret)\s*[:=]\s*\S{6,}"),
]
CARD_RE = re.compile(r"\b(?:\d[ -]?){13,19}\b")
IBAN_RE = re.compile(r"\b[A-Z]{2}\d{2}(?:[ ]?[A-Z0-9]{4}){2,7}(?:[ ]?[A-Z0-9]{1,4})?\b")


@dataclass
class Finding:
    kind: str
    label: str
    start: int
    end: int

    def to_dict(self) -> dict:
        return {"kind": self.kind, "label": self.label}


def luhn_ok(digits: str) -> bool:
    total = 0
    for i, ch in enumerate(reversed(digits)):
        d = int(ch)
        if i % 2 == 1:
            d *= 2
            if d > 9:
                d -= 9
        total += d
    return total % 10 == 0


def iban_ok(iban: str) -> bool:
    s = iban.replace(" ", "")
    if not (15 <= len(s) <= 34):
        return False
    rearranged = s[4:] + s[:4]
    num = "".join(str(int(c, 36)) for c in rearranged)
    return int(num) % 97 == 1


class DLPEngine:
    def __init__(self, key: bytes, settings_get: Callable[[str], object] | None = None):
        self._key = key
        self._fps: dict[int, set[bytes]] = {}
        self._lock = threading.RLock()
        self._settings_get = settings_get
        self._compiled = [(k, re.compile(p)) for k, p in BUILTIN_PATTERNS]

    # ------------------------------------------------------------ secret fingerprints
    def fingerprint(self, value: str) -> bytes:
        return hmac.new(self._key, value.encode("utf-8"), hashlib.sha256).digest()

    def load_fingerprints(self, rows: Iterable[tuple[int, bytes]]) -> None:
        with self._lock:
            self._fps.clear()
            for length, fp in rows:
                self._fps.setdefault(int(length), set()).add(bytes(fp))

    def add_secret_value(self, value: str) -> tuple[int, bytes] | None:
        if len(value) < MIN_SECRET_LEN:
            return None
        fp = self.fingerprint(value)
        with self._lock:
            self._fps.setdefault(len(value), set()).add(fp)
        return len(value), fp

    def contains_secret(self, text: str) -> bool:
        return any(f.kind == "vault_secret" for f in self._scan_secrets(text))

    def _scan_secrets(self, text: str) -> list[Finding]:
        out: list[Finding] = []
        if not text:
            return out
        text = text[:MAX_SECRET_SCAN_CHARS]
        with self._lock:
            items = list(self._fps.items())
        for length, fps in items:
            if length > len(text):
                continue
            for i in range(0, len(text) - length + 1):
                if self.fingerprint(text[i:i + length]) in fps:
                    out.append(Finding("vault_secret", "a value stored in your Secrets vault", i, i + length))
                    break
        return out

    # ------------------------------------------------------------ scanning
    def scan(self, text: str) -> list[Finding]:
        if not text:
            return []
        text = text[:MAX_SCAN_CHARS]
        findings = self._scan_secrets(text)
        for kind, rx in self._compiled:
            for m in rx.finditer(text):
                findings.append(Finding(kind, kind.replace("_", " "), m.start(), m.end()))
        for m in CARD_RE.finditer(text):
            digits = re.sub(r"\D", "", m.group())
            if 13 <= len(digits) <= 19 and luhn_ok(digits) and len(set(digits)) > 1:
                findings.append(Finding("payment_card", "payment card number", m.start(), m.end()))
        for m in IBAN_RE.finditer(text):
            try:
                if iban_ok(m.group()):
                    findings.append(Finding("iban", "bank account (IBAN)", m.start(), m.end()))
            except ValueError:
                pass
        if self._settings_get:
            for pat in self._settings_get("dlp.custom_patterns") or []:  # type: ignore[union-attr]
                try:
                    for m in re.finditer(pat, text):
                        findings.append(Finding("custom_pattern", "custom blocked pattern", m.start(), m.end()))
                except re.error:
                    continue
            low = text.lower()
            for kw in self._settings_get("dlp.blocked_keywords") or []:  # type: ignore[union-attr]
                idx = low.find(kw.lower())
                if kw and idx >= 0:
                    findings.append(Finding("keyword", "blocked keyword", idx, idx + len(kw)))
        return findings

    def check_outbound(self, *texts: str) -> dict:
        findings: list[Finding] = []
        for t in texts:
            findings.extend(self.scan(t or ""))
        return {"blocked": bool(findings), "findings": [f.to_dict() for f in findings],
                "secret_match": any(f.kind == "vault_secret" for f in findings)}

    def redact(self, text: str) -> str:
        findings = sorted(self.scan(text), key=lambda f: f.start, reverse=True)
        out = text
        for f in findings:
            out = out[:f.start] + f"[REDACTED:{f.kind}]" + out[f.end:]
        return out
