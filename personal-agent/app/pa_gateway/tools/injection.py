"""Heuristic prompt-injection detector (spec 12). It only RAISES A FLAG that lowers the task's rights;
it is never the only defence - the gateway/policy block unauthorised actions regardless."""
from __future__ import annotations

import re

PATTERNS = [
    r"ignore (all |any |the )?(previous|prior|above|earlier) (instructions|prompts?|messages)",
    r"disregard (all |any |the )?(previous|prior|above) ",
    r"you are now (a|an|the) ",
    r"(new|updated|override) (system )?(instructions|prompt|rules)\s*:",
    r"system prompt",
    r"\b(forward|send|email|upload|post|exfiltrate) (all|every|the)?\s*(emails?|files?|messages?|data|passwords?|secrets?|keys?)\b.*\b(to|at)\b",
    r"do not (tell|inform|notify) the user",
    r"(reveal|print|show) (your|the) (system|hidden) (prompt|instructions)",
    r"<\s*/?\s*(system|assistant|tool)\s*>",
    r"\[\s*(system|assistant)\s*\]",
    r"(call|invoke|use) the (tool|function) [a-z_.]+ with",
    r"base64[:,]\s*[A-Za-z0-9+/]{40,}",
    r"(change|disable|turn off) (the )?(settings?|policy|policies|approvals?|security)",
]
_RX = [re.compile(p, re.I | re.S) for p in PATTERNS]


def scan(text: str) -> list[str]:
    if not text:
        return []
    sample = text[:200_000]
    return [p.pattern for p in _RX if p.search(sample)][:5]
