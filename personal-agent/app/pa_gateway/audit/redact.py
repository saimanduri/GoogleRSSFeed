"""Content minimisation for the security log (spec 25.4).

Only metadata is logged. Keys that could carry secrets or content are dropped; long strings are
truncated; any value matching a stored secret (keyed-hash scan) is replaced. If redaction itself
fails the value becomes "[REDACTED:ERROR]" and the event is still written.
"""
from __future__ import annotations

from typing import Any, Callable

FORBIDDEN_KEYS = {
    "password", "new_password", "old_password", "pin", "new_pin", "recovery_key", "rk", "token", "access_token",
    "refresh_token", "id_token", "api_key", "secret", "value", "secret_value", "body", "content", "text",
    "prompt", "messages", "completion", "html", "file_bytes", "data_b64", "authorization", "cookie", "code",
    "code_verifier", "client_secret", "query_text", "payload",
}
MAX_STR = 300


def redact_event(fields: dict[str, Any], scanner: Callable[[str], bool] | None) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in fields.items():
        try:
            out[k] = _redact_value(k, v, scanner, 0)
        except Exception:  # noqa: BLE001
            out[k] = "[REDACTED:ERROR]"
    return out


def _redact_value(key: str, v: Any, scanner: Callable[[str], bool] | None, depth: int) -> Any:
    if key.lower() in FORBIDDEN_KEYS:
        return "[REDACTED]"
    if depth > 4:
        return "[TRUNCATED]"
    if isinstance(v, str):
        if scanner is not None and scanner(v):
            return "[REDACTED:SECRET]"
        return v if len(v) <= MAX_STR else v[:MAX_STR] + "...[TRUNCATED]"
    if isinstance(v, dict):
        return {k2: _redact_value(k2, v2, scanner, depth + 1) for k2, v2 in list(v.items())[:50]}
    if isinstance(v, (list, tuple)):
        return [_redact_value(key, x, scanner, depth + 1) for x in list(v)[:50]]
    if isinstance(v, (int, float, bool)) or v is None:
        return v
    return str(v)[:MAX_STR]
