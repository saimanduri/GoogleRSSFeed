"""Password and PIN rules (spec 4.2 steps 2-3).

Strength estimation is a zxcvbn-style heuristic (no network, no external dependency):
character-class entropy minus penalties for dictionary words, repeats, sequences and keyboard walks.
Score 0-4; "strong" = 4 is required.
"""
from __future__ import annotations

import math
import re
from functools import lru_cache
from importlib import resources

MIN_LEN = 12
MAX_LEN = 256
KEYBOARD_ROWS = ("qwertyuiop", "asdfghjkl", "zxcvbnm", "1234567890", "qwertzuiop", "azertyuiop")


@lru_cache(maxsize=1)
def common_passwords() -> frozenset[str]:
    text = resources.files("pa_gateway.data").joinpath("common_passwords.txt").read_text("utf-8", errors="ignore")
    return frozenset(w.strip().lower() for w in text.splitlines() if w.strip())


@lru_cache(maxsize=1)
def common_pins() -> frozenset[str]:
    text = resources.files("pa_gateway.data").joinpath("common_pins.txt").read_text("utf-8")
    return frozenset(w.strip() for w in text.splitlines() if w.strip())


def _has_sequence(s: str, run: int = 4) -> bool:
    s = s.lower()
    for i in range(len(s) - run + 1):
        chunk = s[i:i + run]
        diffs = {ord(chunk[j + 1]) - ord(chunk[j]) for j in range(run - 1)}
        if diffs in ({1}, {-1}, {0}):
            return True
        for row in KEYBOARD_ROWS:
            if chunk in row or chunk in row[::-1]:
                return True
    return False


def strength(password: str, username: str = "") -> dict:
    pools = 0
    if re.search(r"[a-z]", password):
        pools += 26
    if re.search(r"[A-Z]", password):
        pools += 26
    if re.search(r"[0-9]", password):
        pools += 10
    if re.search(r"[^a-zA-Z0-9]", password):
        pools += 33
    unique = len(set(password))
    bits = len(password) * math.log2(max(pools, 1)) * min(1.0, unique / max(8, len(password) * 0.6))
    feedback: list[str] = []
    lower = password.lower()
    words = common_passwords()
    for w in (x for x in re.split(r"[^a-z]+", lower) if len(x) >= 4):
        if w in words:
            bits -= 12
            feedback.append("Avoid common words")
            break
    if _has_sequence(password):
        bits -= 10
        feedback.append("Avoid sequences like 1234, abcd or qwerty")
    if re.search(r"(.)\1\1", password):
        bits -= 8
        feedback.append("Avoid repeated characters")
    if re.search(r"(19|20)\d\d", password):
        bits -= 4
        feedback.append("Avoid years and dates")
    score = 0 if bits < 28 else 1 if bits < 40 else 2 if bits < 55 else 3 if bits < 70 else 4
    if lower in words:
        score = 0
        feedback.insert(0, "This is a very common password")
    if username and len(username) >= 3 and username.lower() in lower:
        score = min(score, 1)
        feedback.insert(0, "Must not contain your username")
    return {"score": score, "bits": round(max(bits, 0), 1), "label": ["very weak", "weak", "fair", "good", "strong"][score],
            "feedback": feedback}


def validate_password(password: str, username: str, pin: str | None = None) -> list[str]:
    errors: list[str] = []
    if len(password) < MIN_LEN:
        errors.append(f"Password must be at least {MIN_LEN} characters")
    if len(password) > MAX_LEN:
        errors.append(f"Password must be at most {MAX_LEN} characters")
    if username and username.lower() in password.lower():
        errors.append("Password must not contain the username")
    if password.lower() in common_passwords():
        errors.append("Password is on the list of common/breached passwords")
    if pin and password == pin:
        errors.append("Password must differ from the PIN")
    if not errors and strength(password, username)["score"] < 4:
        errors.append("Password is not strong enough - make it longer or less predictable")
    return errors


def validate_username(username: str) -> list[str]:
    if not (3 <= len(username) <= 64):
        return ["Username must be 3-64 characters"]
    if any(c in username for c in "\r\n\t\0"):
        return ["Username contains invalid characters"]
    return []


def validate_pin(pin: str, allow_letters: bool = False, password: str | None = None) -> list[str]:
    errors: list[str] = []
    if not (6 <= len(pin) <= 12):
        errors.append("PIN must be 6-12 characters")
    if allow_letters:
        if not re.fullmatch(r"[A-Za-z0-9]+", pin):
            errors.append("PIN may contain only letters and digits")
    elif not pin.isdigit():
        errors.append("PIN must contain only digits (or enable 'allow letters')")
    if len(set(pin)) <= 2:
        errors.append("PIN is too repetitive")
    if _has_sequence(pin, run=min(len(pin), 6)) or _has_sequence(pin, run=4) and len(set(pin)) < 5:
        errors.append("PIN must not be a simple sequence")
    if pin in common_pins() or pin[:6] in common_pins():
        errors.append("PIN is too common")
    if re.fullmatch(r"(\d{2,3})\1+", pin):
        errors.append("PIN must not repeat a short pattern")
    if password is not None and pin == password:
        errors.append("PIN must differ from the password")
    return errors


def validate_profile_name(name: str, what: str) -> list[str]:
    n = name.strip()
    if not (1 <= len(n) <= 40):
        return [f"{what} must be 1-40 characters"]
    if any(ord(c) < 32 or c in "<>{}\\`" for c in n):
        return [f"{what} contains characters that are not allowed"]
    return []
