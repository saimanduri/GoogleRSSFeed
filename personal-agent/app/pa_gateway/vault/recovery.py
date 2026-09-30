"""Recovery key: 8 groups x 5 Crockford Base32 characters = 200 random bits (spec 4.2 step 4).

The spec text says "160-bit ... 8 groups of 5 characters"; 8x5 Crockford characters carry 200 bits,
so we generate 200 bits (strictly stronger) and keep the specified display format.
"""
from __future__ import annotations

import secrets

ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
GROUPS = 8
GROUP_LEN = 5
TOTAL = GROUPS * GROUP_LEN


def generate() -> str:
    chars = "".join(secrets.choice(ALPHABET) for _ in range(TOTAL))
    return "-".join(chars[i:i + GROUP_LEN] for i in range(0, TOTAL, GROUP_LEN))


def normalize(key: str) -> str:
    s = key.strip().upper().replace("-", "").replace(" ", "")
    s = s.replace("O", "0").replace("I", "1").replace("L", "1").replace("U", "V")
    if len(s) != TOTAL or any(c not in ALPHABET for c in s):
        raise ValueError("recovery key format is invalid")
    return "-".join(s[i:i + GROUP_LEN] for i in range(0, TOTAL, GROUP_LEN))


def groups(key: str) -> list[str]:
    return normalize(key).split("-")


def pick_confirmation_groups() -> list[int]:
    """Two distinct random group indexes (0-based) the user must type back."""
    first = secrets.randbelow(GROUPS)
    second = secrets.randbelow(GROUPS - 1)
    if second >= first:
        second += 1
    return sorted([first, second])


def normalize_group(group: str) -> str:
    s = group.strip().upper().replace("-", "").replace(" ", "")
    return s.replace("O", "0").replace("I", "1").replace("L", "1").replace("U", "V")
