"""Data sensitivity levels (spec section 13)."""
from __future__ import annotations

from enum import IntEnum


class Sensitivity(IntEnum):
    PUBLIC = 0
    INTERNAL = 1
    CONFIDENTIAL = 2
    RESTRICTED = 3

    @classmethod
    def parse(cls, value: "str | int | Sensitivity | None", default: "Sensitivity | None" = None) -> "Sensitivity":
        if value is None:
            if default is None:
                raise ValueError("sensitivity required")
            return default
        if isinstance(value, Sensitivity):
            return value
        if isinstance(value, int):
            return cls(value)
        return cls[str(value).upper()]

    @staticmethod
    def max(*levels: "Sensitivity") -> "Sensitivity":
        return Sensitivity(max(int(x) for x in levels)) if levels else Sensitivity.PUBLIC


class Trust:
    """Trust levels for memory and context items (spec section 21)."""
    TRUSTED = "TRUSTED"
    VERIFIED = "VERIFIED"
    INFERRED = "INFERRED"
    UNTRUSTED = "UNTRUSTED"
    ALL = (TRUSTED, VERIFIED, INFERRED, UNTRUSTED)
