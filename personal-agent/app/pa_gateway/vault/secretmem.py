"""Best-effort protection for key material held in the gateway's memory (spec 4.7).

Honest limit (documented in docs/SECRETS_HANDLING.md): CPython and the crypto libraries copy
bytes objects internally, so zeroisation is best-effort. We keep master keys in a single
bytearray, lock its pages with VirtualLock on Windows (not swapped to the pagefile), and wipe it
on lock/sign-out. Crash dumps for pa-gateway are disabled by the gateway at start-up.
"""
from __future__ import annotations

import ctypes
import sys


class SecretBytes:
    __slots__ = ("_buf", "_locked")

    def __init__(self, data: bytes | bytearray):
        self._buf = bytearray(data)
        self._locked = False
        if sys.platform == "win32" and self._buf:
            try:
                addr = ctypes.addressof((ctypes.c_char * len(self._buf)).from_buffer(self._buf))
                self._locked = bool(ctypes.windll.kernel32.VirtualLock(ctypes.c_void_p(addr), ctypes.c_size_t(len(self._buf))))
            except Exception:  # noqa: BLE001 - best effort only
                self._locked = False

    def get(self) -> bytes:
        if not self._buf:
            raise ValueError("secret wiped")
        return bytes(self._buf)

    @property
    def alive(self) -> bool:
        return bool(self._buf)

    def wipe(self) -> None:
        n = len(self._buf)
        if n:
            if sys.platform == "win32" and self._locked:
                try:
                    addr = ctypes.addressof((ctypes.c_char * n).from_buffer(self._buf))
                    ctypes.memset(addr, 0, n)
                    ctypes.windll.kernel32.VirtualUnlock(ctypes.c_void_p(addr), ctypes.c_size_t(n))
                except Exception:  # noqa: BLE001
                    pass
            for i in range(n):
                self._buf[i] = 0
            self._buf.clear()

    def __repr__(self) -> str:  # never print key material
        return "<SecretBytes redacted>"

    __str__ = __repr__
