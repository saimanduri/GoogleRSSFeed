"""Antivirus scan through Windows' AMSI (Antimalware Scan Interface) - works with whatever antivirus registered itself with Windows
(Microsoft Defender, McAfee, Kaspersky, ...). Used when Microsoft Defender's command-line scanner is not available (another antivirus is the
active one, which switches Defender off).

AMSI cannot say "no antivirus is registered": it just answers "not detected". So availability is PROVEN first by scanning the harmless EICAR test
string: only if the installed antivirus flags it do we trust a "clean" answer for real files. Otherwise the file stays in quarantine.
Data is scanned in memory in blocks (nothing is written to disk by this module).
"""
from __future__ import annotations

import ctypes
import sys
import threading
import time
from ctypes import POINTER, byref, c_void_p, c_wchar_p, wintypes

# assembled at runtime so this source file is never flagged by antivirus engines
EICAR = b"".join([b"X5O!P%@AP[4\\PZX54(P^)7CC)7}$", b"EICAR-STANDARD-", b"ANTIVIRUS-TEST-FILE!$H+H*"])
DETECTED = 32768          # AMSI_RESULT_DETECTED and above
BLOCKED_BY_ADMIN = (16384, 20479)
BLOCK = 4 * 1024 * 1024
MAX_SCAN = 200 * 1024 * 1024
_lock = threading.Lock()
_state: dict[str, float | bool] = {"checked": 0.0, "ok": False}


class _Amsi:
    def __init__(self) -> None:
        self.dll = ctypes.WinDLL("amsi.dll")
        self.dll.AmsiInitialize.argtypes = [c_wchar_p, POINTER(c_void_p)]
        self.dll.AmsiOpenSession.argtypes = [c_void_p, POINTER(c_void_p)]
        self.dll.AmsiScanBuffer.argtypes = [c_void_p, c_void_p, wintypes.ULONG, c_wchar_p, c_void_p, POINTER(wintypes.DWORD)]
        self.dll.AmsiCloseSession.argtypes = [c_void_p, c_void_p]
        self.dll.AmsiUninitialize.argtypes = [c_void_p]
        self.ctx = c_void_p()
        if self.dll.AmsiInitialize("ChiRAGAgent", byref(self.ctx)) != 0:
            raise OSError("AmsiInitialize failed")
        self.session = c_void_p()
        if self.dll.AmsiOpenSession(self.ctx, byref(self.session)) != 0:
            raise OSError("AmsiOpenSession failed")

    def scan(self, data: bytes, name: str) -> int:
        buf = ctypes.create_string_buffer(data, len(data))
        res = wintypes.DWORD(0)
        hr = self.dll.AmsiScanBuffer(self.ctx, buf, len(data), name, self.session, byref(res))
        if hr != 0:
            raise OSError(f"AmsiScanBuffer failed 0x{hr & 0xFFFFFFFF:08x}")
        return int(res.value)

    def close(self) -> None:
        try:
            self.dll.AmsiCloseSession(self.ctx, self.session)
            self.dll.AmsiUninitialize(self.ctx)
        except Exception:  # noqa: BLE001
            pass


def available(force: bool = False) -> bool:
    """True when an installed antivirus answers AMSI (proven with the EICAR test string); re-checked every 10 minutes."""
    if sys.platform != "win32":
        return False
    with _lock:
        if not force and time.time() - float(_state["checked"]) < 600:
            return bool(_state["ok"])
        ok = False
        try:
            a = _Amsi()
            try:
                ok = a.scan(EICAR, "eicar-selftest.com") >= DETECTED
            finally:
                a.close()
        except Exception:  # noqa: BLE001
            ok = False
        _state.update(checked=time.time(), ok=ok)
        return ok


def scan(data: bytes, name: str = "upload") -> dict[str, object]:
    """{'clean': bool, 'engine': 'amsi', 'threat'?: str, 'error'?: str}. Callers must check available() first."""
    try:
        a = _Amsi()
    except Exception as e:  # noqa: BLE001
        return {"clean": False, "engine": "amsi", "error": f"AMSI not available: {e}"}
    try:
        view = data[:MAX_SCAN]
        for i in range(0, max(1, len(view)), BLOCK):
            r = a.scan(view[i:i + BLOCK], name)
            if r >= DETECTED:
                return {"clean": False, "engine": "amsi", "threat": "detected by the installed antivirus"}
            if BLOCKED_BY_ADMIN[0] <= r <= BLOCKED_BY_ADMIN[1]:
                return {"clean": False, "engine": "amsi", "threat": "blocked by an administrator policy"}
        return {"clean": True, "engine": "amsi", **({"note": "only the first 200 MB were scanned"} if len(data) > MAX_SCAN else {})}
    except Exception as e:  # noqa: BLE001
        return {"clean": False, "engine": "amsi", "error": f"scan failed: {e}"}
    finally:
        a.close()
