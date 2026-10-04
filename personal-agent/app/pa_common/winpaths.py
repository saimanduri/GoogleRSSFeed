"""Absolute paths of Windows system programs the app starts (icacls, powershell, taskkill ...).

A bare program name is searched by CreateProcess in the current directory and on PATH; a planted `icacls.exe` in a
user-writable working directory would then run with the gateway's rights (and `powershell -Verb RunAs` would even
elevate it). Always start system tools through these absolute paths (security audit 2026-10-02, finding F-03).
"""
from __future__ import annotations

import os
from pathlib import Path


def _system_root() -> Path:
    root = os.environ.get("SystemRoot") or os.environ.get("windir") or r"C:\Windows"
    return Path(root)


def system32(exe: str) -> str:
    """e.g. system32('icacls.exe') -> C:\\Windows\\System32\\icacls.exe"""
    if os.sep in exe or "/" in exe or ".." in exe:
        raise ValueError("pass a bare program name")
    return str(_system_root() / "System32" / exe)


def powershell() -> str:
    return str(_system_root() / "System32" / "WindowsPowerShell" / "v1.0" / "powershell.exe")
