r"""Portable (copy-and-run) layout, spec 2.5 alternative to the installer.

<root>\pa-ui.exe  <root>\python\python.exe (python.org embeddable, Microsoft/PSF-signed)  <root>\app\...
Only active when buildinfo.PORTABLE_BUILD is set by scripts/build-portable.ps1 and the interpreter lives in
<root>\python, so a source checkout can never claim to be portable.
"""
from __future__ import annotations

import sys
from pathlib import Path

from .buildinfo import PORTABLE_BUILD


def portable_root() -> Path | None:
    if not PORTABLE_BUILD or sys.platform != "win32":
        return None
    exe = Path(sys.executable).resolve()
    if exe.parent.name.lower() != "python" or exe.name.lower() != "python.exe":
        return None
    return exe.parent.parent


def portable_ui_exe() -> Path | None:
    root = portable_root()
    return root / "pa-ui.exe" if root else None


def portable_core_exe() -> Path | None:
    """pa-core runs as the bundled interpreter itself (embeddable python.exe is not a venv launcher)."""
    root = portable_root()
    return root / "python" / "python.exe" if root else None
