"""System tools must be started by absolute path (audit finding F-03: PATH / current-directory search hijacking)."""
import os
import re
from pathlib import Path

import pytest

from pa_common import winpaths

APP = Path(__file__).resolve().parents[2] / "app"


@pytest.mark.skipif(os.name != "nt", reason="Windows paths")
def test_system_tools_resolve_to_system32():
    for exe in ("icacls.exe", "taskkill.exe"):
        p = Path(winpaths.system32(exe))
        assert p.is_absolute() and p.exists() and p.parent.name.lower() == "system32"
    assert Path(winpaths.powershell()).exists()


def test_bare_program_names_are_refused():
    for bad in ("..\\x.exe", "a/b.exe", "C:\\x.exe"):
        with pytest.raises(ValueError):
            winpaths.system32(bad)


def test_no_subprocess_starts_a_bare_program_name():
    bare = re.compile(r"""subprocess\.(?:run|Popen|call|check_output|check_call)\(\s*\[\s*["'][A-Za-z0-9_.\-]+["']""")
    offenders = [f"{p.relative_to(APP)}:{i + 1}" for p in APP.rglob("*.py") for i, line in enumerate(p.read_text(encoding="utf-8").splitlines())
                 if bare.search(line)]
    assert not offenders, offenders
