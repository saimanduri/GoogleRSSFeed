"""The browser preview's settings list (app/ui/src/api/settings_schema.json) is generated from settings_schema.py and must not go stale."""
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_preview_settings_schema_is_current():
    out = subprocess.run([sys.executable, str(ROOT / "scripts" / "gen_mock_schema.py"), "--check"], capture_output=True, text=True, cwd=ROOT)
    assert out.returncode == 0, out.stdout + out.stderr
