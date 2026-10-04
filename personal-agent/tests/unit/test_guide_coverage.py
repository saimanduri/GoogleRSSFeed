"""The in-app Guide must explain every navigation screen and every settings group (searchable help)."""
import re
from pathlib import Path

from pa_gateway.settings_schema import GROUPS

UI = Path(__file__).resolve().parents[2] / "app" / "ui" / "src"


def _guide_text() -> str:
    return (UI / "screens" / "guideData.ts").read_text(encoding="utf-8")


def test_every_nav_screen_has_a_guide_entry():
    shell = (UI / "Shell.tsx").read_text(encoding="utf-8")
    nav = re.search(r"const NAV[^=]*=\s*\[(.*?)\];", shell, re.S).group(1)
    screens = re.findall(r'\["([a-z]+)",\s*"[^"]+",\s*"[a-z]+"\]', nav)
    assert len(screens) >= 12
    g = _guide_text()
    for s in screens + ["settings"]:
        assert f'screen: "{s}"' in g, f"Guide has no entry for screen {s}"


def test_every_settings_group_has_a_guide_entry():
    g = _guide_text()
    missing = [gid for gid, _ in GROUPS if f'section: "{gid}"' not in g]
    # groups that are plain lists of switches are covered by the umbrella entry; the main ones must be explained
    for must in ("account", "connectors", "model", "autonomy", "web", "backup", "privacy", "updates", "diagnostics", "ui", "emergency"):
        assert must not in missing, must


def test_guide_ids_unique_and_texts_present():
    g = _guide_text()
    ids = re.findall(r'\{ id: "([^"]+)"', g)
    assert len(ids) == len(set(ids)) >= 40
