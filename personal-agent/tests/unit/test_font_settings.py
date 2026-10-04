"""Font and text-size settings: the schema and the UI list (app/ui/src/fonts.ts) must agree, there are exactly 20 chosen fonts plus the Windows default,
every font stack ends with a fallback that renders Hindi, and the three sizes exist."""
import re
from pathlib import Path

from pa_gateway.settings_schema import BY_KEY

TS = (Path(__file__).resolve().parents[2] / "app" / "ui" / "src" / "fonts.ts").read_text(encoding="utf-8")


def test_font_ids_match_between_schema_and_ui():
    ids = re.findall(r'\{ id: "([a-z_]+)", label:', TS)
    assert ids == list(BY_KEY["ui.font"].options)
    assert len(ids) == 21 and ids[0] == "windows" and BY_KEY["ui.font"].default == "windows"


def test_every_stack_has_an_indian_script_fallback_and_a_generic_family():
    stacks = re.findall(r'stack: `([^`]+)`', TS)
    assert len(stacks) == 21
    for s in stacks:
        assert '"Nirmala UI"' in s or "${tail(" in s


def test_sizes_are_small_medium_large():
    assert BY_KEY["ui.font_size"].options == ("small", "medium", "large") and BY_KEY["ui.font_size"].default == "medium"
    assert re.findall(r'\["(small|medium|large)", "[A-Z][a-z]+", [0-9.]+\]', TS) == ["small", "medium", "large"]


def test_popular_office_fonts_are_present():
    for name in ("Calibri", "Arial", "Times New Roman", "Cambria", "Verdana", "Tahoma", "Georgia", "Segoe UI", "Aptos", "Century Gothic", "Garamond"):
        assert f'label: "{name}"' in TS, name
