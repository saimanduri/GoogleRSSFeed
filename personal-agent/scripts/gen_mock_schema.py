"""Writes app/ui/src/api/settings_schema.json from pa_gateway/settings_schema.py, so the browser preview (mock gateway)
shows exactly the real Settings screens. `python scripts/gen_mock_schema.py [--check]`; a test fails when it is stale."""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "app"))

from pa_gateway.settings_schema import GROUPS, SETTINGS  # noqa: E402

OUT = ROOT / "app" / "ui" / "src" / "api" / "settings_schema.json"


def build() -> str:
    data = {
        "groups": [{"id": g, "label": label} for g, label in GROUPS],
        "settings": [{
            "key": s.key, "group": s.group, "label": s.label, "type": s.type, "default": s.default, "value": s.default,
            "min": s.min, "max": s.max, "options": list(s.options), "help": s.help, "risk": s.risk, "loosen": s.loosen,
            "stepup": s.stepup, "floor": "floor" in s.tags, "section": s.section, "option_labels": list(s.option_labels),
        } for s in SETTINGS if not s.hidden],
    }
    return json.dumps(data, ensure_ascii=False, indent=1) + "\n"


if __name__ == "__main__":
    text = build()
    if "--check" in sys.argv:
        ok = OUT.exists() and OUT.read_text(encoding="utf-8") == text
        print("settings_schema.json is", "up to date" if ok else "STALE - run python scripts/gen_mock_schema.py")
        sys.exit(0 if ok else 1)
    OUT.write_text(text, encoding="utf-8", newline="\n")
    print("wrote", OUT.relative_to(ROOT), len(json.loads(text)["settings"]), "settings")
