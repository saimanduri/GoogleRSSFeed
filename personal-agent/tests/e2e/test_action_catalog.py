"""Keeps the action catalogue honest (runs everywhere, fast): every backend RPC is covered by a scenario or listed as
manual-only with a reason; scenario and UI-action files only reference real RPCs and real scenario ids."""
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_catalog_has_no_gaps_and_no_dangling_references():
    out = subprocess.run([sys.executable, str(ROOT / "scripts" / "gen_action_catalog.py")], capture_output=True, text=True, cwd=ROOT)
    assert out.returncode == 0, out.stderr
    assert "gaps 0" in out.stdout, out.stdout
    assert "UI calls unknown RPCs" not in out.stdout, out.stdout
    inv = {i["rpc"]: i for i in json.loads((ROOT / "tests/e2e/rpc_inventory.json").read_text(encoding="utf-8"))}
    scen = json.loads((ROOT / "tests/e2e/scenarios.json").read_text(encoding="utf-8"))["steps"]
    ids = [s["id"] for s in scen]
    assert len(ids) == len(set(ids)), "duplicate scenario ids"
    assert all(s["call"] in inv for s in scen), [s["call"] for s in scen if s["call"] not in inv]
    ui = json.loads((ROOT / "tests/e2e/ui_actions.json").read_text(encoding="utf-8"))["controls"]
    bad = [(c["screen"], r) for c in ui for r in c.get("rpc", []) if r not in inv]
    assert not bad, bad
    # every RPC the UI source calls is mentioned in ui_actions.json
    called = {r for i in inv.values() if i["ui_callers"] for r in [i["rpc"]]}
    mentioned = {r for c in ui for r in c.get("rpc", [])}
    assert called - mentioned == set(), sorted(called - mentioned)
