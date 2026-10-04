"""Generate the action inventory: every backend RPC (name, roles, state, step-up, parameters read from the handler
source) joined with every place the UI calls it, plus scenario coverage.

  python scripts/gen_action_catalog.py          -> tests/e2e/rpc_inventory.json and docs/ACTION_CATALOG.md

The curated scenarios live in tests/e2e/scenarios.json (run them with scripts/e2e_runner.py); UI-side expectations
live in tests/e2e/ui_actions.json. This script keeps all three honest: it lists RPCs nobody covers.
"""
from __future__ import annotations

import inspect
import json
import os
import re
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "app"))
os.environ.setdefault("PA_DEV_MODE", "1")

from pa_gateway.ipc.dispatch import REGISTRY, import_all_methods  # noqa: E402

PARAM_RE = re.compile(r"p\.(str|int|float|bool|dict|list|opt)\(\s*\"([a-zA-Z_]+)\"([^)]*)\)")
UI_CALL_RE = re.compile(r"(?:call|rpc)(?:<[^>]*>)?\(\s*\"([a-z_]+\.[a-z_]+)\"")


def handler_params(fn) -> list[dict]:
    try:
        src = inspect.getsource(fn)
    except OSError:
        return []
    out, seen = [], set()
    for kind, name, rest in PARAM_RE.findall(src):
        if name in seen:
            continue
        seen.add(name)
        required = kind not in ("opt",) and not re.search(r"\b(False|default\s*=)", rest) and kind not in ("bool",)
        out.append({"name": name, "type": kind, "required": required})
    return out


def ui_usages() -> dict[str, list[str]]:
    usages: dict[str, list[str]] = defaultdict(list)
    for path in sorted((ROOT / "app" / "ui" / "src").rglob("*.ts*")):
        if path.name == "mock.ts":
            continue
        for n, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            for m in UI_CALL_RE.finditer(line):
                usages[m.group(1)].append(f"{path.relative_to(ROOT / 'app' / 'ui' / 'src').as_posix()}:{n}")
    return usages


def scenario_coverage() -> dict[str, list[str]]:
    p = ROOT / "tests" / "e2e" / "scenarios.json"
    cov: dict[str, list[str]] = defaultdict(list)
    if p.exists():
        for s in json.loads(p.read_text(encoding="utf-8"))["steps"]:
            if "call" in s:
                cov[s["call"]].append(s["id"])
    return cov


def main() -> int:
    import_all_methods()
    uses, cov = ui_usages(), scenario_coverage()
    manual = {}
    mp = ROOT / "tests" / "e2e" / "manual_only.json"
    if mp.exists():
        manual = json.loads(mp.read_text(encoding="utf-8"))
    inv = []
    for name in sorted(REGISTRY):
        m = REGISTRY[name]
        inv.append({"rpc": name, "roles": list(m.roles), "state": m.state, "stepup": m.stepup,
                    "params": handler_params(m.fn), "ui_callers": uses.get(name, []),
                    "scenarios": cov.get(name, []), "manual_reason": manual.get(name)})
    (ROOT / "tests" / "e2e").mkdir(parents=True, exist_ok=True)
    (ROOT / "tests" / "e2e" / "rpc_inventory.json").write_text(json.dumps(inv, indent=1), encoding="utf-8")

    covered = [i for i in inv if i["scenarios"]]
    manual_only = [i for i in inv if not i["scenarios"] and i["manual_reason"]]
    gaps = [i for i in inv if not i["scenarios"] and not i["manual_reason"]]
    ui_unknown = sorted(set(uses) - set(REGISTRY))
    lines = ["# Action catalogue (generated - do not edit; run `python scripts/gen_action_catalog.py`)", "",
             f"{len(inv)} backend actions (RPC methods): **{len(covered)} covered by automated scenarios**, "
             f"{len(manual_only)} manual-only (reason given), **{len(gaps)} gaps**. UI calls to unknown RPCs: {ui_unknown or 'none'}.", "",
             "Columns: state = required session state, step-up = re-auth category, UI = where the UI calls it, "
             "Scenarios = ids in tests/e2e/scenarios.json.", "",
             "| RPC | State | Step-up | Params (* = required) | UI callers | Scenarios / manual reason |", "|---|---|---|---|---|---|"]
    for i in inv:
        ps = ", ".join(f"{p['name']}{'*' if p['required'] else ''}" for p in i["params"]) or "-"
        ui = ", ".join(i["ui_callers"][:3]) + (" …" if len(i["ui_callers"]) > 3 else "") if i["ui_callers"] else "-"
        sc = ", ".join(i["scenarios"][:4]) + (" …" if len(i["scenarios"]) > 4 else "") if i["scenarios"] else (
            f"manual: {i['manual_reason']}" if i["manual_reason"] else "**GAP**")
        lines.append(f"| `{i['rpc']}` | {i['state']} | {i['stepup'] or '-'} | {ps} | {ui} | {sc} |")
    (ROOT / "docs" / "ACTION_CATALOG.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"{len(inv)} RPCs | covered {len(covered)} | manual-only {len(manual_only)} | gaps {len(gaps)}")
    if gaps:
        print("gaps:", ", ".join(i["rpc"] for i in gaps))
    if ui_unknown:
        print("UI calls unknown RPCs:", ui_unknown)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
