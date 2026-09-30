"""End-to-end smoke test (developer mode): runs the real gateway + in-process core against the mock model
in a temporary data folder. Used by CI and by `python -m pa_gateway --selftest` on the laptop."""
from __future__ import annotations

import os
import sys
import tempfile
import time
from pathlib import Path


def run_selftest() -> int:
    os.environ["PA_DEV_MODE"] = "1"
    os.environ["PA_TEST_FAST_KDF"] = "1"
    os.environ["PA_CORE_INPROCESS"] = "1"
    from pa_common.paths import DataPaths

    from .app import Gateway
    from .ipc.dispatch import Dispatcher, import_all_methods
    from .ipc.local import LocalClient

    tmp = Path(tempfile.mkdtemp(prefix="pa-selftest-"))
    ok = True

    def check(name: str, cond: bool) -> None:
        nonlocal ok
        print(("PASS " if cond else "FAIL ") + name, flush=True)
        ok = ok and cond

    gw = Gateway(DataPaths(tmp), start_core=True)
    import_all_methods()
    d = Dispatcher(gw)
    ui = LocalClient(d, "ui")
    r = ui.call("setup.create", {"username": "selftest", "password": "Correct-Horse-Battery-9!x", "pin": "480713"})
    check("setup returns recovery key", len(r["recovery_key"]) == 47)
    groups = r["recovery_key"].split("-")
    ui.call("setup.confirm_recovery", {"answers": {str(i): groups[i] for i in r["confirm_groups"]}})
    ui.call("llm.add", {"model": {"provider": "dev_mock", "name": "Developer mock"}})
    chat = ui.call("chat.create", {})["id"]
    sent = ui.call("chat.send", {"chat_id": chat, "text": "hello there"})
    reply = None
    for _ in range(100):
        msgs = ui.call("chat.get", {"chat_id": chat})["messages"]
        if any(m["role"] == "assistant" for m in msgs):
            reply = [m for m in msgs if m["role"] == "assistant"][-1]["content"]
            break
        time.sleep(0.1)
    check("chat round trip through gateway + core + mock model", bool(reply) and "hello there" in reply)
    run = ui.call("runs.get", {"run_id": sent["run_id"]})
    check("step timeline recorded", len(run["steps"]) >= 2)
    ui.call("chat.send", {"chat_id": chat, "text": "remind me to call the dentist tomorrow at 9"})
    pending = []
    for _ in range(100):
        pending = ui.call("approvals.list", {})
        if pending:
            break
        time.sleep(0.1)
    check("reminder needs confirmation (approval card)", bool(pending) and pending[0]["tool"] == "reminders.propose")
    if pending:
        a = pending[0]
        ui.call("approvals.decide", {"approval_id": a["id"], "approve": True, "payload_hash": a["payload_hash"]})
        for _ in range(100):
            if ui.call("reminders.list", {}):
                break
            time.sleep(0.1)
        check("reminder scheduled after confirmation", len(ui.call("reminders.list", {})) == 1)
    v = ui.call("logs.verify", {})
    check("security log chain verifies", v["ok"])
    gw.shutdown()
    print("SELFTEST", "OK" if ok else "FAILED", "data:", tmp)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(run_selftest())
