"""Live check of chat behaviour with a REAL local model (default qwen3-coder:latest) and the REAL Outlook (read-only), throwaway account.

Sends the questions given on the command line one after another in ONE chat and prints, per question: how it ended, how long it took, the
number of tool calls, and the SHAPE of the model's raw replies (length / empty / JSON) - never mail subjects, senders or text.
    python scripts\\live_chat_check.py [model] "question 1" "question 2" ...
Set LIVE_OUTLOOK=0 to skip Outlook (then only general questions work).
"""
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "app"))
from pa_common.pipeclient import PipeClient  # noqa: E402

MODEL = sys.argv[1] if len(sys.argv) > 1 else "qwen3-coder:latest"
QUESTIONS = sys.argv[2:] or ["How many emails did I receive today, how many are unread, and in how many of them am I marked in To?",
                             "Which are those emails where I am in the To field? Is any action expected from me or are they only for information?"]
data = Path(tempfile.mkdtemp(prefix="pa-chat-"))
env = dict(os.environ, PA_DEV_MODE="1", PA_FORCE_SOFTWARE_PROTECTOR="1", PA_TEST_FAST_KDF="1", PA_SKIP_DEFENDER="1", PA_DATA_DIR=str(data), PYTHONPATH=str(ROOT / "app"))
gw = subprocess.Popen([str(ROOT / ".venv/Scripts/python.exe"), "-m", "pa_gateway", "--data-dir", str(data)], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
try:
    rv = data / "run" / "gateway.json"
    for _ in range(100):
        if rv.exists():
            break
        time.sleep(0.2)
    info = json.loads(rv.read_text())
    ui = PipeClient(info["pipe"], "ui", info["ui_token"], "ui", expected_server_pid=info["pid"])
    r = ui.call("setup.create", {"username": "chatcheck9", "password": "Zq-Live-Check-9921-abc", "pin": "480713"})
    g = r["recovery_key"].split("-")
    ui.call("setup.confirm_recovery", {"answers": {str(i): g[i] for i in r["confirm_groups"]}})
    mid = ui.call("llm.add", {"model": {"provider": "ollama", "name": MODEL, "model_name": MODEL, "endpoint": "http://127.0.0.1:11434", "kind": "chat"}})["id"]
    for _ in range(300):
        m = ui.call("llm.models")["models"][0]
        if not m["testing"] and (m["tested"] or _ > 3):
            break
        time.sleep(2)
    ui.call("llm.set_role", {"role": "standard", "model_id": mid})
    if os.environ.get("LIVE_OUTLOOK", "1") == "1":
        ui.call("connectors.set", {"connector": "outlook_local", "enabled": True, "use_missions": True, "use_chat": True})
    cid = ui.call("chat.create", {})["id"]
    for q in QUESTIONS:
        t0 = time.time()
        ui.call("chat.send", {"chat_id": cid, "text": q})
        n0 = len([x for x in ui.call("chat.get", {"chat_id": cid})["messages"] if x["role"] == "assistant"])
        while time.time() - t0 < 600:
            msgs = [x for x in ui.call("chat.get", {"chat_id": cid})["messages"] if x["role"] == "assistant"]
            if len(msgs) > n0:
                break
            time.sleep(2)
        last = msgs[-1]["content"] if len(msgs) > n0 else ""
        try:
            ui.call("auth.step_up", {"category": "transcripts", "method": "password", "secret": "Zq-Live-Check-9921-abc"})
            events = ui.call("runs.transcript", {"run_id": msgs[-1]["run_id"]}) if len(msgs) > n0 and msgs[-1].get("run_id") else []
        except Exception:  # noqa: BLE001
            events = []
        shapes = []
        for e in events:
            if e.get("kind") == "llm.response" or e.get("type") == "llm.response" or e.get("event_type") == "llm.response":
                c = str(e.get("content", ""))
                shapes.append("EMPTY" if not c.strip() else f"{len(c)}ch" + ("/json" if c.lstrip().startswith("{") else "/text"))
        bad = last.startswith("⚠") or "model_empty" in last
        print(f"Q: {q[:50]!r}: {'FAILED: ' + last[:90] if bad else 'answered, ' + str(len(last)) + ' chars'}; {time.time() - t0:.0f} s; model replies: {shapes}")
finally:
    gw.terminate()
