"""Live check of automatic memory and automatic file summaries with a REAL local model (default qwen3-coder:latest). Throwaway account.

1. Chats: says a few things about itself, waits, then lists what was learned (texts are made up here; nothing from your real data).
2. Files: uploads a made-up PAN-card-like text and an invoice; prints kind, summary, personal-data KINDS, whether the number leaked into the
   summary or memory (must be NO), the label, and finally asks the assistant "show me my PAN card" (files.find + files.read).
    python scripts\\live_memory_check.py [model]
"""
import base64
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
PAN = "ABCDE1234F"
PAN_TEXT = f"INCOME TAX DEPARTMENT\nGOVT. OF INDIA\nPermanent Account Number Card\n{PAN}\nName: SAI TEST\nFather's Name: RAM TEST\nDate of Birth: 01/01/1990\nSignature\n"
INVOICE = "TAX INVOICE\nBill to: Sharma Traders, Pune\nInvoice no 4711 dated 02 October 2026\nConsulting services 40 hours at Rs 1,500\nTotal due Rs 60,000\nThank you for your business\n"
data = Path(tempfile.mkdtemp(prefix="pa-mem-"))
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
    r = ui.call("setup.create", {"username": "memcheck9", "password": "Zq-Live-Check-9921-abc", "pin": "480713"})
    g = r["recovery_key"].split("-")
    ui.call("setup.confirm_recovery", {"answers": {str(i): g[i] for i in r["confirm_groups"]}})
    mid = ui.call("llm.add", {"model": {"provider": "ollama", "name": MODEL, "model_name": MODEL, "endpoint": "http://127.0.0.1:11434", "kind": "chat"}})["id"]
    for _ in range(300):
        m = ui.call("llm.models")["models"][0]
        if not m["testing"] and (m["tested"] or _ > 3):
            break
        time.sleep(2)
    ui.call("llm.set_role", {"role": "standard", "model_id": mid})

    def ask(chat, text, wait=300):
        n0 = len([x for x in ui.call("chat.get", {"chat_id": chat})["messages"] if x["role"] == "assistant"])
        ui.call("chat.send", {"chat_id": chat, "text": text})
        t0 = time.time()
        while time.time() - t0 < wait:
            ms = [x for x in ui.call("chat.get", {"chat_id": chat})["messages"] if x["role"] == "assistant"]
            if len(ms) > n0:
                return ms[-1]["content"]
            time.sleep(2)
        return ""

    cid = ui.call("chat.create", {})["id"]
    t0 = time.time()
    ask(cid, "Hi! I work in the finance team at a company in Pune and I prefer short bullet-point reports. I use Excel every day. Please keep that in mind.")
    for _ in range(60):
        learned = [x for x in ui.call("memory.list", {"status": ""}) if x["source"].startswith("learned:")]
        if learned:
            break
        time.sleep(2)
    print(f"1) learned from the chat ({time.time() - t0:.0f} s):")
    for x in learned:
        print("   -", x["content"], f"[{x['source']}]")
    ids = {}
    for name, text in (("PAN card.txt", PAN_TEXT), ("Invoice 4711.txt", INVOICE)):
        t0 = time.time()
        up = ui.call("files.upload", {"name": name, "data_b64": base64.b64encode(text.encode()).decode(), "folder": "/Documents"})
        ids[name] = up["id"]
        for _ in range(120):
            row = ui.call("files.preview", {"file_id": up["id"]})
            if (row.get("meta") or {}).get("status") in ("READY", "FAILED"):
                break
            time.sleep(2)
        meta = row["meta"]
        mems = [x["content"] for x in ui.call("memory.list", {"status": ""}) if x["source_ref"] == up["id"]]
        leak = PAN in json.dumps(meta) or any(PAN in x for x in mems)
        print(f"2) {name} ({time.time() - t0:.0f} s): type={meta['doc_type']!r} model={meta['model']} label={row['sensitivity']} personal_data={meta['personal_data']} number_leaked={'YES (BUG)' if leak else 'no'}")
        print(f"   summary: {meta['summary']}")
        print(f"   memory: {mems[0] if mems else '(none)'}")
    t0 = time.time()
    ans = ask(ui.call("chat.create", {})["id"], "Show me my PAN card")
    print(f"3) 'Show me my PAN card' ({time.time() - t0:.0f} s): answer contains the number: {'yes' if PAN in ans else 'NO'}; first 160 chars: {ans[:160]!r}")
finally:
    gw.terminate()
