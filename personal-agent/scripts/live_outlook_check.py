"""Live check of the Outlook email skills against the REAL Outlook and a REAL local model (Ollama).

Developer mode only (software key, throwaway data folder, real Outlook read-only). Prints only counts, timings and states -
never mail subjects, senders or bodies.
    python scripts\\live_outlook_check.py [ollama_model] [skill ...]
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

MODEL = sys.argv[1] if len(sys.argv) > 1 else "gemma4:latest"
SKILLS = sys.argv[2:] or ["approvals"]
data = Path(tempfile.mkdtemp(prefix="pa-live-"))
env = dict(os.environ, PA_DEV_MODE="1", PA_FORCE_SOFTWARE_PROTECTOR="1", PA_TEST_FAST_KDF="1", PA_SKIP_DEFENDER="1", PA_DATA_DIR=str(data),
           PYTHONPATH=str(ROOT / "app"))
gw = subprocess.Popen([str(ROOT / ".venv/Scripts/python.exe"), "-m", "pa_gateway", "--data-dir", str(data)], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
try:
    rv = data / "run" / "gateway.json"
    for _ in range(100):
        if rv.exists():
            break
        time.sleep(0.2)
    info = json.loads(rv.read_text())
    ui = PipeClient(info["pipe"], "ui", info["ui_token"], "ui", expected_server_pid=info["pid"])
    r = ui.call("setup.create", {"username": "livecheck9", "password": "Zq-Live-Check-9921-abc", "pin": "480713"})
    g = r["recovery_key"].split("-")
    ui.call("setup.confirm_recovery", {"answers": {str(i): g[i] for i in r["confirm_groups"]}})
    mid = ui.call("llm.add", {"model": {"provider": "ollama", "name": MODEL, "model_name": MODEL, "endpoint": "http://127.0.0.1:11434"}})["id"]
    rep = ui.call("llm.test", {"model_id": mid})
    print("model test passed:", rep.get("passed", rep.get("ok", rep)) if isinstance(rep, dict) else rep)
    ui.call("llm.set_role", {"role": "standard", "model_id": mid})
    ui.call("connectors.set", {"connector": "outlook_local", "enabled": True, "use_missions": True, "use_chat": True})
    if os.environ.get("LIVE_VIPS"):
        ui.call("settings.apply", {"changes": {"emailskills.vips": os.environ["LIVE_VIPS"].split(";")}})
    for s in SKILLS:
        print(f"== skill {s}")
        t0 = time.time()
        ui.call("emailskills.set", {"skill": s, "enabled": True})
        ui.call("emailskills.run", {"skill": s})
        last = None
        while time.time() - t0 < 600:
            time.sleep(4)
            try:
                row = next(x for x in ui.call("emailskills.list")["skills"] if x["id"] == s)
            except Exception as e:  # noqa: BLE001 - e.g. Windows locked the app meanwhile
                print("poll error:", e)
                continue
            if row["last_state"] in ("COMPLETED", "FAILED", "WAITING_FOR_RESOURCE") and row["last_at"] != last:
                last = row["last_at"]
                text = row["last_result"]
                lines = [x for x in text.splitlines() if x.strip()]
                print(f"state={row['last_state']} after {time.time() - t0:.0f}s | nothing_new={row['last_nothing']} | result: {len(text)} chars, {len(lines)} lines, "
                      f"{sum(1 for x in lines if x.lstrip().startswith(('-', '*', '1', '2', '3')))} list lines | error={row['last_error']}")
                if os.environ.get("LIVE_SHOW"):          # developer opt-in: show the start of the result (it contains your mail details)
                    print("----", text[: int(os.environ["LIVE_SHOW"])])
                if os.environ.get("LIVE_STEPS"):
                    run = ui.call("runs.list", {"limit": 1})[0]
                    for st in (ui.call("runs.get", {"run_id": run["id"]}).get("steps") or []):
                        print("   step:", st.get("type"), "|", str(st.get("title"))[:60], "|", st.get("status"), "|", len(str(st.get("detail_json") or st.get("text") or "")), "chars")
                break
        else:
            print("TIMEOUT after 600 s")
finally:
    gw.kill()
