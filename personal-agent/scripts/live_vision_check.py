"""Live check of the vision pipeline with the REAL local vision model (default qwen2.5vl:7b) on sample files made here.

Developer mode only (software key, throwaway data folder + account). Draws an "invoice" picture with Windows' own drawing code, saves it as
PNG and JPEG, wraps the JPEG into a scanned PDF (pages = one picture each, no text layer), then: adds the vision model with the same call as
the 'Add vision model' button, waits for its automatic test, uploads the three files, shares a folder with them and reads them through the
localfile tools. Prints timings and whether the key words were found - never anything else.
    python scripts\\live_vision_check.py [ollama_model]
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
sys.path.insert(0, str(ROOT))
from pa_common.pipeclient import PipeClient  # noqa: E402
from tests.helpers_pdf import scanned_pdf  # noqa: E402

MODEL = sys.argv[1] if len(sys.argv) > 1 else "qwen2.5vl:7b"
data = Path(tempfile.mkdtemp(prefix="pa-vis-"))
share = ROOT / "build" / f"vis-samples-{int(time.time())}"   # not under AppData: the app refuses to share that
share.mkdir(parents=True)
ps = (
    "Add-Type -AssemblyName System.Drawing;"
    "$b=New-Object System.Drawing.Bitmap 900,600;$g=[System.Drawing.Graphics]::FromImage($b);$g.Clear([System.Drawing.Color]::White);"
    "$f1=New-Object System.Drawing.Font 'Arial',34,([System.Drawing.FontStyle]::Bold);$f2=New-Object System.Drawing.Font 'Arial',26;"
    "$g.DrawString('INVOICE 4711',$f1,[System.Drawing.Brushes]::Black,40,40);"
    "$g.DrawString('Customer: Sharma Traders',$f2,[System.Drawing.Brushes]::Black,40,140);"
    "$g.DrawString('Date: 02 October 2026',$f2,[System.Drawing.Brushes]::Black,40,200);"
    "$g.DrawString('Total due: Rs 1,234.50',$f2,[System.Drawing.Brushes]::Black,40,300);"
    "$g.DrawString('Thank you for your business',$f2,[System.Drawing.Brushes]::DarkGray,40,480);"
    f"$b.Save('{share / 'invoice.png'}',[System.Drawing.Imaging.ImageFormat]::Png);$b.Save('{data / 'invoice.jpg'}',[System.Drawing.Imaging.ImageFormat]::Jpeg);")
subprocess.run(["powershell", "-NoProfile", "-Command", ps], check=True)
jpeg = (data / "invoice.jpg").read_bytes()
(share / "scan.pdf").write_bytes(scanned_pdf(1, jpeg))
print("samples:", (share / "invoice.png").stat().st_size, "B png,", len(jpeg), "B jpeg, scanned pdf", (share / "scan.pdf").stat().st_size, "B")

env = dict(os.environ, PA_DEV_MODE="1", PA_FORCE_SOFTWARE_PROTECTOR="1", PA_TEST_FAST_KDF="1", PA_SKIP_DEFENDER="1", PA_DATA_DIR=str(data / "gw"),
           PYTHONPATH=str(ROOT / "app"))
gw = subprocess.Popen([str(ROOT / ".venv/Scripts/python.exe"), "-m", "pa_gateway", "--data-dir", str(data / "gw")], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
KEYS = ("4711", "Sharma", "1,234.50")


def found(t: str) -> str:
    return f"{sum(k.lower() in t.lower() for k in KEYS)}/{len(KEYS)} key words"


try:
    rv = data / "gw" / "run" / "gateway.json"
    for _ in range(100):
        if rv.exists():
            break
        time.sleep(0.2)
    info = json.loads(rv.read_text())
    ui = PipeClient(info["pipe"], "ui", info["ui_token"], "ui", expected_server_pid=info["pid"])
    r = ui.call("setup.create", {"username": "vischeck9", "password": "Zq-Live-Check-9921-abc", "pin": "480713"})
    g = r["recovery_key"].split("-")
    ui.call("setup.confirm_recovery", {"answers": {str(i): g[i] for i in r["confirm_groups"]}})
    ep = "http://127.0.0.1:11434"
    ins = ui.call("llm.inspect", {"provider": "ollama", "endpoint": ep, "model_name": MODEL})
    print("detected:", ins["kind"], "vision" if ins.get("vision") else "no-vision")
    t0 = time.time()
    add = ui.call("llm.add", {"model": {"provider": "ollama", "name": MODEL, "model_name": MODEL, "endpoint": ep, "kind": "vision"}})
    while time.time() - t0 < 600:
        st = ui.call("llm.models")
        m = st["models"][0]
        if not m["testing"] and (m["tested"] or time.time() - t0 > 8):
            break
        time.sleep(2)
    print("vision model tested:", bool(m["tested"]), "default:", st["roles"]["vision"] == add["id"], f"({time.time() - t0:.0f} s)")
    for c in (m.get("test_report") or {}).get("checks", []):
        print("  check", c["name"], "ok" if c["ok"] else "FAIL", c.get("note") or c.get("error") or "")

    def upload(path: Path):
        t = time.time()
        up = ui.call("files.upload", {"name": path.name, "data_b64": base64.b64encode(path.read_bytes()).decode()})
        while time.time() - t < 900:
            row = ui.call("files.preview", {"file_id": up["id"]})
            if row["status"] in ("READY", "REJECTED"):
                return row, time.time() - t
            time.sleep(1)
        return {"status": "TIMEOUT", "text": ""}, 0

    for p in (share / "invoice.png", share / "scan.pdf"):
        row, secs = upload(p)
        print(f"upload {p.name}: {row['status']} in {secs:.0f} s, {found(row.get('text', ''))}; note: {row.get('status_reason')}")
    cid = ui.call("chat.create", {})["id"]
    grant = ui.call("localfiles.grant_folder", {"chat_id": cid, "path": str(share), "include_subfolders": False, "confirm_subfolders": False})
    print("shared folder:", grant["name"], "subfolders allowed:", grant["recursive"])
finally:
    gw.terminate()
    import shutil
    shutil.rmtree(share, ignore_errors=True)
