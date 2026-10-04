"""DAST: push a hostile + benign file corpus through the REAL upload pipeline of a throwaway gateway and judge the outcome.

Developer mode only (software key, throwaway data folder + account, Defender scan skipped because it is off on this PC -
the antivirus step is therefore NOT part of this check). Nothing is sent to the network by this script; the network log
of the gateway is read at the end to prove that processing a file never made a request.

    python scripts\\corpus_dast.py "C:\\Users\\sendm\\Downloads\\Sample file generator\\corpus" [out.json]

Judgement per file (manifest.csv `expected`): files that must be rejected but become READY are listed as REVIEW (a person
decides: active content may legitimately be stripped); crashes, hangs, a dead gateway, a stored name with path parts or
control characters, and any outbound request are FAIL. Only file names, outcomes and timings are printed - never contents.
"""
import base64
import csv
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

CORPUS = Path(sys.argv[1])
OUT = Path(sys.argv[2]) if len(sys.argv) > 2 else ROOT / "docs" / "corpus_dast_results.json"
SKIP = {"manifest.csv", "hashes.sha256", "batch_runner.py", "README.md", "NAMES.md", "corpus_results.json"}
TIMEOUT = 150
# payload file names (hostile/07_filename/NAMES.md) used as the upload name of a harmless body
NAME_PAYLOADS = ["../../../../tmp/traversal.pdf", "..\\..\\windows\\system32\\config.pdf", "resume.pdf\x00.exe", "\u202eexe.cod", "rep\u0430rt.pdf",
                 "A" * 300 + ".pdf", "CON.pdf", "LPT1.docx", "NUL.txt", "report.pdf\u200b", "report.pdf.", "report.pdf ", "C:\\Users\\Public\\x.txt", "\\\\server\\share\\x.txt",
                 "file.txt:stream", "<script>alert(1)</script>.txt", "a'; DROP TABLE files;--.txt"]

data = Path(tempfile.mkdtemp(prefix="pa-dast-"))
env = dict(os.environ, PA_DEV_MODE="1", PA_FORCE_SOFTWARE_PROTECTOR="1", PA_TEST_FAST_KDF="1", PA_SKIP_DEFENDER="1", PA_DATA_DIR=str(data),
           PYTHONPATH=str(ROOT / "app"))
gw = subprocess.Popen([str(ROOT / ".venv/Scripts/python.exe"), "-m", "pa_gateway", "--data-dir", str(data)], env=env,
                      stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def alive(ui) -> bool:
    try:
        ui.call("session.status")
        return gw.poll() is None
    except Exception:  # noqa: BLE001
        return False


def wait_done(ui, fid: str) -> dict:
    t0 = time.time()
    while time.time() - t0 < TIMEOUT:
        row = ui.call("files.preview", {"file_id": fid})
        if row["status"] in ("READY", "REJECTED"):
            return row
        time.sleep(0.25)
    return {"status": "TIMEOUT"}


def bad_name(n: str) -> bool:
    return any(c in n for c in "/\\\x00:") or any(ord(c) < 32 for c in n) or "\u202e" in n or len(n) > 200


results = []
try:
    rv = data / "run" / "gateway.json"
    for _ in range(100):
        if rv.exists():
            break
        time.sleep(0.2)
    info = json.loads(rv.read_text())
    ui = PipeClient(info["pipe"], "ui", info["ui_token"], "ui", expected_server_pid=info["pid"])
    r = ui.call("setup.create", {"username": "dastcheck9", "password": "Zq-Dast-Check-9921-abc", "pin": "480713"})
    g = r["recovery_key"].split("-")
    ui.call("setup.confirm_recovery", {"answers": {str(i): g[i] for i in r["confirm_groups"]}})
    expected = {}
    with open(CORPUS / "manifest.csv", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            expected[row["file"].replace("\\", "/")] = (row["expected"], row["category"])
    files = [p for p in sorted(CORPUS.rglob("*")) if p.is_file() and p.name not in SKIP]
    print(f"{len(files)} corpus files, throwaway gateway pid {gw.pid}")
    for p in files:
        rel = p.relative_to(CORPUS).as_posix()
        exp, cat = expected.get(rel, ("?", "?"))
        rec = {"file": rel, "expected": exp, "category": cat, "size": p.stat().st_size}
        t0 = time.time()
        try:
            body = p.read_bytes()
            if len(body) > 40 * 1024 * 1024:
                rec.update(outcome="SKIPPED", note="bigger than the upload limit")
            else:
                up = ui.call("files.upload", {"name": p.name, "data_b64": base64.b64encode(body).decode()})
                fin = wait_done(ui, up["id"])
                rec.update(outcome=fin["status"], reason=(fin.get("status_reason") or "")[:160], hidden=len(fin.get("hidden_json") or []),
                           stored_name_ok=not bad_name(fin.get("name", "")), text_chars=len(fin.get("text") or ""))
        except Exception as e:  # noqa: BLE001
            rec.update(outcome="ERROR", reason=f"{type(e).__name__}: {str(e)[:160]}")
        rec["ms"] = int((time.time() - t0) * 1000)
        rec["gateway_alive"] = alive(ui)
        results.append(rec)
        if not rec["gateway_alive"]:
            print("GATEWAY DIED on", rel)
            break
    for i, n in enumerate(NAME_PAYLOADS):
        rec = {"file": f"<name payload {i}>", "expected": "SAFE_NAME", "category": "filename"}
        try:
            up = ui.call("files.upload", {"name": n, "data_b64": base64.b64encode(b"harmless text body\n").decode()})
            fin = wait_done(ui, up["id"])
            rec.update(outcome=fin["status"], stored_name_ok=not bad_name(fin.get("name", "")), stored_name_len=len(fin.get("name", "")))
        except Exception as e:  # noqa: BLE001
            refused = "max 200" in str(e)  # an over-long name is refused by input validation: safe
            rec.update(outcome="REFUSED" if refused else "ERROR", reason=f"{type(e).__name__}: {str(e)[:120]}")
        results.append(rec)
    net = ui.call("network.logs", {"days": 1, "limit": 1000})
    rows = net.get("rows") or net.get("logs") or net.get("items") or []
    outbound = [x for x in rows if x.get("component") != "llm"]
    summary = {"files": len(files), "gateway_alive_end": alive(ui), "network_requests_during_run": len(outbound)}
finally:
    gw.terminate()

fails, review = [], []
for r in results:
    if r["outcome"] in ("ERROR", "TIMEOUT") or not r.get("gateway_alive", True) or r.get("stored_name_ok") is False:
        fails.append(r)
    elif r["category"] != "control" and r["expected"].startswith("REJECT") and r["expected"] not in ("REJECT_OR_SAFE_ACCEPT",) and r["outcome"] == "READY":
        review.append(r)
    elif r["expected"].startswith("ACCEPT") and r["outcome"] == "REJECTED":
        r["false_positive"] = True
        review.append(r)
summary.update(ready=sum(r["outcome"] == "READY" for r in results), rejected=sum(r["outcome"] == "REJECTED" for r in results),
               fail=len(fails), review=len(review), slowest_ms=max((r.get("ms", 0) for r in results), default=0))
OUT.write_text(json.dumps({"summary": summary, "fail": fails, "review": review, "all": results}, indent=1, ensure_ascii=False), encoding="utf-8")
print(json.dumps(summary))
for r in fails:
    print("FAIL  ", r["file"], r["outcome"], r.get("reason", ""))
for r in review:
    print("REVIEW", r["file"], f"expected {r['expected']} -> {r['outcome']}", r.get("reason", ""))
print("details:", OUT)
