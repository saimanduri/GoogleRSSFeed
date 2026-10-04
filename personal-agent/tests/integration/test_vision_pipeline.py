"""Pictures and scanned PDFs are read by the vision model automatically (upload and local files); a normal PDF is not;
no vision model / switched off / too confidential for a remote model -> clear note instead of silence. A fake Ollama answers."""
import base64
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from pa_gateway.llm import audio as A
from pa_gateway.tools.base import ExecContext, ToolFailed
from pa_gateway.vision import is_scanned_pdf
from tests.helpers_pdf import scanned_pdf, text_pdf

CALLS = []


class Fake(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, code, obj):
        b = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def do_GET(self):
        self._send(403 if self.headers.get("Origin") else 200, {"models": [{"name": "fake/vl:7b"}], "data": []})

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}")
        if self.path == "/api/show":
            return self._send(200, {"model_info": {"general.architecture": "qwen2vl"}, "capabilities": ["completion", "vision"]})
        if self.path == "/api/chat":
            msg = body["messages"][0]
            img = base64.b64decode(msg["images"][0])
            CALLS.append({"len": len(img), "prompt": msg["content"]})
            if "colour" in msg["content"]:
                return self._send(200, {"message": {"content": "Red"}, "done": True})
            return self._send(200, {"message": {"content": f"INVOICE 4711 total 1,234.50 (read #{len(CALLS)})\nPicture: a scanned page."}, "done": True})
        self._send(404, {})


@pytest.fixture()
def ollama():
    CALLS.clear()
    srv = ThreadingHTTPServer(("127.0.0.1", 0), Fake)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{srv.server_address[1]}"
    srv.shutdown()


def _add_vision(env, ep):
    r = env.ui.call("llm.add", {"model": {"provider": "ollama", "endpoint": ep, "model_name": "fake/vl:7b", "name": "fake vl", "kind": "vision"}})
    assert env.wait(lambda: env.ui.call("llm.models")["roles"]["vision"] == r["id"], 30)
    CALLS.clear()          # forget the model's own test picture
    return r["id"]


def _upload(env, name, data):
    up = env.ui.call("files.upload", {"name": name, "data_b64": base64.b64encode(data).decode()})
    assert env.wait(lambda: env.ui.call("files.preview", {"file_id": up["id"]})["status"] in ("READY", "REJECTED"), 60)
    return env.ui.call("files.preview", {"file_id": up["id"]})


def test_scan_heuristic():
    assert is_scanned_pdf("\n--- page 1 ---\n\n--- page 2 ---\n", 2)
    assert is_scanned_pdf("", 3) and not is_scanned_pdf("x" * 400, 2) and not is_scanned_pdf("", 0)


def test_uploaded_picture_is_read_automatically(env_nocore, ollama):
    env_nocore.setup(model=False)
    _add_vision(env_nocore, ollama)
    row = _upload(env_nocore, "receipt.png", A.sample_png())
    assert row["status"] == "READY" and "INVOICE 4711" in row["text"] and row["text"].startswith("[Picture read by the vision model]")
    assert row["scan_json"]["vision"]["state"] == "read" and len(CALLS) == 1 and CALLS[0]["len"] == len(A.sample_png())


def test_without_a_vision_model_the_user_is_told_and_can_read_again_later(env_nocore, ollama):
    env_nocore.setup(model=False)
    row = _upload(env_nocore, "receipt.png", A.sample_png())
    assert row["status"] == "READY" and "no vision model" in (row["status_reason"] or "") and "INVOICE" not in row["text"]
    _add_vision(env_nocore, ollama)
    again = env_nocore.ui.call("files.reread", {"file_id": row["id"]})
    assert again["status_reason"] is None
    assert "INVOICE 4711" in env_nocore.ui.call("files.preview", {"file_id": row["id"]})["text"]


def test_scanned_pdf_pages_are_read_but_a_text_pdf_is_not(env_nocore, ollama):
    env_nocore.setup(model=False)
    _add_vision(env_nocore, ollama)
    scan = _upload(env_nocore, "scan.pdf", scanned_pdf(3))
    assert scan["status"] == "READY" and scan["text"].count("read by the vision model") == 3 and "INVOICE 4711" in scan["text"] and len(CALLS) == 3
    CALLS.clear()
    doc = _upload(env_nocore, "normal.pdf", text_pdf(2))
    assert doc["status"] == "READY" and "real text layer" in doc["text"] and CALLS == []


def test_page_limit_is_respected_and_reported(env_nocore, ollama):
    env_nocore.setup(model=False)
    _add_vision(env_nocore, ollama)
    env_nocore.gw.db.execute("INSERT OR REPLACE INTO settings(key, value_json, updated_at) VALUES ('vision.max_pages','2','x')")
    env_nocore.gw.settings._cache["vision.max_pages"] = 2
    scan = _upload(env_nocore, "scan.pdf", scanned_pdf(5))
    assert len(CALLS) == 2 and "only the first 2 of 5 pages" in (scan["status_reason"] or "")


def test_switched_off_means_no_call(env_nocore, ollama):
    env_nocore.setup(model=False)
    _add_vision(env_nocore, ollama)
    env_nocore.gw.settings._cache["vision.auto"] = False
    row = _upload(env_nocore, "receipt.png", A.sample_png())
    assert CALLS == [] and "switched off" in (row["status_reason"] or "")


def test_confidential_files_are_never_sent_to_a_remote_vision_model(env_nocore):
    env_nocore.setup(model=False)
    remote = {"id": "m", "location": "remote"}
    v = env_nocore.gw.vision
    assert v._may_send(remote, 2) and v._may_send(remote, 3)
    assert v._may_send(remote, 1) is None and v._may_send({"location": "loopback"}, 3) is None


def test_local_picture_and_scan_in_a_shared_folder(env_nocore, ollama, tmp_path, monkeypatch):
    fake = tmp_path / "fake_home" / "AppData"
    monkeypatch.setenv("APPDATA", str(fake / "Roaming"))
    monkeypatch.setenv("LOCALAPPDATA", str(fake / "Local"))
    env_nocore.setup(model=False)
    _add_vision(env_nocore, ollama)
    d = tmp_path / "Inbox"
    d.mkdir()
    (d / "photo.png").write_bytes(A.sample_png())
    (d / "contract.pdf").write_bytes(scanned_pdf(2))
    cid = env_nocore.ui.call("chat.create", {})["id"]
    g = env_nocore.ui.call("localfiles.grant_folder", {"chat_id": cid, "path": str(d), "include_subfolders": False, "confirm_subfolders": False})
    lf = env_nocore.gw.localfiles
    env_nocore.gw.budgets.add = lambda *a, **k: None
    ctx = ExecContext(task={"id": "t", "chat_id": cid}, run_id=None, session_id=None, approved=False, cancelled=lambda: False)
    out = lf._tool(ctx, {"file_id": g["id"], "path": "photo.png"}, "text", 60).content
    assert "INVOICE 4711" in out and len(CALLS) == 1
    lf._tool(ctx, {"file_id": g["id"], "path": "photo.png"}, "text", 60)      # second time: cached, no new model call
    assert len(CALLS) == 1
    scan = lf._tool(ctx, {"file_id": g["id"], "path": "contract.pdf"}, "text", 60).content
    assert scan.count("read by the vision model") == 2 and len(CALLS) == 3
    with pytest.raises(ToolFailed):
        lf._tool(ctx, {"file_id": g["id"], "path": "..\\x.png"}, "text", 60)
