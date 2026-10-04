"""Speech-to-text through Ollama (/api/chat + WAV), 'Add model' auto-test and defaults, model inspection.
A fake Ollama server stands in for the real one (the real one is covered by scripts/live_asr_check.py)."""
import base64
import io
import json
import threading
import wave
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import numpy as np
import pytest

from pa_common.errors import PAError

CALLS: list[dict] = []
MODE = {"fail": False}


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
        if self.headers.get("Origin"):
            return self._send(403, {"error": "origin not allowed"})
        self._send(200, {"models": [{"name": "fake/asr:1b"}], "data": []})

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}")
        if self.path == "/api/show":
            if "asr" in body.get("model", ""):
                return self._send(200, {"model_info": {"general.architecture": "qwen3vl", "general.tags": ["automatic-speech-recognition"]},
                                        "capabilities": ["completion"]})
            return self._send(200, {"model_info": {"general.architecture": "llama"}, "capabilities": ["completion"]})
        if self.path == "/api/chat":
            if MODE["fail"]:
                return self._send(500, {"error": "unsupported"})
            wav = base64.b64decode(body["messages"][0]["images"][0])
            with wave.open(io.BytesIO(wav)) as w:
                CALLS.append({"rate": w.getframerate(), "ch": w.getnchannels(), "secs": w.getnframes() / w.getframerate(),
                              "content": body["messages"][0]["content"]})
            return self._send(200, {"message": {"role": "assistant", "content": f"language English<asr_text>part {len(CALLS)}"}, "done": True})
        self._send(404, {})


@pytest.fixture()
def fake_ollama():
    CALLS.clear()
    MODE["fail"] = False
    srv = ThreadingHTTPServer(("127.0.0.1", 0), Fake)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{srv.server_address[1]}"
    srv.shutdown()


def _wav(seconds: float) -> str:
    n = int(16000 * seconds)
    x = (6000 * np.sin(2 * np.pi * 300 * np.arange(n) / 16000)).astype("<i2")
    buf = io.BytesIO()
    with wave.open(buf, "wb") as w:
        w.setnchannels(1)
        w.setsampwidth(2)
        w.setframerate(16000)
        w.writeframes(x.tobytes())
    return base64.b64encode(buf.getvalue()).decode()


def _add(env, ep, name="fake/asr:1b", kind="stt"):
    return env.ui.call("llm.add", {"model": {"provider": "ollama", "endpoint": ep, "model_name": name, "name": name, "kind": kind}})


def _ready(env):
    return env.wait(lambda: env.ui.call("llm.models")["models"][0]["tested"] == 1, 30)


def test_add_stt_model_tests_itself_and_becomes_default(env_nocore, fake_ollama):
    env_nocore.setup(model=False)
    r = _add(env_nocore, fake_ollama)
    assert r["testing"] is True
    assert env_nocore.wait(lambda: env_nocore.ui.call("llm.models")["roles"]["stt"] == r["id"], 30)
    st = env_nocore.ui.call("llm.models")
    assert st["models"][0]["tested"] == 1 and st["models"][0]["testing"] is False
    assert CALLS and CALLS[0]["rate"] == 16000 and CALLS[0]["ch"] == 1 and CALLS[0]["content"] == ""


def test_long_recording_is_split_and_joined(env_nocore, fake_ollama):
    env_nocore.setup(model=False)
    _add(env_nocore, fake_ollama)
    assert _ready(env_nocore)
    CALLS.clear()
    out = env_nocore.ui.call("voice.transcribe", {"audio_b64": _wav(120), "mime": "audio/wav"})
    assert len(CALLS) == 3 and all(c["secs"] <= 50.5 for c in CALLS)
    assert out["text"] == "part 1 part 2 part 3" and out["language"] == "English"


def test_webm_is_refused_for_ollama_models(env_nocore, fake_ollama):
    env_nocore.setup(model=False)
    _add(env_nocore, fake_ollama)
    assert _ready(env_nocore)
    junk = base64.b64encode(b"\x1a\x45\xdf\xa3" + b"0" * 500).decode()
    with pytest.raises(PAError) as e:
        env_nocore.ui.call("voice.transcribe", {"audio_b64": junk, "mime": "audio/webm"})
    assert e.value.code == "invalid_request"


def test_failing_model_is_not_marked_tested_or_default(env_nocore, fake_ollama):
    env_nocore.setup(model=False)
    MODE["fail"] = True
    r = _add(env_nocore, fake_ollama)
    assert env_nocore.wait(lambda: not env_nocore.ui.call("llm.models")["models"][0]["testing"], 30)
    st = env_nocore.ui.call("llm.models")
    assert st["models"][0]["tested"] == 0 and not st["roles"]["stt"]
    checks = st["models"][0]["test_report"]["checks"]
    assert not any(c["ok"] for c in checks if c["name"] == "transcribes_audio")
    assert r["id"]


def test_existing_default_is_never_replaced_silently(env_nocore, fake_ollama):
    env_nocore.setup(model=False)
    a = _add(env_nocore, fake_ollama, "fake/asr:1b")
    assert env_nocore.wait(lambda: env_nocore.ui.call("llm.models")["roles"]["stt"] == a["id"], 30)
    b = _add(env_nocore, fake_ollama, "fake/asr:2b")
    assert env_nocore.wait(lambda: all(not m["testing"] for m in env_nocore.ui.call("llm.models")["models"]), 30)
    assert env_nocore.ui.call("llm.models")["roles"]["stt"] == a["id"] and b["id"] != a["id"]


def test_inspect_detects_kind(env_nocore, fake_ollama):
    env_nocore.setup(model=False)
    call = env_nocore.ui.call
    base = {"provider": "ollama", "endpoint": fake_ollama}
    assert call("llm.inspect", {**base, "model_name": "fake/asr:1b"})["kind"] == "stt"
    assert call("llm.inspect", {**base, "model_name": "llama3"})["kind"] == "chat"
    assert call("llm.inspect", {**base, "model_name": "nomic-embed-text"})["kind"] == "embedding"
