"""Chat / voice / vision / embedding models never get mixed up: wrong kind refused on add, wrong role refused, roles only
use models of their own kind, vision models are tested with a real picture. A fake Ollama stands in for the real one."""
import base64
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from pa_common.errors import PAError

MODELS = {
    "fake/asr:1b": ({"general.architecture": "qwen3vl", "general.tags": ["automatic-speech-recognition"]}, ["completion"]),
    "fake/vl:7b": ({"general.architecture": "qwen2vl"}, ["completion", "vision"]),
    "fake/chat:8b": ({"general.architecture": "llama"}, ["completion"]),
    "fake/embed:1b": ({"general.architecture": "bert"}, ["embedding"]),
}
SEEN = []


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
            return self._send(403, {"error": "origin"})
        if self.path.startswith("/api/tags"):
            return self._send(200, {"models": [{"name": n} for n in MODELS]})
        self._send(200, {"data": []})

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}")
        if self.path == "/api/show":
            info, caps = MODELS.get(body.get("model"), ({}, []))
            return self._send(200, {"model_info": info, "capabilities": caps})
        if self.path == "/api/chat":
            msg = body["messages"][0]
            SEEN.append({"model": body["model"], "has_image": bool(msg.get("images")), "prompt": msg.get("content", "")})
            if body["model"] == "fake/vl:7b" and msg.get("images"):
                return self._send(200, {"message": {"role": "assistant", "content": "Red."}, "done": True})
            return self._send(200, {"message": {"role": "assistant", "content": "language English<asr_text>hello"}, "done": True})
        self._send(404, {})


@pytest.fixture()
def ollama():
    SEEN.clear()
    srv = ThreadingHTTPServer(("127.0.0.1", 0), Fake)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{srv.server_address[1]}"
    srv.shutdown()


def _add(env, ep, name, kind, **kw):
    return env.ui.call("llm.add", {"model": {"provider": "ollama", "endpoint": ep, "model_name": name, "name": name, "kind": kind}, **kw})


@pytest.mark.parametrize("name,kind", [("fake/asr:1b", "chat"), ("fake/asr:1b", "vision"), ("fake/embed:1b", "chat"), ("fake/chat:8b", "stt"),
                                       ("fake/chat:8b", "embedding"), ("fake/chat:8b", "vision"), ("fake/vl:7b", "stt")])
def test_wrong_kind_is_refused_on_add(env_nocore, ollama, name, kind):
    env_nocore.setup(model=False)
    with pytest.raises(PAError) as e:
        _add(env_nocore, ollama, name, kind, auto_test=False)
    assert e.value.code == "wrong_kind"
    assert env_nocore.ui.call("llm.models")["models"] == []


def test_vision_capable_model_can_be_chat_or_vision(env_nocore, ollama):
    env_nocore.setup(model=False)
    _add(env_nocore, ollama, "fake/vl:7b", "chat", auto_test=False)
    _add(env_nocore, ollama, "fake/vl:7b", "vision", auto_test=False)
    assert sorted(m["kind"] for m in env_nocore.ui.call("llm.models")["models"]) == ["chat", "vision"]


def test_vision_model_tests_itself_with_a_picture_and_becomes_the_vision_default(env_nocore, ollama):
    env_nocore.setup(model=False)
    r = _add(env_nocore, ollama, "fake/vl:7b", "vision")
    assert env_nocore.wait(lambda: env_nocore.ui.call("llm.models")["roles"]["vision"] == r["id"], 30)
    m = env_nocore.ui.call("llm.models")["models"][0]
    assert m["tested"] == 1 and m["kind"] == "vision"
    assert SEEN and SEEN[0]["has_image"] and "colour" in SEEN[0]["prompt"]
    assert env_nocore.ui.call("llm.models")["roles"]["standard"] == ""      # never mixed into the chat role


def test_roles_only_accept_their_own_kind(env_nocore, ollama):
    env_nocore.setup(model=False)
    ids = {}
    for name, kind in (("fake/asr:1b", "stt"), ("fake/vl:7b", "vision"), ("fake/embed:1b", "embedding")):
        ids[kind] = _add(env_nocore, ollama, name, kind)["id"]
    assert env_nocore.wait(lambda: all(m["tested"] == 1 or m["kind"] == "embedding" for m in env_nocore.ui.call("llm.models")["models"]), 40)
    env_nocore.gw.db.execute("UPDATE models SET tested=1")
    for role, kind in (("standard", "stt"), ("standard", "vision"), ("standard", "embedding"), ("fast", "stt"), ("vision", "stt"), ("stt", "vision"), ("embedding", "stt")):
        with pytest.raises(PAError) as e:
            env_nocore.ui.call("llm.set_role", {"role": role, "model_id": ids[kind]})
        assert e.value.code == "wrong_kind", (role, kind)
    with pytest.raises(PAError):
        env_nocore.ui.call("llm.set_role", {"role": "nonsense", "model_id": ids["stt"]})
    env_nocore.ui.call("llm.set_role", {"role": "stt", "model_id": ids["stt"]})
    env_nocore.ui.call("llm.set_role", {"role": "vision", "model_id": ids["vision"]})


def test_a_misassigned_model_is_never_used_even_if_the_setting_is_forged(env_nocore, ollama):
    env_nocore.setup(model=False)
    a = _add(env_nocore, ollama, "fake/asr:1b", "stt")
    assert env_nocore.wait(lambda: env_nocore.ui.call("llm.models")["models"][0]["tested"] == 1, 30)
    env_nocore.gw.settings.apply({"llm.role.standard": a["id"]})            # bypasses the RPC check on purpose
    assert env_nocore.gw.llm.role_model("standard") is None
    assert env_nocore.gw.llm.role_model("vision") is None


def test_discover_reports_the_kind_of_every_model(env_nocore, ollama):
    env_nocore.setup(model=False)
    r = env_nocore.ui.call("llm.discover", {"provider": "ollama", "endpoint": ollama})
    kinds = {d["name"]: (d["kind"], d["vision"]) for d in r["details"]}
    assert kinds == {"fake/asr:1b": ("stt", False), "fake/vl:7b": ("chat", True), "fake/chat:8b": ("chat", False), "fake/embed:1b": ("embedding", False)}
    assert r["models"] == list(MODELS)


def test_describe_image_needs_a_vision_model_and_sends_the_picture(env_nocore, ollama):
    env_nocore.setup(model=False)
    with pytest.raises(PAError) as e:
        env_nocore.gw.llm.describe_image(b"x", "image/png", "describe")
    assert e.value.code == "no_vision_model"
    r = _add(env_nocore, ollama, "fake/vl:7b", "vision")
    assert env_nocore.wait(lambda: env_nocore.ui.call("llm.models")["roles"]["vision"] == r["id"], 30)
    from pa_gateway.llm import audio as A
    out = env_nocore.gw.llm.describe_image(A.sample_png(), "image/png", "what colour?")
    assert out["text"] == "Red." and SEEN[-1]["has_image"]
    base64.b64decode(base64.b64encode(A.sample_png()))
