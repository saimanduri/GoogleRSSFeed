"""An empty model reply is rescued (retry without the JSON constraint, then Ollama's native API) instead of failing the chat."""
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from pa_common.errors import PAError

MODE = {"v1": "empty_when_constrained", "native": "ok"}
SEEN = []


class Fake(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _json(self, obj):
        b = json.dumps(obj).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))))
        if self.path != "/api/show":
            SEEN.append((self.path, "response_format" in body, body.get("max_tokens"), (body.get("options") or {}).get("num_ctx")))
        if self.path == "/v1/chat/completions":
            empty = MODE["v1"] == "always_empty" or (MODE["v1"] == "empty_when_constrained" and "response_format" in body)
            chunks = [] if empty else [json.dumps({"choices": [{"delta": {"content": '{"thought":"t","action":"final","answer":"ok"}'}}]})]
            payload = "".join(f"data: {c}\n\n" for c in chunks) + "data: [DONE]\n\n"
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Content-Length", str(len(payload.encode())))
            self.end_headers()
            self.wfile.write(payload.encode())
        elif self.path == "/api/chat":
            self._json({"message": {"content": "" if MODE["native"] == "empty" else '{"thought":"t","action":"final","answer":"native"}'}, "prompt_eval_count": 5, "eval_count": 7})
        else:
            self.send_response(404)
            self.end_headers()


@pytest.fixture()
def model(env_nocore):
    SEEN.clear()
    MODE.update(v1="empty_when_constrained", native="ok")
    srv = ThreadingHTTPServer(("127.0.0.1", 0), Fake)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    env_nocore.setup(model=False)
    mid = env_nocore.ui.call("llm.add", {"model": {"provider": "ollama", "endpoint": f"http://127.0.0.1:{srv.server_address[1]}", "model_name": "m", "name": "m",
                                                  "kind": "chat"}, "auto_test": False})["id"]
    yield env_nocore.gw.llm, env_nocore.gw.llm.get_model(mid)
    srv.shutdown()


def _req():
    return {"model": "m", "messages": [{"role": "user", "content": "hi"}], "temperature": 0.2, "max_tokens": 1000, "stream": True,
            "response_format": {"type": "json_schema", "json_schema": {"name": "a", "schema": {}}}}


def test_retry_without_the_json_constraint_succeeds(model):
    llm, m = model
    text, _ = llm._run_with_rescue(m, _req(), None, True)
    assert '"answer":"ok"' in text
    assert SEEN[0][1] is True and SEEN[1][1] is False and SEEN[1][2] == 2000        # second try: no constraint, more room


def test_native_api_is_the_last_resort(model):
    llm, m = model
    MODE["v1"] = "always_empty"
    text, usage = llm._run_with_rescue(m, _req(), None, True)
    assert '"answer":"native"' in text and usage["completion_tokens"] == 7
    assert SEEN[-1][0] == "/api/chat" and SEEN[-1][3] == 16384


def test_three_empties_give_a_clear_final_error(model):
    llm, m = model
    MODE.update(v1="always_empty", native="empty")
    with pytest.raises(PAError) as e:
        llm._run_with_rescue(m, _req(), None, True)
    assert e.value.code == "model_empty" and "three times" in str(e.value)


def test_normal_replies_are_untouched(model):
    llm, m = model
    MODE["v1"] = "ok"
    text, _ = llm._run_with_rescue(m, _req(), None, True)
    assert len(SEEN) == 1 and "ok" in text
