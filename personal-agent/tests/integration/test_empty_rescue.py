"""An empty model reply is rescued (retry without the JSON constraint, then a non-streamed native call with a larger window)
instead of failing the chat. Ollama chat goes through its native /api/chat (the only API that applies num_ctx)."""
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from pa_common.errors import PAError

# "stream": the normal streamed /api/chat call; "native": the last-resort non-streamed /api/chat call
MODE = {"stream": "empty_when_constrained", "native": "ok"}
SEEN = []
ANSWER = '{"thought":"t","action":"final","answer":"%s"}'


class Fake(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, payload: bytes, ctype: str):
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))))
        if self.path != "/api/chat":
            self.send_response(404)
            self.end_headers()
            return
        opts = body.get("options") or {}
        SEEN.append({"stream": body.get("stream"), "format": "format" in body, "num_predict": opts.get("num_predict"),
                     "num_ctx": opts.get("num_ctx"), "temperature": opts.get("temperature"), "think": body.get("think")})
        if body.get("stream"):
            empty = MODE["stream"] == "always_empty" or (MODE["stream"] == "empty_when_constrained" and "format" in body)
            lines = [] if empty else [json.dumps({"message": {"role": "assistant", "content": ANSWER[:20]}, "done": False}),
                                      json.dumps({"message": {"role": "assistant", "content": ANSWER[20:] % "ok"}, "done": False})]
            lines.append(json.dumps({"message": {"content": ""}, "done": True, "prompt_eval_count": 11, "eval_count": 3, "done_reason": "stop"}))
            self._send(("\n".join(lines) + "\n").encode(), "application/x-ndjson")
        else:
            text = "" if MODE["native"] == "empty" else ANSWER % "native"
            self._send(json.dumps({"message": {"content": text}, "prompt_eval_count": 5, "eval_count": 7}).encode(), "application/json")


@pytest.fixture()
def model(env_nocore):
    SEEN.clear()
    MODE.update(stream="empty_when_constrained", native="ok")
    srv = ThreadingHTTPServer(("127.0.0.1", 0), Fake)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    env_nocore.setup(model=False)
    mid = env_nocore.ui.call("llm.add", {"model": {"provider": "ollama", "endpoint": f"http://127.0.0.1:{srv.server_address[1]}", "model_name": "m", "name": "m",
                                                  "kind": "chat"}, "auto_test": False})["id"]
    yield env_nocore.gw.llm, env_nocore.gw.llm.get_model(mid)
    srv.shutdown()


def _req():
    return {"model": "m", "messages": [{"role": "user", "content": "hi"}], "temperature": 0.2, "max_tokens": 1000, "stream": True, "num_ctx": 8192,
            "reasoning_effort": "none", "response_format": {"type": "json_schema", "json_schema": {"name": "a", "schema": {"type": "object"}}}}


def test_retry_without_the_json_constraint_succeeds(model):
    llm, m = model
    text, usage = llm._run_with_rescue(m, _req(), None, True)
    assert '"answer":"ok"' in text and usage["prompt_tokens"] == 11
    assert SEEN[0]["format"] is True and SEEN[1]["format"] is False and SEEN[1]["num_predict"] == 2000   # second try: no constraint, more room
    assert all(s["num_ctx"] == 8192 and s["think"] is False for s in SEEN)


def test_native_api_is_the_last_resort(model):
    llm, m = model
    MODE["stream"] = "always_empty"
    text, usage = llm._run_with_rescue(m, _req(), None, True)
    assert '"answer":"native"' in text and usage["completion_tokens"] == 7
    assert SEEN[-1]["stream"] is False and SEEN[-1]["num_ctx"] == 16384


def test_three_empties_give_a_clear_final_error(model):
    llm, m = model
    MODE.update(stream="always_empty", native="empty")
    with pytest.raises(PAError) as e:
        llm._run_with_rescue(m, _req(), None, True)
    assert e.value.code == "model_empty" and "three times" in str(e.value)


def test_normal_replies_are_untouched(model):
    llm, m = model
    MODE["stream"] = "ok"
    text, _ = llm._run_with_rescue(m, _req(), None, True)
    assert len(SEEN) == 1 and '"answer":"ok"' in text
