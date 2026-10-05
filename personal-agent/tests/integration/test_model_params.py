"""Context length + temperature per model (Settings > AI Model): stored, validated, really sent to the model server.
Ollama gets them through its native /api/chat (its OpenAI-compatible endpoint ignores num_ctx)."""
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from pa_common.errors import PAError
from pa_gateway.llm import service

SEEN = []


class Fake(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))))
        SEEN.append((self.path, body))
        if self.path == "/api/chat":
            out = json.dumps({"message": {"content": '{"thought":"t","action":"final","answer":"hello"}'}, "done": False}) + "\n" + \
                json.dumps({"message": {"content": ""}, "done": True, "prompt_eval_count": 9, "eval_count": 4}) + "\n"
            ctype = "application/x-ndjson"
        elif self.path == "/v1/chat/completions":
            out = 'data: {"choices":[{"delta":{"content":"{\\"thought\\":\\"t\\",\\"action\\":\\"final\\",\\"answer\\":\\"hi\\"}"}}]}\n\ndata: [DONE]\n\n'
            ctype = "text/event-stream"
        else:
            self.send_response(404)
            self.end_headers()
            return
        b = out.encode()
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)


@pytest.fixture()
def srv():
    SEEN.clear()
    s = ThreadingHTTPServer(("127.0.0.1", 0), Fake)
    threading.Thread(target=s.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{s.server_address[1]}"
    s.shutdown()


def _add(env, provider, endpoint, **extra):
    env.setup(model=False)
    return env.ui.call("llm.add", {"model": {"provider": provider, "endpoint": endpoint, "model_name": "m", "name": "m", "kind": "chat", **extra},
                                   "auto_test": False})["id"]


def _ask(env, mid):
    llm = env.gw.llm
    m = llm.get_model(mid)
    eff = llm.effective_params(m)
    req = {"model": "m", "messages": [{"role": "user", "content": "hi"}], "temperature": eff["temperature"], "max_tokens": 500,
           "stream": True, "num_ctx": eff["context_length"]}
    return llm._run(m, req, None)


def test_defaults_then_per_model_values_reach_ollama_native_api(env_nocore, srv):
    mid = _add(env_nocore, "ollama", srv)
    m = env_nocore.ui.call("llm.models")["models"][0]
    assert m["effective"] == {"context_length": 8192, "temperature": 0.2, "context_from": "default", "temperature_from": "default"}
    out = env_nocore.ui.call("llm.update", {"model_id": mid, "context_length": 32768, "temperature": 0.7})
    assert out["context_length"] == 32768 and out["temperature"] == 0.7 and out["context_from"] == "model"
    text, usage = _ask(env_nocore, mid)
    path, body = SEEN[-1]
    assert path == "/api/chat" and body["options"]["num_ctx"] == 32768 and body["options"]["temperature"] == 0.7
    assert "hello" in text and usage["prompt_tokens"] == 9
    env_nocore.ui.call("llm.update", {"model_id": mid, "context_length": None, "temperature": None})     # back to the defaults
    assert env_nocore.gw.llm.effective_params(env_nocore.gw.llm.get_model(mid))["context_from"] == "default"


def test_openai_style_servers_get_temperature_but_never_num_ctx(env_nocore, srv):
    mid = _add(env_nocore, "openai", srv + "/v1", context_length=16384, temperature=1.1)
    text, _ = _ask(env_nocore, mid)
    path, body = SEEN[-1]
    assert path == "/v1/chat/completions" and body["temperature"] == 1.1 and "num_ctx" not in body and "hi" in text


@pytest.mark.parametrize("ctx, temp", [(100, None), (2_000_000, None), ("lots", None), (8192.5, None), (True, None),
                                       (None, -0.1), (None, 2.5), (None, "hot"), (None, True)])
def test_invalid_values_are_refused(env_nocore, srv, ctx, temp):
    mid = _add(env_nocore, "ollama", srv)
    with pytest.raises(PAError) as e:
        env_nocore.ui.call("llm.update", {"model_id": mid, "context_length": ctx, "temperature": temp})
    assert e.value.code in ("invalid_request", "validation_error")


def test_change_is_audited_without_content(env_nocore, srv):
    mid = _add(env_nocore, "ollama", srv)
    env_nocore.ui.call("llm.update", {"model_id": mid, "context_length": 65536, "temperature": 0})
    m = env_nocore.gw.llm.get_model(mid)
    assert m["context_length"] == 65536 and m["temperature"] == 0.0          # 0 is a real temperature, not "default"


def test_answer_budget_is_kept_inside_the_context_window():
    msgs = [{"role": "user", "content": "x" * 35_000}]                       # ~10k tokens
    assert service.fit_answer_budget(msgs, 2048, 32768) == 2048
    assert service.fit_answer_budget(msgs, 2048, 11000) < 2048
    assert service.fit_answer_budget(msgs, 2048, 4096) == 256                 # never below a minimal answer


def test_ollama_body_and_line_parser():
    req = {"model": "m", "messages": [], "temperature": 0.3, "max_tokens": 700, "num_ctx": 12288, "reasoning_effort": "none",
           "response_format": {"type": "json_schema", "json_schema": {"name": "a", "schema": {"type": "object"}}}}
    b = service.ollama_body(req)
    assert b["options"] == {"num_ctx": 12288, "temperature": 0.3, "num_predict": 700} and b["think"] is False and b["format"] == {"type": "object"}
    assert service.ollama_body({**req, "response_format": {"type": "json_object"}})["format"] == "json"
    assert service.parse_ollama_line('{"message":{"content":"ab","thinking":"secret"},"done":false}') == ("ab", False, None)
    assert service.parse_ollama_line("") == ("", False, None) and service.parse_ollama_line("not json") == ("", False, None)
    d = service.parse_ollama_line('{"done":true,"prompt_eval_count":3,"eval_count":2,"done_reason":"length"}')
    assert d[1] is True and d[2]["done_reason"] == "length"
    with pytest.raises(PAError):
        service.parse_ollama_line('{"error":"model ran out of memory"}')
