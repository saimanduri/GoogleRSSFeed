"""Network log: every outbound request is recorded (allowed, redirected, blocked, failed) without query strings or bodies."""
import httpx
import pytest

from pa_gateway.egress.http import EgressClient, EgressDenied, EgressPolicy

POL = EgressPolicy(allowlist=("example.com",))


def _client(env, handler):
    c = EgressClient(lambda host, port: ["93.184.216.34"], transport=httpx.MockTransport(handler))
    c.on_event = lambda i: env.gw.netlog.record("web", i["method"], i["url"], i.get("status"), outcome=i.get("outcome"), reason=i.get("reason", ""),
                                               bytes_out=i.get("bytes_out", 0), bytes_in=i.get("bytes_in", 0), duration_ms=i.get("duration_ms", 0), ip=i.get("ip"))
    return c


def test_allowed_redirect_and_blocked_requests_are_logged(env_nocore):
    env_nocore.setup(model=False)

    def handler(req):
        if req.url.path == "/old":
            return httpx.Response(302, headers={"location": "https://example.com/new?token=SECRET-123"})
        return httpx.Response(200, headers={"content-type": "text/html"}, content=b"<html>hello</html>")

    c = _client(env_nocore, handler)
    with env_nocore.gw.netlog.context(tool="web.fetch", task_id="tsk_1", run_id="run_1"):
        r = c.fetch("https://example.com/old?key=SECRET-ABC", POL)
        assert r.status == 200
        with pytest.raises(EgressDenied):
            c.fetch("https://evil.example.org/x", POL)
    out = env_nocore.ui.call("network.logs", {"days": 7})
    rows = out["rows"]
    assert len(rows) == 3
    by = {(x["host"], x["outcome"]): x for x in rows}
    assert ("example.com", "ok") in by and ("evil.example.org", "blocked") in by
    blocked = by[("evil.example.org", "blocked")]
    assert "domain_not_allowed" in blocked["reason"] and blocked["tool"] == "web.fetch" and blocked["task_id"] == "tsk_1"
    ok = [x for x in rows if x["host"] == "example.com" and x["status"] == 200][0]
    assert ok["bytes_in"] == len(b"<html>hello</html>") and ok["method"] == "GET" and ok["ip"] == "93.184.216.34"
    # nothing sensitive: no query strings (tokens, keys) anywhere in the stored log
    assert "SECRET" not in str(rows)
    s = out["summary"]
    assert s["requests"] == 3 and s["blocked"] == 1 and s["hosts"][0]["host"] == "example.com"


def test_filters_loopback_flag_and_retention(env_nocore):
    env_nocore.setup(model=False)
    nl = env_nocore.gw.netlog
    nl.record("llm", "POST", "http://127.0.0.1:11434/v1/chat/completions", 200, purpose="model chat", bytes_out=500, bytes_in=900)
    nl.record("m365", "GET", "https://graph.microsoft.com/v1.0/me/messages?$top=5", 200, purpose="Microsoft Graph")
    ui = env_nocore.ui
    allr = ui.call("network.logs", {})
    assert {r["host"] for r in allr["rows"]} == {"127.0.0.1", "graph.microsoft.com"}
    assert next(r for r in allr["rows"] if r["host"] == "127.0.0.1")["loopback"] == 1
    assert all("?" not in r["path"] for r in allr["rows"])
    assert [r["host"] for r in ui.call("network.logs", {"include_local": False})["rows"]] == ["graph.microsoft.com"]
    assert [r["host"] for r in ui.call("network.logs", {"component": "llm"})["rows"]] == ["127.0.0.1"]
    assert len(ui.call("network.logs", {"host": "graph"})["rows"]) == 1
    # rows older than the retention period are purged
    env_nocore.gw.db.execute("UPDATE net_log SET ts='2000-01-01T00:00:00Z'")
    nl.purge()
    assert ui.call("network.logs", {"days": 14})["rows"] == []


def test_system_usage_rpc_shape(env_nocore):
    env_nocore.setup(model=False)
    r = env_nocore.ui.call("system.usage")
    assert set(r) >= {"available", "history"}
