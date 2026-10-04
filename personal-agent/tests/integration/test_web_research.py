"""Web reading / crawling / research (Exa /contents, /answer, /research) - no real network, Exa's answers are faked.
Security: every URL must pass the same domain rules as web.fetch BEFORE it is given to Exa (Exa crawls from its own servers), subpages on
other domains are dropped, the API key never appears in results, DLP still blocks secrets in URLs, the kill switch stops a running research."""
import json

import pytest
from pydantic import ValidationError

import pa_gateway.egress.http as http
from pa_gateway.egress.http import EgressClient, EgressDenied, EgressPolicy, FetchResult
from pa_gateway.tools.base import ToolFailed

KEY = "exa-test-key-not-real-7777"
SEEN: list[dict] = []
REPLIES: dict[str, list] = {}


def fake_fetch(self, url, pol, method="GET", headers=None, body=None):
    SEEN.append({"url": url, "method": method, "headers": headers or {}, "body": json.loads(body) if body else None, "allow": pol.allowlist})
    for prefix, queue in REPLIES.items():
        if url.startswith(prefix):
            reply = queue.pop(0) if len(queue) > 1 else queue[0]
            return FetchResult(url, url, 200, "application/json", json.dumps(reply).encode())
    return FetchResult(url, url, 200, "text/html", b"<html><title>Page</title><body>plain page text</body></html>")


@pytest.fixture()
def web(env_nocore, monkeypatch):
    SEEN.clear()
    REPLIES.clear()
    env_nocore.setup(model=False)
    gw = env_nocore.gw
    gw.secrets.upsert_bound("web.search", "Exa API key", KEY, "api_key")
    gw.settings.apply({"web.provider": "exa"}, stepup_ok=True)
    monkeypatch.setattr(gw.connectors.web, "policy", lambda: EgressPolicy(any_site=True, blocklist=("blocked.example",)))
    monkeypatch.setattr(EgressClient, "fetch", fake_fetch)
    return gw


def test_read_sends_only_allowed_urls_to_exa_and_drops_foreign_subpages(web):
    REPLIES["https://api.exa.ai/contents"] = [{
        "results": [{"url": "https://airline.example/flights", "title": "Flights", "publishedDate": "2026-10-04T08:00:00Z", "text": "BOM-BLR 17:30 direct",
                     "highlights": ["Direct flight 17:30"],
                     "subpages": [{"url": "https://airline.example/status", "title": "Status", "text": "on time"},
                                  {"url": "https://blocked.example/x", "title": "Evil", "text": "SHOULD NOT APPEAR"}]}],
        "statuses": [{"id": "https://ota.example/a", "status": "error", "error": {"tag": "CRAWL_TIMEOUT"}}], "costDollars": {"total": 0.01}}]
    r = web.connectors.web.read({"urls": ["https://airline.example/flights", "https://ota.example/a", "https://blocked.example/p", "http://10.0.0.5/admin"],
                                 "max_chars": 4000, "subpages": 3, "subpage_target": ["status"], "live": True, "focus": "evening direct"}, None)
    call = [s for s in SEEN if s["url"] == "https://api.exa.ai/contents"][0]
    assert call["body"]["urls"] == ["https://airline.example/flights", "https://ota.example/a"]          # blocked + private never sent
    assert call["body"]["maxAgeHours"] == 0 and call["body"]["subpages"] == 3 and call["body"]["subpageTarget"] == ["status"]
    assert call["body"]["highlights"]["query"] == "evening direct" and call["allow"] == ("api.exa.ai",)
    assert "BOM-BLR 17:30 direct" in r.content and "on time" in r.content and "SHOULD NOT APPEAR" not in r.content
    assert "CRAWL_TIMEOUT" in r.content and "blocked.example" in r.content and "Published: 2026-10-04" in r.content
    assert KEY not in r.content and r.destination == "exa"


def test_read_refuses_when_nothing_is_allowed(web):
    with pytest.raises(ToolFailed):
        web.connectors.web.read({"urls": ["https://blocked.example/a", "https://localhost/x", "https://printer.local/", "https://169.254.169.254/latest"],
                                 "max_chars": 1000, "subpages": 0, "subpage_target": [], "live": False}, None)
    assert not [s for s in SEEN if "exa.ai" in s["url"]]


@pytest.mark.parametrize("url, code", [("https://127.0.0.1/", "ssrf_blocked"), ("https://192.168.1.10/", "ssrf_blocked"), ("https://intranet/", "ssrf_blocked"),
                                       ("https://nas.lan/", "ssrf_blocked"), ("https://u:p@site.example/", "userinfo_blocked"), ("ftp://site.example/", "scheme_blocked"),
                                       ("https://site.example:8443/", "port_blocked"), ("https://metadata.google.internal/", "ssrf_blocked")])
def test_check_target_rules(url, code):
    with pytest.raises(EgressDenied) as e:
        EgressClient().check_target(url, EgressPolicy(any_site=True))
    assert e.value.code == code


def test_check_target_respects_allowlist_and_connector_off():
    c = EgressClient()
    with pytest.raises(EgressDenied) as e:
        c.check_target("https://news.example/a", EgressPolicy(allowlist=("wikipedia.org",)))
    assert e.value.code == "domain_not_allowed"
    with pytest.raises(EgressDenied) as e:
        c.check_target("https://graph.microsoft.com/v1.0/me", EgressPolicy(any_site=True, denied_domains=("graph.microsoft.com",)))
    assert e.value.code == "connector_off"
    assert c.check_target("https://en.wikipedia.org/wiki/X", EgressPolicy(allowlist=("wikipedia.org",)))[2] == "en.wikipedia.org"


def test_search_filters_reach_exa(web):
    REPLIES["https://api.exa.ai/search"] = [{"results": [{"title": "RBI AI note", "url": "https://rbi.example/n", "publishedDate": "2026-10-03T00:00:00Z", "text": "x" * 2000}]}]
    r = web.connectors.web.search({"query": "AI banking India", "count": 5, "category": "news", "recent_days": 1, "include_domains": ["rbi.example"],
                                   "exclude_domains": ["spam.example"], "mode": "deep", "country": "IN", "longer": True}, None)
    b = [s for s in SEEN if s["url"] == "https://api.exa.ai/search"][0]["body"]
    assert b["category"] == "news" and b["includeDomains"] == ["rbi.example"] and b["excludeDomains"] == ["spam.example"]
    assert b["type"] == "deep" and b["userLocation"] == "IN" and b["startPublishedDate"].endswith("T00:00:00.000Z")
    assert b["contents"]["text"]["maxCharacters"] == 1500 and "(2026-10-03)" in r.content


def test_plain_search_still_works_with_only_query_and_count(web):
    REPLIES["https://api.exa.ai/search"] = [{"results": [{"title": "T", "url": "https://x.example/a", "text": "snippet"}]}]
    r = web.connectors.web.search({"query": "weather", "count": 1}, None)
    b = [s for s in SEEN if s["url"] == "https://api.exa.ai/search"][0]["body"]
    assert b["type"] == "auto" and "category" not in b and "snippet" in r.content


def test_answer_with_sources(web):
    REPLIES["https://api.exa.ai/answer"] = [{"answer": "About 30 direct flights a day.", "citations": [{"url": "https://ota.example/x", "title": "OTA", "publishedDate": "2026-10-04"}]}]
    r = web.connectors.web.answer({"query": "how many direct flights Mumbai Bangalore"}, None)
    assert "30 direct flights" in r.content and "https://ota.example/x" in r.content and "(2026-10-04)" in r.content


def test_research_polls_until_done_and_lists_sources(web, monkeypatch):
    monkeypatch.setattr("pa_gateway.connectors.web.time.sleep", lambda s: None)
    REPLIES["https://api.exa.ai/research/v0/tasks/rt_1"] = [{"status": "running"}, {"status": "running"},
                                                            {"status": "completed", "data": {"report": "Top 5 AI items for Indian banking ..."},
                                                             "citations": {"report": [{"url": "https://rbi.example/c1"}, {"url": "https://news.example/c2"}]}}]
    REPLIES["https://api.exa.ai/research/v0/tasks"] = [{"id": "rt_1"}]
    r = web.connectors.web.research({"instructions": "AI news of the last 24 hours for Indian banking", "thorough": True, "max_minutes": 5}, None)
    create = [s for s in SEEN if s["url"] == "https://api.exa.ai/research/v0/tasks"][0]
    assert create["body"]["model"] == "exa-research-pro" and create["method"] == "POST"
    assert len([s for s in SEEN if s["url"].endswith("/rt_1")]) == 3 and all(s["method"] == "GET" for s in SEEN if s["url"].endswith("/rt_1"))
    assert "Top 5 AI items" in r.content and "https://rbi.example/c1" in r.content and KEY not in r.content


def test_research_stops_on_emergency_stop(web, monkeypatch):
    monkeypatch.setattr("pa_gateway.connectors.web.time.sleep", lambda s: None)
    REPLIES["https://api.exa.ai/research/v0/tasks/rt_2"] = [{"status": "running"}]
    REPLIES["https://api.exa.ai/research/v0/tasks"] = [{"id": "rt_2"}]
    with monkeypatch.context() as m, pytest.raises(ToolFailed) as e:     # restored before teardown (shutdown must not see a stop forever)
        m.setattr(web.killswitch, "active", lambda level=None: True)
        web.connectors.web.research({"instructions": "something long enough", "thorough": False, "max_minutes": 1}, None)
    assert "stopped" in str(e.value)


def test_research_rejects_a_strange_task_id(web):
    REPLIES["https://api.exa.ai/research/v0/tasks"] = [{"id": "../../evil"}]
    with pytest.raises(ToolFailed):
        web.connectors.web.research({"instructions": "something long enough", "thorough": False, "max_minutes": 1}, None)


def test_exa_only_tools_explain_the_setup(web):
    web.settings.apply({"web.provider": "brave"}, stepup_ok=True)
    with pytest.raises(ToolFailed) as e:
        web.connectors.web.answer({"query": "anything at all"}, None)
    assert "Exa" in str(e.value)


def test_read_falls_back_to_local_fetch_without_exa(web, monkeypatch):
    web.settings.apply({"web.provider": "brave"}, stepup_ok=True)
    monkeypatch.setattr(EgressClient, "fetch", lambda self, url, pol, **k: FetchResult(url, url, 200, "text/html", b"<html><title>P</title><body>hello page</body></html>"))
    r = web.connectors.web.read({"urls": ["https://a.example/1", "https://blocked.example/2"], "max_chars": 2000, "subpages": 0, "subpage_target": [], "live": False}, None)
    assert "hello page" in r.content and "blocked.example" in r.content and r.data["mode"] == "fetch"


def test_tool_gateway_denies_secret_in_url_and_unreadable_targets(env_nocore):
    env_nocore.setup()
    env_nocore.ui.call("secrets.create", {"item": {"title": "canary", "type": "password", "value": "CANARY-5e6f7a8b9c"}})
    cid = env_nocore.ui.call("chat.create", {})["id"]
    env_nocore.ui.call("chat.send", {"chat_id": cid, "text": "hi"})
    core = env_nocore.core()
    tid = core.call("work.next", {"wait": 2})["task_id"]
    env_nocore.ui.call("connectors.set", {"connector": "web", "enabled": True, "use_chat": True})
    res = core.call("tools.invoke", {"task_id": tid, "tool": "web.read", "args": {"urls": ["https://en.wikipedia.org/wiki/X?q=CANARY-5e6f7a8b9c"]}})
    assert res["status"] == "denied" and "data-loss prevention" in res["reason"]        # an allowed site, but a stored secret in the URL
    res = core.call("tools.invoke", {"task_id": tid, "tool": "web.read", "args": {"urls": ["https://127.0.0.1/admin"]}})
    assert res["status"] == "denied" and "private" in res["reason"]
    res = core.call("tools.invoke", {"task_id": tid, "tool": "web.read", "args": {"urls": ["file:///C:/Windows/win.ini"]}})
    assert res["status"] == "denied"


def test_argument_validation():
    from pa_gateway.policy.tools_registry import WebReadArgs, WebSearchArgs
    with pytest.raises(ValidationError):
        WebReadArgs(urls=[])
    with pytest.raises(ValidationError):
        WebReadArgs(urls=["https://a.example"] * 11)
    with pytest.raises(ValidationError):
        WebSearchArgs(query="x", include_domains=["not a domain!"])
    with pytest.raises(ValidationError):
        WebSearchArgs(query="x", country="India")
    assert WebSearchArgs(query="x", include_domains=["RBI.org.in"]).include_domains == ["rbi.org.in"]
    _ = http  # keep the module import (monkeypatch target documented above)
