"""Exa web search setup: 'Test web search' button path (no real network; the provider answer is faked)."""
import json

import pytest

from pa_common.errors import PAError
from pa_gateway.egress.http import FetchResult


def test_test_search_needs_setup_first(env_nocore):
    env_nocore.setup(model=False)
    with pytest.raises(PAError) as e:
        env_nocore.ui.call("web.test_search", {})
    assert e.value.code == "needs_setup"


def test_exa_search_uses_bound_key_and_never_returns_it(env_nocore, monkeypatch):
    env_nocore.setup(model=False)
    gw = env_nocore.gw
    key = "exa-test-key-not-real-123456"
    gw.secrets.upsert_bound("web.search", "Exa API key", key, "api_key")
    gw.settings.apply({"web.provider": "exa"}, stepup_ok=True)
    seen = {}

    def fake_fetch(self, url, pol, method="GET", headers=None, body=None):
        seen.update(url=url, headers=headers or {}, body=json.loads(body or b"{}"), allow=pol.allowlist)
        return FetchResult(url, url, 200, "application/json", json.dumps({"results": [{"title": "T", "url": "https://x.example/a", "text": "snippet"}]}).encode())

    import pa_gateway.egress.http as http
    monkeypatch.setattr(http.EgressClient if hasattr(http, "EgressClient") else gw.connectors.web.client.__class__, "fetch", fake_fetch)
    out = env_nocore.ui.call("web.test_search", {})
    assert out["ok"] and out["provider"] == "exa" and out["results"] == 1
    assert seen["url"] == "https://api.exa.ai/search" and seen["headers"]["x-api-key"] == key and seen["allow"] == ("api.exa.ai",)
    assert key not in json.dumps(out)
    assert key not in json.dumps(gw.audit.recent(50) if hasattr(gw.audit, "recent") else "")


def test_provider_failure_is_reported_without_details_of_the_key(env_nocore, monkeypatch):
    env_nocore.setup(model=False)
    gw = env_nocore.gw
    gw.secrets.upsert_bound("web.search", "Exa API key", "exa-test-key-not-real-123456", "api_key")
    gw.settings.apply({"web.provider": "exa"}, stepup_ok=True)
    import pa_gateway.egress.http as http
    cls = http.EgressClient if hasattr(http, "EgressClient") else gw.connectors.web.client.__class__

    def boom(self, *a, **k):
        raise http.EgressDenied("host not allowed", "blocked_by_test")
    monkeypatch.setattr(cls, "fetch", boom)
    with pytest.raises(PAError) as e:
        env_nocore.ui.call("web.test_search", {})
    assert e.value.code == "search_failed" and "exa-test-key" not in str(e.value)
