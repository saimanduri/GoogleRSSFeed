"""Web search + fetch connector (spec 11). All traffic through the egress filter in this process."""
from __future__ import annotations

import json
from typing import Any
from urllib.parse import quote_plus, urlsplit

from pa_common.errors import PAError
from pa_common.html_text import html_to_text
from pa_common.sensitivity import Sensitivity

from ..egress.http import EgressClient, EgressDenied, EgressPolicy
from ..tools.base import ExecContext, ToolFailed, ToolResult
from .service import ConnectorAdapter

PROVIDER_HOSTS = {
    "brave": ("api.search.brave.com",),
    "exa": ("api.exa.ai",),
    "tavily": ("api.tavily.com",),
}


class WebConnector(ConnectorAdapter):
    id = "web"
    label = "Web search + fetch"
    description = "Search the public web and fetch pages through the gateway's egress filter."
    manifest = {"destinations": ["search provider API", "allowlisted websites"], "data_types": ["public web content"],
                "side_effects": ["EGRESS (queries and URLs leave this PC)"], "secrets": ["search provider API key"]}

    def __init__(self, gw, client: EgressClient | None = None):
        super().__init__(gw)
        self.client = client or EgressClient()

    def connection_valid(self) -> tuple[bool, str]:
        return True, ""

    def status(self) -> dict[str, Any]:
        s = self.gw.settings
        prov = s.get("web.provider")
        has_key = prov == "searxng" or (prov != "none" and self.gw.secrets.has_binding("web.search"))
        return {"connection_ok": True, "connection_reason": "", "search_provider": prov, "search_ready": prov != "none" and has_key,
                "fetch_any_site": s.get("web.fetch_any_site")}

    def policy(self) -> EgressPolicy:
        s = self.gw.settings
        return EgressPolicy(
            allow_http=bool(s.get("web.allow_http")), extra_ports=tuple(int(p) for p in s.get("web.extra_ports") if str(p).isdigit()),
            allowlist=tuple(s.get("web.allowlist")), blocklist=tuple(s.get("web.blocklist")), any_site=bool(s.get("web.fetch_any_site")),
            denied_domains=self.gw.connectors.denied_domains(), max_bytes=int(s.get("web.max_page_kb")) * 1024,
            timeout=float(s.get("web.timeout_seconds")), max_redirects=int(s.get("web.max_redirects")))

    def precheck_url(self, url: str) -> None:
        try:
            self.client.check_url(url, self.policy())
        except EgressDenied as e:
            raise PAError(str(e), code=e.code) from e

    def register_tools(self, tg) -> None:
        tg.register("web.search", self.search)
        tg.register("web.fetch", self.fetch)

    # ------------------------------------------------------------------ search
    def _provider_policy(self, host: str) -> EgressPolicy:
        base = self.policy()
        return EgressPolicy(allowlist=(host,), any_site=False, denied_domains=base.denied_domains, max_bytes=2 * 1024 * 1024,
                            timeout=base.timeout, max_redirects=0, content_types=("application/json",))

    def search(self, args: dict[str, Any], ctx: ExecContext) -> ToolResult:
        s = self.gw.settings
        prov = s.get("web.provider")
        q, n = args["query"], args["count"]
        sens = int(Sensitivity[s.get("sensitivity.default.web")])
        if prov == "none":
            raise ToolFailed("no web search provider is configured (Settings > Web Access)")
        try:
            if prov == "searxng":
                base = s.get("web.searxng_url").rstrip("/")
                if not base.startswith("https://"):
                    raise ToolFailed("SearXNG URL must use https")
                host = urlsplit(base).hostname or ""
                r = self.client.fetch(f"{base}/search?q={quote_plus(q)}&format=json", self._provider_policy(host))
                items = [{"title": x.get("title"), "url": x.get("url"), "snippet": x.get("content")}
                         for x in json.loads(r.body).get("results", [])[:n]]
            else:
                key = self.gw.secrets.value_for_binding("web.search")
                if not key:
                    raise ToolFailed("no API key is bound to web.search (Secrets > Used by)")
                host = PROVIDER_HOSTS[prov][0]
                if prov == "brave":
                    r = self.client.fetch(f"https://{host}/res/v1/web/search?q={quote_plus(q)}&count={n}",
                                          self._provider_policy(host), headers={"X-Subscription-Token": key, "Accept": "application/json"})
                    items = [{"title": x.get("title"), "url": x.get("url"), "snippet": x.get("description")}
                             for x in json.loads(r.body).get("web", {}).get("results", [])[:n]]
                elif prov == "exa":
                    body = json.dumps({"query": q, "numResults": n, "contents": {"text": {"maxCharacters": 500}}}).encode()
                    r = self.client.fetch(f"https://{host}/search", self._provider_policy(host), method="POST",
                                          headers={"x-api-key": key, "Content-Type": "application/json"}, body=body)
                    items = [{"title": x.get("title"), "url": x.get("url"), "snippet": (x.get("text") or "")[:500]}
                             for x in json.loads(r.body).get("results", [])[:n]]
                else:  # tavily
                    body = json.dumps({"query": q, "max_results": n}).encode()
                    r = self.client.fetch(f"https://{host}/search", self._provider_policy(host), method="POST",
                                          headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"}, body=body)
                    items = [{"title": x.get("title"), "url": x.get("url"), "snippet": x.get("content")}
                             for x in json.loads(r.body).get("results", [])[:n]]
        except EgressDenied as e:
            raise ToolFailed(f"search blocked: {e}") from e
        except (ValueError, KeyError) as e:
            raise ToolFailed(f"unexpected search response: {e}") from e
        self.gw.connectors.touch("web")
        text = "\n\n".join(f"{i + 1}. {x['title']}\n   {x['url']}\n   {x['snippet'] or ''}" for i, x in enumerate(items)) or "No results."
        return ToolResult(text, sens, "web", {"results": [{"title": x["title"], "url": x["url"]} for x in items]},
                          bytes_out=len(q.encode()), destination=prov)

    # ------------------------------------------------------------------ fetch
    def fetch(self, args: dict[str, Any], ctx: ExecContext) -> ToolResult:
        url = args["url"]
        sens = int(Sensitivity[self.gw.settings.get("sensitivity.default.web")])
        try:
            r = self.client.fetch(url, self.policy())
        except EgressDenied as e:
            self.gw.audit.write("egress.denied", "egress", destination=urlsplit(url).hostname, code=e.code)
            raise ToolFailed(f"fetch blocked: {e}") from e
        self.gw.audit.write("egress.fetch", "egress", destination=urlsplit(r.final_url).hostname, bytes_in=r.size,
                            status=r.status, redirects=len(r.redirects), ip=r.ip)
        self.gw.connectors.touch("web")
        if r.content_type == "application/pdf":
            f = self.gw.files.ingest(name=(urlsplit(r.final_url).path.rsplit("/", 1)[-1] or "download") + ".pdf"
                                     if not r.final_url.lower().endswith(".pdf") else urlsplit(r.final_url).path.rsplit("/", 1)[-1],
                                     data=r.body, source="web", sensitivity=sens, folder="/Web downloads", run_async=False)
            if f["status"] != "READY":
                raise ToolFailed(f"downloaded PDF was quarantined: {f.get('status_reason')}")
            text, _ = self.gw.files.text(f["id"], args["max_chars"])
            return ToolResult(text, sens, "web", {"url": r.final_url, "file_id": f["id"]}, bytes_out=len(url.encode()),
                              destination=urlsplit(r.final_url).hostname, hidden=[])
        raw = r.body.decode("utf-8", errors="replace")
        if r.content_type in ("text/html", "application/xhtml+xml") or "<html" in raw[:1000].lower():
            parsed = html_to_text(raw, args["max_chars"])
            content = (f"Title: {parsed['title']}\n\n" if parsed["title"] else "") + parsed["text"]
            hidden = parsed["hidden"]
        else:
            content, hidden = raw[:args["max_chars"]], []
        return ToolResult(content, sens, "web", {"url": r.final_url, "status": r.status}, bytes_out=len(url.encode()),
                          destination=urlsplit(r.final_url).hostname, hidden=hidden)
