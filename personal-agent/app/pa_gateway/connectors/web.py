"""Web search + fetch connector (spec 11). All traffic through the egress filter in this process."""
from __future__ import annotations

import json
import time
from datetime import date, timedelta
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
        self.client.on_event = lambda i: gw.netlog.record("web", i["method"], i["url"], i.get("status"), outcome=i.get("outcome"),
                                                         reason=i.get("reason", ""), bytes_out=i.get("bytes_out", 0), bytes_in=i.get("bytes_in", 0),
                                                         duration_ms=i.get("duration_ms", 0), ip=i.get("ip"))

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
        tg.register("web.read", self.read)
        tg.register("web.answer", self.answer)
        tg.register("web.research", self.research)

    def check_targets(self, urls: list[str]) -> tuple[list[str], list[str]]:
        """Split URLs into allowed / refused by the same domain rules as web.fetch (no DNS: a provider fetches them)."""
        ok, refused = [], []
        pol = self.policy()
        for u in urls:
            try:
                self.client.check_target(u, pol)
                ok.append(u)
            except EgressDenied as e:
                refused.append(f"{u} ({e})")
        return ok, refused

    # ------------------------------------------------------------------ Exa (contents / answer / research)
    def _exa_key(self) -> str:
        if self.gw.settings.get("web.provider") != "exa":
            raise ToolFailed("this needs Exa as the search provider (Settings > Web Access > Search provider: exa)")
        key = self.gw.secrets.value_for_binding("web.search")
        if not key:
            raise ToolFailed("no API key is bound to web.search (Secrets > Used by)")
        return key

    def _exa(self, path: str, body: dict[str, Any] | None = None, method: str = "POST", max_mb: int = 8) -> dict[str, Any]:
        key = self._exa_key()
        host = PROVIDER_HOSTS["exa"][0]
        pol = self._provider_policy(host)
        pol.max_bytes = max_mb * 1024 * 1024
        try:
            r = self.client.fetch(f"https://{host}{path}", pol, method=method,
                                  headers={"x-api-key": key, "Content-Type": "application/json", "Accept": "application/json"},
                                  body=json.dumps(body).encode() if body is not None else None)
        except EgressDenied as e:
            raise ToolFailed(f"Exa request blocked: {e}") from e
        if r.status >= 400:
            raise ToolFailed(f"Exa returned HTTP {r.status}")
        try:
            data = json.loads(r.body)
        except ValueError as e:
            raise ToolFailed("Exa returned an unreadable answer") from e
        cost = (data.get("costDollars") or {}).get("total")
        self.gw.audit.write("web.exa_call", "egress", endpoint=path.split("?")[0][:60], cost_usd=cost)
        return data

    def read(self, args: dict[str, Any], ctx: ExecContext) -> ToolResult:
        """Read (crawl) whole pages - optionally live, optionally with a site's subpages. Exa crawls from its servers; other providers
        fall back to fetching each page from this PC through the egress filter."""
        sens = int(Sensitivity[self.gw.settings.get("sensitivity.default.web")])
        urls, refused = self.check_targets(list(dict.fromkeys(args["urls"])))
        if not urls:
            raise ToolFailed("none of these addresses may be read: " + "; ".join(refused))
        per = int(args["max_chars"])
        if self.gw.settings.get("web.provider") != "exa":
            parts, used = [], []
            for u in urls[:5]:
                try:
                    r = self.fetch({"url": u, "max_chars": per}, ctx)
                    parts.append(f"## {u}\n{r.content}")
                    used.append(u)
                except ToolFailed as e:
                    parts.append(f"## {u}\n(could not read: {e})")
            note = ("\n\nNot read (rules in Settings > Web Access): " + "; ".join(refused)) if refused else ""
            return ToolResult("\n\n".join(parts) + note, sens, "web", {"urls": used, "mode": "fetch"},
                              bytes_out=sum(len(u) for u in urls), destination=",".join(sorted({urlsplit(u).hostname or "" for u in used})))
        body: dict[str, Any] = {"urls": urls, "text": {"maxCharacters": per}}
        if args.get("subpages"):
            body["subpages"] = int(args["subpages"])
            if args.get("subpage_target"):
                body["subpageTarget"] = list(args["subpage_target"])
        if args.get("live"):
            body["maxAgeHours"] = 0
            body["livecrawlTimeout"] = 15000
        if args.get("focus"):
            body["highlights"] = {"query": args["focus"], "numSentences": 3, "highlightsPerUrl": 3}
        data = self._exa("/contents", body)
        pol = self.policy()
        out, kept = [], []

        def page(x: dict[str, Any], level: str = "##") -> None:
            u = str(x.get("url") or "")
            try:
                self.client.check_target(u, pol)          # subpages must obey the same rules as the pages asked for
            except EgressDenied:
                return
            kept.append(u)
            head = f"{level} {x.get('title') or u}\nURL: {u}" + (f"\nPublished: {x['publishedDate'][:10]}" if x.get("publishedDate") else "")
            hl = "".join(f"\n> {h}" for h in (x.get("highlights") or [])[:3])
            out.append(f"{head}{hl}\n{(x.get('text') or '')[:per]}")
            for sp in (x.get("subpages") or [])[:10]:
                page(sp, "###")

        for x in data.get("results", []):
            page(x)
        errors = [f"{s.get('id')}: {(s.get('error') or {}).get('tag', 'error')}" for s in data.get("statuses", []) if s.get("status") == "error"]
        tail = ""
        if errors:
            tail += "\n\nCould not be read: " + "; ".join(errors)
        if refused:
            tail += "\n\nNot read (rules in Settings > Web Access): " + "; ".join(refused)
        self.gw.connectors.touch("web")
        return ToolResult(("\n\n".join(out) or "Nothing could be read.") + tail, sens, "web", {"urls": kept, "mode": "exa", "live": bool(args.get("live"))},
                          bytes_out=sum(len(u) for u in urls), destination="exa")

    def answer(self, args: dict[str, Any], ctx: ExecContext) -> ToolResult:
        """A short factual answer with citations (Exa /answer)."""
        sens = int(Sensitivity[self.gw.settings.get("sensitivity.default.web")])
        data = self._exa("/answer", {"query": args["query"], "text": False})
        ans = data.get("answer")
        ans = json.dumps(ans, ensure_ascii=False) if isinstance(ans, (dict, list)) else str(ans or "")
        cites = [c for c in data.get("citations", []) if c.get("url")]
        text = ans + ("\n\nSources:\n" + "\n".join(f"- {c.get('title') or c['url']} - {c['url']}" + (f" ({c['publishedDate'][:10]})" if c.get("publishedDate") else "")
                                                     for c in cites[:10]) if cites else "")
        self.gw.connectors.touch("web")
        return ToolResult(text, sens, "web", {"citations": [c["url"] for c in cites[:10]]}, bytes_out=len(args["query"].encode()), destination="exa")

    def research(self, args: dict[str, Any], ctx: ExecContext) -> ToolResult:
        """A long investigation done by Exa's research agent (many searches + reading), polled until it finishes."""
        sens = int(Sensitivity[self.gw.settings.get("sensitivity.default.web")])
        model = "exa-research-pro" if args.get("thorough") else "exa-research"
        task = self._exa("/research/v0/tasks", {"instructions": args["instructions"], "model": model})
        tid = str(task.get("id") or "")
        if not tid or not tid.replace("-", "").replace("_", "").isalnum():
            raise ToolFailed("Exa did not start the research task")
        deadline = time.monotonic() + int(args["max_minutes"]) * 60
        while True:
            if self.gw.killswitch.active("stop_all") or self.gw.killswitch.agent_blocked():
                raise ToolFailed("stopped (emergency stop / agent paused)")
            data = self._exa(f"/research/v0/tasks/{tid}", None, method="GET")
            st = data.get("status")
            if st == "completed":
                break
            if st == "failed":
                raise ToolFailed("Exa research failed")
            if time.monotonic() > deadline:
                raise ToolFailed(f"the research did not finish within {args['max_minutes']} minutes (it may still finish on Exa's side)")
            time.sleep(5)
        result = data.get("data")
        if isinstance(result, dict) and len(result) == 1 and isinstance(next(iter(result.values())), str):
            result = next(iter(result.values()))
        text = result if isinstance(result, str) else json.dumps(result, ensure_ascii=False, indent=1)
        cites = []
        raw_c = data.get("citations") or {}
        for v in (raw_c.values() if isinstance(raw_c, dict) else [raw_c]):
            for c in (v if isinstance(v, list) else []):
                if isinstance(c, dict) and c.get("url") and c["url"] not in cites:
                    cites.append(c["url"])
        if cites:
            text += "\n\nSources:\n" + "\n".join(f"- {u}" for u in cites[:30])
        self.gw.connectors.touch("web")
        return ToolResult(text[:60_000], sens, "web", {"citations": cites[:30], "model": model}, bytes_out=len(args["instructions"].encode()), destination="exa")

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
                    extra = ""
                    if args.get("recent_days"):
                        d = int(args["recent_days"])
                        extra += "&freshness=" + ("pd" if d <= 1 else "pw" if d <= 7 else "pm" if d <= 31 else "py")
                    if args.get("country"):
                        extra += f"&country={quote_plus(args['country'].lower())}"
                    r = self.client.fetch(f"https://{host}/res/v1/web/search?q={quote_plus(q)}&count={n}{extra}",
                                          self._provider_policy(host), headers={"X-Subscription-Token": key, "Accept": "application/json"})
                    items = [{"title": x.get("title"), "url": x.get("url"), "snippet": x.get("description")}
                             for x in json.loads(r.body).get("web", {}).get("results", [])[:n]]
                elif prov == "exa":
                    eb: dict[str, Any] = {"query": q, "numResults": n, "type": args.get("mode") or "auto",
                                          "contents": {"text": {"maxCharacters": 1500 if args.get("longer") else 500}}}
                    if args.get("category"):
                        eb["category"] = args["category"]
                    if args.get("recent_days"):
                        eb["startPublishedDate"] = (date.today() - timedelta(days=int(args["recent_days"]))).isoformat() + "T00:00:00.000Z"
                    if args.get("include_domains"):
                        eb["includeDomains"] = list(args["include_domains"])
                    if args.get("exclude_domains"):
                        eb["excludeDomains"] = list(args["exclude_domains"])
                    if args.get("country"):
                        eb["userLocation"] = args["country"]
                    body = json.dumps(eb).encode()
                    r = self.client.fetch(f"https://{host}/search", self._provider_policy(host), method="POST",
                                          headers={"x-api-key": key, "Content-Type": "application/json"}, body=body)
                    items = [{"title": x.get("title"), "url": x.get("url"), "snippet": (x.get("text") or "")[:1500 if args.get("longer") else 500],
                              "date": (x.get("publishedDate") or "")[:10]} for x in json.loads(r.body).get("results", [])[:n]]
                else:  # tavily
                    tb: dict[str, Any] = {"query": q, "max_results": n}
                    if args.get("include_domains"):
                        tb["include_domains"] = list(args["include_domains"])
                    if args.get("exclude_domains"):
                        tb["exclude_domains"] = list(args["exclude_domains"])
                    if args.get("recent_days") or args.get("category") == "news":
                        tb["topic"] = "news"
                        tb["days"] = int(args.get("recent_days") or 7)
                    body = json.dumps(tb).encode()
                    r = self.client.fetch(f"https://{host}/search", self._provider_policy(host), method="POST",
                                          headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"}, body=body)
                    items = [{"title": x.get("title"), "url": x.get("url"), "snippet": x.get("content")}
                             for x in json.loads(r.body).get("results", [])[:n]]
        except EgressDenied as e:
            raise ToolFailed(f"search blocked: {e}") from e
        except (ValueError, KeyError) as e:
            raise ToolFailed(f"unexpected search response: {e}") from e
        self.gw.connectors.touch("web")
        text = "\n\n".join(f"{i + 1}. {x['title']}\n   {x['url']}" + (f"  ({x['date']})" if x.get("date") else "") + f"\n   {x['snippet'] or ''}"
                           for i, x in enumerate(items)) or "No results."
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
