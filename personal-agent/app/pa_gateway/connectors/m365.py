"""Microsoft 365 / Exchange Online via Microsoft Graph (spec 10, 39.1, 39.2).

Auth: public client, authorization code + PKCE in the SYSTEM browser. Redirect:
  - loopback (default): http://localhost:<random port>/callback served by a one-shot listener in the
    gateway that accepts exactly ONE request whose state matches, and closes within 120 s
  - custom scheme personalagent://auth/m365 (forwarded by pa-ui as an RPC) - same state/PKCE checks
A callback is accepted only for a sign-in the user started in the last 10 minutes. Anything else is
discarded and logged. The refresh token lives only in the vault; access tokens only in memory.
Scopes are incremental: User.Read Mail.Read Calendars.Read offline_access (+Mail.ReadWrite for drafts,
+Mail.Send for approved sending).
"""
from __future__ import annotations

import base64
import hashlib
import html
import secrets
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any
from urllib.parse import parse_qs, urlencode, urlsplit

import httpx

from pa_common.errors import PAError
from pa_common.sensitivity import Sensitivity

from ..tools.base import ExecContext, ToolFailed, ToolResult, ToolUnavailable
from .service import ConnectorAdapter

GRAPH = "https://graph.microsoft.com/v1.0"
LOGIN = "https://login.microsoftonline.com"
BASE_SCOPES = ["User.Read", "Mail.Read", "Calendars.Read", "offline_access"]
AI_DRAFT_NOTE = "[AI Draft - created by Personal Agent. Review before sending.]"
CALLBACK_TTL = 600
LISTENER_TTL = 120


class M365Connector(ConnectorAdapter):
    id = "m365"
    label = "Microsoft 365 / Exchange Online"
    description = "Read and search your Microsoft 365 mail and calendar through Microsoft Graph."
    manifest = {"destinations": ["graph.microsoft.com", "login.microsoftonline.com"], "data_types": ["mail", "calendar"],
                "side_effects": ["read", "drafts (optional)", "send with approval (optional)"], "secrets": ["refresh token"]}

    def __init__(self, gw):
        super().__init__(gw)
        self._access: tuple[str, float] | None = None
        self._pending: dict[str, dict[str, Any]] = {}
        self._lock = threading.RLock()
        self._breaker_until = 0.0
        self._failures = 0

    # ------------------------------------------------------------------ state
    def scopes(self) -> list[str]:
        s = self.gw.settings
        out = list(BASE_SCOPES)
        if s.get("m365.enable_drafts") or s.get("m365.enable_send"):
            out.append("Mail.ReadWrite")
        if s.get("m365.enable_send"):
            out.append("Mail.Send")
        return out

    def connection_valid(self) -> tuple[bool, str]:
        if not self.gw.settings.get("m365.client_id"):
            return False, "Microsoft 365 is not set up (enter the app's client ID in Settings > Connectors)"
        if not self.gw.secrets.has_binding("m365.refresh_token"):
            return False, "Microsoft 365 is not connected (sign in from Settings > Connectors)"
        if time.time() < self._breaker_until:
            return False, "Microsoft 365 is temporarily unavailable (throttled or down); retrying later"
        return True, ""

    def status(self) -> dict[str, Any]:
        ok, reason = self.connection_valid()
        granted = self.gw.db.one("SELECT scopes_json FROM connectors WHERE id='m365'")
        return {"connection_ok": ok, "connection_reason": reason, "requested_scopes": self.scopes(),
                "granted_scopes_plain": [SCOPE_TEXT.get(s, s) for s in self.scopes()], "granted": granted["scopes_json"] if granted else "[]"}

    # ------------------------------------------------------------------ sign-in (system browser + PKCE)
    def begin_sign_in(self, use_custom_scheme: bool = False) -> dict[str, Any]:
        s = self.gw.settings
        client_id, tenant = s.get("m365.client_id"), s.get("m365.tenant_id") or "organizations"
        if not client_id:
            raise PAError("enter the Microsoft 365 client ID first", code="not_configured")
        verifier = base64.urlsafe_b64encode(secrets.token_bytes(48)).rstrip(b"=").decode()
        challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b"=").decode()
        state = secrets.token_urlsafe(24)
        if use_custom_scheme:
            redirect = "personalagent://auth/m365"
        else:
            port = self._start_listener(state)
            redirect = f"http://localhost:{port}/callback"
        with self._lock:
            self._pending = {state: {"verifier": verifier, "redirect": redirect, "started": time.time(), "tenant": tenant,
                                     "client_id": client_id, "scopes": self.scopes()}}
        url = f"{LOGIN}/{tenant}/oauth2/v2.0/authorize?" + urlencode({
            "client_id": client_id, "response_type": "code", "redirect_uri": redirect, "response_mode": "query",
            "scope": " ".join(self.scopes()), "state": state, "code_challenge": challenge, "code_challenge_method": "S256",
            "prompt": "select_account"})
        self.gw.audit.write("connector.sign_in_started", "connector", connector="m365", redirect_kind="scheme" if use_custom_scheme else "loopback")
        return {"authorize_url": url, "redirect_uri": redirect}

    def _start_listener(self, state: str) -> int:
        connector = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802
                parts = urlsplit(self.path)
                ok = False
                msg = "Sign-in request not recognised. You can close this window."
                if parts.path == "/callback":
                    try:
                        connector.complete_sign_in(parts.query)
                        ok = True
                        msg = "Microsoft 365 is connected. You can close this window and return to Personal Agent."
                    except PAError as e:
                        msg = f"Sign-in failed: {html.escape(str(e))}"
                body = f"<!doctype html><meta charset=utf-8><title>Personal Agent</title><p>{msg}</p>".encode()
                self.send_response(200 if ok else 400)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Security-Policy", "default-src 'none'")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                threading.Thread(target=server.shutdown, daemon=True).start()

            def log_message(self, *a):  # no request logging (would contain the auth code)
                pass

        server = HTTPServer(("127.0.0.1", 0), Handler)
        server.timeout = 1
        port = server.server_address[1]

        def serve():
            end = time.time() + LISTENER_TTL
            server.socket.settimeout(1)
            while time.time() < end:
                try:
                    server.handle_request()
                except (OSError, socket.timeout):
                    pass
                with self._lock:
                    if state not in self._pending:
                        break
            server.server_close()

        threading.Thread(target=serve, daemon=True, name="m365-callback").start()
        return port

    def complete_sign_in(self, query_or_url: str) -> None:
        q = parse_qs(urlsplit(query_or_url).query if "://" in query_or_url else query_or_url)
        state = (q.get("state") or [""])[0]
        with self._lock:
            pending = self._pending.pop(state, None)
        if not pending or time.time() - pending["started"] > CALLBACK_TTL:
            self.gw.audit.write("connector.callback_rejected", "security", connector="m365", severity="medium")
            raise PAError("unexpected or expired sign-in response (discarded)", code="oauth_state_mismatch")
        if q.get("error"):
            raise PAError(f"Microsoft sign-in error: {(q.get('error_description') or q['error'])[0][:300]}", code="oauth_error")
        code = (q.get("code") or [""])[0]
        r = self._post_token(f"{LOGIN}/{pending['tenant']}/oauth2/v2.0/token", {
            "client_id": pending["client_id"], "grant_type": "authorization_code", "code": code,
            "redirect_uri": pending["redirect"], "code_verifier": pending["verifier"], "scope": " ".join(pending["scopes"])}, "Microsoft sign-in")
        if r.status_code != 200:
            raise PAError(f"token exchange failed: {r.json().get('error_description', r.text)[:300]}", code="oauth_error")
        tok = r.json()
        self._store_tokens(tok)
        self.gw.db.update("connectors", "id", "m365", {"connected": 1, "scopes_json": tok.get("scope", "").split()})
        self.gw.audit.write("connector.connected", "connector", connector="m365", scopes=tok.get("scope", "").split())
        self.gw.emit("connectors.changed", {"connector": "m365"})

    def _store_tokens(self, tok: dict[str, Any]) -> None:
        if tok.get("refresh_token"):
            self.gw.secrets.upsert_bound("m365.refresh_token", "Microsoft 365 refresh token", tok["refresh_token"], "token")
        self._access = (tok["access_token"], time.time() + int(tok.get("expires_in", 3600)) - 120)

    def _token(self) -> str:
        if self._access and self._access[1] > time.time():
            return self._access[0]
        rt = self.gw.secrets.value_for_binding("m365.refresh_token")
        if not rt:
            raise ToolUnavailable("Microsoft 365 is not connected")
        s = self.gw.settings
        r = self._post_token(f"{LOGIN}/{s.get('m365.tenant_id') or 'organizations'}/oauth2/v2.0/token", {
            "client_id": s.get("m365.client_id"), "grant_type": "refresh_token", "refresh_token": rt,
            "scope": " ".join(self.scopes())}, "Microsoft token refresh")
        if r.status_code != 200:
            err = r.json().get("error_description", r.text)[:300] if r.headers.get("content-type", "").startswith("application/json") else r.text[:300]
            raise ToolUnavailable(f"Microsoft 365 token refresh failed - sign in again ({err})")
        self._store_tokens(r.json())
        return self._access[0]  # type: ignore[index]

    def disconnect(self) -> None:
        self._access = None
        self.gw.secrets.delete_bound("m365.refresh_token")

    # ------------------------------------------------------------------ Graph helper with throttling/circuit breaker
    def _post_token(self, url: str, data: dict[str, Any], purpose: str) -> httpx.Response:
        t0 = time.time()
        try:
            r = httpx.post(url, data=data, timeout=30)
        except httpx.HTTPError as e:
            self.gw.netlog.record("m365", "POST", url, None, outcome="error", reason=type(e).__name__, purpose=purpose,
                                  duration_ms=int((time.time() - t0) * 1000))
            raise
        self.gw.netlog.record("m365", "POST", url, r.status_code, purpose=purpose, bytes_in=len(r.content),
                              duration_ms=int((time.time() - t0) * 1000))
        return r

    def _graph(self, method: str, path: str, **kw: Any) -> httpx.Response:
        if time.time() < self._breaker_until:
            raise ToolUnavailable("Microsoft 365 temporarily unavailable")
        headers = {"Authorization": f"Bearer {self._token()}", **kw.pop("headers", {})}
        for attempt in range(4):
            t0 = time.time()
            try:
                r = httpx.request(method, GRAPH + path, headers=headers, timeout=30, **kw)
            except httpx.HTTPError as e:
                self.gw.netlog.record("m365", method, GRAPH + path.split("?")[0], None, outcome="error", reason=type(e).__name__,
                                      purpose="Microsoft Graph", duration_ms=int((time.time() - t0) * 1000))
                self._trip()
                raise ToolUnavailable(f"cannot reach Microsoft Graph: {e}") from e
            self.gw.netlog.record("m365", method, GRAPH + path.split("?")[0], r.status_code, purpose="Microsoft Graph",
                                  bytes_out=len(kw.get("content") or b"") if isinstance(kw.get("content"), (bytes, bytearray)) else 0,
                                  bytes_in=len(r.content), duration_ms=int((time.time() - t0) * 1000))
            if r.status_code == 429 or r.status_code >= 500:
                wait = min(float(r.headers.get("Retry-After", 2 ** attempt)), 30)
                time.sleep(wait)
                continue
            self._failures = 0
            if r.status_code == 401:
                self._access = None
                raise ToolUnavailable("Microsoft 365 session expired - sign in again")
            if r.status_code >= 400:
                raise ToolFailed(f"Microsoft Graph error {r.status_code}: {r.text[:300]}")
            self.gw.connectors.touch("m365")
            return r
        self._trip()
        raise ToolUnavailable("Microsoft Graph is throttling or unavailable")

    def _trip(self) -> None:
        self._failures += 1
        if self._failures >= 3:
            self._breaker_until = time.time() + 300

    def _sens(self, item_sensitivity: str | None) -> int:
        base = Sensitivity[self.gw.settings.get("sensitivity.default.mail")]
        if item_sensitivity in ("confidential", "private"):
            base = Sensitivity.max(base, Sensitivity.CONFIDENTIAL)
        return int(base)

    # ------------------------------------------------------------------ tools
    def register_tools(self, tg) -> None:
        tg.register("m365.search_mail", self.search_mail)
        tg.register("m365.get_message", self.get_message)
        tg.register("m365.get_attachment", self.get_attachment)
        tg.register("m365.calendar_read", self.calendar_read)
        tg.register("m365.create_draft", self.create_draft)
        tg.register("m365.send_mail", self.send_mail)

    def search_mail(self, args: dict[str, Any], ctx: ExecContext) -> ToolResult:
        sel = "id,subject,from,receivedDateTime,bodyPreview,hasAttachments,sensitivity,importance"
        base = f"/me/mailFolders/{args['folder']}/messages" if args.get("folder") else "/me/messages"
        params: dict[str, Any] = {"$top": args["max_results"], "$select": sel}
        headers = {}
        if args.get("query"):
            params["$search"] = f"\"{args['query'].replace(chr(34), '')}\""
            headers["ConsistencyLevel"] = "eventual"
        else:
            filt = []
            if args.get("since"):
                filt.append(f"receivedDateTime ge {args['since']}")
            if args.get("until"):
                filt.append(f"receivedDateTime le {args['until']}")
            if filt:
                params["$filter"] = " and ".join(filt)
            params["$orderby"] = "receivedDateTime desc"
        items = self._graph("GET", base, params=params, headers=headers).json().get("value", [])
        sens = max([self._sens(i.get("sensitivity")) for i in items] or [self._sens(None)])
        lines = [f"- id={i['id']}\n  {i.get('receivedDateTime', '')} | from {((i.get('from') or {}).get('emailAddress') or {}).get('address', '?')}"
                 f"\n  subject: {i.get('subject', '')}\n  preview: {(i.get('bodyPreview') or '')[:200]}" for i in items]
        return ToolResult("\n".join(lines) or "No messages found.", sens, "m365", {"count": len(items)})

    def get_message(self, args: dict[str, Any], ctx: ExecContext) -> ToolResult:
        m = self._graph("GET", f"/me/messages/{args['message_id']}",
                        params={"$select": "id,subject,from,toRecipients,ccRecipients,receivedDateTime,body,sensitivity,hasAttachments"},
                        headers={"Prefer": 'outlook.body-content-type="text"'}).json()
        atts = []
        if m.get("hasAttachments"):
            atts = self._graph("GET", f"/me/messages/{args['message_id']}/attachments",
                               params={"$select": "id,name,size,contentType"}).json().get("value", [])

        def addrs(x):
            return ", ".join((r.get("emailAddress") or {}).get("address", "") for r in (x or []))
        body = (m.get("body") or {}).get("content", "")[:int(self.gw.settings.get("outlook.max_body_kb")) * 1024]
        text = (f"From: {((m.get('from') or {}).get('emailAddress') or {}).get('address', '')}\nTo: {addrs(m.get('toRecipients'))}\n"
                f"Cc: {addrs(m.get('ccRecipients'))}\nDate: {m.get('receivedDateTime', '')}\nSubject: {m.get('subject', '')}\n\n{body}")
        if atts:
            text += "\n\nAttachments:\n" + "\n".join(f"- id={a['id']} {a.get('name')} ({a.get('size')} bytes)" for a in atts)
        return ToolResult(text, self._sens(m.get("sensitivity")), "m365", {"message_id": m.get("id"), "attachments": len(atts)})

    def get_attachment(self, args: dict[str, Any], ctx: ExecContext) -> ToolResult:
        a = self._graph("GET", f"/me/messages/{args['message_id']}/attachments/{args['attachment_id']}").json()
        if a.get("@odata.type") != "#microsoft.graph.fileAttachment":
            raise ToolFailed("only file attachments can be copied")
        data = base64.b64decode(a.get("contentBytes", ""))
        if len(data) > int(self.gw.settings.get("outlook.max_attachment_mb")) * 1024 * 1024:
            raise ToolFailed("attachment is larger than the allowed maximum")
        f = self.gw.files.ingest(name=a.get("name") or "attachment", data=data, source="m365", sensitivity=self._sens(None),
                                 folder="/Mail attachments", run_async=False)
        return ToolResult(f"Attachment saved to My Files as {f['id']} ({f['name']}); status: {f['status']}"
                          + (f" - {f.get('status_reason')}" if f.get("status_reason") else ""), self._sens(None), "m365",
                          {"file_id": f["id"], "status": f["status"]})

    def calendar_read(self, args: dict[str, Any], ctx: ExecContext) -> ToolResult:
        ev = self._graph("GET", "/me/calendarView", params={"startDateTime": args["start"], "endDateTime": args["end"],
                                                           "$top": args["max_results"], "$orderby": "start/dateTime",
                                                           "$select": "subject,start,end,location,organizer,sensitivity,isAllDay"}).json().get("value", [])
        lines = [f"- {e['start']['dateTime']} -> {e['end']['dateTime']} | {e.get('subject', '')} | "
                 f"{(e.get('location') or {}).get('displayName', '')}" for e in ev]
        return ToolResult("\n".join(lines) or "No events.", max([self._sens(e.get("sensitivity")) for e in ev] or [self._sens(None)]),
                          "m365", {"count": len(ev)})

    def _message(self, args: dict[str, Any], idem: str | None) -> dict[str, Any]:
        msg: dict[str, Any] = {
            "subject": args["subject"], "body": {"contentType": "Text", "content": f"{AI_DRAFT_NOTE}\n\n{args['body']}"},
            "toRecipients": [{"emailAddress": {"address": a}} for a in args["to"]],
            "ccRecipients": [{"emailAddress": {"address": a}} for a in args.get("cc", [])], "categories": ["AI Draft"]}
        if idem:
            msg["internetMessageHeaders"] = [{"name": "x-pa-idempotency", "value": idem}]
        return msg

    def create_draft(self, args: dict[str, Any], ctx: ExecContext) -> ToolResult:
        if not ctx.approved:
            raise ToolFailed("drafts require approval")
        r = self._graph("POST", "/me/messages", json=self._message(args, ctx.task.get("idempotency_key"))).json()
        return ToolResult(f"Draft created (AI-marked) with subject '{args['subject']}'.", 0, "m365", {"draft_id": r.get("id")},
                          bytes_out=len(args["body"].encode()), destination=",".join(args["to"]))

    def send_mail(self, args: dict[str, Any], ctx: ExecContext) -> ToolResult:
        if not ctx.approved:
            raise ToolFailed("sending always requires approval")
        msg = self._message(args, ctx.task.get("idempotency_key"))
        msg["body"]["content"] = args["body"]  # the approved exact body is sent; the AI note is only for drafts
        msg.pop("categories", None)
        self._graph("POST", "/me/sendMail", json={"message": msg, "saveToSentItems": True})
        return ToolResult(f"Email sent to {', '.join(args['to'])}.", 0, "m365", {"sent": True},
                          bytes_out=len(args["body"].encode()), destination=",".join(args["to"]))

    def check_sent(self, idem: str) -> bool:
        """After a crash: look for the idempotency header in recent Sent Items (spec 17.3)."""
        items = self._graph("GET", "/me/mailFolders/SentItems/messages",
                            params={"$top": 50, "$select": "id,internetMessageHeaders"}).json().get("value", [])
        return any(h.get("name", "").lower() == "x-pa-idempotency" and h.get("value") == idem
                   for i in items for h in (i.get("internetMessageHeaders") or []))


SCOPE_TEXT = {
    "User.Read": "Read your basic profile (name, email address)",
    "Mail.Read": "Read your mail",
    "Calendars.Read": "Read your calendar",
    "offline_access": "Stay signed in (refresh token stored in your vault)",
    "Mail.ReadWrite": "Create drafts in your mailbox (marked 'AI Draft')",
    "Mail.Send": "Send mail you approve",
}
