# Connectors

All connectors are **OFF by default** and have: ON/OFF, *Use in chat*, *Use in missions*, a capability manifest,
last-used time and dependent missions (Settings → Connectors). Effective state is computed on every call
(spec 8.2). Turning a connector off takes effect immediately, suspends dependent missions and blocks
`web.fetch` to its domains.

## Local Outlook (classic) - `outlook_local`
- Requires classic Outlook for Windows (the new Outlook has no Object Model → use Microsoft 365).
- The gateway starts `pa-outlook-worker.exe` on demand (Job Object, no network, exits after 60 s idle).
- Tools: `list_folders`, `search` (DASL on subject/body/sender + date range), `get_message` (plain text),
  `get_attachment` (→ quarantine → My Files), `calendar_read`; optional `create_draft` (AI-marked, never sent,
  always needs approval).
- Settings: start Outlook when needed (off), folder allow/deny lists (Deleted Items, Junk excluded), include PST,
  max items/body/attachment size, date range, highest sensitivity label → metadata-only (default) or excluded.
- Outlook's Object Model Guard is never suppressed: if Outlook shows a prompt, the call waits (90 s) then fails
  with "a security prompt may be waiting for you in Outlook".

## Microsoft 365 / Exchange Online - `m365`
1. Create (or ask IT for) an **Entra ID app registration**: *Mobile and desktop applications* platform, redirect
   URI `http://localhost`, **public client** (no secret). Delegated permissions: `User.Read`, `Mail.Read`,
   `Calendars.Read`, `offline_access` (+ `Mail.ReadWrite` for drafts, `Mail.Send` for sending). Your organisation
   may require admin consent.
2. Settings → Connectors → Microsoft 365 options: enter the **Client ID** and **Tenant ID** (re-authentication).
3. Turn the connector ON → *Sign in with Microsoft* → your browser opens Microsoft's page → approve.
   The gateway listens once on `http://localhost:<random port>/callback` for ≤ 120 s and accepts only the
   matching state + PKCE response.
- Tools: `search_mail`, `get_message`, `get_attachment`, `calendar_read`; optional `create_draft`, `send_mail`
  (always approval; CONFIDENTIAL context needs the password; idempotency header `x-pa-idempotency`).
- Tokens: refresh token in the Secrets vault (binding `m365.refresh_token`); access token in memory.
- Throttling: honours `Retry-After`, circuit breaker after repeated failures (task waits instead of retry storms).
- Disconnect deletes the stored token (optionally also imported data). Microsoft offers no per-app revoke for
  public clients; remove the app's consent at https://myapps.microsoft.com if you want it revoked server-side.

## Web - `web`
- Search providers: Brave Search API, SearXNG (your https URL), Exa, Tavily. Put the API key in **Secrets** and
  bind it to `web.search`. The query is egress: DLP + sensitivity rules apply.
- Fetch: https only (http optional), domain allowlist (or "Fetch any site"), blocklist, SSRF protection with DNS
  pinning and per-redirect re-checks, size/time/content-type limits, no cookies, no JavaScript. HTML → text with
  hidden text/comments/metadata separated and labelled; PDFs go through the file pipeline.

## Adding a connector (e.g. ChiRAG)
See DEVELOPMENT.md → "Add a connector". Future third-party connectors ship as signed packages whose manifest
(destinations, data types, side effects, secrets, tools, package hash) is verified with a pinned Ed25519
publisher key (`pa_gateway/extensions.py`) and enforced at runtime.

### Local Outlook - tools as of 0.1.7
`outlook_local.search` (newest first; scope inbox = Inbox + sub-folders / sent / all; since, until, sender, unread_only, since_minutes; text query matches subject, sender, or the body of the newest 300 messages),
`digest` (recent mail with flags APPROVAL / DEADLINE:date / URGENT / QUESTION, "you are in: To|CC|other", UNREAD, attachments, replied yes/no; filters only_flag, only_flagged, unanswered_only, deadline_within_days, any_of, scan_limit),
`mail_stats` (counts only: total, unread, To vs CC, important, flagged, per day, top senders - table API, seconds for thousands of mails), `awaiting_reply` (sent mail with no reply),
`get_message`, `get_attachment` (-> My Files, scanned), `calendar_read`, `create_draft` (off by default, approval). Flags come from deterministic English patterns in `mailscan.py`.
Measured on a real mailbox (~700 mails/day, 112 folders): stats 6 s (24 h) / 12 s (7 days), digest of 50 mails 7 s, awaiting_reply 8 s.
Email-monitoring skills (Settings > Email monitoring) are routines built on these tools - see `agentdata/email_skills.py`.
