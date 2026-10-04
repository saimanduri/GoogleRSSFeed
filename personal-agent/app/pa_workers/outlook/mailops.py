"""Mail operations of the Outlook worker: search, digest, mail_stats, awaiting_reply (read-only).

Shared helpers (_folders, _walk, _label_ok ...) live in __main__; they are imported lazily because __main__
registers these operations. Dates arriving from the model may carry a Z / offset: they are converted to local time.

Outlook parses date literals in filters with the user's REGIONAL settings (on an Indian PC '10/01/2026' means
10 January). ol_date() therefore formats dates with the Windows short-date pattern. Filtering by date uses the
Jet syntax ([ReceivedTime] >= '...'); text filters run in Python (the server-side body search takes minutes).
"""
from __future__ import annotations

import re
import sys
from datetime import datetime, timedelta

from . import mailscan

_COLS = ("EntryID", "ReceivedTime", "SenderName", "Subject", "To", "CC", "Importance", "UnRead", "MessageClass", "Sensitivity", "ConversationIndex", "ConversationTopic")
BODY_SCAN_CAP = 300       # messages whose body is read to answer a text query / flag scan


def _b():
    from . import __main__ as base  # noqa: PLC0415
    return base


def parse_dt(value, default=None):
    """ISO text -> naive LOCAL datetime. Text ending in Z / with an offset is converted from that zone."""
    if not value:
        return default
    d = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
    if d.tzinfo is not None:
        d = d.astimezone().replace(tzinfo=None)
    return d


def _short_date_pattern() -> str:
    if sys.platform != "win32":
        return "M/d/yyyy"
    try:
        import ctypes  # noqa: PLC0415
        buf = ctypes.create_unicode_buffer(80)
        ctypes.windll.kernel32.GetLocaleInfoEx(None, 0x1F, buf, 80)  # LOCALE_SSHORTDATE
        return buf.value or "M/d/yyyy"
    except Exception:  # noqa: BLE001
        return "M/d/yyyy"


def strftime_pattern(win_pattern: str) -> str:
    """Windows date pattern (dd-MM-yyyy, M/d/yyyy, yyyy/MM/dd ...) -> strftime pattern."""
    out = []
    for tok in re.findall(r"y+|M+|d+|[^yMd]+", win_pattern):
        if tok[0] == "y":
            out.append("%Y" if len(tok) >= 3 else "%y")
        elif tok[0] == "M":
            out.append("%m")
        elif tok[0] == "d":
            out.append("%d")
        else:
            out.append(tok)
    return "".join(out)


def ol_date(d: datetime) -> str:
    """Date-time text Outlook understands in a Jet filter on this PC."""
    return d.strftime(strftime_pattern(_short_date_pattern()) + " %I:%M %p")


def conv_key(index, topic) -> str:
    """Stable id of a mail thread: the 22-byte root of Outlook's ConversationIndex (44 hex chars), else the normalised topic."""
    idx = str(index or "")
    return idx[:44] if len(idx) >= 44 else "T:" + str(topic or "").strip().lower()


def _naive(t):
    try:
        return datetime(t.year, t.month, t.day, t.hour, t.minute, t.second)
    except Exception:  # noqa: BLE001
        return datetime.min


def _fmt(d):
    return d.strftime("%Y-%m-%d %H:%M")


def _me(app):
    """Lower-case names/addresses that identify the user (to tell 'in To' from 'in CC')."""
    names = set()
    try:
        ns = app.GetNamespace("MAPI")
        cu = ns.CurrentUser
        names.add(str(cu.Name).lower())
        try:
            names.add(str(cu.AddressEntry.GetExchangeUser().PrimarySmtpAddress).lower())
        except Exception:  # noqa: BLE001
            pass
        for acc in ns.Accounts:
            names.add(str(acc.SmtpAddress).lower())
            names.add(str(acc.DisplayName).lower())
    except Exception:  # noqa: BLE001
        pass
    return {n for n in names if len(n) > 3 and n != "none"}


def _role_text(to, cc, me):
    to, cc = str(to or "").lower(), str(cc or "").lower()
    if any(n in to for n in me):
        return "To"
    if any(n in cc for n in me):
        return "CC"
    return "other"


def _scope_folders(app, cfg, scope):
    """inbox = Inbox and its sub-folders (rules often file mail there); sent = Sent Items; all = everything allowed."""
    base = _b()
    if scope == "all":
        return base._folders(app, cfg)
    ns = app.GetNamespace("MAPI")
    root = ns.GetDefaultFolder(6 if scope != "sent" else 5)
    out = []
    try:
        prefix = "/" + str(root.Parent.Name)
    except Exception:  # noqa: BLE001
        prefix = ""
    base._walk(root, prefix, out)
    return [(pth, f) for pth, f in out if base._folder_allowed(pth, cfg)]


def _jet(prop, since, until=None, unread_only=False):
    filt = f"[{prop}] >= '{ol_date(since)}'"
    if until:
        filt += f" AND [{prop}] <= '{ol_date(until)}'"
    if unread_only:
        filt += " AND [UnRead] = True"
    return filt


def window(cfg, p):
    now = datetime.now()
    floor = now - timedelta(days=cfg["date_range_days"])
    if p.get("since_minutes"):
        since = now - timedelta(minutes=int(p["since_minutes"]))
    else:
        since = parse_dt(p.get("since"), floor)
    return max(since, floor), parse_dt(p.get("until"))


def _table(folder, filt, columns):
    tbl = folder.GetTable(filt, 0)
    tbl.Columns.RemoveAll()
    have = []
    for c in columns:
        try:
            tbl.Columns.Add(c)
            have.append(c)
        except Exception:  # noqa: BLE001
            pass
    return tbl, have


def _table_rows(app, cfg, p):
    """Metadata of every matching message (fast table API, no item objects), newest first."""
    since, until = window(cfg, p)
    filt = _jet("ReceivedTime", since, until, bool(p.get("unread_only")))
    me = _me(app)
    snd = (p.get("sender") or "").lower()
    rows, cap = [], 20000
    for path, folder in _scope_folders(app, cfg, p.get("scope") or "inbox"):
        if p.get("folder") and p["folder"].lower() not in path.lower():
            continue
        try:
            tbl, have = _table(folder, filt, _COLS)
        except Exception:  # noqa: BLE001
            continue
        while not tbl.EndOfTable and len(rows) < cap:
            row = tbl.GetNextRow()
            vals = {c: row[c] for c in have}
            if not str(vals.get("MessageClass") or "").startswith("IPM.Note"):
                continue
            who = str(vals.get("SenderName") or "")
            if snd and snd not in who.lower():
                continue
            rows.append({"id": vals.get("EntryID"), "received": _naive(vals.get("ReceivedTime")), "folder": path, "from": who,
                         "subject": str(vals.get("Subject") or ""), "role": _role_text(vals.get("To"), vals.get("CC"), me),
                         "unread": bool(vals.get("UnRead")), "importance": int(vals.get("Importance") or 1),
                         "sens": int(vals.get("Sensitivity") or 0), "conv": conv_key(vals.get("ConversationIndex"), vals.get("ConversationTopic"))})
    rows.sort(key=lambda r: r["received"], reverse=True)
    return rows, len(rows)


def _rows(app, cfg, p, need_body):
    """Newest-first mail as plain dicts. Bodies are read only for rows that may be kept. Returns (rows, max_sens, matching)."""
    limit = min(max(int(p.get("max_results", 10)), int(p.get("scan_limit", 0))), BODY_SCAN_CAP)
    if not p.get("scan_limit"):
        limit = min(limit, cfg["max_items"])
    q = (p.get("query") or "").strip().lower()
    meta, matching = _table_rows(app, cfg, p)
    ns = app.GetNamespace("MAPI")
    rows, max_sens, scanned = [], 0, 0
    for r in meta:
        if len(rows) >= limit:
            break
        sens = r["sens"]
        if sens >= 3 and cfg.get("highest_label") == "exclude":
            continue
        r["meta_only"] = sens >= 3 and cfg.get("highest_label") == "metadata_only"
        hit = bool(q) and (q in r["subject"].lower() or q in r["from"].lower())
        r["body"], r["attachments"] = "", 0
        if q and not hit:
            scanned += 1
            if scanned > BODY_SCAN_CAP:
                break
        if (need_body or (q and not hit)) and not r["meta_only"]:
            try:
                it = ns.GetItemFromID(r["id"])
                r["body"] = str(it.Body or "")[:6000]
                r["attachments"] = int(it.Attachments.Count)
            except Exception:  # noqa: BLE001
                pass
        if q and not hit and q not in r["body"].lower():
            continue
        max_sens = max(max_sens, sens)
        rows.append(r)
    return rows, max_sens, matching


def op_search(app, cfg, p):
    rows, max_sens, matching = _rows(app, cfg, p, need_body=True)
    out = []
    for r in rows:
        out.append(f"- id={r['id']}\n  {_fmt(r['received'])} | {r['folder']} | from {r['from']} | you are in: {r['role']}"
                   f"{' | UNREAD' if r['unread'] else ''}\n  subject: {r['subject']}"
                   + ("" if r["meta_only"] else f"\n  preview: {' '.join(r['body'].split())[:200]}"))
    head = ""
    if not p.get("query") and matching > len(rows):
        head = f"Showing the newest {len(rows)} of {matching} matching message(s).\n"
    return {"text": head + ("\n".join(out) or "No messages found."), "count": len(rows), "max_sensitivity": max_sens}


def _conversations(app, scope, cfg, since, sent):
    """conversation id -> latest time (received for inbox, sent for Sent Items) since `since`."""
    got = {}
    prop = "SentOn" if sent else "ReceivedTime"
    filt = _jet(prop, since)
    for _, folder in _scope_folders(app, cfg, scope):
        try:
            tbl, have = _table(folder, filt, ("ConversationIndex", "ConversationTopic", prop, "MessageClass"))
        except Exception:  # noqa: BLE001
            continue
        if prop not in have:
            continue
        while not tbl.EndOfTable:
            row = tbl.GetNextRow()
            cid = conv_key(row["ConversationIndex"], row["ConversationTopic"])
            if str(row["MessageClass"] or "").startswith("IPM.Note"):
                t = _naive(row[prop])
                if t > got.get(cid, datetime.min):
                    got[cid] = t
    return got


def op_digest(app, cfg, p):
    """New mail with deterministic flags (approval / deadline / urgent / question) and whether you already replied."""
    p = dict(p)
    p.setdefault("max_results", 30)
    if not p.get("scan_limit") and (p.get("only_flagged") or p.get("only_flag") or p.get("unanswered_only") or p.get("any_of")
                                    or p.get("deadline_within_days") is not None):
        p["scan_limit"] = 200       # needles in a haystack: read more mail than we list
    want = min(int(p["max_results"]), cfg["max_items"])
    rows, max_sens, matching = _rows(app, cfg, p, need_body=True)
    since, _ = window(cfg, p)
    inbox = (p.get("scope") or "inbox") == "inbox"
    replied = _conversations(app, "sent", cfg, since - timedelta(days=1), sent=True) if inbox else {}
    today = datetime.now().date()
    terms = [str(t).lower() for t in (p.get("any_of") or []) if str(t).strip()]
    lines, counts, kept = [], {"approval": 0, "deadline": 0, "urgent": 0, "question": 0, "unanswered": 0}, 0
    for r in rows:
        if kept >= want:
            break
        f = mailscan.scan(r["subject"], r["body"], r["importance"], today)
        did = bool(r["conv"] and replied.get(r["conv"], datetime.min) > r["received"])
        if p.get("only_flagged") and not any((f["approval"], f["deadline"], f["urgent"], f["question"])):
            continue
        if p.get("only_flag") and not f.get(p["only_flag"]):
            continue
        if p.get("deadline_within_days") is not None:
            if not f["deadline"] or datetime.fromisoformat(f["deadline"]).date() > today + timedelta(days=int(p["deadline_within_days"])):
                continue
        if terms and not any(t in (r["subject"] + " " + r["body"]).lower() for t in terms):
            continue
        if p.get("unanswered_only") and did:
            continue
        kept += 1
        for k in ("approval", "deadline", "urgent", "question"):
            counts[k] += 1 if f[k] else 0
        counts["unanswered"] += 0 if did else 1
        att = f" | attachments {r['attachments']}" if r["attachments"] else ""
        lines.append(f"- id={r['id']}\n  {_fmt(r['received'])} | {r['folder']} | from {r['from']} | you are in: {r['role']}"
                     f"{' | UNREAD' if r['unread'] else ''}{att} | replied: {'yes' if did else 'no'}\n"
                     f"  subject: {r['subject']}\n  flags: {mailscan.flag_text(f) or '-'}"
                     + ("" if r["meta_only"] else f"\n  preview: {' '.join(r['body'].split())[:300]}"))
    head = (f"{kept} listed (from {len(rows)} read of {matching} matching) | with approval wording: {counts['approval']} | with a deadline: "
            f"{counts['deadline']} | urgent: {counts['urgent']} | questions: {counts['question']} | not yet replied: {counts['unanswered']}")
    return {"text": head + "\n" + ("\n".join(lines) or "No messages in this period."), "count": kept, "max_sensitivity": max_sens}


def op_mail_stats(app, cfg, p):
    """Counts only (no bodies): received, unread, in To / CC, important, flagged, per day, top senders.
    Uses Outlook's table API (no item objects), so thousands of messages take seconds."""
    since, until = window(cfg, p)
    filt = _jet("ReceivedTime", since, until, bool(p.get("unread_only")))
    me = _me(app)
    total = unread = to_me = cc_me = other = high = flagged = folders = 0
    per_day, senders = {}, {}
    cap, truncated = int(p.get("max_items", 20000)), False
    for _path, folder in _scope_folders(app, cfg, p.get("scope") or "inbox"):
        try:
            tbl, have = _table(folder, filt, ("ReceivedTime", "UnRead", "SenderName", "Importance", "FlagStatus", "To", "CC", "MessageClass"))
        except Exception:  # noqa: BLE001
            continue
        folders += 1
        while not tbl.EndOfTable:
            row = tbl.GetNextRow()
            if not str(row["MessageClass"] or "").startswith("IPM.Note"):
                continue
            total += 1
            if total > cap:
                truncated = True
                break
            unread += 1 if row["UnRead"] else 0
            role = _role_text(row["To"], row["CC"], me)
            to_me += role == "To"
            cc_me += role == "CC"
            other += role == "other"
            high += 1 if int(row["Importance"] or 1) >= 2 else 0
            flagged += 1 if int(row["FlagStatus"] or 0) == 2 else 0
            day = _naive(row["ReceivedTime"]).strftime("%Y-%m-%d")
            per_day[day] = per_day.get(day, 0) + 1
            name = str(row["SenderName"])
            senders[name] = senders.get(name, 0) + 1
        if truncated:
            break
    top = sorted(senders.items(), key=lambda kv: -kv[1])[: int(p.get("top_senders", 10))]
    text = (f"Period: {_fmt(since)} to {_fmt(until) if until else 'now'} | scope: {p.get('scope') or 'inbox'} ({folders} folders)\n"
            f"Total received: {total} | unread: {unread} | you are in To: {to_me} | in CC: {cc_me} | other (lists/BCC): {other}\n"
            f"High importance: {high} | flagged for follow-up: {flagged}"
            + (f"\n(Stopped after {cap} items - the real number is higher.)" if truncated else "")
            + "\nPer day: " + (", ".join(f"{d}: {n}" for d, n in sorted(per_day.items())) or "-")
            + "\nTop senders: " + (", ".join(f"{n} ({c})" for n, c in top) or "-"))
    return {"text": text, "count": total}


def op_awaiting_reply(app, cfg, p):
    """Mail YOU sent that nobody has answered yet (no newer message in the same conversation)."""
    days = int(p.get("days", 7))
    min_age = timedelta(hours=int(p.get("min_age_hours", 24)))
    now = datetime.now()
    since = now - timedelta(days=days)
    inbox_latest = _conversations(app, "inbox", cfg, since, sent=False)
    out = []
    for _path, folder in _scope_folders(app, cfg, "sent"):
        try:
            tbl, have = _table(folder, _jet("SentOn", since), ("EntryID", "SentOn", "To", "Subject", "ConversationIndex", "ConversationTopic", "MessageClass"))
        except Exception:  # noqa: BLE001
            continue
        found = []
        while not tbl.EndOfTable:
            row = tbl.GetNextRow()
            if not str(row["MessageClass"] or "").startswith("IPM.Note"):
                continue
            st = _naive(row["SentOn"])
            cid = conv_key(row["ConversationIndex"], row["ConversationTopic"])
            if now - st < min_age or inbox_latest.get(cid, datetime.min) > st:
                continue
            found.append((st, row["EntryID"], str(row["To"] or ""), str(row["Subject"] or "")))
        for st, eid, to, subj in sorted(found, reverse=True):
            if len(out) >= min(int(p.get("max_results", 25)), cfg["max_items"]):
                break
            out.append(f"- id={eid}\n  sent {_fmt(st)} ({(now - st).days} day(s) ago) | to {to}\n  subject: {subj}")
    return {"text": f"{len(out)} sent message(s) without a reply\n" + ("\n".join(out) or "None."), "count": len(out)}


OPS = {"search": op_search, "digest": op_digest, "mail_stats": op_mail_stats, "awaiting_reply": op_awaiting_reply}
