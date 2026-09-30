"""pa-outlook-worker: classic Outlook Object Model (COM), read-only (+ optional AI-marked draft).

Protocol: one JSON request per stdin line, one JSON response per stdout line. The worker never talks
to the network (firewall) and never sees prompts or model output. It does NOT suppress, bypass or
auto-click Outlook's Object Model Guard.
Operations: list_folders, search, get_message, get_attachment, calendar_read, create_draft.
Not supported (by design): send, forward, delete, move, rules, contacts export, account settings.
"""
from __future__ import annotations

import base64
import json
import os
import sys
import tempfile
from datetime import datetime, timedelta

OL_FOLDER_CALENDAR = 9
OL_MAIL_ITEM = 0


class Unavailable(Exception):
    pass


def _outlook(cfg):
    import pythoncom  # type: ignore[import-not-found]
    import win32com.client  # type: ignore[import-not-found]
    pythoncom.CoInitialize()
    try:
        return win32com.client.GetActiveObject("Outlook.Application")
    except Exception:  # noqa: BLE001
        if not cfg.get("start_when_needed"):
            raise Unavailable("Outlook is closed. Open Outlook (or enable 'Start Outlook when needed').")
        try:
            return win32com.client.Dispatch("Outlook.Application")
        except Exception as e:  # noqa: BLE001
            raise Unavailable(f"cannot start Outlook: {e}")


def _folder_allowed(path: str, cfg) -> bool:
    low = path.lower()
    if any(d.lower() in low for d in cfg.get("denylist") or []):
        return False
    allow = cfg.get("allowlist") or []
    return not allow or any(a.lower() in low for a in allow)


def _walk(folder, prefix, out, depth=0):
    if depth > 6:
        return
    try:
        path = f"{prefix}/{folder.Name}"
        out.append((path, folder))
        for sub in folder.Folders:
            _walk(sub, path, out, depth + 1)
    except Exception:  # noqa: BLE001
        pass


def _folders(app, cfg):
    ns = app.GetNamespace("MAPI")
    out = []
    for store in ns.Stores:
        try:
            is_pst = str(store.FilePath or "").lower().endswith(".pst")
        except Exception:  # noqa: BLE001
            is_pst = False
        if is_pst and not cfg.get("include_pst"):
            continue
        _walk(store.GetRootFolder(), "", out)
    return [(p, f) for p, f in out if _folder_allowed(p, cfg)]


def _label_ok(item, cfg):
    sens = int(getattr(item, "Sensitivity", 0) or 0)
    if sens >= 3 and cfg.get("highest_label") == "exclude":
        return False, sens
    return True, sens


def op_list_folders(app, cfg, p):
    fs = _folders(app, cfg)
    return {"text": "\n".join(f"- {path} ({f.Items.Count} items)" for path, f in fs[:500]), "count": len(fs)}


def op_search(app, cfg, p):
    maxn = min(int(p.get("max_results", 10)), cfg["max_items"])
    since = datetime.now() - timedelta(days=cfg["date_range_days"])
    if p.get("since"):
        since = max(since, datetime.fromisoformat(p["since"][:19]))
    q = (p.get("query") or "").replace("'", "''")
    filt = f"@SQL=\"urn:schemas:httpmail:datereceived\" >= '{since.strftime('%m/%d/%Y %H:%M')}'"
    if q:
        filt += (f" AND (\"urn:schemas:httpmail:subject\" LIKE '%{q}%' OR \"urn:schemas:httpmail:textdescription\" LIKE '%{q}%'"
                 f" OR \"urn:schemas:httpmail:fromname\" LIKE '%{q}%')")
    results, max_sens = [], 0
    for path, folder in _folders(app, cfg):
        if p.get("folder") and p["folder"].lower() not in path.lower():
            continue
        try:
            items = folder.Items.Restrict(filt)
            items.Sort("[ReceivedTime]", True)
        except Exception:  # noqa: BLE001
            continue
        for it in items:
            if len(results) >= maxn:
                break
            if getattr(it, "Class", 0) != 43:  # olMail
                continue
            ok, sens = _label_ok(it, cfg)
            if not ok:
                continue
            max_sens = max(max_sens, sens)
            meta_only = sens >= 3 and cfg.get("highest_label") == "metadata_only"
            results.append(f"- id={it.EntryID}\n  {it.ReceivedTime} | {path} | from {it.SenderName}\n  subject: {it.Subject}"
                           + ("" if meta_only else f"\n  preview: {str(it.Body or '')[:200]!s}"))
        if len(results) >= maxn:
            break
    return {"text": "\n".join(results) or "No messages found.", "count": len(results), "max_sensitivity": max_sens}


def op_get_message(app, cfg, p):
    it = app.GetNamespace("MAPI").GetItemFromID(p["message_id"])
    ok, sens = _label_ok(it, cfg)
    if not ok:
        raise ValueError("this message is excluded by your sensitivity-label setting")
    body = "" if (sens >= 3 and cfg.get("highest_label") == "metadata_only") else str(it.Body or "")[:cfg["max_body"]]
    atts = [f"- id={i} {a.FileName} ({a.Size} bytes)" for i, a in enumerate(it.Attachments, start=1)]
    text = (f"From: {it.SenderName}\nTo: {it.To}\nCc: {it.CC}\nDate: {it.ReceivedTime}\nSubject: {it.Subject}\n\n{body}"
            + ("\n\nAttachments:\n" + "\n".join(atts) if atts else ""))
    return {"text": text, "max_sensitivity": sens, "entry_id": it.EntryID}


def op_get_attachment(app, cfg, p):
    it = app.GetNamespace("MAPI").GetItemFromID(p["message_id"])
    ok, sens = _label_ok(it, cfg)
    if not ok:
        raise ValueError("excluded by sensitivity label")
    att = it.Attachments.Item(int(p["attachment_id"]))
    if att.Size > cfg["max_attachment"]:
        raise ValueError("attachment larger than allowed")
    fd, path = tempfile.mkstemp(dir=os.environ.get("TEMP"))
    os.close(fd)
    try:
        att.SaveAsFile(path)
        with open(path, "rb") as f:
            data = f.read()
    finally:
        os.unlink(path)
    return {"name": att.FileName, "data_b64": base64.b64encode(data).decode(), "sensitivity": sens}


def op_calendar_read(app, cfg, p):
    cal = app.GetNamespace("MAPI").GetDefaultFolder(OL_FOLDER_CALENDAR)
    items = cal.Items
    items.IncludeRecurrences = True
    items.Sort("[Start]")
    start = datetime.fromisoformat(p["start"][:19])
    end = datetime.fromisoformat(p["end"][:19])
    r = items.Restrict(f"[Start] >= '{start.strftime('%m/%d/%Y %H:%M')}' AND [End] <= '{end.strftime('%m/%d/%Y %H:%M')}'")
    out, max_sens = [], 0
    for i, ev in enumerate(r):
        if i >= int(p.get("max_results", 25)):
            break
        sens = int(getattr(ev, "Sensitivity", 0) or 0)
        max_sens = max(max_sens, sens)
        out.append(f"- {ev.Start} -> {ev.End} | {ev.Subject} | {ev.Location}")
    return {"text": "\n".join(out) or "No events.", "count": len(out), "max_sensitivity": max_sens}


def op_create_draft(app, cfg, p):
    if not cfg.get("drafts"):
        raise ValueError("Outlook drafts are disabled in Settings")
    mail = app.CreateItem(OL_MAIL_ITEM)
    mail.To = "; ".join(p["to"])
    mail.CC = "; ".join(p.get("cc") or [])
    mail.Subject = p["subject"]
    mail.Body = "[AI Draft - created by Personal Agent. Review before sending.]\n\n" + p["body"]
    mail.Categories = "AI Draft"
    mail.Save()  # never .Send()
    return {"text": f"Draft saved in Outlook Drafts (subject: {p['subject']}).", "entry_id": mail.EntryID}


OPS = {"list_folders": op_list_folders, "search": op_search, "get_message": op_get_message,
       "get_attachment": op_get_attachment, "calendar_read": op_calendar_read, "create_draft": op_create_draft}


def main() -> int:
    for line in sys.stdin:
        try:
            req = json.loads(line)
            fn = OPS.get(req.get("op"))
            if fn is None:
                raise ValueError("operation not supported")
            if sys.platform != "win32":
                raise Unavailable("Outlook requires Windows")
            app = _outlook(req["config"])
            res = {"ok": True, "result": fn(app, req["config"], req.get("params") or {})}
        except Unavailable as e:
            res = {"ok": False, "unavailable": True, "error": str(e)}
        except Exception as e:  # noqa: BLE001
            res = {"ok": False, "error": f"{type(e).__name__}: {str(e)[:300]}"}
        sys.stdout.write(json.dumps(res, default=str) + "\n")
        sys.stdout.flush()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
