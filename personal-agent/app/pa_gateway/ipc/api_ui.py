"""Methods available to pa-ui (the single desktop window). Grouped by screen.

Rules implemented here:
  - step-up categories per spec 4.5 (secrets, export, security settings, high-risk approvals,
    connectors, backup restore, transcripts)
  - loosening changes need the password + 10 s read delay (SettingsService)
  - links/URIs can only OPEN a screen (ui.open_link); they can never change anything (39.2)
"""
from __future__ import annotations

import base64
import json
import os
import platform
import sys
import time
from datetime import timedelta
from pathlib import Path
from typing import Any

from pa_common.buildinfo import BUILD_HASH
from pa_common.errors import AuthError, PAError, StepUpRequired
from pa_common.ids import new_id
from pa_common.sensitivity import Sensitivity, Trust
from pa_common.timeutil import now_iso, to_iso, utcnow
from pa_common.version import APP_VERSION
from pa_common.winpaths import powershell

from .. import backup as backup_mod
from ..auth.passwords import strength, validate_password, validate_pin
from ..killswitch import LEVELS
from ..policy.tools_registry import BY_NAME, TOOLS
from .dispatch import UI, ClientInfo, P, rpc

SCREENS = {"home", "chat", "missions", "tasks", "approvals", "files", "memory", "secrets", "activity", "settings", "reminders", "runs"}


def _pw_ok(gw, p: P, key: str = "password") -> bool:
    pw = p.str(key, False, 256)
    if not pw:
        return False
    gw.verify_password(pw)
    return True


# ====================================================================== session / sign-in
@rpc("session.status", state="any", touch=False)
def session_status(gw, p: P, c: ClientInfo) -> Any:
    return gw.status()


@rpc("session.touch", state="unlocked")
def session_touch(gw, p: P, c: ClientInfo) -> Any:
    return {"ok": True}


@rpc("setup.preflight", state="any")
def setup_preflight(gw, p: P, c: ClientInfo) -> Any:
    return gw.preflight()


@rpc("setup.check_password", state="any", touch=False)
def setup_check_password(gw, p: P, c: ClientInfo) -> Any:
    pw, user = p.str("password", max_len=256), p.str("username", False, 64)
    return {"strength": strength(pw, user), "errors": validate_password(pw, user)}


@rpc("setup.check_pin", state="any", touch=False)
def setup_check_pin(gw, p: P, c: ClientInfo) -> Any:
    return {"errors": validate_pin(p.str("pin", max_len=12), p.bool("allow_letters"), p.str("password", False, 256) or None)}


@rpc("setup.create", state="any")
def setup_create(gw, p: P, c: ClientInfo) -> Any:
    return gw.setup(p.str("username", max_len=64), p.str("password", max_len=256), p.str("pin", max_len=12), p.bool("allow_letters"),
                    p.str("display_name", False, 40), p.str("assistant_name", False, 40))


@rpc("setup.confirm_recovery", state="unlocked")
def setup_confirm_recovery(gw, p: P, c: ClientInfo) -> Any:
    gw.confirm_recovery({str(k): str(v) for k, v in p.dict("answers", True).items()})
    return {"ok": True}


@rpc("setup.recovery_pdf", state="unlocked")
def setup_recovery_pdf(gw, p: P, c: ClientInfo) -> Any:
    """Writes a simple printable file with the recovery key to a path the user chose (Save dialog)."""
    if not gw._pending_rk:
        raise PAError("no recovery key to save", code="invalid_state")
    dest = Path(p.str("path", max_len=1000))
    from ..recovery_doc import write_recovery_pdf
    write_recovery_pdf(dest, gw.vault.username, gw._pending_rk)
    gw.audit.write("recovery_key.saved_to_file", "authentication", severity="medium")
    return {"ok": True}


@rpc("auth.sign_in", state="any", touch=False)
def auth_sign_in(gw, p: P, c: ClientInfo) -> Any:
    return gw.sign_in(p.str("username", max_len=64), p.str("password", max_len=256))


@rpc("auth.quick_unlock", state="any", touch=False)
def auth_quick_unlock(gw, p: P, c: ClientInfo) -> Any:
    return gw.quick_unlock(p.str("pin", max_len=12))


@rpc("auth.forgot_password", state="any", touch=False)
def auth_forgot(gw, p: P, c: ClientInfo) -> Any:
    return gw.forgot_password(p.str("pin", max_len=12), p.str("recovery_key", max_len=80), p.str("new_password", max_len=256))


@rpc("auth.lock", state="any")
def auth_lock(gw, p: P, c: ClientInfo) -> Any:
    gw.lock_ui(p.str("reason", False, 40, "user"))
    return gw.status()


@rpc("auth.sign_out", state="any")
def auth_sign_out(gw, p: P, c: ClientInfo) -> Any:
    gw.sign_out(p.str("reason", False, 40, "user"))
    return gw.status()


@rpc("auth.step_up")
def auth_step_up(gw, p: P, c: ClientInfo) -> Any:
    return gw.step_up(p.str("category", max_len=40), p.str("method", max_len=10), p.str("secret", max_len=256))


@rpc("ui.open_link", state="any", touch=False)
def ui_open_link(gw, p: P, c: ClientInfo) -> Any:
    """39.2: a personalagent:// link or notification click may ONLY open a screen. Everything else is dropped."""
    url = p.str("url", max_len=2000)
    if url.startswith("personalagent://auth/m365"):
        gw.connectors.adapters["m365"].complete_sign_in(url)
        return {"screen": "settings", "section": "connectors"}
    screen = url.split("://", 1)[-1].split("/", 1)[0].split("?", 1)[0]
    if screen not in SCREENS:
        gw.audit.write("ui.link_rejected", "security", severity="medium")
        return {"screen": "home"}
    if "?" in url or "=" in url:
        gw.audit.write("ui.link_params_ignored", "security", screen=screen, severity="medium")
    return {"screen": screen}


# ====================================================================== account & security
@rpc("account.change_password", stepup=None)
def account_change_password(gw, p: P, c: ClientInfo) -> Any:
    gw.change_password(p.str("current", max_len=256), p.str("new", max_len=256))
    return {"ok": True}


@rpc("account.set_profile")
def account_set_profile(gw, p: P, c: ClientInfo) -> Any:
    return gw.set_profile(p.str("display_name", max_len=40), p.str("assistant_name", max_len=40))


@rpc("account.change_username")
def account_change_username(gw, p: P, c: ClientInfo) -> Any:
    gw.change_username(p.str("password", max_len=256), p.str("username", max_len=64))
    return {"ok": True}


@rpc("account.set_pin")
def account_set_pin(gw, p: P, c: ClientInfo) -> Any:
    return gw.set_new_pin(p.str("password", max_len=256), p.str("pin", max_len=12), p.str("recovery_key", False, 80) or None)


@rpc("account.new_recovery_key")
def account_new_recovery_key(gw, p: P, c: ClientInfo) -> Any:
    return gw.new_recovery_key(p.str("password", max_len=256), p.str("pin", max_len=12))


@rpc("account.signin_history")
def account_signin_history(gw, p: P, c: ClientInfo) -> Any:
    return gw.db.all("SELECT * FROM signin_history ORDER BY ts DESC LIMIT 50")


@rpc("posture.run")
def posture_run(gw, p: P, c: ClientInfo) -> Any:
    return gw.posture()


@rpc("posture.fix", stepup="security_settings")
def posture_fix(gw, p: P, c: ClientInfo) -> Any:
    action = p.str("action", max_len=40)
    if action == "verify_log":
        return gw.verify_log()
    if action == "repair_acl":
        from ..app import secure_data_folder
        secure_data_folder(gw.paths.root)
        return {"ok": True}
    if action == "repair_firewall":
        # re-requests admin elevation for the firewall rules ONLY (spec 5.3)
        script = Path(sys.executable).parent / "installer" / "firewall-rules.ps1" if getattr(sys, "frozen", False) \
            else Path(__file__).resolve().parents[3] / "installer" / "windows" / "firewall-rules.ps1"
        if sys.platform == "win32" and script.exists():
            # An elevated script must live where only administrators can write (Program Files): a script in a user-writable
            # folder (dev checkout, portable copy) could be swapped by any program of this user before they press "Repair".
            pf = [os.environ.get(k, "") for k in ("ProgramFiles", "ProgramW6432")]
            if not any(x and str(script.resolve()).lower().startswith(x.lower() + os.sep) for x in pf):
                raise PAError("the firewall repair script is not in a protected folder; run installer\\windows\\firewall-rules.ps1 as administrator yourself",
                              code="unavailable")
            import subprocess
            ps = powershell()
            subprocess.run([ps, "-NoProfile", "-Command",
                            f"Start-Process -FilePath '{ps}' -Verb RunAs -ArgumentList '-NoProfile -ExecutionPolicy Bypass -File \"{script}\"'"],
                           check=False)
            gw.audit.write("posture.firewall_repair_requested", "security")
            return {"ok": True, "elevation_requested": True}
        raise PAError("firewall repair script not found", code="unavailable")
    raise PAError("unknown fix", code="invalid_request")


# ====================================================================== settings
@rpc("settings.describe")
def settings_describe(gw, p: P, c: ClientInfo) -> Any:
    return gw.settings.describe()


@rpc("settings.classify")
def settings_classify(gw, p: P, c: ClientInfo) -> Any:
    return gw.settings.classify(p.dict("changes", True))


@rpc("settings.begin_loosen")
def settings_begin_loosen(gw, p: P, c: ClientInfo) -> Any:
    return gw.settings.begin_loosen(p.dict("changes", True))


@rpc("settings.apply")
def settings_apply(gw, p: P, c: ClientInfo) -> Any:
    changes = p.dict("changes", True)
    password_ok = _pw_ok(gw, p)
    stepup_ok = gw.session.has_stepup("security_settings") or password_ok
    info = gw.settings.classify(changes)
    if info["stepup"] and not stepup_ok:
        raise StepUpRequired("please confirm it's you", category="security_settings")
    return gw.settings.apply(changes, password_ok=password_ok, loosen_token=p.str("loosen_token", False, 100) or None,
                             stepup_ok=stepup_ok)


@rpc("settings.profile_preview")
def settings_profile_preview(gw, p: P, c: ClientInfo) -> Any:
    changes = gw.settings.profile_changes(p.str("profile", max_len=20))
    return {"changes": changes, **gw.settings.classify(changes)}


@rpc("settings.history")
def settings_history(gw, p: P, c: ClientInfo) -> Any:
    return gw.settings.history()


@rpc("settings.rules_plain")
def settings_rules_plain(gw, p: P, c: ClientInfo) -> Any:
    return {"rules": gw.policy.plain_language_rules(), "policy_version": gw.policy.version}


# ====================================================================== home
@rpc("home.summary")
def home_summary(gw, p: P, c: ClientInfo) -> Any:
    since = p.str("since", False, 40) or to_iso(utcnow() - timedelta(days=1))
    q = gw.db.all
    return {
        "events": gw.home.events(),
        "completed": q("SELECT id, objective, mission_id, updated_at FROM tasks WHERE state='COMPLETED' AND updated_at>? "
                       "ORDER BY updated_at DESC LIMIT 20", (since,)),
        "failed": q("SELECT id, objective, error, wait_reason, updated_at FROM tasks WHERE state IN ('FAILED','TIMED_OUT') "
                    "AND updated_at>? ORDER BY updated_at DESC LIMIT 20", (since,)),
        "waiting": q("SELECT id, objective, state, wait_reason FROM tasks WHERE state IN ('WAITING_FOR_RESOURCE','WAITING_FOR_APPROVAL',"
                     "'SUSPENDED','PAUSED') ORDER BY updated_at DESC LIMIT 20"),
        "outcome_unknown": q("SELECT id, objective, updated_at FROM tasks WHERE state='OUTCOME_UNKNOWN' ORDER BY updated_at DESC LIMIT 20"),
        "files_created": q("SELECT id, name, folder, created_at FROM files WHERE source IN ('agent','sandbox') AND deleted=0 AND created_at>? "
                           "ORDER BY created_at DESC LIMIT 20", (since,)),
        "memories_proposed": q("SELECT id, content FROM memories WHERE status='PROPOSED' LIMIT 20"),
        "approvals_pending": gw.approvals.pending_count(),
        "reminders_today": q("SELECT id, text, due_at, status FROM reminders WHERE status IN ('SCHEDULED','FIRED') ORDER BY due_at LIMIT 10"),
        "security": q("SELECT kind, success, ts FROM signin_history WHERE success=0 AND ts>? ORDER BY ts DESC LIMIT 20", (since,)),
        "memory_review": gw.memory.review() if time.localtime().tm_wday == 0 else None,
        "budget": gw.budgets.summary(),
        "recovery_key_confirmed": bool(gw.auth_state.data.get("rk_confirmed", True)),
    }


@rpc("home.widgets")
def home_widgets(gw, p: P, c: ClientInfo) -> Any:
    """The widget catalogue, which ones are on, and the data of the enabled ones. `refresh` forces a new Outlook reading."""
    en = gw.widgets.enabled()
    return {"catalog": gw.widgets.catalog(), "enabled": en, "data": gw.widgets.data(en, refresh_mail=p.bool("refresh"))}


@rpc("home.widgets_set")
def home_widgets_set(gw, p: P, c: ClientInfo) -> Any:
    """Switch widgets on/off and order them (Home > Widgets). Unknown ids are ignored."""
    ids = [str(x) for x in p.list("enabled", True, 40)]
    en = gw.widgets.set_enabled(ids)
    gw.audit.write("home.widgets_changed", "configuration", count=len(en))
    return {"enabled": en, "data": gw.widgets.data(en)}


@rpc("home.dismiss")
def home_dismiss(gw, p: P, c: ClientInfo) -> Any:
    gw.home.dismiss(p.str("id", max_len=100))
    return {"ok": True}


@rpc("notifications.list")
def notifications_list(gw, p: P, c: ClientInfo) -> Any:
    return gw.home.notifications()


@rpc("notifications.mark_read")
def notifications_mark_read(gw, p: P, c: ClientInfo) -> Any:
    gw.home.mark_read(p.str("id", False, 100) or None)
    return {"ok": True}


# ====================================================================== chat
@rpc("chat.list")
def chat_list(gw, p: P, c: ClientInfo) -> Any:
    return gw.db.all("SELECT * FROM chats WHERE deleted=0 AND archived=? ORDER BY updated_at DESC LIMIT 500",
                     (int(p.bool("archived")),))


@rpc("chat.create")
def chat_create(gw, p: P, c: ClientInfo) -> Any:
    cid = new_id("chat")
    gw.db.insert("chats", {"id": cid, "title": p.str("title", False, 200) or "New chat", "created_at": now_iso(),
                           "updated_at": now_iso(), "allow_tools": int(p.bool("allow_tools", default=True))})
    return {"id": cid}


@rpc("chat.get")
def chat_get(gw, p: P, c: ClientInfo) -> Any:
    cid = p.str("chat_id", max_len=100)
    chat = gw.db.one("SELECT * FROM chats WHERE id=? AND deleted=0", (cid,))
    if not chat:
        raise PAError("chat not found", code="not_found")
    chat["sources"] = json.loads(chat.pop("sources_json") or "[]")
    chat["sensitivity"] = Sensitivity(int(chat["hwm"])).name
    msgs = gw.db.all("SELECT * FROM chat_messages WHERE chat_id=? ORDER BY created_at", (cid,))
    for m in msgs:
        m["sources"] = json.loads(m.pop("sources_json") or "[]")
    pending = gw.db.all("SELECT id, tool, payload_json, payload_hash, destination, sensitivity, risk, reason, requires_password, "
                        "expires_at, kind, run_id FROM approvals WHERE chat_id=? AND status='PENDING'", (cid,))
    for a in pending:
        a["payload"] = json.loads(a.pop("payload_json"))
    running = gw.db.all("SELECT id, run_id, state FROM tasks WHERE chat_id=? AND state IN ('QUEUED','RUNNING','WAITING_FOR_APPROVAL')", (cid,))
    return {"chat": chat, "messages": msgs, "pending_approvals": pending, "running": running}


@rpc("chat.send")
def chat_send(gw, p: P, c: ClientInfo) -> Any:
    cid = p.str("chat_id", max_len=100)
    text = p.str("text", max_len=100_000).strip()
    if not text:
        raise PAError("empty message", code="invalid_request")
    chat = gw.db.one("SELECT * FROM chats WHERE id=? AND deleted=0", (cid,))
    if not chat:
        raise PAError("chat not found", code="not_found")
    if gw.killswitch.agent_blocked():
        raise PAError("the agent is paused (emergency stop). Release it in Settings > Emergency Stop.", code="kill_switch_active")
    mid = new_id("msg")
    gw.db.insert("chat_messages", {"id": mid, "chat_id": cid, "role": "user", "content": text, "sensitivity": int(Sensitivity.INTERNAL),
                                   "created_at": now_iso()})
    if chat["title"] in ("New chat", "") and len(text) > 2:
        gw.db.update("chats", "id", cid, {"title": text[:60]})
    gw.db.update("chats", "id", cid, {"updated_at": now_iso()})
    gw.history.index("chat", mid, chat["title"], text, now_iso())
    run_id = gw.runs.start("chat", text[:80], chat_id=cid)
    # the user's words enter the session log as TRUSTED instructions
    gw.sessionlog.append(cid, "user.message", text, role="user", source="user", trust=Trust.TRUSTED, sensitivity=int(Sensitivity.INTERNAL),
                         meta={"voice": p.bool("voice")})
    gw.runs.step(run_id, "input", "Your message" + (" (voice)" if p.bool("voice") else ""), "done", {"text": text[:1000]})
    t = gw.tasks.create(objective=text, trigger="USER", run_id=run_id, chat_id=cid, priority=1,
                        hwm=int(chat["hwm"]), session_id=cid)
    gw.runs.attach_task(run_id, t["id"])
    return {"message_id": mid, "run_id": run_id, "task_id": t["id"]}


@rpc("chat.update")
def chat_update(gw, p: P, c: ClientInfo) -> Any:
    cid = p.str("chat_id", max_len=100)
    changes: dict[str, Any] = {}
    if p.opt("title") is not None:
        from ..files.checks import clean_label
        title = clean_label(p.str("title", max_len=200))
        if not title:
            raise PAError("a chat name cannot be empty", code="invalid_request")
        changes["title"] = title[:120]
    if p.opt("allow_tools") is not None:
        changes["allow_tools"] = int(p.bool("allow_tools"))
    if p.opt("archived") is not None:
        changes["archived"] = int(p.bool("archived"))
    if p.opt("pinned") is not None:
        changes["pinned"] = int(p.bool("pinned"))
    if p.opt("folder") is not None:
        folder = p.str("folder", False, 40).strip()
        if any(ord(ch) < 32 or ch in "<>" for ch in folder):
            raise PAError("folder name has invalid characters", code="invalid_request")
        changes["folder"] = folder
    if changes:
        gw.db.update("chats", "id", cid, changes)
        gw.audit.write("chat.updated", "chat", chat_id=cid, fields=list(changes))
    return {"ok": True}


@rpc("chat.delete")
def chat_delete(gw, p: P, c: ClientInfo) -> Any:
    cid = p.str("chat_id", max_len=100)
    gw.web_grants.pop(cid, None)
    for m in gw.db.all("SELECT id FROM chat_messages WHERE chat_id=?", (cid,)):
        gw.history.remove("chat", m["id"])
    for r in gw.db.all("SELECT id FROM runs WHERE chat_id=?", (cid,)):          # the answers also live in the runs' summaries
        gw.history.remove("run", r["id"])
    gw.db.execute("DELETE FROM chat_messages WHERE chat_id=?", (cid,))
    gw.db.execute("DELETE FROM session_events WHERE session_id=?", (cid,))
    gw.db.update("chats", "id", cid, {"deleted": 1})
    gw.localfiles.revoke_chat(cid)
    gw.audit.write("chat.deleted", "chat", chat_id=cid)
    return {"ok": True}


# ====================================================================== runs (step timeline) / tasks
@rpc("runs.list")
def runs_list(gw, p: P, c: ClientInfo) -> Any:
    return gw.runs.list(p.int("limit", False, 1, 5000, 100), p.bool("archived"), p.str("chat_id", False, 100) or None)


@rpc("runs.get")
def runs_get(gw, p: P, c: ClientInfo) -> Any:
    r = gw.runs.get(p.str("run_id", max_len=100))
    if not r:
        raise PAError("run not found", code="not_found")
    return r


@rpc("runs.transcript")
def runs_transcript(gw, p: P, c: ClientInfo) -> Any:
    """Full prompts/completions/tool payloads (Transcript Store, spec 25.5). Step-up by default."""
    if gw.settings.get("security.transcripts_require_stepup") and not gw.session.has_stepup("transcripts"):
        raise StepUpRequired("please confirm it's you", category="transcripts")
    run = gw.runs.get(p.str("run_id", max_len=100))
    if not run:
        raise PAError("run not found", code="not_found")
    task = gw.tasks.get(run["task_id"]) if run.get("task_id") else None
    session = task["session_id"] if task else run["id"]
    gw.audit.write("transcript.viewed", "privacy", run_id=run["id"])
    return gw.sessionlog.events(session)


@rpc("runs.replay")
def runs_replay(gw, p: P, c: ClientInfo) -> Any:
    """39.5: replay one logged model request (optionally against another model) to see why the agent acted."""
    if not gw.session.has_stepup("transcripts"):
        raise StepUpRequired("please confirm it's you", category="transcripts")
    ev = gw.db.one("SELECT * FROM session_events WHERE id=? AND kind='llm.request'", (p.str("event_id", max_len=100),))
    if not ev:
        raise PAError("request not found", code="not_found")
    req = json.loads(ev["content"])
    model_id = p.str("model_id", False, 100) or json.loads(ev["meta_json"]).get("model_id")
    m = gw.llm.get_model(model_id)
    text, _ = gw.llm._run(m, {**req, "stream": True}, None)
    gw.audit.write("llm.replay", "model", event_id=ev["id"], model_id=model_id)
    original = gw.db.one("SELECT content FROM session_events WHERE session_id=? AND kind='llm.response' AND seq>? ORDER BY seq LIMIT 1",
                         (ev["session_id"], ev["seq"]))
    return {"replayed": text, "original": original["content"] if original else None, "model": m["name"]}


@rpc("tasks.list")
def tasks_list(gw, p: P, c: ClientInfo) -> Any:
    states = tuple(p.list("states")) or None
    rows = gw.tasks.list(states, p.int("limit", False, 1, 2000, 200))
    for r in rows:
        r["usage"] = json.loads(r.pop("usage_json") or "{}")
        r["limits"] = gw.budgets.task_limits(r)
    return rows


@rpc("tasks.get")
def tasks_get(gw, p: P, c: ClientInfo) -> Any:
    t = gw.tasks.get(p.str("task_id", max_len=100))
    if not t:
        raise PAError("task not found", code="not_found")
    t["transitions"] = gw.tasks.transitions(t["id"])
    t["usage"] = json.loads(t["usage_json"] or "{}")
    t["limits"] = gw.budgets.task_limits(t)
    t["run"] = gw.runs.get(t["run_id"]) if t.get("run_id") else None
    return t


@rpc("tasks.stop")
def tasks_stop(gw, p: P, c: ClientInfo) -> Any:
    gw.tasks.cancel(p.str("task_id", max_len=100), "stopped by you")
    return {"ok": True}


@rpc("tasks.resume")
def tasks_resume(gw, p: P, c: ClientInfo) -> Any:
    t = gw.tasks.get(p.str("task_id", max_len=100))
    if not t or t["state"] not in ("WAITING_FOR_RESOURCE", "SUSPENDED", "PAUSED"):
        raise PAError("task cannot be resumed", code="invalid_state")
    gw.tasks.cancel_event(t["id"]).clear()
    gw.tasks.transition(t["id"], "QUEUED", "resumed by you", lease_owner=None)
    return {"ok": True}


# ====================================================================== approvals
@rpc("approvals.list")
def approvals_list(gw, p: P, c: ClientInfo) -> Any:
    return gw.approvals.list(p.str("status", False, 20) or "PENDING")


@rpc("approvals.decide")
def approvals_decide(gw, p: P, c: ClientInfo) -> Any:
    aid = p.str("approval_id", max_len=100)
    approve = p.bool("approve", required=True)
    password_ok = _pw_ok(gw, p)
    a = gw.approvals.get(aid)
    if a and approve and a["risk"] == "high":
        method = gw.settings.get("approvals.high_risk_method")
        if method == "password" and not password_ok and not gw.session.has_stepup("approvals_high", require_password=True):
            raise PAError("your password is required for high-risk approvals", code="password_required")
    edited = p.dict("edited_payload") or None
    if edited is not None and a:
        edited = BY_NAME[a["tool"]].args.model_validate(edited).model_dump()
    return gw.approvals.decide(aid, approve, stepup_ok=gw.session.has_stepup("approvals_high") or password_ok,
                               password_ok=password_ok, shown_hash=p.str("payload_hash", max_len=100),
                               opened_at_ms=p.int("opened_at_ms", False, 0) or None, edited_payload=edited)


# ====================================================================== missions / routines
@rpc("missions.list")
def missions_list(gw, p: P, c: ClientInfo) -> Any:
    return gw.missions.list()


@rpc("missions.create")
def missions_create(gw, p: P, c: ClientInfo) -> Any:
    return {"id": gw.missions.create(p.dict("mission", True))}


@rpc("missions.update")
def missions_update(gw, p: P, c: ClientInfo) -> Any:
    return gw.missions.update(p.str("mission_id", max_len=100), p.dict("mission", True))


@rpc("missions.activate")
def missions_activate(gw, p: P, c: ClientInfo) -> Any:
    mid = p.str("mission_id", max_len=100)
    m = gw.missions.get(mid)
    # floor (spec 23): enabling a mission that uses write tools always needs approval -> password confirmation
    if gw.missions.uses_write_tools(m) and not _pw_ok(gw, p):
        raise PAError("this mission can send or write outside the PC - confirm with your password", code="password_required")
    gw.missions.activate(mid)
    return {"ok": True}


@rpc("missions.set_status")
def missions_set_status(gw, p: P, c: ClientInfo) -> Any:
    status = p.str("status", max_len=20)
    if status not in ("PAUSED", "CANCELLED"):
        raise PAError("use missions.activate to resume", code="invalid_request")
    gw.missions.set_status(p.str("mission_id", max_len=100), status, "by user")
    return {"ok": True}


@rpc("missions.run_now")
def missions_run_now(gw, p: P, c: ClientInfo) -> Any:
    mid = p.str("mission_id", max_len=100)
    m = gw.missions.get(mid)
    if m["status"] not in ("ACTIVE", "DRAFT", "PAUSED"):
        raise PAError(f"mission is {m['status']}", code="invalid_state")
    if m["status"] == "DRAFT" and gw.missions.uses_write_tools(m) and not _pw_ok(gw, p):
        raise PAError("confirm with your password", code="password_required")
    return {"task_id": gw.missions.run_now(mid, "USER", "run now")}


@rpc("missions.describe")
def missions_describe(gw, p: P, c: ClientInfo) -> Any:
    return gw.missions.describe_to_form(p.str("text", max_len=4000))


@rpc("missions.parse_schedule")
def missions_parse_schedule(gw, p: P, c: ClientInfo) -> Any:
    from ..agentdata import schedule as sch
    from ..agentdata.missions import local_tz
    text = p.str("text", max_len=200)
    parsed = sch.parse_plain(text)
    if parsed is None and len(text.split()) == 5:
        parsed = {"type": "cron", "cron": text.strip()}
    nxt = None
    if parsed:
        tz = p.str("timezone", False, 64) or local_tz()
        try:
            sch.validate(parsed, tz)
        except (sch.ScheduleError, ValueError, KeyError):
            return {"schedule": None, "next_run": None, "text": None}
        if parsed["type"] != "event":
            n = sch.next_run(parsed, tz, utcnow())
            nxt = to_iso(n) if n else None
    return {"schedule": parsed, "next_run": nxt, "text": sch.describe(parsed) if parsed else None}


# ====================================================================== local files (read in place, per chat)
@rpc("localfiles.grant")
def localfiles_grant(gw, p: P, c: ClientInfo) -> Any:
    return gw.localfiles.grant(p.str("chat_id", max_len=100), p.str("path", max_len=1000), p.str("sensitivity", False, 20) or None)


@rpc("localfiles.list")
def localfiles_list(gw, p: P, c: ClientInfo) -> Any:
    return gw.localfiles.list(p.str("chat_id", max_len=100))


@rpc("localfiles.folder_info")
def localfiles_folder_info(gw, p: P, c: ClientInfo) -> Any:
    """What the approval dialog shows (counts of files and subfolders) before the user allows a folder."""
    return gw.localfiles.folder_info(p.str("path", max_len=1000))


@rpc("localfiles.grant_folder")
def localfiles_grant_folder(gw, p: P, c: ClientInfo) -> Any:
    return gw.localfiles.grant_folder(p.str("chat_id", max_len=100), p.str("path", max_len=1000), p.bool("include_subfolders"),
                                      p.bool("confirm_subfolders"), p.str("sensitivity", False, 20) or None)


@rpc("localfiles.allow_subfolders")
def localfiles_allow_subfolders(gw, p: P, c: ClientInfo) -> Any:
    return gw.localfiles.allow_subfolders(p.str("grant_id", max_len=100), p.bool("confirm"))


@rpc("localfiles.reapprove")
def localfiles_reapprove(gw, p: P, c: ClientInfo) -> Any:
    return gw.localfiles.reapprove(p.str("grant_id", max_len=100), p.bool("include_subfolders"), p.bool("confirm_subfolders"))


@rpc("localfiles.requests")
def localfiles_requests(gw, p: P, c: ClientInfo) -> Any:
    return gw.localfiles.pending_requests(p.str("chat_id", max_len=100))


@rpc("localfiles.deny_request")
def localfiles_deny_request(gw, p: P, c: ClientInfo) -> Any:
    gw.localfiles.deny_request(p.str("request_id", max_len=100))
    return {"ok": True}


@rpc("localfiles.revoke")
def localfiles_revoke(gw, p: P, c: ClientInfo) -> Any:
    gw.localfiles.revoke(p.str("grant_id", max_len=100))
    return {"ok": True}


# ====================================================================== live GPU meter
@rpc("system.usage", touch=False)
def system_usage(gw, p: P, c: ClientInfo) -> Any:
    return gw.gpu.usage()


# ====================================================================== network log
@rpc("network.logs")
def network_logs(gw, p: P, c: ClientInfo) -> Any:
    return gw.netlog.query(days=p.int("days", False, 1, 14, 7), host=p.str("host", False, 100) or "", component=p.str("component", False, 20) or "",
                           outcome=p.str("outcome", False, 20) or "", limit=p.int("limit", False, 1, 1000, 300), offset=p.int("offset", False, 0, 100000, 0),
                           include_loopback=p.bool("include_local", False, True))


# ====================================================================== email monitoring (Outlook skills)
@rpc("emailskills.list")
def emailskills_list(gw, p: P, c: ClientInfo) -> Any:
    return gw.emailskills.list(mark_seen=p.bool("mark_seen"))


@rpc("emailskills.set")
def emailskills_set(gw, p: P, c: ClientInfo) -> Any:
    return gw.emailskills.set_enabled(p.str("skill", max_len=60), p.bool("enabled"))


@rpc("emailskills.run")
def emailskills_run(gw, p: P, c: ClientInfo) -> Any:
    return gw.emailskills.run_now(p.str("skill", max_len=60))


# ====================================================================== reminders
@rpc("reminders.list")
def reminders_list(gw, p: P, c: ClientInfo) -> Any:
    return gw.reminders.list(p.bool("include_done"))


@rpc("reminders.create")
def reminders_create(gw, p: P, c: ClientInfo) -> Any:
    return {"id": gw.reminders.create(p.str("text", max_len=500), p.str("due_at", max_len=40), p.str("timezone", False, 64) or None)}


@rpc("reminders.action")
def reminders_action(gw, p: P, c: ClientInfo) -> Any:
    rid, action = p.str("id", max_len=100), p.str("action", max_len=20)
    if action == "cancel":
        gw.reminders.cancel(rid)
    elif action == "dismiss":
        gw.reminders.dismiss(rid)
    elif action == "snooze":
        gw.reminders.snooze(rid, p.int("minutes", False, 1, 10080, 10))
    else:
        raise PAError("unknown action", code="invalid_request")
    return {"ok": True}


# ====================================================================== files
def _level(name: str) -> int:
    """A sensitivity label by name (PUBLIC ... RESTRICTED); anything else is a clear 'invalid_request', never a KeyError."""
    try:
        return int(Sensitivity[name])
    except KeyError:
        raise PAError("unknown sensitivity label", code="invalid_request") from None


@rpc("files.list")
def files_list(gw, p: P, c: ClientInfo) -> Any:
    rows = gw.files.list(p.str("folder", False, 500) or None, p.str("query", False, 200))
    for r in rows:
        m = gw.insights.get(r["id"])
        r["meta"] = {"doc_type": m["doc_type"], "summary": m["summary"], "status": m["status"]} if m else None
    return {"files": rows, "storage": gw.files.storage()}


@rpc("files.upload")
def files_upload(gw, p: P, c: ClientInfo) -> Any:
    """Upload by path (Tauri file dialog / drag-and-drop gives paths) or by base64 data (small files)."""
    path = p.str("path", False, 2000)
    if path:
        src = Path(path)
        if not src.is_file():
            raise PAError("file not found", code="not_found")
        data, name = src.read_bytes(), src.name
    else:
        data, name = base64.b64decode(p.str("data_b64", max_len=40_000_000)), p.str("name", max_len=200)
    lvl = p.str("sensitivity", False, 20)
    f = gw.files.ingest(name=name, data=data, source="upload", sensitivity=_level(lvl) if lvl else None,
                        folder=p.str("folder", False, 500) or "/", tags=[str(t) for t in p.list("tags", max_items=30)],
                        on_done=lambda row: gw.missions.on_event("new_file", {"file_id": row.get("id")}) if row.get("status") == "READY" else None)
    return gw.files.public(f)


@rpc("files.release_unscanned", stepup="security_settings")
def files_release_unscanned(gw, p: P, c: ClientInfo) -> Any:
    """Quarantine > 'Allow without antivirus scan' for ONE file (needs re-authentication)."""
    return gw.files.release_unscanned(p.str("file_id", max_len=100))


@rpc("files.meta_update")
def files_meta_update(gw, p: P, c: ClientInfo) -> Any:
    """Edit the automatic summary of a file (title, kind, summary, keywords). Your edit is kept; it also updates the memory about the file."""
    return gw.insights.update(p.str("file_id", max_len=100), p.str("title", max_len=100), p.str("doc_type", False, 100), p.str("summary", False, 600),
                              [str(x) for x in p.list("keywords", max_items=12)])


@rpc("files.analyse")
def files_analyse(gw, p: P, c: ClientInfo) -> Any:
    """'Summarise again' (also when the automatic summary is switched off): runs in the background."""
    fid = p.str("file_id", max_len=100)
    row = gw.files.get(fid)
    if not row or row["deleted"] or row["status"] != "READY":
        raise PAError("only files that are ready can be summarised", code="file_not_ready")
    return {"started": gw.insights.schedule(fid, force=True)}


@rpc("files.reread")
def files_reread(gw, p: P, c: ClientInfo) -> Any:
    """Read a picture / scanned PDF again with the vision model (after adding one)."""
    return gw.files.reread(p.str("file_id", max_len=100))


@rpc("files.preview")
def files_preview(gw, p: P, c: ClientInfo) -> Any:
    fid = p.str("file_id", max_len=100)
    row = gw.files.get(fid)
    if not row:
        raise PAError("file not found", code="not_found")
    out = gw.files.public(row)
    out["meta"] = gw.insights.get(fid)
    if row["status"] == "READY":
        out["text"] = (row["text_content"] or "")[:200_000]
    return out


@rpc("files.update")
def files_update(gw, p: P, c: ClientInfo) -> Any:
    fid = p.str("file_id", max_len=100)
    gw.files.update_meta(fid, folder=p.str("folder", False, 500) or None, tags=[str(t) for t in p.list("tags", max_items=30)] if p.opt("tags") is not None else None,
                         in_knowledge=p.bool("in_knowledge") if p.opt("in_knowledge") is not None else None,
                         name=p.str("name", False, 200) or None)
    return {"ok": True}


@rpc("files.set_label")
def files_set_label(gw, p: P, c: ClientInfo) -> Any:
    fid = p.str("file_id", max_len=100)
    level = _level(p.str("level", max_len=20))
    row = gw.files.get(fid)
    if row and level < int(row["sensitivity"]) and not gw.session.has_stepup("security_settings"):
        raise StepUpRequired("lowering a label needs re-authentication", category="security_settings")  # spec 13.2
    return gw.files.set_label(fid, level)


@rpc("files.delete")
def files_delete(gw, p: P, c: ClientInfo) -> Any:
    return {"deleted": gw.files.delete(p.str("file_id", max_len=100))}


@rpc("files.save_copy")
def files_save_copy(gw, p: P, c: ClientInfo) -> Any:
    gw.files.save_copy(p.str("file_id", max_len=100), p.str("path", max_len=2000))
    return {"ok": True}


# ====================================================================== memory
@rpc("memory.list")
def memory_list(gw, p: P, c: ClientInfo) -> Any:
    return gw.memory.list(p.str("status", False, 20) or None)


@rpc("memory.add")
def memory_add(gw, p: P, c: ClientInfo) -> Any:
    return {"id": gw.memory.add_user(p.str("content", max_len=4000), p.str("type", False, 20) or "preference")}


@rpc("memory.action")
def memory_action(gw, p: P, c: ClientInfo) -> Any:
    mid, action = p.str("id", max_len=100), p.str("action", max_len=20)
    if action == "confirm":
        gw.memory.confirm(mid, p.str("content", False, 4000) or None)
    elif action == "edit":
        gw.memory.edit(mid, p.str("content", max_len=4000))
    elif action in ("disable", "enable", "reject"):
        gw.memory.set_status(mid, {"disable": "DISABLED", "enable": "ACTIVE", "reject": "REJECTED"}[action])
    elif action == "delete":
        gw.memory.delete(mid)
    else:
        raise PAError("unknown action", code="invalid_request")
    return {"ok": True}


@rpc("memory.delete_all")
def memory_delete_all(gw, p: P, c: ClientInfo) -> Any:
    if not _pw_ok(gw, p):
        raise PAError("password required", code="password_required")
    return {"deleted": gw.memory.delete_all()}


@rpc("memory.about_me")
def memory_about_me(gw, p: P, c: ClientInfo) -> Any:
    return gw.memory.about_me()


@rpc("memory.review")
def memory_review(gw, p: P, c: ClientInfo) -> Any:
    return gw.memory.review()


# ====================================================================== secrets (values only with step-up)
@rpc("secrets.list")
def secrets_list(gw, p: P, c: ClientInfo) -> Any:
    return gw.secrets.list()


@rpc("secrets.create")
def secrets_create(gw, p: P, c: ClientInfo) -> Any:
    return {"id": gw.secrets.create(p.dict("item", True), [str(b) for b in p.list("bindings", max_items=20)])}


@rpc("secrets.update", stepup="secrets")
def secrets_update(gw, p: P, c: ClientInfo) -> Any:
    gw.secrets.update(p.str("id", max_len=100), p.dict("item", True))
    return {"ok": True}


@rpc("secrets.delete", stepup="secrets")
def secrets_delete(gw, p: P, c: ClientInfo) -> Any:
    gw.secrets.delete(p.str("id", max_len=100))
    return {"ok": True}


@rpc("secrets.reveal", stepup="secrets")
def secrets_reveal(gw, p: P, c: ClientInfo) -> Any:
    item = gw.secrets.reveal(p.str("id", max_len=100))
    return {**item, "hide_after_seconds": 20}


@rpc("secrets.copy", stepup="secrets")
def secrets_copy(gw, p: P, c: ClientInfo) -> Any:
    return gw.secrets.copy_to_clipboard(p.str("id", max_len=100), p.int("clear_after", False, 10, 120, 30))


@rpc("secrets.versions")
def secrets_versions(gw, p: P, c: ClientInfo) -> Any:
    return gw.secrets.versions(p.str("id", max_len=100))


@rpc("secrets.restore_version", stepup="secrets")
def secrets_restore_version(gw, p: P, c: ClientInfo) -> Any:
    gw.secrets.restore_version(p.str("id", max_len=100), p.str("version_id", max_len=100))
    return {"ok": True}


@rpc("secrets.set_bindings", stepup="secrets")
def secrets_set_bindings(gw, p: P, c: ClientInfo) -> Any:
    gw.secrets.set_bindings(p.str("id", max_len=100), [str(b) for b in p.list("bindings", max_items=20)])
    return {"ok": True}


@rpc("secrets.generate")
def secrets_generate(gw, p: P, c: ClientInfo) -> Any:
    return {"value": gw.secrets.generate(p.int("length", False, 8, 128, 20), p.bool("lower", default=True), p.bool("upper", default=True),
                                         p.bool("digits", default=True), p.bool("symbols", default=True))}


@rpc("secrets.health")
def secrets_health(gw, p: P, c: ClientInfo) -> Any:
    return gw.secrets.health()


@rpc("secrets.import_csv", stepup="secrets")
def secrets_import_csv(gw, p: P, c: ClientInfo) -> Any:
    path = Path(p.str("path", max_len=2000))
    n = gw.secrets.import_csv(path.read_text("utf-8-sig"))
    if p.bool("secure_delete"):
        size = path.stat().st_size
        with open(path, "r+b") as f:
            f.write(os.urandom(size))
        path.unlink()
    return {"imported": n}


@rpc("secrets.export", stepup="export")
def secrets_export(gw, p: P, c: ClientInfo) -> Any:
    if not _pw_ok(gw, p):
        raise PAError("password required", code="password_required")
    from ..vault.crypto import aead_encrypt, argon2id, b64e
    items = gw.secrets.export_items()
    salt, params, kek = backup_mod.derive_kek(p.str("password", max_len=256))
    blob = aead_encrypt(kek, json.dumps(items).encode(), b"pa/secrets-export")
    dest = Path(p.str("path", max_len=2000))
    dest.write_text(json.dumps({"format": "pa-secrets-1", "salt": b64e(salt), "params": params.to_dict(), "data": b64e(blob)}))
    del argon2id
    return {"count": len(items)}


BINDING_TARGETS = ["web.search"]


@rpc("secrets.binding_targets")
def secrets_binding_targets(gw, p: P, c: ClientInfo) -> Any:
    return BINDING_TARGETS + [f"llm:{m['id']}" for m in gw.llm.models()]


# ====================================================================== connectors
@rpc("connectors.list")
def connectors_list(gw, p: P, c: ClientInfo) -> Any:
    return {"connectors": gw.connectors.list(), "pause_all": gw.connectors.pause_all()}


@rpc("connectors.set")
def connectors_set(gw, p: P, c: ClientInfo) -> Any:
    cid = p.str("connector", max_len=40)
    flags = {k: p.bool(k) for k in ("enabled", "use_chat", "use_missions") if p.opt(k) is not None}
    row = gw.connectors.row(cid)
    turning_on = any(v and not row[k] for k, v in flags.items())
    if turning_on and not row["connected"] and cid == "m365" and not gw.session.has_stepup("connectors"):
        raise StepUpRequired("please confirm it's you", category="connectors")
    gw.connectors.set_flags(cid, **flags)
    return {"ok": True}


@rpc("connectors.pause_all")
def connectors_pause_all(gw, p: P, c: ClientInfo) -> Any:
    gw.connectors.set_pause_all(p.bool("paused", required=True))
    return {"ok": True}


@rpc("connectors.m365_sign_in", stepup="connectors")
def connectors_m365_sign_in(gw, p: P, c: ClientInfo) -> Any:
    return gw.connectors.adapters["m365"].begin_sign_in(p.bool("custom_scheme"))


@rpc("connectors.disconnect", stepup="connectors")
def connectors_disconnect(gw, p: P, c: ClientInfo) -> Any:
    gw.connectors.disconnect(p.str("connector", max_len=40), p.bool("delete_data"))
    return {"ok": True}


# ====================================================================== AI model / voice
@rpc("llm.models")
def llm_models(gw, p: P, c: ClientInfo) -> Any:
    return {"models": gw.llm.models(), "roles": {r: gw.settings.get(f"llm.role.{r}") for r in
                                                  ("fast", "standard", "reasoning", "vision", "embedding", "stt")},
            "builtin_runtime": gw.runtime.available()}


@rpc("llm.discover")
def llm_discover(gw, p: P, c: ClientInfo) -> Any:
    try:
        prov, ep, key = p.str("provider", max_len=20), p.str("endpoint", max_len=500), p.str("api_key", False, 500) or None
        details = gw.llm.discover_details(prov, ep, key)
        return {"models": [d["name"] for d in details], "details": details}
    except Exception as e:  # noqa: BLE001
        raise PAError(f"could not list models: {e}", code="unreachable") from e


@rpc("llm.inspect")
def llm_inspect(gw, p: P, c: ClientInfo) -> Any:
    try:
        return gw.llm.inspect(p.str("provider", max_len=20), p.str("endpoint", max_len=500), p.str("model_name", max_len=200), p.str("api_key", False, 500) or None)
    except Exception as e:  # noqa: BLE001
        raise PAError(f"could not inspect the model: {type(e).__name__}", code="unreachable") from e


@rpc("llm.add")
def llm_add(gw, p: P, c: ClientInfo) -> Any:
    from ..llm.service import endpoint_location
    spec = p.dict("model", True)
    if spec.get("provider") == "builtin":
        path = Path(str(spec.get("path", "")))
        if path.suffix.lower() != ".gguf" or not path.is_file():
            raise PAError("choose a .gguf model file", code="invalid_request")
        from ..llm.runtime import sha256_file
        spec["sha256"] = sha256_file(path)
        spec["size_bytes"] = path.stat().st_size
        spec.setdefault("source", "manual import")
    if spec.get("provider") in ("openai", "ollama") and endpoint_location(str(spec.get("endpoint", ""))) == "remote":
        # a remote model receives chat content: loosening change -> password (spec 5.5)
        if not _pw_ok(gw, p):
            raise PAError("remote models receive your chat content - confirm with your password", code="password_required")
    spec["kind_confirmed"] = bool(spec.get("kind_confirmed")) and _pw_ok(gw, p) if spec.get("kind_confirmed") else False
    gw.llm.check_kind(spec)
    mid = gw.llm.add_model(spec)
    # test it in the background so it becomes usable without a manual step (never blocks the window)
    testing = gw.llm.test_in_background(mid, make_default=p.bool("make_default", default=True)) if p.bool("auto_test", default=True) else False
    return {"id": mid, "sha256": spec.get("sha256"), "testing": testing}


@rpc("llm.remove")
def llm_remove(gw, p: P, c: ClientInfo) -> Any:
    gw.llm.remove_model(p.str("model_id", max_len=100))
    return {"ok": True}


@rpc("llm.test")
def llm_test(gw, p: P, c: ClientInfo) -> Any:
    return gw.llm.test_model(p.str("model_id", max_len=100))


@rpc("llm.set_role")
def llm_set_role(gw, p: P, c: ClientInfo) -> Any:
    role, mid = p.str("role", max_len=20), p.str("model_id", False, 100)
    from ..llm.service import ROLE_KIND
    if role not in ROLE_KIND:
        raise PAError("unknown role", code="invalid_request")
    if mid:
        m = gw.llm.get_model(mid)
        if m["kind"] != ROLE_KIND[role]:
            raise PAError(f"a {m['kind']} model cannot be used for '{role}' (that needs a {ROLE_KIND[role]} model)", code="wrong_kind")
        if not m["tested"]:
            raise PAError("run 'Test model' first - a model must pass before it can be a default", code="model_untested")
    gw.settings.apply({f"llm.role.{role}": mid})
    return {"ok": True}


@rpc("voice.transcribe")
def voice_transcribe(gw, p: P, c: ClientInfo) -> Any:
    if not gw.settings.get("voice.enabled"):
        raise PAError("voice input is turned off", code="disabled")
    audio = base64.b64decode(p.str("audio_b64", max_len=40_000_000))
    return gw.llm.transcribe(audio, p.str("mime", False, 100) or "audio/webm", gw.settings.get("voice.language"))


# ====================================================================== tools & skills
@rpc("web.test_search")
def web_test_search(gw, p: P, c: ClientInfo) -> Any:
    """Settings > Connectors > Web: one harmless search through the real provider path (egress filter, bound key). Never returns the key."""
    from ..tools.base import ToolFailed
    web = gw.connectors.web
    if not web.status()["search_ready"]:
        raise PAError("web search is not set up: choose a provider (Settings > Web Access) and bind its API key to web.search (Secrets)", code="needs_setup")
    t0 = time.time()
    try:
        r = web.search({"query": "weather today", "count": 1}, None)
    except ToolFailed as e:
        gw.audit.write("web.search_test", "egress", ok=False)
        raise PAError(f"the search provider did not answer: {str(e)[:200]}", code="search_failed") from e
    gw.audit.write("web.search_test", "egress", ok=True, provider=gw.settings.get("web.provider"))
    return {"ok": True, "provider": gw.settings.get("web.provider"), "results": len(r.data.get("results", [])), "ms": int((time.time() - t0) * 1000)}


@rpc("tools.catalog")
def tools_catalog(gw, p: P, c: ClientInfo) -> Any:
    disabled = set(gw.settings.get("tools.disabled") or [])
    return {"tools": [{"name": t.name, "connector": t.connector, "side_effect": t.side_effect, "risk": t.risk, "description": t.description,
                       "enabled": t.name not in disabled and (not t.optional_setting or gw.settings.get(t.optional_setting)),
                       "optional_setting": t.optional_setting, "always_approval": t.always_approval,
                       "definition_hash": t.definition_hash} for t in TOOLS],
            "sandbox": gw.sandbox.describe()}


@rpc("skills.list")
def skills_list(gw, p: P, c: ClientInfo) -> Any:
    return gw.skills.list()


@rpc("skills.import")
def skills_import(gw, p: P, c: ClientInfo) -> Any:
    defn = json.loads(Path(p.str("path", max_len=2000)).read_text("utf-8"))
    if not isinstance(defn, dict):
        raise PAError("skill file must be a JSON object", code="invalid_request")
    return gw.skills.propose(defn, source="import")


@rpc("skills.activate", stepup="skills")
def skills_activate(gw, p: P, c: ClientInfo) -> Any:
    sid = p.str("skill_id", max_len=100)
    if gw.skills.needs_password(sid) and not gw.session.has_stepup("skills", require_password=True) and not _pw_ok(gw, p):
        raise PAError("this skill uses write/egress tools - confirm with your password", code="password_required")
    gw.skills.activate(sid)
    return {"ok": True}


@rpc("skills.set_status")
def skills_set_status(gw, p: P, c: ClientInfo) -> Any:
    gw.skills.set_status(p.str("skill_id", max_len=100), p.str("status", max_len=20))
    return {"ok": True}


# ====================================================================== activity / logs / history
@rpc("activity.events")
def activity_events(gw, p: P, c: ClientInfo) -> Any:
    filters = {k: v for k, v in p.dict("filters").items() if k in ("category", "connector", "tool", "mission_id", "policy_decision",
                                                                   "event_type", "severity") and isinstance(v, str)}
    return {"events": gw.audit.read_events(p.int("limit", False, 1, 5000, 500), p.str("since", False, 40) or None,
                                           p.str("until", False, 40) or None, filters),
            "integrity": gw.last_log_verify}


@rpc("activity.what_did_agent_do")
def activity_what(gw, p: P, c: ClientInfo) -> Any:
    since, until = p.str("since", max_len=40), p.str("until", max_len=40)
    evs = gw.audit.read_events(5000, since, until)
    counts: dict[str, int] = {}
    for e in evs:
        counts[e["event_type"]] = counts.get(e["event_type"], 0) + 1
    tools = [e for e in evs if e["event_type"] == "tool.execute"]
    denied = [e for e in evs if e["event_type"] in ("tool.denied", "dlp.blocked")]
    runs = gw.db.all("SELECT id, kind, title, status, started_at FROM runs WHERE started_at BETWEEN ? AND ? ORDER BY started_at", (since, until))
    return {"counts": counts, "tool_calls": [{"tool": e.get("tool"), "at": e["timestamp"], "destination": e.get("destination")} for e in tools][:500],
            "denied": [{"tool": e.get("tool"), "at": e["timestamp"], "code": e.get("code"), "reason": e.get("reason")} for e in denied][:500],
            "runs": runs}


@rpc("logs.verify")
def logs_verify(gw, p: P, c: ClientInfo) -> Any:
    return gw.verify_log()


@rpc("logs.status")
def logs_status(gw, p: P, c: ClientInfo) -> Any:
    return {"path": str(gw.audit.active_path), "segments": [s.name for s in gw.audit.segments()], "siem": gw.siem.status(),
            "integrity": gw.last_log_verify}


@rpc("logs.siem_test", stepup="security_settings")
def logs_siem_test(gw, p: P, c: ClientInfo) -> Any:
    return gw.siem.test_connection()


@rpc("logs.export", stepup="export")
def logs_export(gw, p: P, c: ClientInfo) -> Any:
    dest = Path(p.str("path", max_len=2000))
    evs = gw.audit.read_events(1_000_000)
    dest.write_text("\n".join(json.dumps(e) for e in reversed(evs)) + "\n", "utf-8")
    gw.audit.write("logs.exported", "audit", count=len(evs))
    return {"count": len(evs)}


@rpc("logs.rotate")
def logs_rotate(gw, p: P, c: ClientInfo) -> Any:
    gw.audit.rotate_now()
    return {"ok": True}


@rpc("history.search")
def history_search(gw, p: P, c: ClientInfo) -> Any:
    return gw.history.search(p.str("query", max_len=300), p.int("limit", False, 1, 200, 50), p.list("kinds") or None)


# ====================================================================== emergency stop
@rpc("killswitch.state", touch=False)
def killswitch_state(gw, p: P, c: ClientInfo) -> Any:
    return gw.killswitch.state()


@rpc("killswitch.activate", state="keys")
def killswitch_activate(gw, p: P, c: ClientInfo) -> Any:
    """Always available - even when the UI is locked (spec 5.4.10: kill switch always available)."""
    level = p.str("level", max_len=40)
    if level not in LEVELS:
        raise PAError("unknown level", code="invalid_request")
    gw.killswitch.activate(level, p.str("source", False, 20) or "ui")
    return gw.killswitch.state()


@rpc("killswitch.release")
def killswitch_release(gw, p: P, c: ClientInfo) -> Any:
    if not _pw_ok(gw, p):
        raise PAError("releasing the emergency stop requires your password", code="password_required")
    gw.killswitch.release(p.str("level", False, 40) or None, "ui")
    return gw.killswitch.state()


# ====================================================================== backup / restore
@rpc("backup.status")
def backup_status(gw, p: P, c: ClientInfo) -> Any:
    folder = gw.settings.get("backup.folder")
    files = []
    if folder and Path(folder).exists():
        files = [{"name": x.name, "size": x.stat().st_size, "modified": x.stat().st_mtime}
                 for x in sorted(Path(folder).glob("PersonalAgent-*.pabk"), key=lambda q: q.stat().st_mtime, reverse=True)]
    last = gw.db.scalar("SELECT value FROM meta WHERE key='backup.last'")
    return {"folder": folder, "last": last, "files": files, "scheduled_enabled": gw.backup_kek() is not None,
            "warn": not last or (utcnow() - __import__("pa_common.timeutil", fromlist=["parse_iso"]).parse_iso(last)).days >= 14}


@rpc("backup.run_now")
def backup_run_now(gw, p: P, c: ClientInfo) -> Any:
    folder = p.str("folder", False, 2000) or gw.settings.get("backup.folder")
    if not folder:
        raise PAError("choose a backup folder first", code="invalid_request")
    pw = p.str("password", max_len=256)
    gw.verify_password(pw)
    out = backup_mod.create_backup(gw, pw, Path(folder), bool(gw.settings.get("backup.include_logs")))
    backup_mod.prune(Path(folder), int(gw.settings.get("backup.keep")))
    return {"file": str(out)}


@rpc("backup.enable_schedule")
def backup_enable_schedule(gw, p: P, c: ClientInfo) -> Any:
    gw.enable_scheduled_backups(p.str("password", max_len=256))
    return {"ok": True}


@rpc("backup.verify")
def backup_verify(gw, p: P, c: ClientInfo) -> Any:
    res = backup_mod.verify_backup(Path(p.str("path", max_len=2000)), p.str("password", max_len=256))
    gw.audit.write("backup.verified", "backup", ok=res["ok"])
    return res


@rpc("backup.restore", stepup="backup_restore")
def backup_restore(gw, p: P, c: ClientInfo) -> Any:
    staging = backup_mod.stage_restore(Path(p.str("path", max_len=2000)), p.str("password", max_len=256), gw.paths.root)
    gw.audit.write("backup.restore_staged", "backup", severity="high")
    gw.sign_out("restore staged")
    return {"staged": str(staging), "restart_required": True}


@rpc("backup.restore_signed_out", state="any")
def backup_restore_signed_out(gw, p: P, c: ClientInfo) -> Any:
    """Restore onto a NEW PC (no vault yet): needs only the backup password."""
    if gw.state != "SETUP_REQUIRED":
        raise PAError("sign in and use Settings > Backup & Restore", code="invalid_state")
    staging = backup_mod.stage_restore(Path(p.str("path", max_len=2000)), p.str("password", max_len=256), gw.paths.root)
    return {"staged": str(staging), "restart_required": True}


# ====================================================================== privacy / diagnostics / updates
@rpc("privacy.data_map")
def privacy_data_map(gw, p: P, c: ClientInfo) -> Any:
    root = gw.paths.root

    def size(path: Path) -> int:
        return sum(f.stat().st_size for f in path.rglob("*") if f.is_file()) if path.exists() else 0
    return [
        {"what": "Vault header (wrapped master key)", "where": str(gw.paths.vault_header), "protection": "Argon2id + TPM + AES-256-GCM", "bytes": gw.paths.vault_header.stat().st_size},
        {"what": "Database: chats, tasks, memory, secrets, settings, transcripts", "where": str(gw.paths.db_file), "protection": "SQLCipher AES-256 (key from vault)", "bytes": size(gw.paths.db_dir)},
        {"what": "My Files and agent outputs", "where": str(gw.paths.files_dir), "protection": "AES-256-GCM per file", "bytes": size(gw.paths.files_dir)},
        {"what": "Security log (metadata only)", "where": str(gw.paths.logs_dir), "protection": "HMAC chain; folder ACL", "bytes": size(gw.paths.logs_dir)},
        {"what": "Brute-force counters (not secret)", "where": str(gw.paths.auth_state), "protection": "folder ACL", "bytes": size(gw.paths.state_dir)},
        {"what": "Temporary work folders (deleted after each run)", "where": str(gw.paths.tmp_dir), "protection": "folder ACL", "bytes": size(gw.paths.tmp_dir)},
        {"what": "Data folder", "where": str(root), "protection": "NTFS ACL: you + SYSTEM only", "bytes": size(root)},
    ]


@rpc("privacy.export", stepup="export")
def privacy_export(gw, p: P, c: ClientInfo) -> Any:
    pw = p.str("password", max_len=256)
    gw.verify_password(pw)
    out = backup_mod.create_backup(gw, pw, Path(p.str("folder", max_len=2000)), include_logs=True, label="export")
    gw.audit.write("privacy.exported", "privacy", severity="medium")
    return {"file": str(out)}


@rpc("privacy.delete_everything")
def privacy_delete_everything(gw, p: P, c: ClientInfo) -> Any:
    gw.delete_everything(p.str("password", max_len=256), p.str("confirmation", max_len=40))
    return {"ok": True}


@rpc("diagnostics.health")
def diagnostics_health(gw, p: P, c: ClientInfo) -> Any:
    core = gw.core_supervisor
    return {
        "gateway": {"ok": True, "uptime_s": int(time.time() - gw.started_at), "pid": os.getpid()},
        "core": {"ok": bool(core and (core.inprocess or (core.proc and core.proc.poll() is None))), "mode": "in-process" if core and core.inprocess else "process"},
        "model": {"builtin_running": bool(gw.runtime.proc and gw.runtime.proc.poll() is None), "models": len(gw.llm.models())},
        "outlook_worker": {"running": bool(getattr(gw.connectors.adapters["outlook_local"], "_proc", None))},
        "sandbox": gw.sandbox.describe(),
        "metrics": {
            "tasks": {r["state"]: r["n"] for r in gw.db.all("SELECT state, count(*) n FROM tasks GROUP BY state")},
            "approvals": {r["status"]: r["n"] for r in gw.db.all("SELECT status, count(*) n FROM approvals GROUP BY status")},
            "budget": gw.budgets.summary(), "log": gw.last_log_verify,
        },
    }


@rpc("diagnostics.bundle")
def diagnostics_bundle(gw, p: P, c: ClientInfo) -> Any:
    """Metadata only (no content, no secrets). Preview first; saving writes the same JSON."""
    bundle = {"version": APP_VERSION, "build": BUILD_HASH, "python": sys.version.split()[0], "os": platform.platform(),
              "health": diagnostics_health(gw, p, c), "posture": gw.posture(),
              "settings_changed": [h["key"] for h in gw.settings.history(100)], "recent_errors":
              [e for e in gw.audit.read_events(200) if e.get("severity") in ("medium", "high", "critical")][:100]}
    path = p.str("path", False, 2000)
    if path:
        Path(path).write_text(json.dumps(bundle, indent=1, default=str), "utf-8")
        gw.audit.write("diagnostics.bundle_saved", "diagnostics")
    return bundle


@rpc("about", state="any", touch=False)
def about(gw, p: P, c: ClientInfo) -> Any:
    return {"name": "Personal Agent - Desktop Edition", "version": APP_VERSION, "build": BUILD_HASH,
            "licences": ["cryptography (Apache-2.0/BSD)", "argon2-cffi (MIT)", "SQLCipher (BSD)", "pydantic (MIT)", "httpx (BSD)",
                         "numpy (BSD)", "pypdf (BSD)", "pywin32 (PSF)", "Tauri (MIT/Apache-2.0)", "React (MIT)",
                         "llama.cpp (MIT)", "SecLists common-password list (MIT)"]}


@rpc("updates.status")
def updates_status(gw, p: P, c: ClientInfo) -> Any:
    return {"current": APP_VERSION, "auto_check": gw.settings.get("updates.auto_check"),
            "available": None, "note": "Signed update channel not configured in this build (see PENDING_WORK.md)."}


def _unused() -> None:  # keep imports referenced for linters
    _ = (AuthError, UI)
