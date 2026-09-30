"""Methods available to pa-core (spec 39.3). pa-core gets NO settings, secrets, connectors, skill
activation, approvals.approve or kill-switch methods. Every call is bound to a task the calling
worker has leased (caller identity comes from the gateway's own records)."""
from __future__ import annotations

import json
from typing import Any

from pa_common.errors import PAError
from pa_common.sensitivity import Sensitivity, Trust
from pa_common.timeutil import now_iso

from .dispatch import CORE, ClientInfo, P, rpc

ALLOWED_APPEND_KINDS = {"summary", "plan", "skill.used", "note"}


def _worker(p: P, client: ClientInfo) -> str:
    wid = p.str("worker", max_len=100)
    return f"{client.client_id}:{wid}"


@rpc("work.next", roles=(CORE,), state="keys", touch=False)
def work_next(gw, p: P, c: ClientInfo) -> Any:
    slots = int(gw.settings.get("budget.concurrent_tasks"))
    t = gw.tasks.claim(_worker(p, c), slots, gw.killswitch.agent_blocked(), timeout=float(p.int("wait", False, 0, 30, 20)))
    return {"task_id": t["id"]} if t else None


@rpc("task.context", roles=(CORE,), state="keys", touch=False)
def task_context(gw, p: P, c: ClientInfo) -> Any:
    t = gw.tasks.require_owner(p.str("task_id", max_len=100), _worker(p, c))
    session = t["session_id"]
    events = [e for e in gw.sessionlog.events(session) if e["kind"] not in ("llm.request",)]
    mission = None
    if t.get("mission_id"):
        m = gw.missions.get(t["mission_id"])
        mission = {"name": m["name"], "output_format": m["output_format"], "kind": m["kind"]}
    limits = gw.budgets.task_limits(t)
    return {
        "task": {"id": t["id"], "objective": t["objective"], "trigger": t["trigger_type"], "chat_id": t.get("chat_id"),
                 "mission_id": t.get("mission_id"), "depth": t["depth"], "hwm": Sensitivity(int(t["hwm"])).name,
                 "session_id": session, "run_id": t.get("run_id")},
        "events": [{"seq": e["seq"], "kind": e["kind"], "role": e["role"], "content": e["content"], "source": e["source"],
                    "trust": e["trust"], "sensitivity": Sensitivity(int(e["sensitivity"])).name, "meta": e["meta"]} for e in events],
        "tools": gw.tools.available_tools(t),
        "preferences": [{"content": m["content"], "trust": m["trust"]} for m in gw.memory.preferences_for_prompt()]
        if gw.settings.get("memory.enabled") else [],
        "skills": gw.skills.active_for_prompt(),
        "mission": mission,
        "limits": {"steps": int(limits.get("steps", 25)), "context_tokens": int(gw.settings.get("llm.context_tokens"))},
        "now": now_iso(),
    }


@rpc("llm.complete", roles=(CORE,), state="keys", touch=False)
def llm_complete(gw, p: P, c: ClientInfo) -> Any:
    t = gw.tasks.require_owner(p.str("task_id", max_len=100), _worker(p, c))
    messages = p.list("messages", required=True, max_items=500)
    for m in messages:
        if not isinstance(m, dict) or m.get("role") not in ("system", "user", "assistant") or not isinstance(m.get("content"), str):
            raise PAError("bad message", code="invalid_request")
    role = p.str("role", False, 20, "standard")
    run_id = t.get("run_id")
    gw.runs.step(run_id, "llm", f"Thinking ({role} model)", "done", {"messages": len(messages)})
    res = gw.llm.complete(task=t, session_ids=[t["session_id"]], messages=messages, role=role,
                          json_mode=p.bool("json_mode"), max_tokens=p.int("max_tokens", False, 16, 16000, 2048),
                          stream_to_run=run_id if p.bool("stream") else None, interactive=bool(t.get("chat_id")),
                          purpose="summary" if p.str("purpose", False, 20) == "summary" else "agent")
    gw.budgets.add(t["id"], "steps")
    return res


@rpc("tools.invoke", roles=(CORE,), state="keys", touch=False)
def tools_invoke(gw, p: P, c: ClientInfo) -> Any:
    return gw.tools.invoke(_worker(p, c), p.str("task_id", max_len=100), p.str("tool", max_len=100), p.dict("args"))


@rpc("session.append", roles=(CORE,), state="keys", touch=False)
def session_append(gw, p: P, c: ClientInfo) -> Any:
    t = gw.tasks.require_owner(p.str("task_id", max_len=100), _worker(p, c))
    kind = p.str("kind", max_len=40)
    if kind not in ALLOWED_APPEND_KINDS:
        raise PAError("pa-core may not append this kind of event", code="forbidden")
    content = p.str("content", max_len=200_000)
    session = t["session_id"]
    meta = p.dict("meta")
    sens = int(gw.sessionlog.hwm(session)) if kind == "summary" else 0
    if kind == "summary":
        # 39.7: a summary keeps the highest sensitivity and the full source list of what it replaces
        meta = {"covers_to_seq": int(meta.get("covers_to_seq", 0)), "sources": gw.sessionlog.sources(session)}
    if kind == "skill.used":
        meta = {"skill_id": str(meta.get("skill_id", ""))[:60]}
        gw.skills.mark_used(meta["skill_id"])
    ev = gw.sessionlog.append(session, kind, content, role="assistant" if kind == "summary" else "system", source="model",
                              trust=Trust.INFERRED, sensitivity=sens, meta=meta)
    if kind == "plan":
        gw.runs.step(t.get("run_id"), "plan", "Plan", "done", {"plan": content[:2000]})
    if kind == "summary":
        gw.runs.step(t.get("run_id"), "summary", "Summarised earlier conversation", "done",
                     {"sensitivity": Sensitivity(sens).name, "sources": meta["sources"]})
    return {"seq": ev["seq"]}


@rpc("run.step", roles=(CORE,), state="keys", touch=False)
def run_step(gw, p: P, c: ClientInfo) -> Any:
    t = gw.tasks.require_owner(p.str("task_id", max_len=100), _worker(p, c))
    typ = p.str("type", max_len=30)
    if typ not in ("thought", "plan", "info", "retry", "error"):
        raise PAError("bad step type", code="invalid_request")
    sid = gw.runs.step(t.get("run_id"), typ, p.str("title", max_len=300), "done", {"text": p.str("text", False, 4000)})
    return {"step_id": sid}


@rpc("task.heartbeat", roles=(CORE,), state="keys", touch=False)
def task_heartbeat(gw, p: P, c: ClientInfo) -> Any:
    gw.tasks.heartbeat(p.str("task_id", max_len=100), _worker(p, c))
    t = gw.tasks.get(p.str("task_id", max_len=100))
    return {"cancelled": gw.tasks.is_cancelled(t["id"]) or gw.killswitch.agent_blocked(), "state": t["state"]}


@rpc("task.finish", roles=(CORE,), state="keys", touch=False)
def task_finish(gw, p: P, c: ClientInfo) -> Any:
    t = gw.tasks.require_owner(p.str("task_id", max_len=100), _worker(p, c))
    status = p.str("status", max_len=30)
    if status not in ("COMPLETED", "FAILED", "WAITING_FOR_RESOURCE", "TIMED_OUT"):
        raise PAError("bad status", code="invalid_request")
    result = p.str("result", False, 200_000)
    reason = p.str("reason", False, 1000)
    return finish_task(gw, t, status, result, reason)


def finish_task(gw, t: dict[str, Any], status: str, result: str, reason: str) -> Any:
    run_id = t.get("run_id")
    hwm = int((gw.tasks.get(t["id"]) or t)["hwm"])
    sources = gw.sessionlog.sources(t["session_id"])
    if status == "COMPLETED" and result:
        gw.sessionlog.append(t["session_id"], "assistant.message", result, role="assistant", source="model",
                             trust=Trust.INFERRED, sensitivity=hwm)
        if t.get("chat_id"):
            from pa_common.ids import new_id
            mid = new_id("msg")
            gw.db.insert("chat_messages", {"id": mid, "chat_id": t["chat_id"], "role": "assistant", "content": result,
                                           "run_id": run_id, "sources_json": sources, "sensitivity": hwm, "created_at": now_iso()})
            chat = gw.db.one("SELECT hwm, sources_json FROM chats WHERE id=?", (t["chat_id"],))
            merged = sorted(set(json.loads(chat["sources_json"] or "[]")) | set(sources))
            gw.db.update("chats", "id", t["chat_id"], {"updated_at": now_iso(), "hwm": max(int(chat["hwm"]), hwm), "sources_json": merged})
            gw.history.index("chat", mid, "Assistant reply", result, now_iso())
            gw.emit("chat.message", {"chat_id": t["chat_id"], "message_id": mid, "run_id": run_id})
        if t.get("mission_id") and not t.get("parent_task_id"):
            m = gw.missions.get(t["mission_id"])
            f = gw.files.ingest(name=f"{m['name']} {now_iso()[:16].replace(':', '')}.md", data=result.encode("utf-8"),
                                source="agent", sensitivity=hwm, folder="/Mission outputs", run_async=False)
            gw.home_event("mission_done", "info", f"{m['name']} finished", "Output saved to My Files.", f["id"])
            if m["notification_level"] != "silent":
                gw.notify("mission", f"{m['name']} finished", result[:200], hwm, "missions", m["id"])
    if status in ("FAILED", "WAITING_FOR_RESOURCE") and t.get("chat_id") and not t.get("parent_task_id"):
        from pa_common.ids import new_id
        mid = new_id("msg")
        gw.db.insert("chat_messages", {"id": mid, "chat_id": t["chat_id"], "role": "assistant", "content": f"\u26a0 {reason or status}",
                                       "run_id": run_id, "sensitivity": 0, "created_at": now_iso()})
        gw.emit("chat.message", {"chat_id": t["chat_id"], "message_id": mid, "run_id": run_id})
    if status == "FAILED" and t.get("mission_id"):
        gw.home_event("mission_failed", "medium", "A mission run failed", reason[:300], t["mission_id"])
    if status == "WAITING_FOR_RESOURCE":
        gw.home_event("task_waiting", "info", "A task is waiting", reason[:300], t["id"])
    gw.tasks.transition(t["id"], status, reason[:300] or status.lower(), result=result[:200_000] if result else None,
                        lease_owner=None)
    if run_id:
        gw.runs.finish(run_id, {"COMPLETED": "COMPLETED", "FAILED": "FAILED", "WAITING_FOR_RESOURCE": "WAITING",
                                "TIMED_OUT": "FAILED"}[status], summary=result or reason, hwm=hwm, sources=sources)
    return {"ok": True}
