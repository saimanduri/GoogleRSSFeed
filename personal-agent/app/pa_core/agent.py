"""Agent loop: mission/chat -> plan -> gateway-controlled tool calls -> verification -> answer.

The loop is deliberately simple and deterministic around the model:
  1. fetch context (built ONLY from the session log) from the gateway
  2. ask the model for ONE JSON action
  3. tool action  -> tools.invoke (the gateway decides; the model never authorises anything)
     final action -> task.finish
Stops on: final answer, step budget, kill switch / cancellation, budget exhaustion, repeated errors.
"""
from __future__ import annotations

import json
import threading
import time
from typing import Any, Callable

from pa_common.errors import PAError

from . import prompts
from .actions import ActionError, parse_action
from .context import label_summary, messages_from_events, needs_summary, summary_cut

MAX_FORMAT_RETRIES = 2
MAX_CONSECUTIVE_ERRORS = 3



def _readable(text: str) -> str:
    """Last resort when the model never produced a valid action: show its words, never raw JSON plumbing."""
    t = (text or "").strip()
    try:
        obj = json.loads(t[t.find("{"):t.rfind("}") + 1]) if "{" in t else None
    except json.JSONDecodeError:
        obj = None
    if isinstance(obj, dict):
        words = obj.get("answer") or obj.get("thought")
        if isinstance(words, str) and words.strip():
            return words.strip()[:20000] + "\n\n(The model did not follow the expected reply format, so no action was taken.)"
    return t[:20000] or "(no answer)"


class Stop(Exception):
    pass


class AgentLoop:
    def __init__(self, client, task_id: str):
        self.c = client
        self.task_id = task_id

    def call(self, method: str, **params: Any) -> Any:
        return self.c.call(method, {"task_id": self.task_id, **params})

    def _check_cancel(self) -> None:
        hb = self.call("task.heartbeat")
        if hb["cancelled"] or hb["state"] not in ("RUNNING", "VERIFYING"):
            raise Stop(hb["state"])

    def _messages(self, ctx: dict[str, Any]) -> list[dict[str, str]]:
        system = prompts.system_prompt(ctx)
        msgs = messages_from_events(ctx["events"])
        return [{"role": "system", "content": system}] + msgs

    def _maybe_summarise(self, ctx: dict[str, Any]) -> dict[str, Any]:
        msgs = self._messages(ctx)
        if not needs_summary(msgs[0]["content"], msgs[1:], ctx["limits"]["context_tokens"]):
            return ctx
        cut = summary_cut(ctx["events"])
        if cut <= 0:
            return ctx
        old = messages_from_events([e for e in ctx["events"] if e["seq"] <= cut])
        res = self.call("llm.complete", messages=[{"role": "system", "content": prompts.SUMMARY_SYSTEM}] + old,
                        role="fast", max_tokens=600, purpose="summary")
        # 39.7: the gateway stores it with the highest sensitivity + source list; limits are re-loaded from
        # trusted storage on the next context fetch (system prompt is rebuilt, never taken from the summary)
        self.call("session.append", kind="summary", content=label_summary(res["text"], ctx["task"]["hwm"]),
                  meta={"covers_to_seq": cut})
        return self.call("task.context")

    def run(self) -> None:
        ctx = self.call("task.context")
        max_steps = int(ctx["limits"]["steps"])
        chat = bool(ctx["task"]["chat_id"])
        format_retries = 0
        errors = 0
        for skill in ctx.get("skills", []):
            if skill.get("name", "").lower() in ctx["task"]["objective"].lower():
                self.call("session.append", kind="skill.used", content=f"using skill {skill['name']}", meta={"skill_id": skill["id"]})
        pre = (ctx.get("mission") or {}).get("prefetch") or []
        if pre and not any(e["kind"] in ("tool.result", "tool.denied") for e in ctx["events"]):
            # The application itself gathers the data first (deterministic, through the normal policy gate); the model then only explains it.
            for item in pre:
                out = self.call("tools.invoke", tool=item["tool"], args=item["args"])
                if out.get("status") in ("unavailable",) and not chat:
                    self.finish("WAITING_FOR_RESOURCE", reason=out.get("reason", "resource unavailable"))
                    return
            ctx = self.call("task.context")
        for step in range(max_steps):
            self._check_cancel()
            ctx = self._maybe_summarise(self.call("task.context") if step else ctx)
            try:
                res = self.call("llm.complete", messages=self._messages(ctx), role="standard", json_mode=True, action_schema=True, stream=chat,
                                max_tokens=4096)
            except PAError as e:
                if e.code in ("no_model", "budget_exhausted", "kill_switch_active", "policy_denied", "unlogged_context"):
                    self.finish("FAILED", reason=_friendly(e))
                    return
                errors += 1
                if errors >= MAX_CONSECUTIVE_ERRORS:
                    self.finish("FAILED", reason=f"model error: {e}")
                    return
                self.c.call("run.step", {"task_id": self.task_id, "type": "retry", "title": "Model error - retrying", "text": str(e)[:500]})
                time.sleep(min(2 ** errors, 10))
                continue
            errors = 0
            try:
                action = parse_action(res["text"])
            except ActionError as e:
                format_retries += 1
                if format_retries > MAX_FORMAT_RETRIES:
                    self.finish("COMPLETED", result=_readable(res["text"]))
                    return
                self.call("session.append", kind="note",
                          content=f"[format] Your last reply was not valid ({e}). Reply with exactly one JSON object as instructed.")
                continue
            thought = str(action.get("thought", ""))[:1000]
            if thought:
                self.c.call("run.step", {"task_id": self.task_id, "type": "thought", "title": "Reasoning", "text": thought})
            if action["action"] == "final":
                self.finish("COMPLETED", result=action["answer"])
                return
            out = self.call("tools.invoke", tool=action["tool"], args=action.get("args") or {})
            status = out.get("status")
            if status == "ok":
                continue
            if out.get("code") in ("kill_switch_active", "cancelled", "task_not_running"):
                raise Stop(out.get("code"))
            if out.get("code") == "budget_exhausted":
                self.finish("FAILED", reason=f"Budget exhausted: {out.get('reason')}")
                return
            if (out.get("waiting_resource") or status == "unavailable") and not chat:
                self.finish("WAITING_FOR_RESOURCE", reason=out.get("reason", "resource unavailable"))
                return
            if status in ("error", "unavailable"):
                # the gateway already logged a tool.denied/tool result; add a note so the model sees the error
                self.call("session.append", kind="note", content=f"[{action['tool']}] error: {out.get('reason', '')[:500]}")
            # denied: the gateway appended a tool.denied event the model will see next step
        self.finish("FAILED" if not chat else "COMPLETED",
                    result="" if not chat else "I stopped because this request reached its step limit. Try narrowing it down.",
                    reason="step limit reached")

    def finish(self, status: str, result: str = "", reason: str = "") -> None:
        self.call("task.finish", status=status, result=result, reason=reason)


def _friendly(e: PAError) -> str:
    return {"no_model": "No AI model is configured. Add one in Settings > AI Model.",
            "budget_exhausted": f"Budget exhausted: {e}",
            "kill_switch_active": "The agent was stopped (emergency stop)."}.get(e.code, str(e))


class CoreRuntime:
    """Worker pool. Each worker has its own gateway connection and leases one task at a time."""

    def __init__(self, client_factory: Callable[[str], Any], stop: threading.Event | None = None, workers: int = 5):
        self.client_factory = client_factory
        self.stop = stop or threading.Event()
        self.workers = workers

    def run(self) -> None:
        threads = [threading.Thread(target=self._worker, args=(f"w{i}",), daemon=True, name=f"core-w{i}")
                   for i in range(self.workers)]
        for t in threads:
            t.start()
        while not self.stop.is_set() and any(t.is_alive() for t in threads):
            self.stop.wait(1)

    def _worker(self, wid: str) -> None:
        client = self.client_factory(wid)
        while not self.stop.is_set():
            try:
                job = client.call("work.next", {"wait": 5})
            except PAError as e:
                if e.code in ("locked", "disconnected"):
                    self.stop.wait(2)
                    if e.code == "disconnected":
                        return
                    continue
                self.stop.wait(1)
                continue
            if not job:
                continue
            loop = AgentLoop(client, job["task_id"])
            try:
                loop.run()
            except Stop:
                pass
            except PAError as e:
                try:
                    loop.finish("FAILED", reason=f"{e.code}: {e}")
                except PAError:
                    pass
            except Exception as e:  # noqa: BLE001
                try:
                    loop.finish("FAILED", reason=f"internal error: {type(e).__name__}")
                except PAError:
                    pass
