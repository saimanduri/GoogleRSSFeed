"""Parsing the model's JSON action. Anything that is not a well-formed action is treated as a final answer
text only when it contains no JSON at all; malformed JSON gets one corrective note."""
from __future__ import annotations

import json
import re
from typing import Any


class ActionError(ValueError):
    pass


_TOOL_NAME = re.compile(r"[a-z][a-z0-9_]*\.[a-z][a-z0-9_.]*")


def parse_action(text: str) -> dict[str, Any]:
    s = text.strip()
    if s.startswith("```"):
        s = s.strip("`")
        s = s[s.find("{"):] if "{" in s else s
    start, end = s.find("{"), s.rfind("}")
    if start < 0 or end <= start:
        if s:
            return {"action": "final", "answer": s}
        raise ActionError("empty reply")
    try:
        obj = json.loads(s[start:end + 1])
    except json.JSONDecodeError as e:
        raise ActionError(f"invalid JSON: {e.msg}") from e
    if not isinstance(obj, dict):
        raise ActionError("reply must be a JSON object")
    act = obj.get("action")
    if act == "final":
        ans = obj.get("answer")
        if not isinstance(ans, str):
            raise ActionError("final action needs an 'answer' string")
        if ans.strip() in ("", "...", "\u2026", "<answer>"):
            raise ActionError("the answer is empty - write the complete answer, not a placeholder")
        return obj
    # Small models often vary the shape: {"action": "reminders.propose", "tool": "reminders.propose", ...},
    # {"tool": "x.y", "arguments": {...}} or {"action": "x.y", "args": {...}}. Accept those as tool calls.
    tool = obj.get("tool") if isinstance(obj.get("tool"), str) else obj.get("tool_name") if isinstance(obj.get("tool_name"), str) else None
    if tool is None and isinstance(act, str) and _TOOL_NAME.fullmatch(act):
        tool = act
    if tool is not None and (act in (None, "tool", "call", "tool_call", "use_tool", "invoke") or act == tool or isinstance(act, str)):
        args = obj.get("args", obj.get("arguments", obj.get("parameters", {})))
        if isinstance(args, str):
            try:
                args = json.loads(args)
            except json.JSONDecodeError as e:
                raise ActionError("'args' must be an object") from e
        if not isinstance(args, dict):
            raise ActionError("'args' must be an object")
        out = {"action": "tool", "tool": tool, "args": args}
        if "thought" in obj:
            out["thought"] = obj["thought"]
        return out
    if act == "tool":
        raise ActionError("tool action needs a 'tool' name")
    if "answer" in obj and isinstance(obj["answer"], str):
        return {"action": "final", "answer": obj["answer"]}
    raise ActionError("action must be 'tool' or 'final'")
