"""Parsing the model's JSON action. Anything that is not a well-formed action is treated as a final answer
text only when it contains no JSON at all; malformed JSON gets one corrective note."""
from __future__ import annotations

import json
from typing import Any


class ActionError(ValueError):
    pass


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
        return obj
    if act == "tool":
        if not isinstance(obj.get("tool"), str):
            raise ActionError("tool action needs a 'tool' name")
        if not isinstance(obj.get("args", {}), dict):
            raise ActionError("'args' must be an object")
        obj.setdefault("args", {})
        return obj
    if "answer" in obj and isinstance(obj["answer"], str):
        return {"action": "final", "answer": obj["answer"]}
    raise ActionError("action must be 'tool' or 'final'")
