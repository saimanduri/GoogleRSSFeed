from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable


@dataclass
class ExecContext:
    task: dict[str, Any]
    run_id: str | None
    session_id: str | None
    approved: bool
    cancelled: Callable[[], bool]
    hwm: int = 0


@dataclass
class ToolResult:
    content: str
    sensitivity: int
    source: str
    data: dict[str, Any] = field(default_factory=dict)
    bytes_out: int = 0
    destination: str | None = None
    hidden: list[dict] = field(default_factory=list)
    trust: str = "UNTRUSTED"


class ToolUnavailable(Exception):
    """The resource is not available right now (Outlook closed, token expired...): task WAITS."""


class ToolFailed(Exception):
    pass
