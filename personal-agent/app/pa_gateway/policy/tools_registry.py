"""Tool definitions (spec 7.1). The definition hash is pinned: a changed definition is a new tool version.

Argument schemas are pydantic models with strict constraints; the gateway validates arguments
before anything else touches them.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

NONE = "NONE"
INTERNAL_WRITE = "INTERNAL_WRITE"
EXTERNAL_WRITE = "EXTERNAL_WRITE"
EGRESS = "EGRESS"


class _Args(BaseModel):
    model_config = ConfigDict(extra="forbid", str_max_length=200_000)


class WebSearchArgs(_Args):
    query: str = Field(min_length=1, max_length=400)
    count: int = Field(default=5, ge=1, le=10)


class WebFetchArgs(_Args):
    url: str = Field(min_length=8, max_length=2048)
    max_chars: int = Field(default=20_000, ge=500, le=200_000)


class MailSearchArgs(_Args):
    query: str = Field(default="", max_length=400)
    folder: Optional[str] = Field(default=None, max_length=200)
    since: Optional[str] = Field(default=None, max_length=40)
    until: Optional[str] = Field(default=None, max_length=40)
    max_results: int = Field(default=10, ge=1, le=50)


class MessageIdArgs(_Args):
    message_id: str = Field(min_length=1, max_length=1024)


class AttachmentArgs(_Args):
    message_id: str = Field(min_length=1, max_length=1024)
    attachment_id: str = Field(min_length=1, max_length=1024)


class CalendarArgs(_Args):
    start: str = Field(max_length=40)
    end: str = Field(max_length=40)
    max_results: int = Field(default=25, ge=1, le=100)


class EmptyArgs(_Args):
    pass


class DraftArgs(_Args):
    to: list[str] = Field(min_length=1, max_length=20)
    cc: list[str] = Field(default_factory=list, max_length=20)
    subject: str = Field(max_length=500)
    body: str = Field(max_length=100_000)
    reply_to_message_id: Optional[str] = Field(default=None, max_length=1024)


class FilesListArgs(_Args):
    folder: str = Field(default="/", max_length=500)
    query: str = Field(default="", max_length=200)


class FilesReadArgs(_Args):
    file_id: str = Field(min_length=3, max_length=100)
    max_chars: int = Field(default=20_000, ge=100, le=200_000)


class FilesWriteArgs(_Args):
    name: str = Field(min_length=1, max_length=200)
    content: str = Field(max_length=2_000_000)
    folder: str = Field(default="/Agent output", max_length=500)


class PythonArgs(_Args):
    code: str = Field(min_length=1, max_length=100_000)
    file_ids: list[str] = Field(default_factory=list, max_length=20)
    timeout_seconds: int = Field(default=60, ge=5, le=600)


class NotifyArgs(_Args):
    title: str = Field(min_length=1, max_length=120)
    message: str = Field(default="", max_length=500)


class HistorySearchArgs(_Args):
    query: str = Field(min_length=1, max_length=300)
    limit: int = Field(default=10, ge=1, le=50)


class MemorySearchArgs(_Args):
    query: str = Field(min_length=1, max_length=500)
    limit: int = Field(default=8, ge=1, le=30)


class MemoryProposeArgs(_Args):
    content: str = Field(min_length=3, max_length=2000)
    type: Literal["preference", "semantic", "episodic", "procedural", "project"] = "preference"
    importance: Literal["low", "normal", "high"] = "normal"


class ReminderArgs(_Args):
    text: str = Field(min_length=1, max_length=500)
    due_at: str = Field(min_length=10, max_length=40, description="ISO date/time in the user's local time, e.g. 2026-10-05T09:00")
    timezone: Optional[str] = Field(default=None, max_length=64)


class MissionProposeArgs(_Args):
    name: str = Field(min_length=1, max_length=120)
    objective: str = Field(min_length=1, max_length=4000)
    schedule: str = Field(min_length=1, max_length=200, description="plain words or cron, e.g. 'every weekday at 07:30'")
    tools: list[str] = Field(default_factory=list, max_length=30)


class SubtaskArgs(_Args):
    objective: str = Field(min_length=1, max_length=4000)
    public_research: bool = False


class SkillProposeArgs(_Args):
    name: str = Field(min_length=1, max_length=80)
    description: str = Field(max_length=2000)
    steps: list[dict] = Field(min_length=1, max_length=30)
    tools: list[str] = Field(min_length=1, max_length=20)


@dataclass(frozen=True)
class ToolDef:
    name: str
    version: str
    connector: str
    side_effect: str
    risk: str
    output_source: str  # mail | files | web | internal | none
    args: type[_Args]
    description: str
    timeout: int = 60
    rate_per_minute: int = 30
    bound_secret: str | None = None
    always_approval: bool = False
    optional_setting: str | None = None  # tool exists only when this setting is on

    @property
    def definition_hash(self) -> str:
        body = {"name": self.name, "version": self.version, "connector": self.connector, "side_effect": self.side_effect,
                "risk": self.risk, "schema": self.args.model_json_schema(), "always_approval": self.always_approval}
        return "sha256:" + hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest()

    def for_llm(self) -> dict:
        schema = self.args.model_json_schema()
        props = {k: {kk: vv for kk, vv in v.items() if kk in ("type", "description", "enum", "default", "items")}
                 for k, v in schema.get("properties", {}).items()}
        return {"name": self.name, "description": self.description, "args": props, "required": schema.get("required", [])}


TOOLS: list[ToolDef] = [
    ToolDef("web.search", "1", "web", EGRESS, "medium", "web", WebSearchArgs,
            "Search the public web. The query leaves this PC.", bound_secret="web.search"),
    ToolDef("web.fetch", "1", "web", EGRESS, "medium", "web", WebFetchArgs,
            "Fetch a public web page (https) and return its text."),
    ToolDef("outlook_local.list_folders", "1", "outlook_local", NONE, "low", "mail", EmptyArgs,
            "List folders in classic Outlook on this PC."),
    ToolDef("outlook_local.search", "1", "outlook_local", NONE, "low", "mail", MailSearchArgs,
            "Search classic Outlook mail (incl. archives if enabled). Read-only."),
    ToolDef("outlook_local.get_message", "1", "outlook_local", NONE, "low", "mail", MessageIdArgs,
            "Read one Outlook message as plain text."),
    ToolDef("outlook_local.get_attachment", "1", "outlook_local", INTERNAL_WRITE, "low", "mail", AttachmentArgs,
            "Copy an Outlook attachment into My Files (quarantined and scanned first)."),
    ToolDef("outlook_local.calendar_read", "1", "outlook_local", NONE, "low", "mail", CalendarArgs,
            "Read Outlook calendar entries in a date range."),
    ToolDef("outlook_local.create_draft", "1", "outlook_local", EXTERNAL_WRITE, "high", "mail", DraftArgs,
            "Create an AI-marked draft in Outlook (never sends).", always_approval=True, optional_setting="outlook.enable_drafts"),
    ToolDef("m365.search_mail", "1", "m365", NONE, "low", "mail", MailSearchArgs,
            "Search Microsoft 365 mail. Read-only."),
    ToolDef("m365.get_message", "1", "m365", NONE, "low", "mail", MessageIdArgs,
            "Read one Microsoft 365 message as plain text."),
    ToolDef("m365.get_attachment", "1", "m365", INTERNAL_WRITE, "low", "mail", AttachmentArgs,
            "Copy a Microsoft 365 attachment into My Files (quarantined and scanned first)."),
    ToolDef("m365.calendar_read", "1", "m365", NONE, "low", "mail", CalendarArgs,
            "Read Microsoft 365 calendar events in a date range."),
    ToolDef("m365.create_draft", "1", "m365", EXTERNAL_WRITE, "high", "mail", DraftArgs,
            "Create an AI-marked draft in Microsoft 365 (never sends).", always_approval=True,
            optional_setting="m365.enable_drafts"),
    ToolDef("m365.send_mail", "1", "m365", EXTERNAL_WRITE, "high", "mail", DraftArgs,
            "Propose an email to send. It is sent only after you approve the exact message.", always_approval=True,
            optional_setting="m365.enable_send"),
    ToolDef("files.list", "1", "files", NONE, "low", "files", FilesListArgs, "List files in My Files."),
    ToolDef("files.read", "1", "files", NONE, "low", "files", FilesReadArgs, "Read the extracted text of a file in My Files."),
    ToolDef("files.write", "1", "files", INTERNAL_WRITE, "low", "internal", FilesWriteArgs,
            "Save a text/markdown file into My Files."),
    ToolDef("python.run", "1", "sandbox", NONE, "medium", "internal", PythonArgs,
            "Run Python in an isolated sandbox with no network. Granted files are available read-only in ./input.",
            timeout=600, rate_per_minute=10, optional_setting="tools.python_enabled"),
    ToolDef("notify.user", "1", "notify", INTERNAL_WRITE, "low", "none", NotifyArgs, "Show the user a notification."),
    ToolDef("history.search", "1", "history", NONE, "low", "internal", HistorySearchArgs,
            "Full-text search over your past chats, task results and files."),
    ToolDef("memory.search", "1", "memory", NONE, "low", "internal", MemorySearchArgs,
            "Search what the agent remembers about the user."),
    ToolDef("memory.propose", "1", "memory", INTERNAL_WRITE, "low", "none", MemoryProposeArgs,
            "Propose something to remember. The user confirms important memories."),
    ToolDef("reminders.propose", "1", "reminders", INTERNAL_WRITE, "low", "none", ReminderArgs,
            "Propose a reminder. It is scheduled after the user confirms.", always_approval=True),
    ToolDef("missions.propose", "1", "missions", INTERNAL_WRITE, "low", "none", MissionProposeArgs,
            "Propose a mission/routine as a DRAFT for the user to review and activate."),
    ToolDef("skills.propose", "1", "skills", INTERNAL_WRITE, "low", "none", SkillProposeArgs,
            "Propose a reusable skill (declarative steps). The user reviews it."),
    ToolDef("agent.subtask", "1", "agent", NONE, "low", "internal", SubtaskArgs,
            "Run a focused sub-task with no more rights than this task and wait for its result.", timeout=1800),
    ToolDef("time.now", "1", "builtin", NONE, "low", "none", EmptyArgs, "Current local date and time."),
]

BY_NAME: dict[str, ToolDef] = {t.name: t for t in TOOLS}

# Domains that belong to connectors: while a connector is OFF, web.fetch to them is denied (spec 8.3.5)
CONNECTOR_DOMAINS = {
    "m365": ("graph.microsoft.com", "outlook.office.com", "outlook.office365.com", "outlook.live.com",
             "login.microsoftonline.com", "substrate.office.com"),
    "outlook_local": ("outlook.office.com", "outlook.office365.com"),
}
