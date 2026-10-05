"""Tool definitions (spec 7.1). The definition hash is pinned: a changed definition is a new tool version.

Argument schemas are pydantic models with strict constraints; the gateway validates arguments
before anything else touches them.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

NONE = "NONE"
INTERNAL_WRITE = "INTERNAL_WRITE"
EXTERNAL_WRITE = "EXTERNAL_WRITE"
EGRESS = "EGRESS"


class _Args(BaseModel):
    model_config = ConfigDict(extra="forbid", str_max_length=200_000)


class WebSearchArgs(_Args):
    query: str = Field(min_length=1, max_length=400)
    count: int = Field(default=5, ge=1, le=10)
    # optional filters (Exa supports all; Brave: recent_days, country; Tavily: domains, recent_days)
    category: Optional[Literal["news", "company", "financial report", "pdf", "research paper", "github", "personal site", "people"]] = None
    recent_days: Optional[int] = Field(default=None, ge=1, le=3650)
    include_domains: list[str] = Field(default_factory=list, max_length=10)
    exclude_domains: list[str] = Field(default_factory=list, max_length=10)
    mode: Optional[Literal["auto", "fast", "deep"]] = None
    country: Optional[str] = Field(default=None, pattern=r"^[A-Za-z]{2}$")
    longer: bool = False

    @field_validator("include_domains", "exclude_domains")
    @classmethod
    def _domains(cls, v: list[str]) -> list[str]:
        out = []
        for d in v:
            d = d.strip().lower()
            if not re.fullmatch(r"[a-z0-9.-]{1,253}", d) or "." not in d:
                raise ValueError(f"not a domain name: {d[:60]}")
            out.append(d)
        return out


class WebReadArgs(_Args):
    urls: list[str] = Field(min_length=1, max_length=10)
    max_chars: int = Field(default=8000, ge=500, le=50_000)
    subpages: int = Field(default=0, ge=0, le=10)
    subpage_target: list[str] = Field(default_factory=list, max_length=5)
    live: bool = False
    focus: Optional[str] = Field(default=None, max_length=200)

    @field_validator("urls")
    @classmethod
    def _urls(cls, v: list[str]) -> list[str]:
        for u in v:
            if not (8 <= len(u) <= 2048) or not u.lower().startswith(("https://", "http://")):
                raise ValueError("each url must start with https:// (max 2048 characters)")
        return v

    @field_validator("subpage_target")
    @classmethod
    def _targets(cls, v: list[str]) -> list[str]:
        if any(len(x) > 60 for x in v):
            raise ValueError("subpage_target words must be short")
        return v


class WebAnswerArgs(_Args):
    query: str = Field(min_length=3, max_length=400)


class WebResearchArgs(_Args):
    instructions: str = Field(min_length=10, max_length=4000)
    thorough: bool = False
    max_minutes: int = Field(default=8, ge=1, le=20)


class WebFetchArgs(_Args):
    url: str = Field(min_length=8, max_length=2048)
    max_chars: int = Field(default=20_000, ge=500, le=200_000)


class MailSearchArgs(_Args):
    query: str = Field(default="", max_length=400)
    folder: Optional[str] = Field(default=None, max_length=200)
    since: Optional[str] = Field(default=None, max_length=40)
    until: Optional[str] = Field(default=None, max_length=40)
    max_results: int = Field(default=10, ge=1, le=50)


class OutlookSearchArgs(MailSearchArgs):
    scope: Literal["inbox", "sent", "all"] = "inbox"      # inbox = Inbox + its sub-folders
    unread_only: bool = False
    since_minutes: Optional[int] = Field(default=None, ge=1, le=525_600)
    sender: Optional[str] = Field(default=None, max_length=200)


class OutlookDigestArgs(_Args):
    since: Optional[str] = Field(default=None, max_length=40)
    until: Optional[str] = Field(default=None, max_length=40)
    since_minutes: Optional[int] = Field(default=None, ge=1, le=525_600, description="only mail from the last N minutes")
    scope: Literal["inbox", "sent", "all"] = "inbox"
    unread_only: bool = False
    only_flagged: bool = Field(default=False, description="only mail with approval wording, a deadline, urgency or a question")
    unanswered_only: bool = Field(default=False, description="hide mail you already replied to")
    only_flag: Optional[Literal["approval", "deadline", "urgent", "question"]] = Field(default=None, description="only mail carrying this flag")
    deadline_within_days: Optional[int] = Field(default=None, ge=0, le=60, description="only mail whose DEADLINE is overdue or within this many days from today")
    sender: Optional[str] = Field(default=None, max_length=200)
    query: str = Field(default="", max_length=400)
    folder: Optional[str] = Field(default=None, max_length=200)
    any_of: list[str] = Field(default_factory=list, max_length=8, description="only mail containing at least one of these words (subject or body)")
    scan_limit: int = Field(default=0, ge=0, le=300, description="read up to this many messages (newest first) before applying only_flagged / unanswered_only")
    max_results: int = Field(default=30, ge=1, le=50)


class OutlookStatsArgs(_Args):
    since: Optional[str] = Field(default=None, max_length=40)
    until: Optional[str] = Field(default=None, max_length=40)
    since_minutes: Optional[int] = Field(default=None, ge=1, le=525_600)
    scope: Literal["inbox", "sent", "all"] = "inbox"
    unread_only: bool = False
    top_senders: int = Field(default=10, ge=1, le=25)


class AwaitingReplyArgs(_Args):
    days: int = Field(default=7, ge=1, le=60)
    min_age_hours: int = Field(default=24, ge=0, le=720)
    max_results: int = Field(default=25, ge=1, le=50)


class LocalFileArgs(_Args):
    file_id: str = Field(min_length=3, max_length=64, description="id of a local file or folder shared with this chat (from the shared list)")
    path: Optional[str] = Field(default=None, max_length=400, description="for a FOLDER: path of the file inside the folder, e.g. 'reports\\2026.xlsx'")
    sheet: Optional[str] = Field(default=None, max_length=120)


class LocalBrowseArgs(_Args):
    grant_id: str = Field(min_length=3, max_length=64, description="id of a shared FOLDER")
    subpath: Optional[str] = Field(default=None, max_length=400, description="subfolder to list; needs the user's approval for subfolders")


class LocalInspectArgs(LocalFileArgs):
    sample_rows: int = Field(default=5, ge=1, le=20)


class LocalRowsArgs(LocalFileArgs):
    start: int = Field(default=0, ge=0, le=50_000_000)
    count: int = Field(default=20, ge=1, le=200)
    columns: list[str] = Field(default_factory=list, max_length=40)


class LocalFilter(_Args):
    col: str = Field(min_length=1, max_length=120)
    op: Literal["=", "!=", ">", ">=", "<", "<=", "contains", "startswith", "in", "is_empty", "not_empty"]
    value: Optional[str | int | float | list[str | int | float]] = None


class LocalAgg(_Args):
    fn: Literal["count", "sum", "avg", "min", "max", "count_distinct"]
    col: Optional[str] = Field(default=None, max_length=120)
    name: Optional[str] = Field(default=None, max_length=60)


class LocalOrder(_Args):
    col: str = Field(min_length=1, max_length=120)
    dir: Literal["asc", "desc"] = "asc"


class LocalQueryArgs(LocalFileArgs):
    select: list[str] = Field(default_factory=list, max_length=40, description="columns to list when not grouping")
    where: list[LocalFilter] = Field(default_factory=list, max_length=20)
    group_by: list[str] = Field(default_factory=list, max_length=6)
    aggregates: list[LocalAgg] = Field(default_factory=list, max_length=12)
    order_by: list[LocalOrder] = Field(default_factory=list, max_length=4)
    limit: int = Field(default=50, ge=1, le=200)


class LocalDigestArgs(_Args):
    grant_id: str = Field(min_length=3, max_length=64, description="id of a shared FOLDER")
    subpath: Optional[str] = Field(default=None, max_length=400, description="subfolder (needs the user's approval for subfolders)")
    max_files: int = Field(default=25, ge=1, le=60)
    chars: int = Field(default=500, ge=100, le=1500, description="characters of each file to include")


class LocalTextArgs(_Args):
    file_id: str = Field(min_length=3, max_length=64)
    path: Optional[str] = Field(default=None, max_length=400, description="for a FOLDER: path of the file inside the folder")
    offset: int = Field(default=0, ge=0, le=500_000_000)
    max_chars: int = Field(default=20_000, ge=500, le=60_000)


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


class FilesFindArgs(_Args):
    query: str = Field(min_length=2, max_length=200, description="what the user is looking for, e.g. 'PAN card', 'rent agreement', 'invoice from Sharma'")
    limit: int = Field(default=8, ge=1, le=20)


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
    ToolDef("web.read", "1", "web", EGRESS, "medium", "web", WebReadArgs,
            "Read up to 10 whole web pages at once (crawl). Options: live=true for fresh content (prices, schedules, status), subpages=N "
            "to also read N pages of the same site (subpage_target words choose which), focus='...' to get the most relevant sentences. "
            "Use after web.search to read the best results fully. The URLs leave this PC."),
    ToolDef("web.answer", "1", "web", EGRESS, "medium", "web", WebAnswerArgs,
            "Quick factual answer from the web with sources (Exa). The question leaves this PC.", bound_secret="web.search"),
    ToolDef("web.research", "1", "web", EGRESS, "medium", "web", WebResearchArgs,
            "Hand a long investigation to Exa's research agent (it searches and reads many pages, takes minutes, costs more). Give clear "
            "instructions (what to find, period, region, output table columns). Returns a report with sources. The instructions leave this PC.",
            bound_secret="web.search"),
    ToolDef("outlook_local.list_folders", "1", "outlook_local", NONE, "low", "mail", EmptyArgs,
            "List folders in classic Outlook on this PC."),
    ToolDef("outlook_local.search", "2", "outlook_local", NONE, "low", "mail", OutlookSearchArgs,
            "Search classic Outlook mail, newest first (default scope: Inbox and its sub-folders; since/until accept ISO dates). Read-only."),
    ToolDef("outlook_local.digest", "1", "outlook_local", NONE, "low", "mail", OutlookDigestArgs,
            "List recent Outlook mail with built-in flags: APPROVAL wording, DEADLINE date, URGENT, QUESTION, whether you are in To or CC, "
            "unread, attachments and whether you already replied. Best first step for any inbox review. Read-only."),
    ToolDef("outlook_local.mail_stats", "1", "outlook_local", NONE, "low", "mail", OutlookStatsArgs,
            "Count Outlook mail in a period: total, unread, you in To vs CC, important, attachments, per day, top senders. Use this for "
            "'how many' questions - never count from a search list. Read-only."),
    ToolDef("outlook_local.awaiting_reply", "1", "outlook_local", NONE, "low", "mail", AwaitingReplyArgs,
            "Mail you sent that has had no reply yet (follow-up tracker). Read-only."),
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
    ToolDef("files.find", "1", "files", NONE, "low", "files", FilesFindArgs,
            "Find the user's own documents in My Files by what they ARE (type, summary, keywords, name): 'my PAN card', 'the rent agreement'. Returns ids, "
            "folder, kind, a short summary and the KINDS of personal data inside (never the values). Then use files.read with the id to show the content."),
    ToolDef("files.read", "1", "files", NONE, "low", "files", FilesReadArgs, "Read the extracted text of a file in My Files."),
    ToolDef("localfile.list", "1", "files", NONE, "low", "files", EmptyArgs,
            "List the local files the user attached to this chat from their own PC (read in place, never uploaded)."),
    ToolDef("localfile.browse", "1", "files", NONE, "low", "files", LocalBrowseArgs,
            "List the files directly inside a shared FOLDER (and its subfolders only if the user allowed them). Use the names it returns as `path`."),
    ToolDef("localfile.digest", "1", "files", NONE, "low", "files", LocalDigestArgs,
            "ONE call that gives an overview of a shared FOLDER: every readable file with its type, size and the first lines of its content "
            "(documents, text, sheets). Use it first for 'analyse / summarise / what is in this folder', then read single files in full with localfile.text.",
            timeout=300),
    ToolDef("localfile.inspect", "1", "files", NONE, "low", "files", LocalInspectArgs,
            "Describe an attached spreadsheet/CSV: sheets, row counts, columns with types and ranges, first rows. Call this first. "
            "The first call on a big workbook indexes it (can take a minute or two)."),
    ToolDef("localfile.rows", "1", "files", NONE, "low", "files", LocalRowsArgs,
            "Read rows start..start+count of an attached spreadsheet/CSV (max 200)."),
    ToolDef("localfile.query", "1", "files", NONE, "low", "files", LocalQueryArgs,
            "Filter / group / aggregate an attached spreadsheet or CSV exactly (count, sum, avg, min, max, count_distinct; filters =, !=, >, <, "
            "contains, startswith, in, is_empty). Use this for every number - never estimate from a few rows.", timeout=300),
    ToolDef("localfile.text", "1", "files", NONE, "low", "files", LocalTextArgs,
            "Read the text of a shared document (.txt .md .json .docx .pdf) in pages of up to 60000 characters. Pictures (.png .jpg .gif .webp) and "
            "scanned PDFs are read by the vision model and come back as text."),
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
