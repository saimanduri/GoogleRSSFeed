"""Every user-configurable setting (spec 5.3). Defaults live here (in code); user values live in the
encrypted database (rule 35.8). The UI renders generic settings pages from this schema.

`loosen` describes which direction makes the agent LESS restrictive (spec 5.5):
  "up"          larger number is looser         "down"   smaller number is looser
  "true"        switching ON is looser          "false"  switching OFF is looser
  ["a","b",..]  enum ordered strict -> loose    "list_add" adding items is looser
  "list_remove" removing items is looser         None     neutral (no security impact)
Loosening needs the password, a 10-second read delay and is logged with before/after values.
Security-floor items (spec 5.4) are NOT settings at all, so they cannot be switched off.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class S:
    key: str
    group: str
    label: str
    type: str  # int | float | bool | enum | str | list | time
    default: Any
    loosen: Any = None
    min: float | None = None
    max: float | None = None
    options: tuple = ()
    help: str = ""
    risk: str = ""  # plain-language risk text shown in the loosening dialog
    stepup: bool = False  # changing it at all needs step-up (endpoints, 39.2)
    hidden: bool = False
    tags: tuple = field(default_factory=tuple)


SETTINGS: list[S] = [
    # ---------------- Account & Security
    S("security.auto_lock_minutes", "account", "Auto-lock after idle (minutes)", "int", 10, "up", 1, 60,
      help="The window locks after this much inactivity. Auto-lock cannot be switched off.",
      risk="A longer timeout leaves the app open for longer if you walk away."),
    S("security.lock_on_windows_lock", "account", "Lock when Windows locks", "bool", True, "false",
      risk="The app stays unlocked while Windows is locked."),
    S("security.pin_quick_unlock", "account", "Allow PIN quick unlock", "bool", True, "true",
      risk="Anyone who knows your PIN can unlock the window while the agent is running."),
    S("security.quick_unlock_max_hours", "account", "Quick-unlock maximum age (hours)", "int", 24, "up", 1, 72,
      risk="Your password is asked for less often."),
    S("security.stepup_minutes", "account", "Step-up validity (minutes)", "int", 5, "up", 1, 15,
      risk="A re-authentication stays valid for longer."),
    S("security.wipe_keys_on_sleep", "account", "Wipe keys on sleep", "enum", "if_no_bitlocker",
      ["always", "if_no_bitlocker"], options=("always", "if_no_bitlocker"),
      help="'always' is stricter: keys are wiped on every sleep and the password is needed after resume.",
      risk="Keys stay in memory during sleep when BitLocker is on.", tags=("floor",)),
    S("security.pause_missions_when_locked", "account", "Pause missions while Windows is locked", "bool", False, "false",
      help="Stricter mode: when the UI locks, keys are wiped and the password is required."),
    S("security.transcripts_require_stepup", "account", "Require step-up to view full transcripts", "bool", True, "false",
      risk="Full prompts, completions and tool payloads can be viewed without re-authenticating."),
    # ---------------- Autonomy & Budgets
    S("autonomy.profile", "autonomy", "Autonomy profile", "enum", "cautious", ["cautious", "balanced"],
      options=("cautious", "balanced"), risk="Balanced raises default budgets and batches low-risk approvals."),
    S("budget.task.runtime_minutes", "autonomy", "Per task: max runtime (minutes)", "int", 30, "up", 1, 480),
    S("budget.task.tokens", "autonomy", "Per task: max LLM tokens", "int", 200_000, "up", 1000, 5_000_000),
    S("budget.task.tool_calls", "autonomy", "Per task: max tool calls", "int", 100, "up", 1, 2000),
    S("budget.task.web_requests", "autonomy", "Per task: max web requests", "int", 30, "up", 0, 1000),
    S("budget.task.files", "autonomy", "Per task: max files processed", "int", 20, "up", 0, 1000),
    S("budget.task.egress_kb", "autonomy", "Per task: max egress (KB)", "int", 100, "up", 0, 102_400),
    S("budget.task.external_writes", "autonomy", "Per task: max external writes", "int", 5, "up", 0, 100),
    S("budget.task.retries", "autonomy", "Per task: max retries", "int", 3, "up", 0, 20),
    S("budget.task.subtasks", "autonomy", "Per task: max sub-tasks", "int", 5, "up", 0, 50),
    S("budget.task.depth", "autonomy", "Per task: max sub-task depth", "int", 3, "up", 1, 6),
    S("budget.task.steps", "autonomy", "Per task: max agent steps", "int", 25, "up", 1, 200),
    S("budget.daily.runtime_hours", "autonomy", "Per day: total runtime (hours)", "float", 4, "up", 0.5, 24),
    S("budget.daily.tokens", "autonomy", "Per day: total LLM tokens", "int", 2_000_000, "up", 10_000, 100_000_000),
    S("budget.daily.web_requests", "autonomy", "Per day: total web requests", "int", 150, "up", 0, 10_000),
    S("budget.daily.egress_kb", "autonomy", "Per day: total egress (KB)", "int", 1024, "up", 0, 1_048_576),
    S("budget.concurrent_tasks", "autonomy", "Concurrent background tasks", "int", 3, "up", 1, 8),
    S("power.keep_awake", "autonomy", "Keep PC awake during mission windows", "bool", False, None),
    S("power.wake_timers", "autonomy", "Allow wake timers for scheduled missions", "bool", False, None),
    # ---------------- Rules & Safety
    S("sensitivity.default.mail", "rules", "Default sensitivity: mail & calendar", "enum", "CONFIDENTIAL",
      ["RESTRICTED", "CONFIDENTIAL"], options=("CONFIDENTIAL", "RESTRICTED"),
      help="Can only be made stricter than the shipped default.", tags=("floor",)),
    S("sensitivity.default.files", "rules", "Default sensitivity: My Files", "enum", "INTERNAL",
      ["RESTRICTED", "CONFIDENTIAL", "INTERNAL"], options=("INTERNAL", "CONFIDENTIAL", "RESTRICTED"), tags=("floor",)),
    S("sensitivity.default.web", "rules", "Default sensitivity: web content", "enum", "PUBLIC",
      ["RESTRICTED", "CONFIDENTIAL", "INTERNAL", "PUBLIC"], options=("PUBLIC", "INTERNAL", "CONFIDENTIAL"), tags=("floor",)),
    S("flow.public_egress", "rules", "Web search/fetch with PUBLIC context", "enum", "allow", ["approval", "allow"],
      options=("allow", "approval")),
    S("flow.internal_egress", "rules", "Web search/fetch with INTERNAL context", "enum", "allow", ["approval", "allow"],
      options=("allow", "approval")),
    S("flow.confidential_egress", "rules", "Web search/fetch with CONFIDENTIAL context", "enum", "approval",
      ["deny", "approval"], options=("approval", "deny"),
      help="Floor: never looser than 'approval'. RESTRICTED context is always denied.", tags=("floor",)),
    S("dlp.custom_patterns", "rules", "Blocked outbound patterns (regular expressions)", "list", [], "list_remove",
      help="E.g. your account numbers. Matching outbound text is blocked."),
    S("dlp.blocked_keywords", "rules", "Blocked outbound keywords", "list", [], "list_remove"),
    S("triggers.mail.allowed_senders", "rules", "Mail triggers: allowed senders/domains", "list", [], "list_add"),
    S("triggers.mail.folders", "rules", "Mail triggers: folders", "list", ["Inbox"], "list_add"),
    S("triggers.rate_per_hour", "rules", "Triggered runs per hour (max)", "int", 20, "up", 1, 200),
    # ---------------- Approvals
    S("approvals.expiry_hours", "approvals", "Default approval expiry (hours)", "int", 24, "up", 1, 168),
    S("approvals.high_risk_expiry_hours", "approvals", "High-risk approval expiry (hours)", "int", 1, "up", 1, 24),
    S("approvals.high_risk_method", "approvals", "High-risk approvals require", "enum", "pin", ["password", "pin"],
      options=("pin", "password")),
    S("approvals.extra_tools", "approvals", "Additional tools that always need approval", "list", [], "list_remove"),
    S("approvals.fast_warning_seconds", "approvals", "Warn if approvals are accepted faster than (s)", "int", 2, "down", 1, 10),
    # ---------------- Tools & Skills
    S("tools.disabled", "tools", "Disabled tools", "list", [], "list_remove"),
    S("tools.python_enabled", "tools", "Python analysis (sandbox)", "bool", True, "true",
      risk="Model-written Python runs in the sandbox."),
    S("tools.scripts_call_tools", "tools", "Scripts that call tools (39.9)", "bool", False, "true",
      risk="Python in the sandbox may call tools through the gateway."),
    S("tools.scripts_allow_standard_isolation", "tools", "Allow scripts-call-tools under Standard isolation", "bool", False, "true"),
    S("tools.subagents_max", "tools", "Max concurrent sub-agents per task", "int", 3, "up", 0, 3),
    S("tools.external_agents", "tools", "External agent delegation", "enum", "off", ["off"], options=("off",),
      help="Not available in V1."),
    S("tools.isolated_browser", "tools", "Isolated browser (later)", "enum", "off", ["off"], options=("off",),
      help="Not available in V1."),
    # ---------------- Web Access
    S("web.provider", "web", "Search provider", "enum", "none", None,
      options=("none", "brave", "searxng", "exa", "tavily"), stepup=True),
    S("web.searxng_url", "web", "SearXNG URL (https)", "str", "", None, stepup=True),
    S("web.allowlist", "web", "Allowed domains", "list",
      ["wikipedia.org", "github.com", "arxiv.org", "huggingface.co", "python.org", "microsoft.com"], "list_add"),
    S("web.blocklist", "web", "Blocked domains", "list", [], "list_remove"),
    S("web.fetch_any_site", "web", "Fetch any site", "bool", False, "true",
      risk="The agent may fetch any public website (still subject to SSRF rules, DLP and sensitivity rules)."),
    S("web.max_page_kb", "web", "Max page size (KB)", "int", 5120, "up", 64, 51_200),
    S("web.timeout_seconds", "web", "Fetch timeout (s)", "int", 20, "up", 3, 120),
    S("web.max_redirects", "web", "Max redirects", "int", 5, "up", 0, 5),
    S("web.extra_ports", "web", "Extra allowed ports", "list", [], "list_add"),
    S("web.allow_http", "web", "Allow plain HTTP", "bool", False, "true", risk="Pages may be fetched without encryption."),
    # ---------------- Files & Storage
    S("files.quota_mb", "files", "Storage quota (MB)", "int", 10_240, None, 100, 1_048_576),
    S("files.max_upload_mb", "files", "Max file size (MB)", "int", 100, "up", 1, 2048),
    S("retention.chats_days", "files", "Keep chats (days, 0 = forever)", "int", 0, None, 0, 36500),
    S("retention.transcripts_days", "files", "Keep transcripts (days)", "int", 30, None, 1, 3650),
    S("retention.tasks_days", "files", "Keep task history (days)", "int", 365, None, 1, 36500),
    S("retention.quarantine_days", "files", "Keep quarantined files (days)", "int", 30, None, 1, 3650),
    S("retention.runs_visible", "files", "Runs shown in the step timeline (older are archived)", "int", 100, None, 100, 5000),
    # ---------------- Memory
    S("memory.enabled", "memory", "Memory enabled", "bool", True, None),
    S("memory.auto_confirm_low", "memory", "Auto-confirm low-importance preferences", "bool", False, "true",
      risk="The agent may store small preferences without asking."),
    S("memory.retention_days", "memory", "Memory retention (days, 0 = forever)", "int", 0, None, 0, 36500),
    # ---------------- Notifications
    S("notifications.toasts", "notifications", "Windows notifications", "bool", True, None),
    S("notifications.content_level", "notifications", "Notification content", "enum", "notify", ["notify", "summary"],
      options=("notify", "summary"), help="'summary' never includes CONFIDENTIAL content."),
    S("notifications.quiet_start", "notifications", "Quiet hours start", "time", "", None),
    S("notifications.quiet_end", "notifications", "Quiet hours end", "time", "", None),
    S("notifications.email_to_self", "notifications", "Email-to-self summary via Microsoft 365", "bool", False, "true",
      risk="A summary email is sent through Microsoft 365."),
    # ---------------- Logs & SIEM
    S("logs.rotate_mb", "logs", "Rotate log at (MB)", "int", 100, None, 1, 10_240),
    S("logs.rotate_days", "logs", "Rotate log after (days)", "int", 7, None, 1, 365),
    S("logs.retention_days", "logs", "Keep rotated logs (days)", "int", 365, "down", 30, 3650,
      risk="Security logs are deleted sooner."),
    S("siem.enabled", "logs", "Forward to SIEM", "bool", False, "true", risk="Log metadata is sent to your SIEM.", stepup=True),
    S("siem.transport", "logs", "SIEM transport", "enum", "https", None, options=("https", "syslog"), stepup=True),
    S("siem.url", "logs", "SIEM HTTPS URL", "str", "", None, stepup=True),
    S("siem.host", "logs", "Syslog host", "str", "", None, stepup=True),
    S("siem.port", "logs", "Syslog port", "int", 6514, None, 1, 65535, stepup=True),
    S("siem.fingerprint", "logs", "Pinned server certificate SHA-256", "str", "", None, stepup=True),
    S("siem.format", "logs", "Forwarding format", "enum", "json", None, options=("json", "cef")),
    # ---------------- Backup
    S("backup.folder", "backup", "Backup folder", "str", "", None, stepup=True),
    S("backup.schedule", "backup", "Backup schedule", "enum", "weekly", None, options=("off", "daily", "weekly")),
    S("backup.keep", "backup", "Versions kept", "int", 7, None, 1, 100),
    S("backup.include_logs", "backup", "Include sealed logs", "bool", False, None),
    # ---------------- Emergency stop
    S("emergency.hotkey", "emergency", "STOP ALL hotkey", "str", "Ctrl+Alt+Shift+S", None),
    # ---------------- Updates
    S("updates.auto_check", "updates", "Check for updates automatically", "bool", True, None,
      help="Updates are never installed silently."),
    # ---------------- AI model
    S("llm.role.fast", "model", "Model for 'fast' role", "str", "", None),
    S("llm.role.standard", "model", "Model for 'standard' role", "str", "", None),
    S("llm.role.reasoning", "model", "Model for 'reasoning' role", "str", "", None),
    S("llm.role.vision", "model", "Model for 'vision' role", "str", "", None),
    S("llm.role.embedding", "model", "Embedding model", "str", "", None),
    S("llm.role.stt", "model", "Speech-to-text model", "str", "", None),
    S("llm.context_tokens", "model", "Context size (tokens)", "int", 8192, None, 1024, 262_144),
    S("llm.gpu_layers", "model", "GPU layers (built-in runtime)", "int", 99, None, 0, 999),
    S("llm.max_concurrent", "model", "Max concurrent model requests", "int", 2, None, 1, 8),
    S("llm.temperature", "model", "Temperature", "float", 0.2, None, 0, 2),
    # ---------------- Connectors (outlook scope)
    S("outlook.start_when_needed", "connectors", "Start Outlook when needed", "bool", False, "true"),
    S("outlook.folder_denylist", "connectors", "Outlook folders excluded", "list", ["Deleted Items", "Junk Email"], "list_remove"),
    S("outlook.folder_allowlist", "connectors", "Outlook folders allowed (empty = all not excluded)", "list", [], None),
    S("outlook.include_pst", "connectors", "Include PST archives", "bool", False, "true"),
    S("outlook.max_items", "connectors", "Max items per request", "int", 50, "up", 1, 500),
    S("outlook.max_body_kb", "connectors", "Max body size (KB)", "int", 256, "up", 1, 4096),
    S("outlook.max_attachment_mb", "connectors", "Max attachment size (MB)", "int", 25, "up", 1, 200),
    S("outlook.date_range_days", "connectors", "Max date range (days)", "int", 3650, "up", 1, 36500),
    S("outlook.highest_label", "connectors", "Items with highest sensitivity label", "enum", "metadata_only",
      ["exclude", "metadata_only"], options=("metadata_only", "exclude")),
    S("m365.client_id", "connectors", "Microsoft 365 app (client) ID", "str", "", None, stepup=True),
    S("m365.tenant_id", "connectors", "Microsoft 365 tenant ID", "str", "organizations", None, stepup=True),
    S("m365.enable_drafts", "connectors", "Allow drafts (Mail.ReadWrite)", "bool", False, "true",
      risk="The agent may create AI-marked drafts in your mailbox."),
    S("m365.enable_send", "connectors", "Allow sending (Mail.Send, always with approval)", "bool", False, "true",
      risk="The agent may propose emails for you to approve and send."),
    S("outlook.enable_drafts", "connectors", "Allow Outlook drafts", "bool", False, "true",
      risk="The agent may create AI-marked drafts in classic Outlook."),
    # ---------------- UI
    S("ui.theme", "ui", "Theme", "enum", "system", None, options=("system", "light", "dark")),
    S("ui.text_scale", "ui", "Text size (%)", "int", 100, None, 80, 160),
    S("ui.reduce_motion", "ui", "Reduce motion", "bool", False, None),
    S("chat.show_steps", "ui", "Show agent steps live in chat", "bool", True, None),
    S("voice.enabled", "ui", "Voice input", "bool", True, None),
    S("voice.language", "ui", "Voice language (ISO code, blank = auto)", "str", "", None),
    S("reminders.default_time", "ui", "Default reminder time", "time", "09:00", None),
]

BY_KEY: dict[str, S] = {s.key: s for s in SETTINGS}

GROUPS = [
    ("account", "Account & Security"), ("connectors", "Connectors"), ("model", "AI Model"),
    ("autonomy", "Autonomy & Budgets"), ("rules", "Rules & Safety"), ("approvals", "Approvals"),
    ("tools", "Tools & Skills"), ("web", "Web Access"), ("files", "Files & Storage"), ("memory", "Memory"),
    ("notifications", "Notifications"), ("logs", "Logs & SIEM"), ("backup", "Backup & Restore"),
    ("emergency", "Emergency Stop"), ("updates", "Updates"), ("privacy", "Privacy & Data"),
    ("diagnostics", "Diagnostics & About"), ("ui", "Appearance & Voice"),
]

# Balanced profile raises these defaults (only applied when the user switches profile).
BALANCED_OVERRIDES = {
    "budget.task.runtime_minutes": 60, "budget.task.tokens": 400_000, "budget.task.web_requests": 60,
    "budget.task.egress_kb": 500, "budget.daily.runtime_hours": 8, "budget.daily.tokens": 4_000_000,
    "budget.daily.web_requests": 300, "budget.daily.egress_kb": 4096,
}


def coerce(spec: S, value: Any) -> Any:
    """Validate and normalise a user value. Raises ValueError with a readable message."""
    t = spec.type
    if t == "bool":
        if not isinstance(value, bool):
            raise ValueError(f"{spec.label}: expected on/off")
        return value
    if t in ("int", "float"):
        try:
            v = int(value) if t == "int" else float(value)
        except (TypeError, ValueError) as e:
            raise ValueError(f"{spec.label}: expected a number") from e
        if spec.min is not None and v < spec.min or spec.max is not None and v > spec.max:
            raise ValueError(f"{spec.label}: must be between {spec.min} and {spec.max}")
        return v
    if t == "enum":
        if value not in spec.options:
            raise ValueError(f"{spec.label}: must be one of {', '.join(spec.options)}")
        return value
    if t in ("str", "time"):
        if not isinstance(value, str) or len(value) > 2048:
            raise ValueError(f"{spec.label}: expected text")
        if t == "time" and value and not _is_hhmm(value):
            raise ValueError(f"{spec.label}: expected HH:MM")
        return value.strip()
    if t == "list":
        if not isinstance(value, list) or not all(isinstance(x, str) and len(x) <= 512 for x in value) or len(value) > 500:
            raise ValueError(f"{spec.label}: expected a list of text items")
        return [x.strip() for x in value if x.strip()]
    raise ValueError("unknown setting type")


def _is_hhmm(v: str) -> bool:
    parts = v.split(":")
    return len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit() and 0 <= int(parts[0]) < 24 and 0 <= int(parts[1]) < 60


def is_loosening(spec: S, before: Any, after: Any) -> bool:
    lo = spec.loosen
    if lo is None or before == after:
        return False
    if lo == "up":
        return after > before
    if lo == "down":
        return after < before
    if lo == "true":
        return bool(after) and not bool(before)
    if lo == "false":
        return bool(before) and not bool(after)
    if lo == "list_add":
        return bool(set(after) - set(before))
    if lo == "list_remove":
        return bool(set(before) - set(after))
    if isinstance(lo, list):
        # enum ordered strict -> loose; values not in list are the shipped default floor-equivalents
        def rank(v: Any) -> int:
            return lo.index(v) if v in lo else len(lo)
        return rank(after) > rank(before)
    return False


def exceeds_floor(spec: S, value: Any) -> bool:
    """Values that would breach the security floor are rejected outright (spec 5.4)."""
    if "floor" in spec.tags and isinstance(spec.loosen, list) and spec.type == "enum":
        # for floor-bound enums the shipped default is the loosest permitted value
        if value in spec.loosen and spec.default in spec.loosen:
            return spec.loosen.index(value) > spec.loosen.index(spec.default)
    return False
