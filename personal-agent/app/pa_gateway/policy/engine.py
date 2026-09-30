"""Deterministic policy engine (spec 7.3, 13.3, 16.3, 23). Rules as code; default deny.

Decision conflict order: DENY > REQUIRE_APPROVAL > SANDBOX > ALLOW. No rule matched -> DENY.
Engine failure -> DENY (the caller catches every exception and denies).
User settings are overlays that can only make rules stricter than the floor.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

from pa_common.sensitivity import Sensitivity

from .tools_registry import EGRESS, EXTERNAL_WRITE, INTERNAL_WRITE, NONE, ToolDef

ALLOW = "ALLOW"
DENY = "DENY"
REQUIRE_APPROVAL = "REQUIRE_APPROVAL"
SANDBOX = "SANDBOX"
ORDER = {DENY: 3, REQUIRE_APPROVAL: 2, SANDBOX: 1, ALLOW: 0}
POLICY_BASE_VERSION = "local-1.0.0"

TRIGGER_USER = "USER"
TRIGGER_SCHEDULE = "SCHEDULE"
TRIGGER_LOCAL_EVENT = "LOCAL_EVENT"
TRIGGER_EXTERNAL_EVENT = "EXTERNAL_EVENT"


@dataclass
class PolicyContext:
    tool: ToolDef
    args: dict[str, Any]
    context: str  # "chat" | "mission"
    trigger: str
    hwm: Sensitivity
    allowed_tools: list[str] | None  # mission allowlist (None = not restricted by a mission)
    chat_tools_allowed: bool = True
    connector_usable: bool = True
    connector_reason: str = ""
    tool_enabled: bool = True
    sandbox_available: bool = True
    injection_suspected: bool = False
    public_research: bool = False


@dataclass
class Decision:
    decision: str
    reasons: list[str] = field(default_factory=list)
    rules: list[str] = field(default_factory=list)
    requires_password: bool = False
    risk: str = "low"
    approval_reason: str = ""
    policy_version: str = POLICY_BASE_VERSION

    def to_dict(self) -> dict[str, Any]:
        return {"decision": self.decision, "reasons": self.reasons, "rules": self.rules,
                "requires_password": self.requires_password, "risk": self.risk, "policy_version": self.policy_version}


class PolicyEngine:
    def __init__(self, settings_get: Callable[[str], Any], version_suffix: Callable[[], str] | None = None):
        self.get = settings_get
        self._suffix = version_suffix

    @property
    def version(self) -> str:
        return POLICY_BASE_VERSION + (f"+user-{self._suffix()}" if self._suffix else "")

    def evaluate(self, c: PolicyContext) -> Decision:
        try:
            return self._evaluate(c)
        except Exception as e:  # noqa: BLE001 - engine failure -> DENY (spec 7.3)
            return Decision(DENY, [f"policy engine error: {type(e).__name__}"], ["engine.failure"], policy_version=self.version)

    def _evaluate(self, c: PolicyContext) -> Decision:
        votes: list[tuple[str, str, str]] = []  # (decision, rule, reason)
        t = c.tool
        risk = t.risk

        def vote(d: str, rule: str, reason: str) -> None:
            votes.append((d, rule, reason))

        # --- floor: tool must be enabled
        if not c.tool_enabled:
            vote(DENY, "tool.disabled", f"{t.name} is disabled in Settings > Tools & Skills")
        # --- mission / chat scoping
        if c.allowed_tools is not None and t.name not in c.allowed_tools and t.name not in ("time.now",):
            vote(DENY, "mission.tool_not_allowed", f"{t.name} is not in this mission's allowed tools")
        if c.context == "chat" and not c.chat_tools_allowed and t.name != "time.now":
            vote(DENY, "chat.tools_off", "tools are switched off for this chat")
        # --- connector effective state (spec 8.2)
        if not c.connector_usable:
            vote(DENY, "connector.unusable", c.connector_reason or f"connector {t.connector} is not usable")
        # --- trigger trust (spec 16.3): external events are read-only, no egress, no external drafts
        if c.trigger == TRIGGER_EXTERNAL_EVENT and t.side_effect in (EGRESS, EXTERNAL_WRITE):
            vote(DENY, "trigger.external_reduced_rights", "tasks triggered by incoming mail cannot send data out")
        if c.trigger == TRIGGER_EXTERNAL_EVENT and t.name in ("missions.propose", "skills.propose", "reminders.propose"):
            vote(DENY, "trigger.external_reduced_rights", "tasks triggered by incoming mail cannot propose missions, skills or reminders")
        # --- sandbox
        if t.name == "python.run":
            if not c.sandbox_available:
                vote(DENY, "sandbox.unavailable", "no sandbox is available on this PC, so Python is disabled")
            else:
                vote(SANDBOX, "sandbox.required", "model-written code runs only in the sandbox")
        # --- data-flow control (spec 13.3)
        if t.side_effect == EGRESS:
            if c.hwm >= Sensitivity.RESTRICTED:
                vote(DENY, "flow.restricted_egress", "this context contains RESTRICTED data; nothing may leave the PC")
            elif c.hwm >= Sensitivity.CONFIDENTIAL:
                mode = self.get("flow.confidential_egress")
                if mode == "deny":
                    vote(DENY, "flow.confidential_egress", "web access is blocked while the context is CONFIDENTIAL")
                else:
                    vote(REQUIRE_APPROVAL, "flow.confidential_egress",
                         "the context contains CONFIDENTIAL data - confirm the exact query/URL")
                    risk = "high"
            elif c.hwm >= Sensitivity.INTERNAL:
                if self.get("flow.internal_egress") == "approval":
                    vote(REQUIRE_APPROVAL, "flow.internal_egress", "your rules require approval for web access with INTERNAL context")
                else:
                    vote(ALLOW, "flow.internal_egress", "INTERNAL context: web access allowed (allowlists apply)")
            else:
                if self.get("flow.public_egress") == "approval":
                    vote(REQUIRE_APPROVAL, "flow.public_egress", "your rules require approval for web access")
                else:
                    vote(ALLOW, "flow.public_egress", "PUBLIC context: web access allowed (allowlists apply)")
        if t.side_effect == EXTERNAL_WRITE:
            if c.hwm >= Sensitivity.RESTRICTED:
                vote(DENY, "flow.restricted_write", "RESTRICTED data can never be sent out")
            else:
                vote(REQUIRE_APPROVAL, "floor.external_write", "sending or writing outside this PC always needs your approval")
                risk = "high"
        # --- explicit approvals
        if t.always_approval and t.side_effect != EXTERNAL_WRITE:
            vote(REQUIRE_APPROVAL, "tool.always_approval", "this action needs your confirmation")
        if t.name in (self.get("approvals.extra_tools") or []):
            vote(REQUIRE_APPROVAL, "user.extra_approval", "you asked to approve this tool every time")
        if c.injection_suspected and t.side_effect in (EGRESS, EXTERNAL_WRITE):
            vote(REQUIRE_APPROVAL, "injection.reduced_rights", "possible prompt injection detected in this task's inputs")
            risk = "high"
        # --- base allows for reads and internal writes
        if t.side_effect in (NONE, INTERNAL_WRITE) and t.name != "python.run":
            vote(ALLOW, "base.read_or_internal", "read-only or internal action")

        if not votes:
            return Decision(DENY, ["no rule allows this action (default deny)"], ["floor.default_deny"], risk=risk,
                            policy_version=self.version)
        top = max(votes, key=lambda v: ORDER[v[0]])[0]
        chosen = [v for v in votes if v[0] == top]
        requires_password = top == REQUIRE_APPROVAL and (
            (t.side_effect == EXTERNAL_WRITE and c.hwm >= Sensitivity.CONFIDENTIAL)
            or (risk == "high" and self.get("approvals.high_risk_method") == "password"))
        return Decision(top, [v[2] for v in chosen], [v[1] for v in chosen], requires_password=requires_password,
                        risk=risk, approval_reason="; ".join(v[2] for v in chosen) if top == REQUIRE_APPROVAL else "",
                        policy_version=self.version)

    def plain_language_rules(self) -> list[dict[str, Any]]:
        """'Show me the rules in plain language' (spec 5.3)."""
        g = self.get
        return [
            {"rule": "Default deny: any action not explicitly allowed is refused.", "floor": True},
            {"rule": "Sending, forwarding or deleting email and any external write always need your approval.", "floor": True},
            {"rule": "RESTRICTED data can never be sent out.", "floor": True},
            {"rule": "Model-written code runs only in the sandbox; there is no shell tool.", "floor": True},
            {"rule": "Untrusted content (mail, web, files) can never change settings, policies, connectors, memory trust or skills.", "floor": True},
            {"rule": "Tasks started by incoming mail are read-only and cannot send data out.", "floor": True},
            {"rule": "Only the gateway may reach the network; every call is logged before it runs.", "floor": True},
            {"rule": f"Web access with PUBLIC context: {g('flow.public_egress')}.", "floor": False},
            {"rule": f"Web access with INTERNAL context: {g('flow.internal_egress')}.", "floor": False},
            {"rule": f"Web access with CONFIDENTIAL context: {g('flow.confidential_egress')} (never looser than approval).", "floor": True},
            {"rule": f"High-risk approvals need your {g('approvals.high_risk_method')}.", "floor": False},
            {"rule": f"Extra tools that always need approval: {', '.join(g('approvals.extra_tools') or []) or 'none'}.", "floor": False},
            {"rule": f"Disabled tools: {', '.join(g('tools.disabled') or []) or 'none'}.", "floor": False},
            {"rule": f"Fetch any site: {'on' if g('web.fetch_any_site') else 'off (allowlist only)'}.", "floor": False},
        ]
