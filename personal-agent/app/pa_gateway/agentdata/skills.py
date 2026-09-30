"""Skills: declarative, reviewed, versioned, signed locally (spec 22, 39.4, 39.6).

A skill is JSON: {name, description, tools[], steps[{instruction, tool?}], examples[]} referencing
BUILT-IN tools only. No code, no shell commands, no download links, no install steps.
- proposals from runs with injection flags, denials or untrusted-instruction risk are blocked /
  marked "tainted origin"
- every version is HMAC-signed with K_skill; a changed or unsigned definition never loads
- activation = user review of the full definition + diff, automated checks, step-up
  (password when the skill uses write/egress tools); the active version never changes in place
- unused for 90 days -> retired; repeated denials -> suspended and shown on Home
"""
from __future__ import annotations

import difflib
import hashlib
import hmac
import json
import re
from datetime import timedelta
from typing import Any

from pa_common.errors import PAError
from pa_common.ids import new_id
from pa_common.timeutil import now_iso, to_iso, utcnow

from ..policy.tools_registry import BY_NAME, EGRESS, EXTERNAL_WRITE
from ..tools import injection

CODE_PATTERNS = [r"```", r"\b(import|def|class|lambda|exec|eval|subprocess|os\.system)\b", r"\b(powershell|cmd\.exe|bash|sh -c|curl|wget|iwr|invoke-webrequest)\b",
                 r"\bpip install\b|\bnpm install\b|\bchoco\b|\bwinget\b", r"https?://\S+\.(exe|msi|ps1|bat|zip|dll|sh)\b",
                 r"\bdownload (and )?(run|install|execute)\b", r"[A-Za-z0-9+/]{80,}={0,2}"]
_CODE_RX = [re.compile(p, re.I) for p in CODE_PATTERNS]
MAX_DENIALS = 3


def canonical(defn: dict[str, Any]) -> bytes:
    return json.dumps(defn, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def static_checks(defn: dict[str, Any]) -> list[dict[str, Any]]:
    """Automated checks run before a version can be activated."""
    results: list[dict[str, Any]] = []

    def add(name: str, ok: bool, note: str = "") -> None:
        results.append({"name": name, "ok": ok, "note": note})

    keys = set(defn)
    add("only_declarative_fields", keys <= {"name", "description", "tools", "steps", "examples"}, ", ".join(sorted(keys)))
    tools = defn.get("tools") or []
    unknown = [t for t in tools if t not in BY_NAME]
    add("tools_are_builtin", not unknown, ", ".join(unknown))
    steps = defn.get("steps") or []
    add("has_steps", 0 < len(steps) <= 30)
    step_tools = [s.get("tool") for s in steps if isinstance(s, dict) and s.get("tool")]
    add("steps_use_declared_tools", all(t in tools for t in step_tools), ", ".join(t for t in step_tools if t not in tools))
    text = json.dumps(defn, ensure_ascii=False)
    code_hits = [p.pattern for p in _CODE_RX if p.search(text)]
    add("no_code_commands_or_downloads", not code_hits, "; ".join(code_hits[:3]))
    inj = injection.scan(text)
    add("no_embedded_model_instructions", not inj, "; ".join(inj[:3]))
    add("no_secret_references", not re.search(r"(?i)\b(password|api[_ -]?key|secret|token)\b\s*[:=]", text))
    return results


def review_highlights(old: dict[str, Any] | None, new: dict[str, Any]) -> dict[str, Any]:
    """New destinations/recipients/tools, skip-check instructions, encoded text -> highlighted (39.6)."""
    text = json.dumps(new, ensure_ascii=False)
    old_text = json.dumps(old or {}, ensure_ascii=False)
    new_urls = set(re.findall(r"https?://[^\s\"']+", text)) - set(re.findall(r"https?://[^\s\"']+", old_text))
    new_emails = set(re.findall(r"[\w.+-]+@[\w-]+\.[\w.]+", text)) - set(re.findall(r"[\w.+-]+@[\w-]+\.[\w.]+", old_text))
    new_tools = set(new.get("tools") or []) - set((old or {}).get("tools") or [])
    skip = re.findall(r"(?i)(skip|bypass|disable|without) (the )?(approval|check|confirmation|review|policy)", text)
    diff = list(difflib.unified_diff(json.dumps(old or {}, indent=1, sort_keys=True).splitlines(),
                                     json.dumps(new, indent=1, sort_keys=True).splitlines(), "active", "proposed", lineterm=""))
    return {"new_destinations": sorted(new_urls), "new_recipients": sorted(new_emails), "new_tools": sorted(new_tools),
            "skip_check_instructions": [" ".join(x) for x in skip], "encoded_text": bool(re.search(r"[A-Za-z0-9+/]{60,}={0,2}", text)),
            "diff": diff}


class SkillService:
    def __init__(self, gw):
        self.gw = gw
        self.db = gw.db

    def _mac(self, defn: dict[str, Any]) -> str:
        return hmac.new(self.gw.keys.key("K_skill"), canonical(defn), hashlib.sha256).hexdigest()

    def verify(self, row: dict[str, Any]) -> bool:
        return hmac.compare_digest(row["mac"], self._mac(json.loads(row["definition_json"])))

    def propose(self, defn: dict[str, Any], *, source: str, task: dict[str, Any] | None = None) -> dict[str, Any]:
        defn = {k: defn[k] for k in ("name", "description", "tools", "steps", "examples") if k in defn}
        tainted = False
        lineage: list[dict[str, Any]] = []
        if task is not None:
            run = task["session_id"]
            denials = self.db.scalar("SELECT count(*) FROM session_events WHERE session_id=? AND kind='tool.denied'", (run,)) or 0
            rejected = self.db.scalar("SELECT count(*) FROM approvals WHERE task_id=? AND status IN ('DENIED','VOIDED')", (task["id"],)) or 0
            untrusted = self.db.scalar("SELECT count(*) FROM session_events WHERE session_id=? AND trust='UNTRUSTED'", (run,)) or 0
            flagged = self.gw.sessionlog.has_untrusted_instructions_risk(run)
            if task["trigger_type"] not in ("USER", "SCHEDULE"):
                raise PAError("skills can only be proposed from runs you or a schedule started", code="skill_blocked")
            if flagged:
                raise PAError("skill proposal blocked: the run contained possible prompt injection", code="skill_blocked")
            tainted = bool(denials or rejected or untrusted)
            lineage = [{"task_id": task["id"], "run_id": run, "denials": denials, "rejected_approvals": rejected,
                        "untrusted_inputs": untrusted}]
        name = str(defn.get("name", ""))[:80]
        prev = self.db.one("SELECT * FROM skills WHERE name=? ORDER BY version DESC LIMIT 1", (name,))
        version = (prev["version"] + 1) if prev else 1
        checks = static_checks(defn)
        active = self.db.one("SELECT * FROM skills WHERE name=? AND status='ACTIVE'", (name,))
        review = review_highlights(json.loads(active["definition_json"]) if active else None, defn)
        sid = new_id("skl")
        self.db.insert("skills", {"id": sid, "name": name, "version": version, "definition_json": defn, "status": "PROPOSED",
                                  "lineage_json": lineage, "tainted": int(tainted), "review_json": review,
                                  "test_json": {"checks": checks, "passed": all(c["ok"] for c in checks)},
                                  "mac": self._mac(defn), "created_at": now_iso()})
        self.gw.audit.write("skill.proposed", "skill", skill_id=sid, name=name, version=version, source=source, tainted=tainted)
        self.gw.emit("skills.changed", {"skill_id": sid})
        return {"id": sid, "version": version, "tainted": tainted, "checks_passed": all(c["ok"] for c in checks)}

    def list(self) -> list[dict[str, Any]]:
        rows = self.db.all("SELECT * FROM skills ORDER BY name, version DESC")
        for r in rows:
            r["definition"] = json.loads(r.pop("definition_json"))
            r["lineage"] = json.loads(r.pop("lineage_json"))
            r["review"] = json.loads(r.pop("review_json"))
            r["tests"] = json.loads(r.pop("test_json"))
            r["signature_ok"] = hmac.compare_digest(r.pop("mac"), self._mac(r["definition"]))
            r["uses_write_or_egress"] = any(BY_NAME[t].side_effect in (EGRESS, EXTERNAL_WRITE)
                                            for t in r["definition"].get("tools", []) if t in BY_NAME)
        return rows

    def needs_password(self, sid: str) -> bool:
        r = next((x for x in self.list() if x["id"] == sid), None)
        return bool(r and r["uses_write_or_egress"])

    def activate(self, sid: str) -> None:
        r = self.db.one("SELECT * FROM skills WHERE id=?", (sid,))
        if not r:
            raise PAError("skill not found", code="not_found")
        if not self.verify(r):
            raise PAError("skill signature invalid - it was changed outside the app", code="skill_tampered")
        tests = json.loads(r["test_json"])
        tests = {"checks": static_checks(json.loads(r["definition_json"]))}
        tests["passed"] = all(c["ok"] for c in tests["checks"])
        self.db.update("skills", "id", sid, {"test_json": tests})
        if not tests["passed"]:
            failed = ", ".join(c["name"] for c in tests["checks"] if not c["ok"])
            raise PAError(f"automated checks failed: {failed}", code="skill_tests_failed")
        disabled = set(self.gw.settings.get("tools.disabled") or [])
        if disabled & set(json.loads(r["definition_json"]).get("tools", [])):
            raise PAError("the skill uses tools you disabled", code="skill_blocked")
        with self.db.tx():
            self.db.execute("UPDATE skills SET status='SUPERSEDED' WHERE name=? AND status='ACTIVE'", (r["name"],))
            self.db.update("skills", "id", sid, {"status": "ACTIVE", "denials": 0, "last_used_at": now_iso()})
        self.gw.audit.write("skill.activated", "skill", skill_id=sid, name=r["name"], version=r["version"])
        self.gw.emit("skills.changed", {"skill_id": sid})

    def set_status(self, sid: str, status: str) -> None:
        if status not in ("RETIRED", "REJECTED", "SUSPENDED"):
            raise PAError("invalid status", code="invalid_request")
        self.db.update("skills", "id", sid, {"status": status})
        self.gw.audit.write("skill.status", "skill", skill_id=sid, status=status)
        self.gw.emit("skills.changed", {"skill_id": sid})

    def active_for_prompt(self) -> list[dict[str, Any]]:
        out = []
        disabled = set(self.gw.settings.get("tools.disabled") or [])
        for r in self.db.all("SELECT * FROM skills WHERE status='ACTIVE'"):
            if not self.verify(r):
                self.gw.audit.write("skill.signature_invalid", "skill", skill_id=r["id"], severity="high")
                continue
            d = json.loads(r["definition_json"])
            if disabled & set(d.get("tools", [])):
                continue
            if any(t in BY_NAME and BY_NAME[t].connector in ("web", "m365", "outlook_local")
                   and not self.gw.connectors.usable(BY_NAME[t].connector, "chat")[0] for t in d.get("tools", [])):
                continue  # skills whose connectors are OFF cannot run
            out.append({"id": r["id"], "name": r["name"], "version": r["version"], **d})
        return out

    def mark_used(self, sid: str) -> None:
        self.db.update("skills", "id", sid, {"last_used_at": now_iso()})

    def note_denial(self, skill_ids: list[str]) -> None:
        for sid in skill_ids:
            self.db.execute("UPDATE skills SET denials=denials+1 WHERE id=?", (sid,))
            r = self.db.one("SELECT name, denials FROM skills WHERE id=?", (sid,))
            if r and r["denials"] >= MAX_DENIALS:
                self.set_status(sid, "SUSPENDED")
                self.gw.home_event("skill_suspended", "medium", f"Skill suspended: {r['name']}",
                                   "It caused repeated denials. Review it in Settings > Tools & Skills.", sid)

    def retire_unused(self) -> int:
        cutoff = to_iso(utcnow() - timedelta(days=90))
        rows = self.db.all("SELECT id FROM skills WHERE status='ACTIVE' AND coalesce(last_used_at, created_at) < ?", (cutoff,))
        for r in rows:
            self.set_status(r["id"], "RETIRED")
        return len(rows)
