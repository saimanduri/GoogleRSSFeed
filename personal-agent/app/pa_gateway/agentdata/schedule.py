"""Schedules: 5-field cron, intervals, one-off times and event triggers - IANA timezone, DST-safe.

Schedule JSON:
  {"type": "cron", "cron": "30 7 * * 1-5"}
  {"type": "interval", "minutes": 60}
  {"type": "once", "at": "2026-10-05T09:00"}            (local time in the mission's timezone)
  {"type": "event", "event": "new_mail" | "new_file", "senders": [...], "folders": [...]}
"""
from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

DOW = {"sun": 0, "mon": 1, "tue": 2, "wed": 3, "thu": 4, "fri": 5, "sat": 6}
MON = {m: i for i, m in enumerate(["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"], 1)}


class ScheduleError(ValueError):
    pass


def _field(spec: str, lo: int, hi: int, names: dict[str, int] | None = None) -> set[int]:
    out: set[int] = set()
    for part in spec.lower().split(","):
        step = 1
        if "/" in part:
            part, s = part.split("/", 1)
            step = int(s)
            if step < 1:
                raise ScheduleError("bad step")
        if names:
            for k, v in names.items():
                part = part.replace(k, str(v))
        if part in ("*", ""):
            a, b = lo, hi
        elif "-" in part:
            a, b = (int(x) for x in part.split("-", 1))
        else:
            a = b = int(part)
        if a < lo or b > hi or a > b:
            raise ScheduleError(f"value out of range in '{spec}'")
        out.update(range(a, b + 1, step))
    return out


class Cron:
    def __init__(self, expr: str):
        parts = expr.split()
        if len(parts) != 5:
            raise ScheduleError("cron needs 5 fields: minute hour day month weekday")
        self.expr = expr
        self.minute = _field(parts[0], 0, 59)
        self.hour = _field(parts[1], 0, 23)
        self.dom = _field(parts[2], 1, 31)
        self.month = _field(parts[3], 1, 12, MON)
        dow = _field(parts[4].replace("7", "0") if parts[4] == "7" else parts[4], 0, 7, DOW)
        self.dow = {0 if d == 7 else d for d in dow}
        self.dom_any, self.dow_any = parts[2] == "*", parts[4] == "*"

    def matches(self, dt: datetime) -> bool:
        wd = (dt.weekday() + 1) % 7
        day_ok = (dt.day in self.dom) if self.dow_any else (wd in self.dow) if self.dom_any else (dt.day in self.dom or wd in self.dow)
        return dt.minute in self.minute and dt.hour in self.hour and dt.month in self.month and day_ok

    def next_after(self, after_local: datetime) -> datetime:
        dt = after_local.replace(second=0, microsecond=0) + timedelta(minutes=1)
        for _ in range(60 * 24 * 370):
            if dt.month not in self.month:
                dt = (dt.replace(day=1, hour=0, minute=0) + timedelta(days=32)).replace(day=1)
                continue
            if self.matches(dt):
                return dt
            if dt.hour not in self.hour:
                dt = dt.replace(minute=0) + timedelta(hours=1)
                continue
            dt += timedelta(minutes=1)
        raise ScheduleError("cron never fires")


def next_run(schedule: dict, tz: str, after_utc: datetime) -> datetime | None:
    """Next fire time in UTC (None for event triggers / finished one-offs)."""
    zone = ZoneInfo(tz)
    t = schedule.get("type")
    if t == "cron":
        local = after_utc.astimezone(zone).replace(tzinfo=None)
        nxt = Cron(schedule["cron"]).next_after(local)
        return nxt.replace(tzinfo=zone).astimezone(timezone.utc)  # zoneinfo resolves DST gaps/folds
    if t == "interval":
        minutes = int(schedule["minutes"])
        if minutes < 5:
            raise ScheduleError("minimum interval is 5 minutes")
        return after_utc + timedelta(minutes=minutes)
    if t == "once":
        at = datetime.fromisoformat(schedule["at"])
        at = at.replace(tzinfo=zone) if at.tzinfo is None else at
        at_utc = at.astimezone(timezone.utc)
        return at_utc if at_utc > after_utc else None
    if t == "event":
        return None
    raise ScheduleError("unknown schedule type")


def validate(schedule: dict, tz: str) -> None:
    try:
        ZoneInfo(tz)
    except Exception as e:  # noqa: BLE001
        raise ScheduleError(f"unknown timezone {tz}") from e
    t = schedule.get("type")
    if t == "cron":
        Cron(schedule.get("cron", ""))
    elif t == "interval":
        if int(schedule.get("minutes", 0)) < 5:
            raise ScheduleError("minimum interval is 5 minutes")
    elif t == "once":
        datetime.fromisoformat(schedule.get("at", ""))
    elif t == "event":
        if schedule.get("event") not in ("new_mail", "new_file"):
            raise ScheduleError("event must be new_mail or new_file")
    else:
        raise ScheduleError("unknown schedule type")


_DAYS = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]


def _every(n: int, unit: str) -> str:
    return f"Every {unit}" if n == 1 else f"Every {n} {unit}s"


def describe_cron(expr: str) -> str:
    """Plain-language text for the cron shapes the UI and the plain-words parser produce; falls back to the raw cron."""
    try:
        mi, ho, dom, mon, dow = expr.split()
        c = Cron(expr)
    except (ValueError, ScheduleError):
        return f"cron {expr}"
    days = ""
    if dow != "*" and dom == "*" and mon == "*":
        ds = sorted(c.dow)
        days = " on weekdays" if ds == [1, 2, 3, 4, 5] else " on weekends" if ds == [0, 6] else " on " + ", ".join(_DAYS[d] for d in ds)
    elif dom != "*" or mon != "*":
        return f"cron {expr}"
    plain = lambda s: s.isdigit()  # noqa: E731
    if plain(mi) and plain(ho):
        return f"Every day at {int(ho):02d}:{int(mi):02d}" if not days else f"At {int(ho):02d}:{int(mi):02d}{days}"
    window = ""
    if "-" in ho.split("/")[0]:
        a, b = ho.split("/")[0].split("-")
        window = f" between {int(a):02d}:00 and {int(b):02d}:59"
    elif ho != "*" and not plain(ho) and not ho.startswith("*/"):
        return f"cron {expr}"
    if mi.startswith("*/") and mi[2:].isdigit():
        return f"{_every(int(mi[2:]), 'minute')}{window}{days}"
    if plain(mi) and "/" in ho and ho.split("/")[1].isdigit():
        return f"{_every(int(ho.split('/')[1]), 'hour')}{window}{days}" + (f" (at :{int(mi):02d})" if int(mi) else "")
    return f"cron {expr}"


def describe(schedule: dict) -> str:
    t = schedule.get("type")
    if t == "cron":
        return describe_cron(schedule["cron"])
    if t == "interval":
        m = int(schedule["minutes"])
        return _every(m // 60, "hour") if m % 60 == 0 else _every(m, "minute")
    if t == "once":
        return f"once at {schedule['at']}"
    if t == "event":
        return f"when {schedule['event'].replace('_', ' ')} arrives"
    return "?"


_TIME = r"(?:at\s+)?(\d{1,2})(?::(\d{2}))?\s*(am|pm)?"


def parse_plain(text: str) -> dict | None:
    """Deterministic plain-words schedule parsing (spec 39.15). Returns None if not understood."""
    s = text.lower().strip()
    m = re.search(r"every\s+(\d+)\s*(minute|min|hour|hr)s?", s)
    if m:
        n = int(m.group(1)) * (60 if m.group(2).startswith("h") else 1)
        return {"type": "interval", "minutes": max(n, 5)}
    if re.search(r"\bevery\s+hour\b|\bhourly\b", s):
        return {"type": "interval", "minutes": 60}
    if "new mail" in s or "new email" in s or "email arrives" in s:
        return {"type": "event", "event": "new_mail"}
    if "new file" in s:
        return {"type": "event", "event": "new_file"}
    tm = re.search(_TIME, s[s.find(" at ") :] if " at " in s else "")
    hour, minute = 9, 0
    if tm:
        hour, minute = int(tm.group(1)), int(tm.group(2) or 0)
        if tm.group(3) == "pm" and hour < 12:
            hour += 12
        if tm.group(3) == "am" and hour == 12:
            hour = 0
    if not (0 <= hour < 24 and 0 <= minute < 60):
        return None
    if "weekday" in s:
        return {"type": "cron", "cron": f"{minute} {hour} * * 1-5"}
    if "weekend" in s:
        return {"type": "cron", "cron": f"{minute} {hour} * * 0,6"}
    days = [d for name, d in DOW.items() if re.search(rf"\b{name}[a-z]*\b", s)]
    if days and "every" in s:
        return {"type": "cron", "cron": f"{minute} {hour} * * {','.join(str(d) for d in sorted(days))}"}
    if re.search(r"every\s+(day|morning|evening|night)|daily|each day", s):
        return {"type": "cron", "cron": f"{minute} {hour} * * *"}
    m = re.search(r"every\s+month\s+on\s+(?:the\s+)?(\d{1,2})", s)
    if m:
        return {"type": "cron", "cron": f"{minute} {hour} {int(m.group(1))} * *"}
    if re.search(r"every\s+week\b|weekly", s):
        return {"type": "cron", "cron": f"{minute} {hour} * * 1"}
    return None
