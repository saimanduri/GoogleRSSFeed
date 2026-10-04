"""Every setting in settings_schema.py (one GUI control each): change it, read it back, see it persisted in the encrypted DB,
and put it back to the default. Generated from the schema, so a new setting is covered automatically (pending item R6)."""
import json

import pytest

import pa_gateway.settings as settings_mod
from pa_common.errors import PAError
from pa_gateway.settings_schema import SETTINGS


def _other_value(s):
    """A valid value different from the default (or None when the schema allows only one)."""
    if s.type == "bool":
        return not s.default
    if s.type == "enum":
        return next((o for o in s.options if o != s.default), None)
    if s.type in ("int", "float"):
        lo = s.min if s.min is not None else s.default
        hi = s.max if s.max is not None else s.default + 1
        for c in (lo, hi, (lo + hi) // 2 if s.type == "int" else (lo + hi) / 2):
            if c != s.default:
                return int(c) if s.type == "int" else float(c)
        return None
    if s.type == "time":
        return "07:30" if s.default != "07:30" else "08:45"
    if s.type == "list":
        return list(s.default) + ["roundtrip.example"] if s.default is not None else ["roundtrip.example"]
    if s.type == "str":
        return "roundtrip-test" if s.default != "roundtrip-test" else "roundtrip-test-2"
    return None


def _apply(gw, changes):
    """Apply like the UI does: loosening changes go through begin_loosen + (shortened) read delay + password."""
    info = gw.settings.classify(changes)
    token = gw.settings.begin_loosen(changes)["token"] if info["loosening"] else None
    return gw.settings.apply(changes, password_ok=True, loosen_token=token, stepup_ok=True)


def test_every_setting_roundtrips(env_nocore, monkeypatch):
    env_nocore.setup(model=False)
    gw = env_nocore.gw
    monkeypatch.setattr(settings_mod, "LOOSEN_DELAY_SECONDS", 0)
    problems, changed, skipped = [], 0, []
    for s in SETTINGS:
        new = _other_value(s)
        if new is None:
            skipped.append(s.key)
            continue
        try:
            _apply(gw, {s.key: new})
        except PAError as e:
            if e.code == "floor_violation":      # the schema forbids this direction on purpose
                skipped.append(s.key)
                continue
            problems.append(f"{s.key}: set {new!r} -> {e.code}: {e}")
            continue
        except Exception as e:  # noqa: BLE001
            problems.append(f"{s.key}: set {new!r} -> {type(e).__name__}: {e}")
            continue
        got = gw.settings.get(s.key)
        stored = json.loads(gw.db.scalar("SELECT value_json FROM settings WHERE key=?", (s.key,)))
        if got != new or stored != new:
            problems.append(f"{s.key}: wrote {new!r}, read {got!r}, stored {stored!r}")
            continue
        changed += 1
        try:
            _apply(gw, {s.key: s.default})
        except PAError as e:
            problems.append(f"{s.key}: reset -> {e.code}")
            continue
        if gw.settings.get(s.key) != s.default:
            problems.append(f"{s.key}: reset to default failed")
    assert not problems, "\n".join(problems)
    assert changed > 100, (changed, skipped)


def test_out_of_range_and_wrong_types_are_refused_for_every_setting(env_nocore):
    env_nocore.setup(model=False)
    gw = env_nocore.gw
    bad = {"bool": ["maybe", [1]], "int": ["x", 10**12, -10**12, [1]], "float": ["x", [1]], "enum": ["no-such-option", 5], "list": ["not-a-list", {"a": 1}],
           "time": ["25:99", 7], "str": [["x"], {"a": 1}]}
    leaks = []
    for s in SETTINGS:
        for v in bad.get(s.type, []):
            try:
                gw.settings.classify({s.key: v})
            except PAError:
                continue
            except Exception as e:  # noqa: BLE001
                leaks.append(f"{s.key} <- {v!r}: {type(e).__name__}")
                continue
            if s.type in ("bool", "int", "float", "enum", "time"):      # strings like "x" must never be accepted for these
                if not (s.type == "int" and isinstance(v, int) and (s.min is None and s.max is None)):
                    leaks.append(f"{s.key} accepted {v!r}")
    assert not leaks, "\n".join(leaks[:20])


def test_unknown_setting_is_rejected(env_nocore):
    env_nocore.setup(model=False)
    with pytest.raises(PAError):
        env_nocore.gw.settings.classify({"no.such.setting": 1})
