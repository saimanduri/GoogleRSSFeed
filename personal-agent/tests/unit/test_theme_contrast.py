"""WCAG AA contrast (4.5:1 text, 3:1 large/graphics) for every theme, computed from the real CSS tokens."""
import re
from pathlib import Path

import pytest

UI = Path(__file__).resolve().parents[2] / "app" / "ui" / "src"


def _lin(c: float) -> float:
    c /= 255
    return c / 12.92 if c <= 0.03928 else ((c + 0.055) / 1.055) ** 2.4


def _lum(h: str) -> float:
    h = h.lstrip("#")
    r, g, b = (int(h[i:i + 2], 16) for i in (0, 2, 4))
    return 0.2126 * _lin(r) + 0.7152 * _lin(g) + 0.0722 * _lin(b)


def ratio(a: str, b: str) -> float:
    la, lb = sorted((_lum(a), _lum(b)), reverse=True)
    return (la + 0.05) / (lb + 0.05)


def _vars(block: str) -> dict:
    return dict(re.findall(r"--([a-z0-9-]+):\s*(#[0-9a-fA-F]{6})", block))


def _themes() -> dict:
    css = (UI / "themes.css").read_text(encoding="utf-8")
    base = _vars(re.search(r":root\s*\{(.*?)\n\}", (UI / "styles.css").read_text(encoding="utf-8"), re.S).group(1))
    out = {}
    for name, body in re.findall(r'\[data-theme="([a-z]+)"\]\s*\{(.*?)\n\}', css, re.S):
        out[name] = {**base, **_vars(body)}
    return out


THEMES = _themes()


def test_all_eight_themes_found():
    assert set(THEMES) >= {"light", "dark", "aurora", "ocean", "forest", "sunset"}


@pytest.mark.parametrize("name", sorted(THEMES))
def test_theme_meets_aa(name):
    t = THEMES[name]
    fails = []

    def need(fg, bg, minimum=4.5):
        r = ratio(t[fg], t[bg])
        if r < minimum:
            fails.append(f"{fg} on {bg} = {r:.2f} (< {minimum})")

    for surf in ("bg", "bg-elev", "bg-sunken", "bg-hover"):
        for fg in ("text", "text-2", "text-3"):
            need(fg, surf)
    for surf in ("bg-elev", "bg-hover", "accent-soft"):
        need("accent", surf)
    need("accent-text", "accent")
    need("accent-text", "accent-2")
    for tone in ("ok", "warn", "danger", "info"):
        need(tone, f"{tone}-soft")
        need(tone, "bg-elev")
    assert not fails, f"{name}: " + "; ".join(fails)


def test_solid_danger_has_white_text_contrast():
    base = _vars(re.search(r":root\s*\{(.*?)\n\}", (UI / "styles.css").read_text(encoding="utf-8"), re.S).group(1))
    assert ratio("#ffffff", base["danger-solid"]) >= 4.5


def test_os_accessibility_preferences_are_honoured():
    css = (UI / "polish.css").read_text(encoding="utf-8")
    for needle in ("prefers-reduced-motion", "prefers-reduced-transparency", "prefers-contrast: more", "@starting-style", "exit-ghost"):
        assert needle in css, needle
    # no endless animation may survive reduced motion
    assert "animation: none" in css


def test_animated_background_is_off_by_default():
    from pa_gateway.settings_schema import BY_KEY  # noqa: PLC0415
    assert BY_KEY["ui.background"].default == "off"


def _accents() -> list:
    css = (UI / "accents.css").read_text(encoding="utf-8")
    return [(n, s, _vars(b)) for n, s, b in re.findall(r':root\[data-accent="([a-z]+)"\]\[data-scheme="(light|dark)"\]\s*\{(.*?)\}', css)]


def test_five_accent_colours_in_both_schemes():
    names = {n for n, _, _ in _accents()}
    assert names == {"blue", "teal", "emerald", "violet", "graphite"}
    assert len(_accents()) == 10


@pytest.mark.parametrize("name,scheme,acc", _accents())
def test_accent_colour_meets_aa_on_every_theme_of_its_scheme(name, scheme, acc):
    themes = ["light", "ocean", "sunset"] if scheme == "light" else ["dark", "aurora", "forest"]
    fails = []
    for t in themes:
        T = THEMES[t]
        for surf in ("bg", "bg-elev", "bg-hover"):
            if ratio(acc["accent"], T[surf]) < 4.5:
                fails.append(f"{t}: accent on {surf}")
    if ratio(acc["accent"], acc["accent-soft"]) < 4.5:
        fails.append("accent on accent-soft")
    for k in ("accent", "accent-2"):
        if ratio(acc["accent-text"], acc[k]) < 4.5:
            fails.append(f"accent-text on {k}")
    assert not fails, f"{name}/{scheme}: {fails}"
