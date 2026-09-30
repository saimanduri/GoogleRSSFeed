"""HTML -> text in the gateway (no JavaScript, no remote resources).

Visible text is returned as the main content. Text that a human would not see (display:none,
visibility:hidden, aria-hidden, tiny fonts, HTML comments, <meta> content, alt/title attributes)
is returned SEPARATELY and labelled, because it is a common prompt-injection carrier.
"""
from __future__ import annotations

import re
from html.parser import HTMLParser

SKIP_TAGS = {"script", "style", "noscript", "template", "svg", "canvas", "iframe", "object", "embed", "head"}
BLOCK_TAGS = {"p", "div", "br", "li", "tr", "h1", "h2", "h3", "h4", "h5", "h6", "section", "article", "header",
              "footer", "table", "ul", "ol", "pre", "blockquote", "hr", "dd", "dt"}
VOID_TAGS = {"br", "hr", "img", "meta", "link", "input", "area", "base", "col", "source", "track", "wbr"}
HIDDEN_STYLE = re.compile(r"display\s*:\s*none|visibility\s*:\s*hidden|font-size\s*:\s*0|opacity\s*:\s*0(\.0+)?\s*(;|$)|"
                          r"color\s*:\s*(#fff(fff)?|white)\b.*background(-color)?\s*:\s*(#fff(fff)?|white)", re.I)


class _Extractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.visible: list[str] = []
        self.hidden: list[dict] = []
        self.title = ""
        self._stack: list[tuple[str, bool, bool]] = []  # (tag, skip, hidden)
        self._in_title = False

    def _state(self) -> tuple[bool, bool]:
        skip = any(s for _, s, _ in self._stack)
        hidden = any(h for _, _, h in self._stack)
        return skip, hidden

    def handle_starttag(self, tag, attrs):
        a = {k.lower(): (v or "") for k, v in attrs}
        if tag == "title":
            self._in_title = True
        if tag == "meta" and a.get("content"):
            self.hidden.append({"kind": "meta", "text": f"{a.get('name') or a.get('property') or 'meta'}: {a['content']}"[:2000]})
        for attr in ("alt", "title", "aria-label"):
            if a.get(attr):
                self.hidden.append({"kind": f"attr:{attr}", "text": a[attr][:2000]})
        is_hidden = ("hidden" in a or a.get("aria-hidden") == "true" or bool(HIDDEN_STYLE.search(a.get("style", "")))
                     or a.get("type") == "hidden")
        if tag in BLOCK_TAGS:
            self.visible.append("\n")
        if tag not in VOID_TAGS:
            self._stack.append((tag, tag in SKIP_TAGS, is_hidden))

    def handle_endtag(self, tag):
        if tag == "title":
            self._in_title = False
        for i in range(len(self._stack) - 1, -1, -1):
            if self._stack[i][0] == tag:
                del self._stack[i:]
                break
        if tag in BLOCK_TAGS:
            self.visible.append("\n")

    def handle_data(self, data):
        if self._in_title:
            self.title += data
            return
        skip, hidden = self._state()
        if skip or not data.strip():
            if not skip:
                self.visible.append(" ")
            return
        if hidden:
            self.hidden.append({"kind": "hidden_text", "text": data.strip()[:2000]})
        else:
            self.visible.append(data)

    def handle_comment(self, data):
        if data.strip():
            self.hidden.append({"kind": "comment", "text": data.strip()[:2000]})


def html_to_text(html: str, max_chars: int = 200_000) -> dict:
    p = _Extractor()
    try:
        p.feed(html)
        p.close()
    except Exception:  # noqa: BLE001 - malformed HTML must not crash the gateway
        pass
    text = "".join(p.visible)
    text = re.sub(r"[ \t\r\f\v]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n\n", text).strip()
    return {"title": p.title.strip()[:500], "text": text[:max_chars], "hidden": p.hidden[:200],
            "truncated": len(text) > max_chars}
