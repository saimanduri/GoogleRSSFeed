// Sanitised markdown: no remote images, no scripts/iframes/styles; links show the full URL and open in the
// default browser only after a click-through (spec 5.2 Chat, 14.3).
import DOMPurify from "dompurify";
import { marked } from "marked";
import { useMemo, useState } from "react";
import { openExternal } from "../api/gateway";
import { Button, Modal } from "./ui";

marked.setOptions({ gfm: true, breaks: true });

export function Markdown({ text }: { text: string }) {
  const [link, setLink] = useState<string | null>(null);
  const html = useMemo(() => {
    const raw = marked.parse(text ?? "", { async: false }) as string;
    return DOMPurify.sanitize(raw, {
      FORBID_TAGS: ["img", "picture", "source", "video", "audio", "iframe", "object", "embed", "style", "script", "form", "input", "svg", "math", "link", "meta"],
      FORBID_ATTR: ["style", "srcset", "src", "onerror", "onload"],
      ALLOWED_URI_REGEXP: /^https:\/\//i,
    });
  }, [text]);
  return (
    <>
      <div className="md" dangerouslySetInnerHTML={{ __html: html }} onClick={(e) => {
        const a = (e.target as HTMLElement).closest("a");
        if (a) { e.preventDefault(); const href = a.getAttribute("href"); if (href) setLink(href); }
      }} />
      {link && (
        <Modal title="Open external link?" onClose={() => setLink(null)}
          actions={<><Button onClick={() => setLink(null)}>Cancel</Button><Button kind="primary" onClick={() => { void openExternal(link); setLink(null); }}>Open in browser</Button></>}>
          <p className="muted">This link came from the agent's answer and may originate from untrusted content. Check the full address:</p>
          <pre className="code">{link}</pre>
        </Modal>
      )}
    </>
  );
}
