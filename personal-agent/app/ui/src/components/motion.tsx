// Exit animations without changing how callers mount/unmount things.
// useExitGhost: when the element unmounts, a non-interactive copy stays on screen for a moment and transitions to its
// closed state (CSS: .exit-ghost.exit-out ...), then removes itself. Enter animations are CSS transitions (@starting-style),
// so both directions are interruptible: reopening while the ghost fades simply cross-fades into the new element.
import { useLayoutEffect, useRef } from "react";

const EXIT_MS = 440;

export function useExitGhost<T extends HTMLElement>(mode: "overlay" | "rect" = "rect") {
  const ref = useRef<T | null>(null);
  useLayoutEffect(() => {
    const node = ref.current;
    if (!node) return;
    return () => {
      const r = node.getBoundingClientRect(); // still attached while React runs the cleanup of an unmounting component
      queueMicrotask(() => {
        // React StrictMode re-runs effects on the same live node: only ghost a node that really left the page
        if (node.isConnected || document.visibilityState === "hidden") return;
        if (mode === "rect" && r.width === 0) return;
        const g = node.cloneNode(true) as HTMLElement;
        g.removeAttribute("id");
        g.setAttribute("aria-hidden", "true");
        g.setAttribute("inert", "");
        g.classList.add("exit-ghost");
        if (mode === "rect") {
          Object.assign(g.style, { position: "fixed", left: `${r.left}px`, top: `${r.top}px`, width: `${r.width}px`, height: `${r.height}px`, margin: "0", right: "auto", bottom: "auto" });
        }
        document.body.appendChild(g);
        requestAnimationFrame(() => requestAnimationFrame(() => g.classList.add("exit-out")));
        setTimeout(() => g.remove(), EXIT_MS);
      });
    };
  }, [mode]);
  return ref;
}

/** Where the dialog came from: the control that had focus when it opened (spatial consistency). */
export function anchorOrigin(modal: HTMLElement | null, trigger: Element | null) {
  if (!modal || !(trigger instanceof HTMLElement) || trigger === document.body) return;
  const t = trigger.getBoundingClientRect(), m = modal.getBoundingClientRect();
  if (!t.width || !m.width) return;
  const ox = Math.min(Math.max(t.left + t.width / 2 - m.left, 0), m.width);
  const oy = Math.min(Math.max(t.top + t.height / 2 - m.top, 0), m.height);
  modal.style.setProperty("--ox", `${ox}px`);
  modal.style.setProperty("--oy", `${oy}px`);
}
