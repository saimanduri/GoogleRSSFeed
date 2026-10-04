// One place for the keyboard shortcuts of the left menu (used by the menu, the key handler and the Guide).
// Alt+letter is used because Ctrl+C / Ctrl+V / Ctrl+A etc. already mean copy / paste / select-all.
import type { Screen } from "./app";

export const NAV_KEYS: Record<Screen, { key: string; label: string }> = {
  home: { key: "h", label: "Alt+H" }, chat: { key: "c", label: "Alt+C" }, history: { key: "i", label: "Alt+I" }, missions: { key: "m", label: "Alt+M" },
  reminders: { key: "r", label: "Alt+R" }, tasks: { key: "t", label: "Alt+T" },  // Alt+T opens Activity log > Requests & steps (Tasks merged there)
  approvals: { key: "a", label: "Alt+A" }, files: { key: "f", label: "Alt+F" },
  memory: { key: "e", label: "Alt+E" }, secrets: { key: "s", label: "Alt+S" }, activity: { key: "l", label: "Alt+L" }, outlook: { key: "o", label: "Alt+O" },
  guide: { key: "g", label: "Alt+G" }, settings: { key: ",", label: "Alt+," },
};

export const OTHER_KEYS: [string, string][] = [
  ["Ctrl+K", "Command box: jump anywhere, search your history"], ["Alt+N", "New chat"], ["F1", "Open the Guide"], ["Ctrl+Shift+L", "Lock now"],
  ["Ctrl+Alt+Shift+S", "Emergency stop (works when the window is hidden)"], ["Esc", "Close a dialog or menu"],
];

export function screenForKey(e: KeyboardEvent): Screen | "new" | "guide" | null {
  if (e.key === "F1" && !e.ctrlKey && !e.altKey) return "guide";
  if (!e.altKey || e.ctrlKey || e.metaKey || e.shiftKey) return null;
  const k = e.key.toLowerCase();
  if (k === "n") return "new";
  const hit = (Object.entries(NAV_KEYS) as [Screen, { key: string }][]).find(([, v]) => v.key === k);
  return hit ? hit[0] : null;
}
