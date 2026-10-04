// Gateway access. Inside the Tauri app every call goes through the Rust shell to pa-gateway's named pipe.
// In a plain browser (npm run dev) a mock gateway is used so the UI can be developed on any OS.
import { mockCall, mockListen } from "./mock";

export type GwError = { code: string; message: string; details?: Record<string, unknown> };
export class ApiError extends Error {
  code: string;
  details: Record<string, unknown>;
  constructor(e: GwError) {
    super(e.message);
    this.code = e.code;
    this.details = e.details ?? {};
  }
}

export const inTauri = typeof window !== "undefined" && "__TAURI_INTERNALS__" in window;

type Listener = (topic: string, data: any) => void;
const listeners = new Set<Listener>();
let wired = false;

async function wire() {
  if (wired) return;
  wired = true;
  if (inTauri) {
    const { listen } = await import("@tauri-apps/api/event");
    await listen<any>("gw-event", (e) => listeners.forEach((l) => l(e.payload.topic, e.payload.data)));
    await listen<any>("gw-status", (e) => listeners.forEach((l) => l("gw.status", e.payload)));
    await listen<any>("gw-local", (e) => listeners.forEach((l) => l("gw.local", e.payload)));
  } else {
    mockListen((topic, data) => listeners.forEach((l) => l(topic, data)));
  }
}

export function onEvent(fn: Listener): () => void {
  listeners.add(fn);
  void wire();
  return () => listeners.delete(fn);
}

export async function rpc<T = any>(method: string, params: Record<string, unknown> = {}): Promise<T> {
  await wire();
  try {
    if (inTauri) {
      const { invoke } = await import("@tauri-apps/api/core");
      return (await invoke("rpc", { method, params })) as T;
    }
    return (await mockCall(method, params)) as T;
  } catch (e: any) {
    if (e instanceof ApiError) throw e;
    if (e && typeof e === "object" && "code" in e) throw new ApiError(e as GwError);
    throw new ApiError({ code: "error", message: String(e?.message ?? e) });
  }
}

export async function native<T = any>(cmd: string, args: Record<string, unknown> = {}): Promise<T | undefined> {
  if (!inTauri) return undefined;
  const { invoke } = await import("@tauri-apps/api/core");
  return (await invoke(cmd, args)) as T;
}

export async function pickFile(opts: { directory?: boolean; filters?: { name: string; extensions: string[] }[]; multiple?: boolean } = {}) {
  if (!inTauri) return window.prompt("Path (browser preview):") || null;
  const { open } = await import("@tauri-apps/plugin-dialog");
  return (await open({ directory: !!opts.directory, multiple: !!opts.multiple, filters: opts.filters })) as any;
}

/** Files dropped onto the window (Tauri gives real paths; a browser preview does not). Returns an unsubscribe function. */
export async function onFileDrop(cb: (paths: string[], phase: "over" | "drop" | "leave") => void): Promise<() => void> {
  if (!inTauri) return () => undefined;
  const { getCurrentWebview } = await import("@tauri-apps/api/webview");
  const off = await getCurrentWebview().onDragDropEvent((e: any) => {
    const t = e.payload.type;
    if (t === "over" || t === "enter") cb([], "over");
    else if (t === "drop") cb(e.payload.paths ?? [], "drop");
    else cb([], "leave");
  });
  return off;
}

export async function pickSavePath(defaultPath: string, filters?: { name: string; extensions: string[] }[]) {
  if (!inTauri) return window.prompt("Save to path (browser preview):", defaultPath) || null;
  const { save } = await import("@tauri-apps/plugin-dialog");
  return await save({ defaultPath, filters });
}

export async function openExternal(url: string) {
  if (!/^https:\/\//i.test(url)) return;
  if (!inTauri) {
    window.open(url, "_blank", "noopener,noreferrer");
    return;
  }
  const { openUrl } = await import("@tauri-apps/plugin-opener");
  await openUrl(url);
}
