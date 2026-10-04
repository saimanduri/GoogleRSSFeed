// Twenty widely used office / professional fonts (all ship with Windows 10/11 or Microsoft Office) for Settings > Appearance & Voice.
// The app never downloads fonts: a font only shows if it is installed on this PC, and the picker marks the ones that are not.
// Every stack ends with Nirmala UI (Hindi and other Indian scripts) and a generic family, so mixed-language text always renders.
export type FontDef = { id: string; label: string; stack: string; family: string; note: string };
const tail = (generic: "sans-serif" | "serif") => `"Nirmala UI", "Segoe UI", ${generic}`;
export const FONTS: FontDef[] = [
  { id: "windows", label: "Windows default", family: "Segoe UI Variable", stack: `"Segoe UI Variable", "Segoe UI", system-ui, -apple-system, Roboto, ${tail("sans-serif")}`, note: "Segoe UI Variable - the Windows 11 look" },
  { id: "segoe_ui", label: "Segoe UI", family: "Segoe UI", stack: `"Segoe UI", ${tail("sans-serif")}`, note: "Windows classic" },
  { id: "calibri", label: "Calibri", family: "Calibri", stack: `"Calibri", ${tail("sans-serif")}`, note: "Office favourite" },
  { id: "aptos", label: "Aptos", family: "Aptos", stack: `"Aptos", "Calibri", ${tail("sans-serif")}`, note: "New Microsoft 365 default" },
  { id: "arial", label: "Arial", family: "Arial", stack: `"Arial", ${tail("sans-serif")}`, note: "Universal" },
  { id: "verdana", label: "Verdana", family: "Verdana", stack: `"Verdana", ${tail("sans-serif")}`, note: "Wide and very readable" },
  { id: "tahoma", label: "Tahoma", family: "Tahoma", stack: `"Tahoma", ${tail("sans-serif")}`, note: "Compact and clear" },
  { id: "trebuchet", label: "Trebuchet MS", family: "Trebuchet MS", stack: `"Trebuchet MS", ${tail("sans-serif")}`, note: "Friendly humanist" },
  { id: "georgia", label: "Georgia", family: "Georgia", stack: `"Georgia", ${tail("serif")}`, note: "Serif for long reading" },
  { id: "times", label: "Times New Roman", family: "Times New Roman", stack: `"Times New Roman", ${tail("serif")}`, note: "Classic document serif" },
  { id: "cambria", label: "Cambria", family: "Cambria", stack: `"Cambria", ${tail("serif")}`, note: "Office serif" },
  { id: "candara", label: "Candara", family: "Candara", stack: `"Candara", ${tail("sans-serif")}`, note: "Soft and warm" },
  { id: "corbel", label: "Corbel", family: "Corbel", stack: `"Corbel", ${tail("sans-serif")}`, note: "Clean on screens" },
  { id: "constantia", label: "Constantia", family: "Constantia", stack: `"Constantia", ${tail("serif")}`, note: "Elegant serif" },
  { id: "franklin", label: "Franklin Gothic", family: "Franklin Gothic Medium", stack: `"Franklin Gothic Medium", "Franklin Gothic", ${tail("sans-serif")}`, note: "Newspaper sans" },
  { id: "century_gothic", label: "Century Gothic", family: "Century Gothic", stack: `"Century Gothic", ${tail("sans-serif")}`, note: "Geometric (Office)" },
  { id: "garamond", label: "Garamond", family: "Garamond", stack: `"Garamond", ${tail("serif")}`, note: "Refined serif (Office)" },
  { id: "book_antiqua", label: "Book Antiqua", family: "Book Antiqua", stack: `"Book Antiqua", "Palatino Linotype", ${tail("serif")}`, note: "Formal serif" },
  { id: "palatino", label: "Palatino Linotype", family: "Palatino Linotype", stack: `"Palatino Linotype", "Book Antiqua", ${tail("serif")}`, note: "Book serif" },
  { id: "bahnschrift", label: "Bahnschrift", family: "Bahnschrift", stack: `"Bahnschrift", "Segoe UI", ${tail("sans-serif")}`, note: "Modern industrial sans" },
  { id: "nirmala", label: "Nirmala UI", family: "Nirmala UI", stack: `"Nirmala UI", "Segoe UI", ${tail("sans-serif")}`, note: "Best for Hindi and Indian languages" },
];
export const SIZES: [string, string, number][] = [["small", "Small", 0.9], ["medium", "Medium", 1], ["large", "Large", 1.15]];

export function fontStack(id: string | undefined): string { return (FONTS.find((f) => f.id === id) ?? FONTS[0]).stack; }
export function sizeFactor(id: string | undefined): number { return (SIZES.find((s) => s[0] === id) ?? SIZES[1])[2]; }

const cache = new Map<string, boolean>();
/** Is this font installed? Measures a test string against two generic families (document.fonts.check cannot tell). */
export function fontInstalled(family: string): boolean {
  if (family === "Segoe UI Variable") family = "Segoe UI";
  const hit = cache.get(family);
  if (hit !== undefined) return hit;
  let ok = true;
  try {
    const c = document.createElement("canvas").getContext("2d")!;
    const text = "mmmmmmmmmmlliWWWW 0123456789 नमस्ते";
    const w = (f: string) => { c.font = `72px ${f}`; return c.measureText(text).width; };
    const a = w("monospace"), b = w("serif"), d = w("sans-serif");
    const x = w(`"${family}", monospace`), y = w(`"${family}", serif`), z = w(`"${family}", sans-serif`);
    ok = x !== a || y !== b || z !== d;
  } catch { ok = true; }
  cache.set(family, ok);
  return ok;
}
