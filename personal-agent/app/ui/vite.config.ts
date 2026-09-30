import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// The UI is bundled into the Tauri app (no remote content). `npm run dev` also works in a normal browser
// with a built-in mock gateway for UI development on any OS (see src/api/mock.ts).
export default defineConfig({
  plugins: [react()],
  clearScreen: false,
  server: { port: 5173, strictPort: true, host: "127.0.0.1" },
  build: { target: "es2022", sourcemap: false, outDir: "dist", chunkSizeWarningLimit: 2000 },
});
