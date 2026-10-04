import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import "./styles.css";
import "./themes.css";
import "./extras.css";
import "./accents.css";
import "./polish.css";
import { AppProvider } from "./app";
import { Root } from "./Root";

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <AppProvider>
      <Root />
    </AppProvider>
  </StrictMode>,
);
