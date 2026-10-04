import { useApp } from "./app";
import { Shell } from "./Shell";
import { Onboarding } from "./screens/Onboarding";
import { SignIn } from "./screens/SignIn";

export function Root() {
  const { status, connected } = useApp();
  if (!status) {
    return (
      <div className="auth-wrap">
        <div className="auth-card center" style={{ gap: 12 }}>
          <span className="spinner" />
          <div className="muted">{connected ? "Starting ChiRAG Agent..." : "Waiting for the ChiRAG Agent service (pa-gateway)..."}</div>
        </div>
      </div>
    );
  }
  if (status.state === "SETUP_REQUIRED") return <Onboarding />;
  if (status.state !== "UNLOCKED") return <SignIn />;
  return <Shell />;
}
