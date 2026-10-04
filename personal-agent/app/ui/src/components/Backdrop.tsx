// Subtle animated background (CSS only, no canvas, no network). Variant chosen in Settings > Appearance.
export function Backdrop({ kind }: { kind: string }) {
  if (!kind || kind === "off") return null;
  const r = (i: number, m: number) => ((i * 9301 + 49297) % 233280) / 233280 * m; // deterministic pseudo-random
  return (
    <div className={`backdrop ${kind}`} aria-hidden="true">
      {kind === "aurora" && <><span className="blob" /><span className="blob" /><span className="blob" /></>}
      {kind === "bubbles" && Array.from({ length: 16 }, (_, i) => {
        const size = 14 + r(i + 1, 46);
        return <span key={i} className="bub" style={{ left: `${r(i + 3, 100)}%`, width: size, height: size, animationDuration: `${16 + r(i + 7, 22)}s`, animationDelay: `${-r(i + 11, 30)}s` }} />;
      })}
      {kind === "waves" && (
        <>
          {[["w1", 0], ["w2", 1], ["w3", 2]].map(([c, i]) => (
            <svg key={c as string} viewBox="0 0 1440 320" preserveAspectRatio="none" style={{ height: `${28 + (i as number) * 5}vh` }}>
              <path className={c as string} d="M0 160 C 180 60 360 60 540 160 S 900 260 1080 160 S 1260 60 1440 160 V320 H0 Z" />
            </svg>
          ))}
        </>
      )}
      {kind === "stars" && Array.from({ length: 44 }, (_, i) => (
        <span key={i} className="star" style={{ left: `${r(i + 2, 100)}%`, top: `${r(i + 5, 100)}%`, width: 2 + r(i + 8, 3), height: 2 + r(i + 8, 3),
          animationDuration: `${3 + r(i + 4, 6)}s`, animationDelay: `${-r(i + 9, 8)}s` }} />
      ))}
    </div>
  );
}
