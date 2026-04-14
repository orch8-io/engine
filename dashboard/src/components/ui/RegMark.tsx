import type { ReactNode } from "react";

/**
 * <RegMark> — corner registration marks, like the crop/register marks
 * printed on the margins of a printing plate. Purely decorative, but they
 * give blocks a measurable, engineered quality that ordinary borders lack.
 *
 * Wrap any block:
 *   <RegMark><YourContent /></RegMark>
 *
 * Options:
 *   corners: which corners get marks (default: "all")
 *   as:      element tag ("div" by default)
 */
export function RegMark({
  children,
  corners = "all",
  className = "",
  inset = 0,
}: {
  children: ReactNode;
  corners?: "all" | "outer" | "top" | "bottom";
  className?: string;
  /** px inset for marks from the corner; default flush with edge */
  inset?: number;
}) {
  const show = (pos: "tl" | "tr" | "bl" | "br") => {
    if (corners === "all") return true;
    if (corners === "outer") return pos === "tl" || pos === "tr" || pos === "bl" || pos === "br";
    if (corners === "top") return pos === "tl" || pos === "tr";
    if (corners === "bottom") return pos === "bl" || pos === "br";
    return false;
  };
  const style = inset ? { inset: `${inset}px` } : undefined;

  return (
    <div className={`relative ${className}`}>
      <div className="pointer-events-none absolute inset-0" style={style}>
        {show("tl") && <span className="regmark regmark-tl" />}
        {show("tr") && <span className="regmark regmark-tr" />}
        {show("bl") && <span className="regmark regmark-bl" />}
        {show("br") && <span className="regmark regmark-br" />}
      </div>
      {children}
    </div>
  );
}
