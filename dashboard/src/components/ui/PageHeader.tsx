import type { ReactNode } from "react";

/**
 * <PageHeader> — the masthead of every page.
 *
 *   OPERATOR · ORCH8                                  [meta · actions]
 *   ──────────────────────────────────────────────────
 *   Overview
 *   Real-time health, throughput, and failure signals
 *   across every tenant routed through this cluster.
 *   ──────────────────────────────────────────────────
 *
 * The page title uses the display-title class (40px/-0.035em). Description
 * is a single Tufte-style annotation paragraph capped at 72ch. A single
 * hairline delineates the header from the page body — no boxes, no chrome.
 */
export function PageHeader({
  eyebrow,
  title,
  description,
  actions,
}: {
  eyebrow?: ReactNode;
  title: ReactNode;
  description?: ReactNode;
  actions?: ReactNode;
}) {
  return (
    <header className="pb-8 mb-2 border-b border-rule">
      <div className="flex items-start justify-between gap-6 mb-6">
        {eyebrow && <div className="eyebrow">{eyebrow}</div>}
        {actions && (
          <div className="flex items-center gap-3 shrink-0 -mt-1">{actions}</div>
        )}
      </div>
      <div className="min-w-0">
        <h1 className="display-title">{title}</h1>
        {description && (
          <p className="mt-3 prose-body max-w-[72ch]">{description}</p>
        )}
      </div>
    </header>
  );
}
