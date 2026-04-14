import type { ReactNode } from "react";

/**
 * <Section> — a labeled region of a page.
 *
 * Replaces the old <Panel>/<PanelHeader>/<PanelBody> triad. The visual
 * language is hairlines and registration marks rather than card chrome.
 *
 * Layout:
 *   ── EYEBROW · MONO · CAPS ─────────────── meta ──
 *   Section Title                                    ← h2
 *   One-line description explaining what you're      ← prose
 *   looking at and how to read it.
 *   ────────────────────────────────────────────────
 *   [children]
 */
export function Section({
  eyebrow,
  title,
  description,
  meta,
  children,
  annotation,
  id,
}: {
  eyebrow?: string;
  title?: string;
  description?: ReactNode;
  meta?: ReactNode;
  annotation?: ReactNode;
  children: ReactNode;
  id?: string;
}) {
  return (
    <section id={id} className="relative">
      {(eyebrow || title || description || meta) && (
        <header className="mb-5">
          {(eyebrow || meta) && (
            <div className="flex items-center justify-between mb-3">
              {eyebrow && <span className="eyebrow">{eyebrow}</span>}
              {meta && <div className="flex items-center gap-3 text-[11px] font-mono tabular text-faint">{meta}</div>}
            </div>
          )}
          {title && <h2 className="section-title mb-1.5">{title}</h2>}
          {description && (
            <p className="annotation max-w-[72ch]">{description}</p>
          )}
          <div className="rule mt-4" />
        </header>
      )}

      {annotation ? (
        /* Tufte margin layout: data on the left, annotation on the right. */
        <div className="grid grid-cols-1 lg:grid-cols-[minmax(0,1fr)_260px] gap-8">
          <div>{children}</div>
          <aside className="lg:pt-1">
            <div className="annotation border-l border-rule pl-4">{annotation}</div>
          </aside>
        </div>
      ) : (
        children
      )}
    </section>
  );
}
