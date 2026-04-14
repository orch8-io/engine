import { useState, type ReactNode } from "react";

/**
 * <Glossary> — an opt-in "How to read this page" module.
 *
 * Rendered as a collapsed strip at the top of a page. Click "How to read
 * this page" to expand a two-column definition list explaining every
 * non-obvious term used on the page.
 *
 * The purpose is trust: the operator should never have to guess what a
 * word means. This is the antidote to jargon fog.
 */
export function Glossary({
  items,
  defaultOpen = false,
}: {
  items: GlossaryItem[];
  defaultOpen?: boolean;
}) {
  const [open, setOpen] = useState(defaultOpen);

  return (
    <div className="border-t border-b border-rule">
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        className="w-full flex items-center justify-between py-2.5 px-0 text-left group"
        aria-expanded={open}
      >
        <span className="flex items-center gap-3">
          <span className="eyebrow">How to read this page</span>
          <span className="text-[11px] font-mono tabular text-faint">
            {items.length} terms
          </span>
        </span>
        <span className="font-mono text-[11px] uppercase tracking-[0.16em] text-muted group-hover:text-ink transition-colors">
          {open ? "hide —" : "show +"}
        </span>
      </button>

      {open && (
        <div className="pb-6 pt-2 fade-in">
          <dl className="grid grid-cols-1 md:grid-cols-2 gap-x-10 gap-y-4">
            {items.map((item) => (
              <div key={item.term} className="grid grid-cols-[120px_minmax(0,1fr)] gap-4">
                <dt className="field-label pt-0.5">{item.term}</dt>
                <dd className="text-[13px] leading-relaxed text-ink-dim">
                  {item.definition}
                </dd>
              </div>
            ))}
          </dl>
        </div>
      )}
    </div>
  );
}

export interface GlossaryItem {
  term: string;
  definition: ReactNode;
}
