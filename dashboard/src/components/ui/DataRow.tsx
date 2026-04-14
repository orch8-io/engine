import type { ReactNode } from "react";

/**
 * <DataRow> — a key/value row laid out as a field label (left) next to a
 * value (right). Used in detail panels, metadata blocks, definition lists.
 *
 *   FIELD · LABEL              value aligned to the right column
 *   FIELD · LABEL              value
 *
 * The field-label column is narrow and in mono caps. The value column is
 * the reading — either text, a number, a badge, or composed content.
 */
export function DataRow({
  label,
  value,
  hint,
  align = "right",
}: {
  label: string;
  value: ReactNode;
  hint?: string;
  align?: "left" | "right";
}) {
  return (
    <div className="grid grid-cols-[140px_minmax(0,1fr)] gap-6 py-2 border-b border-rule-faint last:border-b-0">
      <div className="field-label pt-0.5">{label}</div>
      <div className={`text-[13px] text-ink ${align === "right" ? "text-right" : "text-left"}`}>
        <div>{value}</div>
        {hint && <div className="mt-0.5 text-[11px] text-faint">{hint}</div>}
      </div>
    </div>
  );
}

export function DataList({ children }: { children: ReactNode }) {
  return <dl className="divide-y-0">{children}</dl>;
}
