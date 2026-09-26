import { Fragment, useEffect, useId, useRef, useState, type KeyboardEvent } from "react";
import { connectionInfo } from "../api";
import { generateSnippet, SNIPPET_FORMATS, type SnippetFormat } from "../lib/copyAs";
import type { ApiRequestSpec } from "../lib/requests";
import { Button } from "./ui/Button";

/**
 * "Copy as" menu: renders the exact request an action button sends as a
 * curl / Node SDK / Python SDK / orch8 CLI snippet. The API key is always
 * redacted to `$ORCH8_API_KEY`.
 *
 * `spec` is evaluated lazily when the menu opens so it reflects current form
 * state. It may return several specs (e.g. one per approval choice); the menu
 * then groups formats under each spec's label. `null` means "nothing valid to
 * copy yet" and the menu does not open.
 */
export function CopyAsMenu({
  spec,
  disabledReason,
  align = "right",
}: {
  spec: () => ApiRequestSpec | ApiRequestSpec[] | null;
  disabledReason?: string;
  align?: "left" | "right";
}) {
  const [open, setOpen] = useState(false);
  const [preview, setPreview] = useState<{ title: string; text: string; copied: boolean } | null>(null);
  const [specs, setSpecs] = useState<ApiRequestSpec[]>([]);
  const [invalid, setInvalid] = useState(false);
  const triggerRef = useRef<HTMLButtonElement>(null);
  const menuRef = useRef<HTMLDivElement>(null);
  const rootRef = useRef<HTMLDivElement>(null);
  const menuId = useId();

  useEffect(() => {
    if (!open && !preview) return;
    const onDown = (e: MouseEvent) => {
      if (rootRef.current && !rootRef.current.contains(e.target as Node)) {
        setOpen(false);
        setPreview(null);
      }
    };
    document.addEventListener("mousedown", onDown);
    return () => document.removeEventListener("mousedown", onDown);
  }, [open, preview]);

  useEffect(() => {
    if (open) menuRef.current?.querySelector<HTMLButtonElement>("[role=menuitem]:not(:disabled)")?.focus();
  }, [open]);

  const toggle = () => {
    if (open) {
      setOpen(false);
      return;
    }
    const s = spec();
    const list = s === null ? [] : Array.isArray(s) ? s : [s];
    setSpecs(list);
    setPreview(null);
    setInvalid(list.length === 0);
    if (list.length > 0) setOpen(true);
  };

  const choose = async (target: ApiRequestSpec, format: SnippetFormat) => {
    const text = generateSnippet(format, target, connectionInfo());
    if (text === null) return;
    let copied = false;
    try {
      await navigator.clipboard.writeText(text);
      copied = true;
    } catch {
      // Clipboard needs a secure context; the preview still allows a manual copy.
    }
    const fmt = SNIPPET_FORMATS.find((f) => f.id === format)?.label ?? format;
    setOpen(false);
    setPreview({ title: specs.length > 1 ? `${target.label} · ${fmt}` : fmt, text, copied });
    triggerRef.current?.focus();
  };

  const onMenuKey = (e: KeyboardEvent<HTMLDivElement>) => {
    const items = Array.from(
      menuRef.current?.querySelectorAll<HTMLButtonElement>("[role=menuitem]:not(:disabled)") ?? [],
    );
    const idx = items.indexOf(document.activeElement as HTMLButtonElement);
    const focus = (i: number) => items[(i + items.length) % items.length]?.focus();
    if (e.key === "Escape") {
      e.preventDefault();
      setOpen(false);
      triggerRef.current?.focus();
    } else if (e.key === "ArrowDown") {
      e.preventDefault();
      focus(idx + 1);
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      focus(idx - 1);
    } else if (e.key === "Home") {
      e.preventDefault();
      focus(0);
    } else if (e.key === "End") {
      e.preventDefault();
      focus(items.length - 1);
    } else if (e.key === "Tab") {
      setOpen(false);
    }
  };

  const conn = open ? connectionInfo() : null;
  const pos = align === "right" ? "right-0" : "left-0";

  return (
    <div ref={rootRef} className="relative inline-block">
      <Button
        ref={triggerRef}
        size="sm"
        variant="ghost"
        aria-haspopup="menu"
        aria-expanded={open}
        aria-controls={open ? menuId : undefined}
        title={disabledReason ?? "Copy the exact request this action sends as curl, SDK, or CLI"}
        disabled={!!disabledReason}
        onClick={toggle}
      >
        Copy as ▾
      </Button>
      {invalid && !open && (
        <span role="status" className="sr-only">
          Nothing to copy yet — the form is incomplete or invalid.
        </span>
      )}
      {open && conn && (
        <div
          id={menuId}
          ref={menuRef}
          role="menu"
          aria-label="Copy request as"
          onKeyDown={onMenuKey}
          className={`absolute ${pos} z-30 mt-1 min-w-48 bg-surface border border-hairline rounded-sm py-1 shadow-lg`}
        >
          {specs.map((s, si) => (
            <Fragment key={si}>
              {specs.length > 1 && (
                <div role="presentation" className="px-3 pt-1.5 pb-0.5 eyebrow">
                  {s.label}
                </div>
              )}
              {SNIPPET_FORMATS.map((f) => {
                const unavailable = f.id === "cli" && generateSnippet("cli", s, conn) === null;
                return (
                  <button
                    key={f.id}
                    type="button"
                    role="menuitem"
                    disabled={unavailable}
                    title={unavailable ? "No orch8 CLI command sends this exact request — use curl or an SDK" : undefined}
                    onClick={() => choose(s, f.id)}
                    className="block w-full text-left px-3 py-1.5 text-[12px] text-fg hover:bg-raised focus:bg-raised focus:outline-none disabled:opacity-40 disabled:cursor-not-allowed"
                  >
                    {f.label}
                    {unavailable && <span className="text-faint"> — n/a</span>}
                  </button>
                );
              })}
            </Fragment>
          ))}
        </div>
      )}
      {preview && (
        <div
          role="dialog"
          aria-label={`${preview.title} snippet`}
          onKeyDown={(e) => {
            if (e.key === "Escape") {
              setPreview(null);
              triggerRef.current?.focus();
            }
          }}
          className={`absolute ${pos} z-30 mt-1 w-[min(36rem,90vw)] bg-surface border border-hairline rounded-sm p-3 shadow-lg text-left`}
        >
          <div className="flex items-center gap-2 mb-2">
            <span className="eyebrow">{preview.title}</span>
            <span className={`text-[11px] ${preview.copied ? "text-ok" : "text-muted"}`} role="status">
              {preview.copied ? "Copied to clipboard" : "Select and copy"}
            </span>
            <Button size="sm" variant="ghost" className="ml-auto" onClick={() => setPreview(null)} autoFocus>
              Close
            </Button>
          </div>
          <pre className="well max-h-72 whitespace-pre overflow-auto text-[11px]">{preview.text}</pre>
          <p className="annotation mt-1 text-[11px]">API key redacted — export ORCH8_API_KEY before running.</p>
        </div>
      )}
    </div>
  );
}
