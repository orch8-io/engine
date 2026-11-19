import type { RefObject } from "react";
import { Select } from "./ui/Input";

export function PathSuggestions({
  control,
  paths,
  template = false,
  replaceField = false,
  onInsert,
}: {
  control: RefObject<HTMLInputElement | HTMLTextAreaElement | null>;
  paths: string[];
  template?: boolean;
  replaceField?: boolean;
  onInsert: (value: string) => void;
}) {
  if (!paths.length) return null;
  return (
    <Select
      aria-label="Insert a reference path"
      className="w-full text-xs"
      value=""
      onChange={(event) => {
        const field = control.current;
        const path = event.target.value;
        if (!field || !path) return;
        const start = field.selectionStart ?? field.value.length;
        const end = field.selectionEnd ?? start;
        const reference = template ? `{{${path}}}` : path;
        if (replaceField) {
          onInsert(reference);
          field.focus();
          return;
        }
        const before = field.value.slice(0, start);
        const after = field.value.slice(end);
        const templatePrefix = template && /\{\{\s*[\w.-]*$/.exec(before);
        const expressionPrefix = !template && start === end && /(?:^|[\s(=!<>])([\w.-]+)$/.exec(before);
        const prefixLength = templatePrefix ? templatePrefix[0].length : expressionPrefix ? expressionPrefix[1].length : 0;
        const trailingBraces = templatePrefix ? (/^\s*\}\}/.exec(after)?.[0].length ?? 0) : 0;
        const offset = start - prefixLength;
        onInsert(field.value.slice(0, offset) + reference + after.slice(trailingBraces));
        field.focus();
        requestAnimationFrame(() => field.setSelectionRange(offset + reference.length, offset + reference.length));
      }}
    >
      <option value="">Insert a reference path…</option>
      {paths.map((path) => <option key={path} value={path}>{path}</option>)}
    </Select>
  );
}
