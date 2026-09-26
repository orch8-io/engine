import { useId, useState } from "react";
import { coerceField, fieldText, schemaFields, type FormField } from "../lib/schemaForm";
import { FieldLabel, Input, Select } from "./ui/Input";

const TEXTAREA =
  "w-full bg-sunken border border-hairline rounded-sm px-2.5 py-2 text-[12px] font-mono text-fg placeholder:text-faint focus:border-signal focus:outline-none";

/**
 * Renders a JSON Schema (object with properties) as a form over `value`.
 * Each valid edit calls `onChange(key, value | undefined)`; `undefined`
 * means "remove the key". Invalid input stays local with an inline error,
 * so the underlying JSON is never written in a broken state.
 *
 * Callers should key this component by the edited object's identity so
 * drafts reset when the selection changes.
 */
export function SchemaForm({
  schema,
  value,
  onChange,
  exclude = [],
}: {
  schema: unknown;
  value: Record<string, unknown>;
  onChange: (key: string, next: unknown) => void;
  exclude?: string[];
}) {
  const fields = schemaFields(schema).filter((f) => !exclude.includes(f.key));
  if (fields.length === 0) return null;
  return (
    <div className="space-y-3">
      {fields.map((f) => (
        <FieldInput key={f.key} field={f} value={value[f.key]} onChange={(v) => onChange(f.key, v)} />
      ))}
    </div>
  );
}

function FieldInput({
  field,
  value,
  onChange,
}: {
  field: FormField;
  value: unknown;
  onChange: (next: unknown) => void;
}) {
  const id = useId();
  const [draft, setDraft] = useState(() => fieldText(field, value));
  const [error, setError] = useState<string | null>(null);

  const apply = (raw: string | boolean) => {
    const r = coerceField(field, raw);
    if (!r.ok) {
      setError(r.error);
      return;
    }
    setError(null);
    onChange(r.remove ? undefined : r.value);
  };

  const label = (
    <FieldLabel htmlFor={id}>
      {field.title}
      {field.required && <span className="text-warn"> *</span>}
    </FieldLabel>
  );
  const help = field.description && (
    <p id={`${id}-help`} className="annotation mt-0.5 text-[11px]">
      {field.description}
    </p>
  );
  const err = error && (
    <p role="alert" className="text-warn text-[11px] mt-0.5">
      {error}
    </p>
  );

  if (field.kind === "boolean") {
    return (
      <div>
        <label htmlFor={id} className="flex items-center gap-2 text-[12px] text-fg">
          <input
            id={id}
            type="checkbox"
            className="accent-signal"
            checked={value === true}
            onChange={(e) => apply(e.target.checked)}
          />
          <span className="font-mono">{field.title}</span>
        </label>
        {help}
      </div>
    );
  }

  if (field.kind === "enum") {
    return (
      <div>
        {label}
        <Select
          id={id}
          value={typeof value === "string" ? value : ""}
          onChange={(e) => apply(e.target.value)}
          className="w-full font-mono"
        >
          {!field.required && <option value="">(unset)</option>}
          {field.options!.map((o) => (
            <option key={o} value={o}>
              {o}
            </option>
          ))}
        </Select>
        {help}
      </div>
    );
  }

  if (field.kind === "json") {
    return (
      <div>
        {label}
        <textarea
          id={id}
          value={draft}
          rows={Math.min(10, Math.max(3, draft.split("\n").length))}
          spellCheck={false}
          aria-describedby={field.description ? `${id}-help` : undefined}
          aria-invalid={!!error}
          placeholder={field.required ? "" : "(unset)"}
          onChange={(e) => setDraft(e.target.value)}
          onBlur={() => apply(draft)}
          className={TEXTAREA}
        />
        {help}
        {err}
      </div>
    );
  }

  return (
    <div>
      {label}
      <Input
        id={id}
        type={field.kind === "string" ? "text" : "number"}
        step={field.kind === "integer" ? 1 : "any"}
        value={draft}
        placeholder={field.placeholder}
        aria-describedby={field.description ? `${id}-help` : undefined}
        aria-invalid={!!error}
        onChange={(e) => {
          setDraft(e.target.value);
          apply(e.target.value);
        }}
        className="w-full font-mono"
      />
      {help}
      {err}
    </div>
  );
}
