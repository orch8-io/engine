import { useId, useState } from "react";
import { isObject, type JsonObject } from "../lib/sequenceModel";
import { Button } from "./ui/Button";
import { FieldLabel, Input } from "./ui/Input";

export function HumanInputBuilder({
  value,
  blockId,
  onChange,
}: {
  value: unknown;
  blockId: string;
  onChange: (value: JsonObject | undefined) => void;
}) {
  const id = useId();
  const form = isObject(value) ? value : null;
  const [timeout, setTimeoutDraft] = useState(() => typeof form?.timeout === "number" ? String(form.timeout) : "");
  const choices = Array.isArray(form?.choices) ? form.choices : null;
  const field = (key: string) => typeof form?.[key] === "string" ? String(form[key]) : "";
  const setField = (key: string, value: JsonObject[string] | undefined) => {
    const next = { ...form };
    if (value === undefined) delete next[key];
    else next[key] = value;
    onChange(next);
  };
  const editChoice = (index: number, key: "label" | "value", next: string) => {
    setField("choices", choices?.map((choice, i) => i === index ? { ...(isObject(choice) ? choice : {}), [key]: next } : choice));
  };
  const invalidTimeout = timeout !== "" && (!Number.isSafeInteger(Number(timeout)) || Number(timeout) <= 0);
  const invalidChoices = choices && (
    choices.length === 0 ||
    choices.some((choice) => !isObject(choice) || typeof choice.label !== "string" || !choice.label.trim() ||
      typeof choice.value !== "string" || !choice.value.trim()) ||
    new Set(choices.map((choice) => isObject(choice) ? choice.value : null)).size !== choices.length
  );

  return (
    <details className="border border-hairline rounded-sm p-3" open={!!form}>
      <summary className="cursor-pointer text-sm font-medium text-ink">Human input</summary>
      <div className="mt-3 space-y-3">
        {!form ? (
          <Button size="sm" onClick={() => { setTimeoutDraft(""); onChange({ prompt: "" }); }}>Add human input</Button>
        ) : (
          <>
            <div>
              <FieldLabel htmlFor={`${id}-prompt`}>Review prompt</FieldLabel>
              <textarea id={`${id}-prompt`} className="well w-full" rows={3} value={field("prompt")} onChange={(event) => setField("prompt", event.target.value)} />
            </div>
            <div>
              <FieldLabel htmlFor={`${id}-store`}>Store answer in context.data</FieldLabel>
              <Input id={`${id}-store`} className="w-full" placeholder={blockId} value={field("store_as")} onChange={(event) => setField("store_as", event.target.value || undefined)} />
            </div>
            <div>
              <FieldLabel htmlFor={`${id}-timeout`}>Timeout (milliseconds)</FieldLabel>
              <Input id={`${id}-timeout`} className="w-full" type="number" min="1" step="1" aria-invalid={invalidTimeout} value={timeout} onChange={(event) => {
                const next = event.target.value;
                const number = Number(next);
                setTimeoutDraft(next);
                if (next === "" || (Number.isSafeInteger(number) && number > 0)) setField("timeout", next === "" ? undefined : number);
              }} />
              {invalidTimeout && <p role="alert" className="text-warn text-xs">Enter a positive whole number.</p>}
            </div>
            <div>
              <FieldLabel htmlFor={`${id}-escalation`}>Escalation target</FieldLabel>
              <Input id={`${id}-escalation`} className="w-full" value={field("escalation_handler")} onChange={(event) => setField("escalation_handler", event.target.value || undefined)} />
            </div>
            <label className="flex items-center gap-2 text-xs">
              <input type="checkbox" checked={form.allow_comment === true} onChange={(event) => setField("allow_comment", event.target.checked || undefined)} />
              Allow a comment
            </label>
            {choices === null ? (
              <div className="space-y-2">
                <p className="text-muted text-xs">Reviewers get the default yes/no choices.</p>
                <Button size="sm" onClick={() => setField("choices", [{ label: "Yes", value: "yes" }, { label: "No", value: "no" }])}>Customize choices</Button>
              </div>
            ) : (
              <fieldset className="space-y-2">
                <legend className="text-sm text-ink">Review choices</legend>
                {choices.map((choice, index) => (
                  <div key={index} className="grid grid-cols-2 gap-2">
                    <Input aria-label={`Choice ${index + 1} label`} placeholder="Label" className="w-full" value={isObject(choice) ? String(choice.label ?? "") : ""} onChange={(event) => editChoice(index, "label", event.target.value)} />
                    <Input aria-label={`Choice ${index + 1} stored value`} placeholder="Stored value" className="w-full" value={isObject(choice) ? String(choice.value ?? "") : ""} onChange={(event) => editChoice(index, "value", event.target.value)} />
                    <Button size="sm" variant="ghost" onClick={() => setField("choices", choices.filter((_, i) => i !== index))}>Remove choice {index + 1}</Button>
                  </div>
                ))}
                {invalidChoices && <p role="alert" className="text-warn text-xs">Add at least one labeled choice with a unique stored value.</p>}
                <div className="flex flex-wrap gap-2">
                  <Button size="sm" onClick={() => setField("choices", [...choices, { label: "", value: "" }])}>Add choice</Button>
                  <Button size="sm" variant="ghost" onClick={() => setField("choices", undefined)}>Use default choices</Button>
                </div>
              </fieldset>
            )}
            <Button size="sm" variant="ghost" onClick={() => onChange(undefined)}>Remove human input</Button>
          </>
        )}
      </div>
    </details>
  );
}
