import { useId, useState } from "react";
import { draftInputSchema } from "../lib/sequenceAuthoring";
import { isObject, type JsonObject } from "../lib/sequenceModel";
import { Button } from "./ui/Button";
import { FieldLabel } from "./ui/Input";

export function SequenceInputDraft({ onApply }: { onApply: (schema: JsonObject) => void }) {
  const id = useId();
  const [sample, setSample] = useState("");
  const [source, setSource] = useState<JsonObject | null>(null);
  const [required, setRequired] = useState<Set<string>>(new Set());
  const [error, setError] = useState("");

  const preview = () => {
    try {
      const next: unknown = JSON.parse(sample);
      if (!isObject(next)) throw new Error("The sample must be a JSON object.");
      setSource(next);
      setRequired(new Set());
      setError("");
    } catch (err) {
      setSource(null);
      setError(err instanceof SyntaxError ? "Enter a valid JSON sample." : "The sample must be a JSON object.");
    }
  };

  return (
    <details className="border border-hairline rounded-sm p-3">
      <summary className="cursor-pointer text-sm font-medium text-ink">Draft an input schema from JSON</summary>
      <div className="mt-3 space-y-3">
        <div>
          <FieldLabel htmlFor={id}>Sample context.data</FieldLabel>
          <textarea
            id={id}
            className="well w-full"
            rows={4}
            spellCheck={false}
            value={sample}
            onChange={(event) => { setSample(event.target.value); setSource(null); setError(""); }}
          />
        </div>
        <Button size="sm" onClick={preview}>Preview inferred fields</Button>
        {error && <p role="alert" className="text-warn text-xs">{error}</p>}
        {source && (
          <>
            <fieldset className="space-y-1">
              <legend className="text-sm text-ink">Select required top-level fields</legend>
              {Object.keys(source).map((key) => (
                <label key={key} className="flex items-center gap-2 text-xs">
                  <input
                    type="checkbox"
                    checked={required.has(key)}
                    onChange={(event) => {
                      const checked = event.target.checked;
                      setRequired((current) => {
                        const next = new Set(current);
                        if (checked) next.add(key);
                        else next.delete(key);
                        return next;
                      });
                    }}
                  />
                  <code>{key}</code>
                </label>
              ))}
            </fieldset>
            <pre className="well overflow-auto max-h-56" aria-label="Draft input schema">
              {JSON.stringify(draftInputSchema(source, required), null, 2)}
            </pre>
            <Button size="sm" onClick={() => {
              onApply(draftInputSchema(source, required));
              setSample("");
              setSource(null);
            }}>
              Use this input schema
            </Button>
          </>
        )}
      </div>
    </details>
  );
}
