import { useEffect, useRef, useState } from "react";
import { getPlacementPreview, listPlacementRecipes, type PlacementPreview } from "../api";
import { usePolling } from "../hooks/usePolling";
import { Badge } from "./ui/Badge";
import { Button } from "./ui/Button";
import { FieldLabel, Input, Select } from "./ui/Input";
import { Section } from "./ui/Section";

type PreviewResult =
  | { key: string; state: "loading" }
  | { key: string; state: "ready"; preview: PlacementPreview }
  | { key: string; state: "failed"; message: string };

export function WorkerPlacementPreview({ sequenceId, rolloutRevision }: { sequenceId?: string; rolloutRevision?: string }) {
  const [sequence, setSequence] = useState(sequenceId ?? "");
  const [recipe, setRecipe] = useState("");
  const [tags, setTags] = useState("");
  const [version, setVersion] = useState("");
  const [stagedPins, setStagedPins] = useState(false);
  const [result, setResult] = useState<PreviewResult>();
  const request = useRef<AbortController | null>(null);
  useEffect(() => () => request.current?.abort(), []);
  const { data: recipes, error: recipeError } = usePolling(listPlacementRecipes, 60_000);

  const id = (sequenceId ?? sequence).trim();
  const options = {
    recipe, tags: tags.trim(), worker_version: version.trim(),
    staged_pins: stagedPins ? "true" as const : undefined,
  };
  const key = JSON.stringify([id, options, rolloutRevision]);
  const current = result?.key === key ? result : undefined;
  const busy = current?.state === "loading";
  const preview = current?.state === "ready" ? current.preview : undefined;
  const status = current?.state === "failed" ? current.message : undefined;

  const runPreview = async () => {
    if (!id || busy) return;
    request.current?.abort();
    const controller = new AbortController();
    request.current = controller;
    setResult({ key, state: "loading" });
    try {
      const preview = await getPlacementPreview(id, options, controller.signal);
      if (!controller.signal.aborted) setResult({ key, state: "ready", preview });
    } catch (error) {
      if (!controller.signal.aborted) {
        setResult({ key, state: "failed", message: error instanceof Error ? error.message : String(error) });
      }
    }
  };

  return (
    <Section title="Preview placement" description="Check routing for a saved sequence against the current fleet.">
      <form className="flex flex-wrap items-end gap-3" onSubmit={(event) => { event.preventDefault(); void runPreview(); }}>
        {sequenceId === undefined && (
          <div>
            <FieldLabel htmlFor="placement-sequence">Sequence UUID</FieldLabel>
            <Input id="placement-sequence" value={sequence} onChange={(event) => setSequence(event.target.value)} required />
          </div>
        )}
        <div>
          <FieldLabel htmlFor="placement-recipe">Recipe</FieldLabel>
          <Select id="placement-recipe" value={recipe} onChange={(event) => setRecipe(event.target.value)}>
            <option value="">Saved placement</option>
            {recipes?.map((item) => <option key={item.id} value={item.id}>{item.label}</option>)}
          </Select>
        </div>
        <div>
          <FieldLabel htmlFor="placement-tags">Tags</FieldLabel>
          <Input id="placement-tags" value={tags} onChange={(event) => setTags(event.target.value)} placeholder="pilot, private" />
        </div>
        <div>
          <FieldLabel htmlFor="placement-version">Canary worker version</FieldLabel>
          <Input id="placement-version" value={version} onChange={(event) => setVersion(event.target.value)} />
        </div>
        <label className="inline-flex items-center gap-2 text-sm">
          <input type="checkbox" checked={stagedPins} onChange={(event) => setStagedPins(event.target.checked)} />
          Preview staged version pins
        </label>
        <Button type="submit" size="sm" disabled={busy}>{busy ? "Checking…" : "Run preview"}</Button>
      </form>
      {(status || recipeError) && <p role="status" className="mt-4 text-muted">{status ?? recipeError?.message}</p>}
      {preview && (
        <div className="mt-4 space-y-4" aria-live="polite">
          <Badge tone={preview.ready ? "ok" : "warn"}>{preview.ready ? "Fleet ready" : "Blocked"}</Badge>
          <p className="text-sm text-muted">{stagedPins ? "Evaluated staged pins where present" : "Evaluated active pins"}</p>
          {!preview.inventory_complete && <p>Worker inventory reached the preview limit.</p>}
          <ul className="space-y-3">
            {preview.steps.map((step) => (
              <li key={`${step.tree}/${step.block_id}`} className="border-t border-rule pt-3 space-y-1">
                <p className="font-mono">{step.block_id} <span className="text-muted">· {step.handler} · {step.route}</span></p>
                {!!step.matched_policies.length && <p>Policies {step.matched_policies.join(", ")}</p>}
                {step.route === "worker" && (
                  <dl className="grid grid-cols-2 gap-x-4 gap-y-1 text-sm sm:grid-cols-[auto_1fr]">
                    <dt>Effective queue</dt><dd>{step.queue_name ?? "Default"}</dd>
                    <dt>Eligible workers</dt>
                    <dd>{step.worker_inventory?.completeness === "observation_limit_reached" ? "At least " : ""}
                      {step.worker_inventory?.eligible_count ?? step.eligible_workers.length}</dd>
                    <dt>Minimum version evaluated</dt><dd>{step.minimum_worker_version ?? "None"}</dd>
                    {step.staged_worker_version && <><dt>Staged minimum</dt><dd>{step.staged_worker_version}</dd></>}
                    <dt>Pending tasks</dt><dd>{step.pending_tasks ?? "Unknown"}</dd>
                    <dt>Oldest pending task</dt>
                    <dd>{step.oldest_pending_age_secs == null ? "—" : `${step.oldest_pending_age_secs}s`}</dd>
                    <dt>Unused slots at last poll</dt><dd>{step.available_poll_slots ?? "Unknown"}</dd>
                    <dt>Estimated saved params transfer</dt>
                    <dd>{step.estimated_params_bytes == null ? "Unknown" : `${step.estimated_params_bytes} bytes`}</dd>
                  </dl>
                )}
                {step.next_action && <p>{step.next_action}</p>}
                {!!step.excluded_workers.length && (
                  <details>
                    <summary>Excluded workers{step.worker_inventory?.completeness === "complete"
                      && step.worker_inventory.excluded_count > step.excluded_workers.length
                      ? ` (showing ${step.excluded_workers.length} of ${step.worker_inventory.excluded_count})` : ""}</summary>
                    <ul className="pl-5">
                      {step.excluded_workers.map((worker) => <li key={worker.worker_id}>{worker.worker_id}: {worker.reasons.join(", ")}</li>)}
                    </ul>
                  </details>
                )}
              </li>
            ))}
          </ul>
        </div>
      )}
    </Section>
  );
}
