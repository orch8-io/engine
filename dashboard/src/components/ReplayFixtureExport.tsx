import { useEffect, useId, useRef, useState, type FormEvent } from "react";
import { exportReplayFixture, type ReplayFixture } from "../api";
import { parseReplayFixtureSelection } from "../lib/replayFixtureSelection";
import { Button } from "./ui/Button";
import { FieldLabel, Input } from "./ui/Input";

function downloadJson(filename: string, value: unknown) {
  const blob = new Blob([JSON.stringify(value, null, 2) + "\n"], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  link.remove();
  window.setTimeout(() => URL.revokeObjectURL(url), 1000);
}

export function ReplayFixtureExport({ releaseId, instanceId }: { releaseId: string; instanceId: string }) {
  const [inputFields, setInputFields] = useState("");
  const [outputFields, setOutputFields] = useState("");
  const [fixture, setFixture] = useState<ReplayFixture>();
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const request = useRef<AbortController | null>(null);
  useEffect(() => () => request.current?.abort(), []);
  const formId = useId();
  const filename = `replay-${releaseId}-${instanceId}`;

  const generate = async (event: FormEvent) => {
    event.preventDefault();
    request.current?.abort();
    setError("");
    setFixture(undefined);
    const controller = new AbortController();
    request.current = controller;
    setBusy(true);
    try {
      const fields = parseReplayFixtureSelection(inputFields, outputFields);
      const result = await exportReplayFixture(releaseId, instanceId, fields, controller.signal);
      if (!controller.signal.aborted) setFixture(result);
    } catch (problem) {
      if (!controller.signal.aborted) setError(problem instanceof Error ? problem.message : "Could not generate the replay fixture.");
    } finally {
      if (!controller.signal.aborted) setBusy(false);
    }
  };

  return (
    <details className="mt-2 rounded border border-hairline p-2">
      <summary className="cursor-pointer text-signal">Create regression fixture</summary>
      <form onSubmit={generate} className="mt-2 space-y-2">
        <p className="text-muted">
          The fixture checks the recorded state and path. Historical values are omitted by default.
          Select only fields you can share. Recognized secrets are redacted.
        </p>
        <div>
          <FieldLabel htmlFor={`${formId}-inputs`}>Input keys (optional, comma-separated or JSON array)</FieldLabel>
          <Input id={`${formId}-inputs`} value={inputFields} onChange={(event) => { setInputFields(event.target.value); setFixture(undefined); }} disabled={busy} />
        </div>
        <div>
          <FieldLabel htmlFor={`${formId}-outputs`}>Output keys (optional, block.field pairs or JSON mapping)</FieldLabel>
          <Input id={`${formId}-outputs`} value={outputFields} onChange={(event) => { setOutputFields(event.target.value); setFixture(undefined); }} disabled={busy} />
        </div>
        <Button type="submit" disabled={busy}>{busy ? "Checking replay…" : "Generate fixture"}</Button>
        {error && <p role="alert" className="text-warn">{error}</p>}
      </form>
      {fixture && (
        <div className="mt-2 space-y-2">
          <p role="status">{fixture.reproduces_divergence
            ? "The redacted fixture reproduces the saved candidate divergence."
            : "The redacted fixture ran but produced a different result. Review the selected keys before relying on it as a regression."}</p>
          <div className="flex flex-wrap gap-2">
            <Button type="button" onClick={() => downloadJson(`${filename}.json`, fixture.sequence)}>Download sequence</Button>
            <Button type="button" onClick={() => downloadJson(`${filename}.contracts.json`, fixture.contract)}>Download contract</Button>
          </div>
          <p>Save both files in the same directory and run <code>orch8 test run {filename}.contracts.json</code>.</p>
        </div>
      )}
    </details>
  );
}
