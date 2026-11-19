import { createContext, useContext, useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { Link } from "react-router-dom";
import {
  getExecutionTree, getInstanceTimeline, getResolvedBlockInput, listReleases,
  type ExecutionNode, type ResolutionStatus, type ResolvedBlockInput, type SequenceDefinition, type WorkflowRelease,
} from "../api";
import { replayDivergences, safeTimelineEntry } from "../lib/sequenceAuthoring";
import { blockId, walkBlocks } from "../lib/sequenceModel";
import { blocksAt, type TimelineEntryDto, type TimelineTransitionDto } from "../lib/timeTravel";
import { ReplayFixtureExport } from "./ReplayFixtureExport";
import { Badge } from "./ui/Badge";
import { Button } from "./ui/Button";
import { FieldLabel, Input, Select } from "./ui/Input";

const TIMELINE_LIMIT = 5000;
const PAGE_SIZE = 500;

interface Inspection {
  id: string;
  sequenceId: string;
  state: string;
  entries: TimelineEntryDto[];
  transitions: TimelineTransitionDto[];
  nodes: ExecutionNode[];
  hasMore: boolean;
}

interface BlockEvidence {
  runtime?: string;
  attempts?: number;
  activations?: number;
  replay?: "Replay only" | "Recorded only";
}

const Overlay = createContext<ReadonlyMap<string, BlockEvidence>>(new Map());

export function SequenceBlockEvidence({ id }: { id: string }) {
  const evidence = useContext(Overlay).get(id);
  if (!evidence) return null;
  return (
    <span className="inline-flex flex-wrap gap-1.5 text-xs text-muted">
      {evidence.runtime && <Badge tone={evidence.runtime === "failed" ? "warn" : "dim"}>{evidence.runtime}</Badge>}
      {!!evidence.attempts && <span>attempt {evidence.attempts}</span>}
      {!!evidence.activations && <span>{evidence.activations} activation{evidence.activations === 1 ? "" : "s"}</span>}
      {evidence.replay && <Badge tone="hold">{evidence.replay}</Badge>}
    </span>
  );
}

export function SequenceRuntime({ sequence, children }: { sequence: SequenceDefinition; children: ReactNode }) {
  const [instanceId, setInstanceId] = useState("");
  const [loadedInspection, setInspection] = useState<Inspection | null>(null);
  const inspection = loadedInspection?.sequenceId === sequence.id ? loadedInspection : null;
  const [position, setPosition] = useState(0);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [releases, setReleases] = useState<WorkflowRelease[] | null>(null);
  const [releaseError, setReleaseError] = useState("");
  const [replayBusy, setReplayBusy] = useState(false);
  const [releaseId, setReleaseId] = useState("");
  const [sampleIndex, setSampleIndex] = useState("");
  const executionRequest = useRef<AbortController | null>(null);
  const replayRequest = useRef<AbortController | null>(null);
  useEffect(() => () => {
    executionRequest.current?.abort();
    replayRequest.current?.abort();
  }, []);

  const inspect = async (event: React.FormEvent) => {
    event.preventDefault();
    executionRequest.current?.abort();
    const controller = new AbortController();
    executionRequest.current = controller;
    setError("");
    setInspection(null);
    setBusy(true);
    const id = instanceId.trim();
    try {
      const first = await getInstanceTimeline(id, { limit: PAGE_SIZE, include_outputs: false }, controller.signal);
      if (first.instance.sequence_id !== sequence.id || first.instance.id !== id.toLowerCase()) {
        throw new Error("The execution does not belong to this revision.");
      }
      const entries = first.entries.map(safeTimelineEntry);
      const nodesRequest = getExecutionTree(id, controller.signal).catch(() => []);
      let hasMore = first.has_more;
      while (hasMore && entries.length < TIMELINE_LIMIT) {
        const page = await getInstanceTimeline(id, { offset: entries.length, limit: PAGE_SIZE, include_outputs: false }, controller.signal);
        if (page.instance.sequence_id !== sequence.id || page.instance.id !== first.instance.id) {
          throw new Error("The execution revision changed while loading the timeline.");
        }
        if (page.entries.length === 0) break;
        entries.push(...page.entries.map(safeTimelineEntry));
        hasMore = page.has_more;
      }
      const nodes = (await nodesRequest).filter((node) => node.instance_id === first.instance.id);
      if (controller.signal.aborted) return;
      setInspection({
        id: first.instance.id,
        sequenceId: first.instance.sequence_id,
        entries,
        nodes,
        hasMore,
        state: first.instance.state,
        transitions: first.state_transitions,
      });
      setPosition(entries.length);
    } catch (problem) {
      if (!controller.signal.aborted) setError(problem instanceof Error && problem.message.startsWith("The execution")
        ? problem.message : "Could not load this execution. Check the ID and your access.");
    } finally {
      if (!controller.signal.aborted) setBusy(false);
    }
  };

  const loadReplays = async () => {
    replayRequest.current?.abort();
    const controller = new AbortController();
    replayRequest.current = controller;
    setReplayBusy(true);
    setReleaseError("");
    setReleases(null);
    setReleaseId("");
    setSampleIndex("");
    try {
      const result = await listReleases({ tenant_id: sequence.tenant_id, limit: "200" }, controller.signal);
      if (controller.signal.aborted) return;
      setReleases(result.filter((release) => release.tenant_id === sequence.tenant_id &&
        release.namespace === sequence.namespace && release.candidate_sequence_id === sequence.id &&
        !!release.validation_summary));
    } catch {
      if (!controller.signal.aborted) setReleaseError("Could not load replay validation evidence.");
    } finally {
      if (!controller.signal.aborted) setReplayBusy(false);
    }
  };

  const release = releases?.find((item) => item.id === releaseId && item.candidate_sequence_id === sequence.id);
  const samples = useMemo(() => replayDivergences(release?.validation_summary), [release]);
  const selected = sampleIndex === "" ? undefined : samples[Number(sampleIndex)];
  const evidence = useMemo(() => {
    const byId = new Map<string, BlockEvidence>();
    if (inspection) {
      for (const block of blocksAt(inspection.entries, position)) {
        byId.set(block.block_id, { runtime: block.state, attempts: block.attempt });
      }
      if (position === inspection.entries.length && !inspection.hasMore) {
        for (const node of inspection.nodes) {
          const prior = byId.get(node.block_id);
          byId.set(node.block_id, { ...prior, runtime: node.state, activations: (prior?.activations ?? 0) + 1 });
        }
      }
    }
    for (const id of selected?.added ?? []) byId.set(id, { ...byId.get(id), replay: "Replay only" });
    for (const id of selected?.removed ?? []) byId.set(id, { ...byId.get(id), replay: "Recorded only" });
    return byId;
  }, [inspection, position, selected]);
  const timelineEvent = inspection?.entries[position - 1];
  const recordedSteps = useMemo(() => new Set(
    walkBlocks({ blocks: sequence.blocks }).filter(({ block }) => block.type === "step").map(({ block }) => blockId(block)),
  ), [sequence.blocks]);
  const selectedStep = timelineEvent && !timelineEvent.is_sentinel && recordedSteps.has(timelineEvent.block_id)
    ? timelineEvent.block_id
    : null;

  return (
    <div className="space-y-5">
      <details className="border border-hairline rounded-sm p-4">
        <summary className="cursor-pointer text-sm font-medium text-ink">Debug an execution of v{sequence.version}</summary>
        <form className="flex gap-2 items-end flex-wrap mt-3" onSubmit={inspect}>
          <div>
            <FieldLabel htmlFor="sequence-debug-id">Execution ID</FieldLabel>
            <Input id="sequence-debug-id" value={instanceId} onChange={(event) => setInstanceId(event.target.value)} />
          </div>
          <Button type="submit" size="sm" disabled={busy || !instanceId.trim()}>{busy ? "Loading…" : "Load timeline"}</Button>
          {inspection && <Button type="button" size="sm" variant="ghost" onClick={() => setInspection(null)}>Clear overlay</Button>}
        </form>
        <p className="annotation mt-2">The graph uses block states, attempts and timestamps. Execution context and output values are hidden.</p>
        {error && <p className="notice notice-warn mt-3" role="alert">{error}</p>}
        {inspection && <>
          <p className="mt-3 text-xs text-muted">
            <Link className="text-signal underline" to={`/instances/${encodeURIComponent(inspection.id)}`}>Open execution</Link>
            {" · "}{inspection.state}
            {inspection.hasMore && " · timeline truncated"}
          </p>
          <div className="mt-3">
            <FieldLabel htmlFor="sequence-debug-position">Recorded event {position} of {inspection.entries.length}</FieldLabel>
            <input id="sequence-debug-position" type="range" min="0" max={inspection.entries.length} disabled={!inspection.entries.length} value={position} onChange={(event) => setPosition(Number(event.target.value))} className="w-full accent-signal" />
            <p className="text-xs text-muted" aria-live="polite">{timelineEvent ? <><code>{timelineEvent.block_id}</code> · Attempt {timelineEvent.attempt} · {new Date(timelineEvent.completed_at).toLocaleString()}</> : "Before the first recorded block event."}</p>
          </div>
          {selectedStep && (
            <RecordedStepInputs key={position} instanceId={inspection.id} stepId={selectedStep} />
          )}
          {inspection.transitions.length > 0 && (
            <details className="mt-3 text-xs">
              <summary className="cursor-pointer text-ink">State transitions</summary>
              <ol className="mt-2 space-y-1 text-muted">
                {inspection.transitions.map((transition, index) => <li key={index}>{new Date(transition.at).toLocaleString()} · {transition.from_state ?? "Created"} → {transition.to_state ?? "Unknown"}</li>)}
              </ol>
            </details>
          )}
        </>}
      </details>
      <details className="border border-hairline rounded-sm p-4">
        <summary className="cursor-pointer text-sm font-medium text-ink">Replay divergence</summary>
        <div className="mt-3 space-y-3">
          <Button size="sm" disabled={replayBusy} onClick={loadReplays}>{replayBusy ? "Loading…" : releases ? "Refresh replay evidence" : "Load release replay evidence"}</Button>
          {releaseError && <p className="notice notice-warn" role="alert">{releaseError}</p>}
          {releases?.length === 0 && <p className="text-xs text-muted">No recorded release validations use this revision as the candidate.</p>}
          {!!releases?.length && (
            <div>
              <FieldLabel htmlFor="sequence-debug-release">Release validation</FieldLabel>
              <Select id="sequence-debug-release" value={releaseId} onChange={(event) => { setReleaseId(event.target.value); setSampleIndex(""); }}>
                <option value="">Select a release</option>
                {releases.map((item) => <option key={item.id} value={item.id}>v{item.baseline_version} → v{item.candidate_version} · {new Date(item.updated_at).toLocaleString()}</option>)}
              </Select>
            </div>
          )}
          {release && (
            <div>
              <p className="text-xs text-muted">
                {typeof release.validation_summary?.matches === "number" && `${release.validation_summary.matches} matched · `}
                {samples.length} recorded divergences
                {typeof release.validation_summary?.inconclusive === "number" && ` · ${release.validation_summary.inconclusive} inconclusive`}
              </p>
              {!!samples.length && <>
                <FieldLabel htmlFor="sequence-debug-sample" className="mt-2">Divergent execution</FieldLabel>
                <Select id="sequence-debug-sample" value={sampleIndex} onChange={(event) => setSampleIndex(event.target.value)}>
                  <option value="">Select an execution</option>
                  {samples.map((sample, index) => <option key={sample.instanceId} value={index}>{sample.instanceId}</option>)}
                </Select>
              </>}
            </div>
          )}
          {selected && release && (
            <div className="text-xs space-y-1">
              <p>Final state: {selected.recordedState} → {selected.replayedState}</p>
              {selected.added.length > 0 && <p>Replay only: <code>{selected.added.join(", ")}</code></p>}
              {selected.removed.length > 0 && <p>Recorded only: <code>{selected.removed.join(", ")}</code></p>}
              <Link className="text-signal underline" to={`/instances/${encodeURIComponent(selected.instanceId)}`}>Open recorded execution</Link>
              <ReplayFixtureExport key={`${release.id}:${selected.instanceId}`} releaseId={release.id} instanceId={selected.instanceId} />
            </div>
          )}
        </div>
      </details>
      <Overlay.Provider value={evidence}>{children}</Overlay.Provider>
    </div>
  );
}

const RESOLUTION_LABELS: Record<ResolutionStatus, { label: string; tone: "ok" | "hold" | "warn" | "dim" }> = {
  ok: { label: "Resolved", tone: "ok" },
  null: { label: "Explicit null", tone: "dim" },
  missing: { label: "Missing", tone: "hold" },
  redacted: { label: "Redacted", tone: "dim" },
  error: { label: "Error", tone: "warn" },
};

function RecordedStepInputs({ instanceId, stepId }: { instanceId: string; stepId: string }) {
  const [trace, setTrace] = useState<ResolvedBlockInput | null>(null);
  const [busy, setBusy] = useState(false);
  const [failed, setFailed] = useState(false);
  const request = useRef<AbortController | null>(null);
  useEffect(() => () => request.current?.abort(), []);

  const load = async () => {
    request.current?.abort();
    const controller = new AbortController();
    request.current = controller;
    setTrace(null);
    setBusy(true);
    setFailed(false);
    try {
      const result = await getResolvedBlockInput(instanceId, stepId, controller.signal);
      if (controller.signal.aborted) return;
      if (result.block_id !== stepId) {
        setFailed(true);
        return;
      }
      setTrace(result);
    } catch {
      if (!controller.signal.aborted) setFailed(true);
    } finally {
      if (!controller.signal.aborted) setBusy(false);
    }
  };

  return (
    <div className="mt-3 space-y-2" aria-label="Recorded step inputs">
      <Button type="button" size="sm" disabled={busy} onClick={load}>
        {busy ? "Resolving inputs…" : "Inspect resolved inputs"}
      </Button>
      <p className="annotation text-xs">
        The engine reconstructs these parameters from the execution’s current context and outputs recorded before the step’s first output.
        Repeated attempts share that boundary.
      </p>
      {failed && <p role="alert" className="notice notice-warn">Could not resolve inputs for this recorded step.</p>}
      {trace && (
        <div className="space-y-2 text-xs" aria-label="Resolved input trace">
          {trace.resolved_params === undefined
            ? <p className="text-muted">The engine could not resolve the complete parameter object.</p>
            : <pre className="well max-h-64 overflow-auto" aria-label="Redacted resolved parameters">{JSON.stringify(trace.resolved_params, null, 2)}</pre>}
          {trace.entries.length > 0 ? (
            <ul aria-label="Parameter path statuses" className="space-y-1">
              {trace.entries.map((entry, index) => {
                const status = Object.hasOwn(RESOLUTION_LABELS, entry.status) ? RESOLUTION_LABELS[entry.status] : null;
                return (
                  <li key={index} className="flex flex-wrap items-center gap-2">
                    <code>{entry.param_path || "(root)"}</code>
                    <Badge tone={status?.tone ?? "dim"}>{status?.label ?? "Unknown"}</Badge>
                    {entry.fallback_used && <span className="text-muted">Fallback used</span>}
                  </li>
                );
              })}
            </ul>
          ) : <p className="text-muted">No template paths to resolve.</p>}
        </div>
      )}
    </div>
  );
}
