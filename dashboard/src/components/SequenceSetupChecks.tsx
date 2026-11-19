import { useEffect, useMemo, useRef, useState } from "react";
import { listHandlers, preflightDraft, runDraftContract, type DraftContractReport, type HandlerCatalog, type PreflightReport } from "../api";
import { walkBlocks, type JsonObject } from "../lib/sequenceModel";
import { Badge } from "./ui/Badge";
import { Button } from "./ui/Button";

export function SequenceHandlerCheck({ doc }: { doc: JsonObject | null }) {
  const [catalog, setCatalog] = useState<HandlerCatalog | null>(null);
  const [error, setError] = useState("");
  const [request, setRequest] = useState(0);
  const handlers = useMemo(() => {
    const names = new Set<string>();
    for (const { block } of walkBlocks(doc)) {
      if (block.type !== "step") continue;
      for (const handler of [block.handler, block.fallback_handler]) {
        if (typeof handler === "string" && handler) names.add(handler);
      }
    }
    return [...names].sort();
  }, [doc]);

  useEffect(() => {
    const controller = new AbortController();
    listHandlers(controller.signal).then((result) => {
      if (!controller.signal.aborted) setCatalog(result);
    }).catch(() => {
      if (!controller.signal.aborted) setError("Could not load the handler catalog.");
    });
    return () => controller.abort();
  }, [request]);

  return (
    <div className="space-y-3">
      <p className="text-sm text-muted">Compare step handlers with the engine catalog. Add or change handlers in the definition.</p>
      {!doc && <p role="alert" className="text-warn text-sm">Fix the definition JSON first.</p>}
      {doc && handlers.length === 0 && <p className="text-muted text-sm">No handlers are configured.</p>}
      {error && <p role="alert" className="text-warn text-sm">{error}</p>}
      {!catalog && !error && <p role="status" className="text-muted text-sm">Loading handler catalog…</p>}
      {handlers.length > 0 && (
        <ul className="space-y-2">
          {handlers.map((name) => {
            const kind = catalog?.builtin.includes(name) ? "Built-in" : catalog?.external.includes(name) ? "Registered worker" : "Missing from catalog";
            return <li key={name} className="flex items-center gap-2 text-sm"><code>{name}</code>{catalog && <Badge tone={kind === "Missing from catalog" ? "warn" : "ok"}>{kind}</Badge>}</li>;
          })}
        </ul>
      )}
      {(catalog || error) && <Button size="sm" onClick={() => { setCatalog(null); setError(""); setRequest((value) => value + 1); }}>Refresh handler catalog</Button>}
    </div>
  );
}

export function SequenceDraftTest({
  buildBody,
  draftText,
  initialFixture,
  report,
  onReport,
}: {
  buildBody: () => { body: Record<string, unknown> } | { error: string };
  draftText: string;
  initialFixture?: { cases: readonly unknown[] };
  report: PreflightReport | null;
  onReport: (value: PreflightReport | null) => void;
}) {
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const [fixture, setFixture] = useState(() => JSON.stringify(initialFixture ?? {
    cases: [{ name: "Sample execution", input: {}, mocks: [], expect: { terminal_state: "completed" } }],
  }, null, 2));
  const [testedFixture, setTestedFixture] = useState<{ draft: string; report: DraftContractReport } | null>(null);
  const fixtureResult = testedFixture?.draft === draftText ? testedFixture.report : null;
  const active = useRef<AbortController | null>(null);
  useEffect(() => () => active.current?.abort(), []);

  const test = async () => {
    const request = buildBody();
    if ("error" in request) {
      setError(request.error);
      return;
    }
    active.current?.abort();
    const controller = new AbortController();
    active.current = controller;
    setError("");
    onReport(null);
    setBusy(true);
    try {
      const next = await preflightDraft(request.body, controller.signal);
      if (!controller.signal.aborted) onReport(next);
    } catch (problem) {
      if (!controller.signal.aborted) setError(problem instanceof Error ? problem.message : "Preflight unavailable.");
    } finally {
      if (!controller.signal.aborted) setBusy(false);
    }
  };

  const runFixture = async () => {
    const request = buildBody();
    if ("error" in request) {
      setError(request.error);
      return;
    }
    let contract: unknown;
    try {
      contract = JSON.parse(fixture);
    } catch {
      setError("Contract fixture must be valid JSON.");
      return;
    }
    const controller = new AbortController();
    active.current?.abort();
    active.current = controller;
    setBusy(true);
    setError("");
    setTestedFixture(null);
    try {
      const next = await runDraftContract(request.body, contract, controller.signal);
      if (!controller.signal.aborted) setTestedFixture({ draft: draftText, report: next });
    } catch (problem) {
      if (!controller.signal.aborted) setError(problem instanceof Error ? problem.message : "Could not run the fixture.");
    } finally {
      if (!controller.signal.aborted) setBusy(false);
    }
  };

  return (
    <div className="space-y-3">
      <p className="text-sm text-muted">Preflight checks the draft definition and engine readiness without saving a version or dispatching handlers.</p>
      <Button size="sm" disabled={busy} onClick={test}>Run readiness preflight</Button>
      {error && <p className="notice notice-warn" role="alert">{error}</p>}
      {report && (
        <div role="status" className="space-y-2 text-sm">
          <p>Preflight result <Badge tone={report.overall === "pass" ? "ok" : report.overall === "fail" ? "warn" : "hold"}>{report.overall}</Badge></p>
          <ul className="space-y-1">
            {report.checks.map((check) => (
              <li key={check.id}>
                <span className="font-mono">{check.status}</span>{" · "}{check.summary}
                {check.findings && check.findings.length > 0 && <ul className="ml-4 text-xs text-muted">{check.findings.map((finding, index) => <li key={index}>{finding.summary}</li>)}</ul>}
              </li>
            ))}
          </ul>
        </div>
      )}
      <div className="space-y-2">
        <label htmlFor="draft-contract-fixture" className="block text-sm">Contract fixture</label>
        <p className="text-sm text-muted">Set sample input, mock outputs, and expected results. The fixture runs without publishing the draft.</p>
        <textarea
          id="draft-contract-fixture"
          className="w-full rounded border border-border bg-panel p-3 font-mono text-sm"
          value={fixture}
          rows={10}
          disabled={busy}
          onChange={(event) => { setFixture(event.target.value); setTestedFixture(null); }}
        />
        <Button size="sm" variant="primary" disabled={busy} onClick={runFixture}>{busy ? "Testing…" : "Run fixture"}</Button>
        {fixtureResult && (
          <div role="status" className="space-y-1 text-sm">
            <Badge tone={fixtureResult.passed ? "ok" : "warn"}>{fixtureResult.passed ? "Passed" : "Failed"}</Badge>
            <ul>
              {fixtureResult.cases.map((item) => (
                <li key={item.name}>
                  {item.name} · {item.final_state}
                  {item.failures?.length ? <ul className="ml-4">{item.failures.map((failure, index) => <li key={index}>{failure}</li>)}</ul> : null}
                </li>
              ))}
            </ul>
          </div>
        )}
      </div>
    </div>
  );
}
