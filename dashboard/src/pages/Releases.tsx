import { useCallback, useMemo, useState } from "react";
import {
  createRelease,
  evaluateRelease,
  getReleaseDiff,
  listReleaseDecisions,
  listReleases,
  listSequences,
  pauseRelease,
  promoteRelease,
  rollbackRelease,
  startReleaseCanary,
  validateRelease,
  type ReleaseDecision,
  type ReleaseState,
  type SemanticDiff,
  type SequenceDefinition,
  type WorkflowRelease,
} from "../api";
import { usePageTitle } from "../hooks/usePageTitle";
import { usePolling } from "../hooks/usePolling";
import { Badge } from "../components/ui/Badge";
import { Button } from "../components/ui/Button";
import { FieldLabel, Input, Select } from "../components/ui/Input";
import { PageHeader } from "../components/ui/PageHeader";
import { PageMeta } from "../components/ui/PageMeta";
import { Section } from "../components/ui/Section";
import { Empty, Table, TD, TH, THead, TR } from "../components/ui/Table";
import { Relative } from "../components/ui/Relative";
import { SkeletonTable } from "../components/ui/Skeleton";

const STATE_TONE: Record<ReleaseState, "dim" | "signal" | "live" | "ok" | "hold" | "warn"> = {
  draft: "dim",
  validating: "live",
  ready: "signal",
  canary: "hold",
  promoted: "ok",
  paused: "hold",
  rolled_back: "warn",
  failed: "warn",
};

export default function Releases() {
  usePageTitle("Releases");
  const releaseFetcher = useCallback((signal?: AbortSignal) => listReleases({ limit: "200" }, signal), []);
  const sequenceFetcher = useCallback((signal?: AbortSignal) => listSequences({ limit: "500" }, signal), []);
  const releases = usePolling<WorkflowRelease[]>(releaseFetcher, 5000);
  const sequences = usePolling<SequenceDefinition[]>(sequenceFetcher, 15000);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [notice, setNotice] = useState<{ tone: "ok" | "warn"; text: string } | null>(null);

  const selected = (releases.data ?? []).find((release) => release.id === selectedId) ?? null;
  const active = (releases.data ?? []).filter((release) => ["validating", "ready", "canary", "paused"].includes(release.state));

  const refresh = () => {
    releases.refresh();
    sequences.refresh();
  };

  return (
    <div className="space-y-12 pb-16">
      <PageHeader
        eyebrow="Delivery · Workflow safety"
        title="Releases"
        description="Compare immutable workflow versions, replay a candidate against recorded runs, route a deterministic canary cohort, and promote or roll back with an audited decision trail."
        actions={<PageMeta updatedAt={releases.updatedAt} onRefresh={refresh} />}
      />

      {notice && <div role={notice.tone === "warn" ? "alert" : "status"} className={`notice notice-${notice.tone}`}>{notice.text}</div>}
      {releases.error && <div role="alert" className="notice notice-warn">{releases.error.message}</div>}

      <Section
        eyebrow="Create"
        title="New release candidate"
        description="Choose two immutable versions of the same tenant, namespace, and sequence. The engine verifies that relationship again before creating the release."
        meta={<span><span className="text-faint">ACTIVE</span> <span className="text-ink-dim">{active.length}</span></span>}
      >
        <CreateReleaseForm
          sequences={sequences.data ?? []}
          loading={sequences.loading && !sequences.data}
          onCreated={(release) => {
            setSelectedId(release.id);
            setNotice({ tone: "ok", text: `Release v${release.baseline_version} → v${release.candidate_version} created.` });
            releases.refresh();
          }}
          onError={(text) => setNotice({ tone: "warn", text })}
        />
      </Section>

      <Section
        eyebrow="Control plane"
        title="Release ledger"
        description="Newest first. Select a row to inspect its semantic diff, evidence, and legal next actions. State transitions use compare-and-swap, so concurrent operator actions cannot both win."
        meta={<span><span className="text-faint">TOTAL</span> <span className="text-ink-dim">{releases.data?.length ?? 0}</span></span>}
      >
        {releases.loading && !releases.data ? <SkeletonTable rows={5} cols={6} /> : (
          <Table aria-label="Workflow releases">
            <THead>
              <TH>Workflow</TH><TH>Version</TH><TH>State</TH><TH>Canary</TH><TH>Gates</TH><TH>Updated</TH>
            </THead>
            <tbody>
              {(releases.data ?? []).length === 0 ? <Empty colSpan={6}>No releases yet. Create one from two stored sequence versions.</Empty> :
                (releases.data ?? []).map((release) => (
                  <TR key={release.id} active={selectedId === release.id}>
                    <TD>
                      <button
                        type="button"
                        aria-pressed={selectedId === release.id}
                        onClick={() => setSelectedId(release.id)}
                        className="min-h-11 text-left rounded-sm focus:outline-none focus-visible:ring-2 focus-visible:ring-signal lg:min-h-8"
                      >
                        <span className="block text-ink">{release.sequence_name}</span>
                        <span className="field-label mt-1 block">{release.tenant_id} / {release.namespace}</span>
                      </button>
                    </TD>
                    <TD className="font-mono text-ink-dim">v{release.baseline_version} → v{release.candidate_version}</TD>
                    <TD><Badge tone={STATE_TONE[release.state]} dot live={release.state === "validating"}>{release.state.replace("_", " ")}</Badge></TD>
                    <TD className="font-mono">{release.canary_percent}%</TD>
                    <TD className="font-mono">{release.gates.length}</TD>
                    <TD><Relative at={release.updated_at} /></TD>
                  </TR>
                ))}
            </tbody>
          </Table>
        )}
      </Section>

      {selected && (
        <ReleaseInspector
          key={selected.id}
          release={selected}
          onChanged={() => {
            setNotice({ tone: "ok", text: "Release state updated." });
            releases.refresh();
          }}
          onError={(text) => setNotice({ tone: "warn", text })}
        />
      )}
    </div>
  );
}

function CreateReleaseForm({ sequences, loading, onCreated, onError }: {
  sequences: SequenceDefinition[];
  loading: boolean;
  onCreated: (release: WorkflowRelease) => void;
  onError: (text: string) => void;
}) {
  const [tenant, setTenant] = useState("");
  const [baseline, setBaseline] = useState("");
  const [candidate, setCandidate] = useState("");
  const [maxRegression, setMaxRegression] = useState("0.05");
  const [minSample, setMinSample] = useState("20");
  const [busy, setBusy] = useState(false);

  const options = useMemo(() => sequences.filter((sequence) => !tenant || sequence.tenant_id === tenant), [sequences, tenant]);
  const label = (sequence: SequenceDefinition) => `${sequence.tenant_id} · ${sequence.namespace}/${sequence.name} · v${sequence.version}`;

  const submit = async (event: React.FormEvent) => {
    event.preventDefault();
    if (!tenant || !baseline || !candidate) {
      onError("Choose a tenant, baseline, and candidate version.");
      return;
    }
    setBusy(true);
    try {
      const max = Number(maxRegression);
      const sample = Number(minSample);
      const release = await createRelease({
        tenant_id: tenant,
        baseline_sequence_id: baseline,
        candidate_sequence_id: candidate,
        gates: Number.isFinite(max) && Number.isFinite(sample) ? [{ metric: "error_rate", max_regression: max, min_sample: sample }] : [],
      });
      onCreated(release);
    } catch (error) {
      onError(error instanceof Error ? error.message : String(error));
    } finally {
      setBusy(false);
    }
  };

  return (
    <form onSubmit={submit} className="slab grid grid-cols-1 lg:grid-cols-6 gap-4 items-end">
      <div><FieldLabel htmlFor="release-tenant">Tenant</FieldLabel><Input id="release-tenant" value={tenant} onChange={(event) => setTenant(event.target.value)} placeholder="acme" className="w-full" /></div>
      <div className="lg:col-span-2"><FieldLabel htmlFor="release-baseline">Baseline</FieldLabel><Select id="release-baseline" value={baseline} onChange={(event) => { setBaseline(event.target.value); const seq = sequences.find((item) => item.id === event.target.value); if (seq) setTenant(seq.tenant_id); }} className="w-full"><option value="">{loading ? "Loading versions…" : "Select version"}</option>{options.map((sequence) => <option key={sequence.id} value={sequence.id}>{label(sequence)}</option>)}</Select></div>
      <div className="lg:col-span-2"><FieldLabel htmlFor="release-candidate">Candidate</FieldLabel><Select id="release-candidate" value={candidate} onChange={(event) => setCandidate(event.target.value)} className="w-full"><option value="">Select version</option>{options.filter((sequence) => sequence.id !== baseline).map((sequence) => <option key={sequence.id} value={sequence.id}>{label(sequence)}</option>)}</Select></div>
      <Button type="submit" variant="primary" disabled={busy}>{busy ? "Creating…" : "Create release"}</Button>
      <div><FieldLabel htmlFor="release-regression">Max error regression</FieldLabel><Input id="release-regression" type="number" min="0" max="1" step="0.01" value={maxRegression} onChange={(event) => setMaxRegression(event.target.value)} className="w-full" /></div>
      <div><FieldLabel htmlFor="release-sample">Minimum sample / variant</FieldLabel><Input id="release-sample" type="number" min="1" value={minSample} onChange={(event) => setMinSample(event.target.value)} className="w-full" /></div>
    </form>
  );
}

function ReleaseInspector({ release, onChanged, onError }: { release: WorkflowRelease; onChanged: () => void; onError: (text: string) => void }) {
  const [diff, setDiff] = useState<SemanticDiff | null>(null);
  const [decisions, setDecisions] = useState<ReleaseDecision[]>([]);
  const [busy, setBusy] = useState<string | null>(null);
  const [canaryPercent, setCanaryPercent] = useState(String(release.canary_percent || 10));

  const run = async (name: string, operation: () => Promise<unknown>) => {
    setBusy(name);
    try { await operation(); onChanged(); } catch (error) { onError(error instanceof Error ? error.message : String(error)); } finally { setBusy(null); }
  };

  const loadEvidence = async () => {
    setBusy("evidence");
    try {
      const [nextDiff, nextDecisions] = await Promise.all([getReleaseDiff(release.id), listReleaseDecisions(release.id)]);
      setDiff(nextDiff); setDecisions(nextDecisions);
    } catch (error) { onError(error instanceof Error ? error.message : String(error)); } finally { setBusy(null); }
  };

  return (
    <Section eyebrow="Selected release" title={`${release.sequence_name} · v${release.baseline_version} → v${release.candidate_version}`} description="Only legal actions for the current state are enabled. Evaluation is safe to repeat and automatically rolls back a live canary when a configured gate fails." meta={<Badge tone={STATE_TONE[release.state]} dot>{release.state.replace("_", " ")}</Badge>}>
      <div className="slab flex flex-wrap items-end gap-3">
        <Button onClick={loadEvidence} disabled={busy !== null}>{busy === "evidence" ? "Loading…" : "Load diff & audit"}</Button>
        <Button variant="primary" onClick={() => run("validate", () => validateRelease(release.id))} disabled={busy !== null || release.state !== "draft"}>{busy === "validate" ? "Validating…" : "Validate history"}</Button>
        <div><FieldLabel htmlFor="canary-percent">Canary traffic</FieldLabel><div className="flex gap-2"><Input id="canary-percent" type="number" min="1" max="100" value={canaryPercent} onChange={(event) => setCanaryPercent(event.target.value)} className="w-20" /><Button onClick={() => run("canary", () => startReleaseCanary(release.id, Number(canaryPercent)))} disabled={busy !== null || !["ready", "paused"].includes(release.state)}>Start</Button></div></div>
        <Button onClick={() => run("evaluate", () => evaluateRelease(release.id))} disabled={busy !== null || release.state !== "canary"}>Evaluate gates</Button>
        <Button variant="primary" onClick={() => run("promote", () => promoteRelease(release.id))} disabled={busy !== null || release.state !== "canary"}>Promote</Button>
        <Button onClick={() => run("pause", () => pauseRelease(release.id))} disabled={busy !== null || release.state !== "canary"}>Pause</Button>
        <Button variant="danger" onClick={() => { if (confirm("Roll back this release? New traffic returns to the baseline.")) run("rollback", () => rollbackRelease(release.id)); }} disabled={busy !== null || !["canary", "paused"].includes(release.state)}>Roll back</Button>
      </div>
      {release.validation_summary && <pre className="well mb-6" aria-label="Validation summary">{JSON.stringify(release.validation_summary, null, 2)}</pre>}
      {diff && <div className="mb-8"><div className="eyebrow mb-3">Semantic diff · {diff.max_severity ?? "no changes"}</div><Table><THead><TH>Severity</TH><TH>Category</TH><TH>Block</TH><TH>Change</TH></THead><tbody>{diff.entries.length === 0 ? <Empty colSpan={4}>No semantic changes.</Empty> : diff.entries.map((entry, index) => <TR key={`${entry.category}-${index}`}><TD><Badge tone={entry.severity === "incompatible" || entry.severity === "side_effect_risk" ? "warn" : entry.severity === "behavioral" ? "hold" : "dim"}>{entry.severity.replaceAll("_", " ")}</Badge></TD><TD className="font-mono">{entry.category}</TD><TD className="font-mono">{entry.block_id ?? "—"}</TD><TD className="text-ink-dim">{entry.summary}</TD></TR>)}</tbody></Table></div>}
      {decisions.length > 0 && <div><div className="eyebrow mb-3">Immutable decision trail</div><Table><THead><TH>Transition</TH><TH>Actor</TH><TH>Reason</TH><TH>Decided</TH></THead><tbody>{decisions.map((decision) => <TR key={decision.id}><TD className="font-mono">{decision.from_state} → {decision.to_state}</TD><TD>{decision.actor}</TD><TD className="text-ink-dim">{decision.reason}</TD><TD><Relative at={decision.decided_at} /></TD></TR>)}</tbody></Table></div>}
    </Section>
  );
}
