import { useState, useCallback } from "react";
import { usePolling } from "../hooks/usePolling";
import { usePageTitle } from "../hooks/usePageTitle";
import {
  connectionInfo,
  listWorkers,
  listWorkerCommands,
  listHandlers,
  enqueueWorkerCommand,
  listWorkerVersionRollouts,
  stageWorkerVersionPin,
  changeWorkerVersionRollout,
  deleteVersionPin,
  type WorkerInfo,
  type WorkerCommand,
  type HandlerCatalog,
  type WorkerCommandKind,
  type WorkerVersionRollout,
  type WorkerVersionRolloutAction,
} from "../api";
import { EffectReviewQueue } from "../components/EffectReviewQueue";
import { WorkerPlacementPreview } from "../components/WorkerPlacementPreview";
import { WorkerEnrollment } from "../components/WorkerEnrollment";
import { Button } from "../components/ui/Button";
import { PageHeader } from "../components/ui/PageHeader";
import { PageMeta } from "../components/ui/PageMeta";
import { Section } from "../components/ui/Section";
import { Glossary, type GlossaryItem } from "../components/ui/Glossary";
import { Badge } from "../components/ui/Badge";
import { Input } from "../components/ui/Input";
import { Table, THead, TH, TR, TD, Empty } from "../components/ui/Table";
import { Id } from "../components/ui/Mono";
import { Relative } from "../components/ui/Relative";
import { SkeletonTable } from "../components/ui/Skeleton";
import { StatusDot } from "../components/ui/StatusDot";

const PAGE_GLOSSARY: GlossaryItem[] = [
  {
    term: "Worker",
    definition:
      "An external process that polls the engine for tasks. One worker can serve many handlers. Workers register themselves implicitly — every poll refreshes their entry here.",
  },
  {
    term: "Alive",
    definition:
      "The worker polled within the liveness window (60s by default). A worker that stops polling goes stale — its claimed tasks are reclaimed by the stale-task reaper.",
  },
  {
    term: "Handlers",
    definition:
      "The handler names this worker has polled for. A task whose handler appears in no live worker's list will sit pending until a matching worker shows up.",
  },
  {
    term: "In flight",
    definition:
      "Tasks currently claimed by this worker — work it has taken but not yet completed or failed.",
  },
  {
    term: "Version",
    definition:
      "Optional build/deploy version the worker self-reports on poll. Different versions across workers on the same handler indicate a rolling deploy in progress.",
  },
  {
    term: "Built-in handler",
    definition:
      "A handler the engine executes in-process (http_request, llm_call, sleep, …). It never appears as a worker — no polling involved.",
  },
];

export default function Workers() {
  usePageTitle("Workers");
  const tenant = connectionInfo().tenantId ?? "default";
  const [includeStale, setIncludeStale] = useState(false);
  const [drainTarget, setDrainTarget] = useState<string | null>(null);
  const [drainCommand, setDrainCommand] = useState<WorkerCommand | null>(null);
  const drainFetcher = useCallback((signal?: AbortSignal) => (
    drainCommand ? listWorkerCommands(drainCommand.worker_id, signal) : Promise.resolve([])
  ), [drainCommand]);
  const { data: drainCommands, error: drainError } = usePolling(drainFetcher, 3000);

  const workersFetcher = useCallback(
    (signal?: AbortSignal) =>
      listWorkers(
        { include_stale: includeStale || drainCommand ? "true" : undefined },
        signal,
      ),
    [includeStale, drainCommand],
  );
  const {
    data: workers,
    loading,
    updatedAt,
    refresh,
    error,
  } = usePolling<WorkerInfo[]>(workersFetcher, 5000);

  const handlersFetcher = useCallback(
    (signal?: AbortSignal) => listHandlers(signal),
    [],
  );
  const { data: catalog } = usePolling<HandlerCatalog>(handlersFetcher, 30000);

  const pinsFetcher = useCallback(
    (signal?: AbortSignal) => listWorkerVersionRollouts(signal),
    [],
  );
  const {
    data: pins,
    refresh: refreshPins,
    error: pinError,
  } = usePolling<WorkerVersionRollout[]>(pinsFetcher, 10000);

  const [pinTenant, setPinTenant] = useState("");
  const [pinHandler, setPinHandler] = useState("");
  const [pinVersion, setPinVersion] = useState("");
  const [pinBusy, setPinBusy] = useState(false);
  const [rolloutChanges, setRolloutChanges] = useState(0);
  const rolloutRevision = JSON.stringify([
    rolloutChanges,
    pins?.map((pin) => JSON.stringify([
      pin.tenant_id, pin.handler_name, pin.revision, pin.active_min_version, pin.staged_min_version,
    ])).sort(),
  ]);

  const [toast, setToast] = useState<string | null>(null);
  const [busyWorker, setBusyWorker] = useState<string | null>(null);

  const changeRollout = async (pin: WorkerVersionRollout, action: WorkerVersionRolloutAction | "delete") => {
    const label = {
      promote: "Promote staged version",
      rollback: "Roll back active version",
      discard: "Discard staged version",
      delete: "Delete version pin",
    }[action];
    if (!window.confirm(`${label} for ${pin.handler_name} in tenant ${pin.tenant_id}?`)) return;
    setPinBusy(true);
    try {
      if (action === "delete") {
        await deleteVersionPin(pin.tenant_id, pin.handler_name);
      } else {
        await changeWorkerVersionRollout(pin, action);
      }
      setRolloutChanges((count) => count + 1);
      setToast(`Updated version rollout for ${pin.handler_name}`);
    } catch (error) {
      setToast(error instanceof Error ? error.message : String(error));
    } finally {
      setPinBusy(false);
      refreshPins();
      setTimeout(() => setToast(null), 3500);
    }
  };

  const sendCommand = async (workerId: string, command: WorkerCommandKind) => {
    setBusyWorker(workerId);
    try {
      const queued = await enqueueWorkerCommand(workerId, command);
      if (command === "drain") {
        setDrainCommand(queued);
        setDrainTarget(null);
      }
      setToast(`Queued ${command} for ${workerId.slice(0, 12)}…`);
      setTimeout(() => setToast(null), 2500);
    } catch (e) {
      setToast(`Failed: ${e instanceof Error ? e.message : String(e)}`);
      setTimeout(() => setToast(null), 3500);
    } finally {
      setBusyWorker(null);
    }
  };

  return (
    <div className="space-y-12">
      <PageHeader
        eyebrow="Operator"
        title="Workers"
        description="Every external worker that has polled the engine, with liveness, served handlers, and in-flight load. The answer to 'is my worker alive and what can it run?' — without grepping logs."
        actions={<PageMeta updatedAt={updatedAt} onRefresh={refresh} />}
      />

      <Glossary items={PAGE_GLOSSARY} />

      <WorkerEnrollment />

      <WorkerPlacementPreview rolloutRevision={rolloutRevision} />

      <EffectReviewQueue key={tenant} tenant={tenant} />

      {(drainTarget || drainCommand) && (
        <Section title="Worker drain">
          {drainTarget && (
            <div className="space-y-3">
              <p>Ask {drainTarget} to stop taking new tasks on its gRPC worker stream. The worker can finish its active tasks.</p>
              <p className="text-muted">Command scope is {connectionInfo().tenantId ?? "the root worker scope"}. REST workers require a process-level graceful shutdown.</p>
              <div className="flex gap-2">
                <Button size="sm" disabled={busyWorker !== null} onClick={() => void sendCommand(drainTarget, "drain")}>Queue drain</Button>
                <Button size="sm" disabled={busyWorker !== null} onClick={() => setDrainTarget(null)}>Cancel</Button>
              </div>
            </div>
          )}
          {drainCommand && (
            <div className="space-y-2">
              <p>{drainCommand.worker_id}</p>
              {drainError && <p role="status">{drainError.message}</p>}
              <p role="status">{!drainCommands ? "Checking delivery…" : drainCommands.some((item) => item.id === drainCommand.id)
                ? "Drain command pending delivery."
                : "Check intake and active work before stopping the worker."}</p>
              <p>Active tasks: {workers?.find((worker) => worker.worker_id === drainCommand.worker_id)?.in_flight ?? "unavailable"}</p>
              <Button size="sm" onClick={() => setDrainCommand(null)}>Close</Button>
            </div>
          )}
        </Section>
      )}

      {toast && <div className="notice notice-ok">{toast}</div>}
      {error && <div className="notice notice-warn">{error.message}</div>}

      {loading && !workers && <SkeletonTable rows={5} cols={8} />}

      {workers && (
        <Section
          eyebrow="Fleet"
          title="Registered workers"
          description={
            <>
              A worker is <strong className="text-ink">alive</strong> when it
              polled within the last 60 seconds. Stale workers are hidden by
              default — they reappear the moment they poll again.
            </>
          }
          meta={
            <label className="flex items-center gap-2 text-[13px] text-muted cursor-pointer select-none">
              <input
                type="checkbox"
                checked={includeStale}
                onChange={(e) => setIncludeStale(e.target.checked)}
              />
              Show stale workers
            </label>
          }
        >
          <Table>
            <THead>
              <TH>Status</TH>
              <TH>Worker</TH>
              <TH>Handlers</TH>
              <TH>Queues</TH>
              <TH>Version</TH>
              <TH>In flight</TH>
              <TH>Last seen</TH>
              <TH className="text-right">Control</TH>
            </THead>
            <tbody>
              {workers.length === 0 && (
                <Empty colSpan={8}>
                  No workers registered yet. Workers appear here automatically
                  on their first poll of /workers/tasks/poll.
                </Empty>
              )}
              {workers.map((w) => (
                <TR key={w.worker_id}>
                  <TD>
                    <span className="flex items-center gap-2">
                      <StatusDot
                        tone={w.alive ? "live" : "dim"}
                        live={w.alive}
                      />
                      {w.alive ? "alive" : "stale"}
                    </span>
                  </TD>
                  <TD>
                    <Id value={w.worker_id} short={false} />
                  </TD>
                  <TD>
                    <span className="flex flex-wrap gap-1">
                      {w.handlers.map((h) => (
                        <Badge key={h} tone="neutral">
                          {h}
                        </Badge>
                      ))}
                    </span>
                  </TD>
                  <TD>
                    {w.queues.length > 0 ? (
                      <span className="flex flex-wrap gap-1">
                        {w.queues.map((q) => (
                          <Badge key={q} tone="neutral">
                            {q}
                          </Badge>
                        ))}
                      </span>
                    ) : (
                      <span className="text-faint">default</span>
                    )}
                  </TD>
                  <TD>
                    {w.version ? (
                      <span className="font-mono text-[12px]">{w.version}</span>
                    ) : (
                      <span className="text-faint">—</span>
                    )}
                  </TD>
                  <TD>{w.in_flight}</TD>
                  <TD>
                    <Relative at={w.last_seen_at} />
                  </TD>
                  <TD className="text-right">
                    <div className="inline-flex gap-1">
                      {(["drain", "reload", "ping"] as WorkerCommandKind[]).map((c) => (
                        <Button
                          key={c}
                          variant="ghost"
                          size="sm"
                          disabled={busyWorker === w.worker_id}
                          onClick={() => c === "drain" ? setDrainTarget(w.worker_id) : void sendCommand(w.worker_id, c)}
                          title={`Queue a ${c} command for this worker (picked up on its next command poll)`}
                        >
                          {c}
                        </Button>
                      ))}
                    </div>
                  </TD>
                </TR>
              ))}
            </tbody>
          </Table>
        </Section>
      )}

      {catalog && (
        <Section
          eyebrow="Catalog"
          title="Available handlers"
          description={
            <>
              Everything the engine can execute right now. Built-in handlers
              run in-process; external handlers are served by the workers
              above. A sequence referencing a handler in neither list will
              queue tasks until a worker registers it.
            </>
          }
        >
          <div className="space-y-4">
            <div>
              <div className="field-label mb-1.5">
                Built-in ({catalog.builtin.length})
              </div>
              <div className="flex flex-wrap gap-1">
                {catalog.builtin.map((h) => (
                  <Badge key={h} tone="neutral">
                    {h}
                  </Badge>
                ))}
              </div>
            </div>
            <div>
              <div className="field-label mb-1.5">
                External ({catalog.external.length})
              </div>
              {catalog.external.length > 0 ? (
                <div className="flex flex-wrap gap-1">
                  {catalog.external.map((h) => (
                    <Badge key={h} tone="neutral">
                      {h}
                    </Badge>
                  ))}
                </div>
              ) : (
                <div className="text-faint text-[13px]">
                  No external handlers seen yet.
                </div>
              )}
            </div>
          </div>
        </Section>
      )}

      <Section
        eyebrow="Deployment"
        title="Worker version rollouts"
        description="Stage a minimum version and preview it against the fleet. Promote it to enforce it on tenant-scoped HTTP polls, or roll back to the previous minimum."
      >
        <div className="space-y-6">
          <div className="grid grid-cols-1 md:grid-cols-[1fr_1fr_1fr_auto] gap-3 items-end">
            <div>
              <label htmlFor="pin-tenant" className="field-label block mb-1.5">Tenant ID</label>
              <Input
                id="pin-tenant"
                type="text"
                placeholder="tenant-a"
                value={pinTenant}
                onChange={(e) => setPinTenant(e.target.value)}
              />
            </div>
            <div>
              <label htmlFor="pin-handler" className="field-label block mb-1.5">Handler name</label>
              <Input
                id="pin-handler"
                type="text"
                placeholder="my_handler"
                value={pinHandler}
                onChange={(e) => setPinHandler(e.target.value)}
              />
            </div>
            <div>
              <label htmlFor="pin-version" className="field-label block mb-1.5">Minimum version</label>
              <Input
                id="pin-version"
                type="text"
                placeholder="2.0.0"
                value={pinVersion}
                onChange={(e) => setPinVersion(e.target.value)}
              />
            </div>
            <Button
              variant="primary"
              size="sm"
              disabled={pinBusy || !pinTenant.trim() || !pinHandler.trim() || !pinVersion.trim()}
              onClick={async () => {
                setPinBusy(true);
                try {
                  await stageWorkerVersionPin({
                    tenant_id: pinTenant.trim(),
                    handler_name: pinHandler.trim(),
                    min_version: pinVersion.trim(),
                  });
                  setRolloutChanges((count) => count + 1);
                  setToast(`Staged version pin for ${pinHandler.trim()}`);
                  setTimeout(() => setToast(null), 2500);
                  setPinTenant("");
                  setPinHandler("");
                  setPinVersion("");
                  refreshPins();
                } catch (e) {
                  setToast(`Failed: ${e instanceof Error ? e.message : String(e)}`);
                  setTimeout(() => setToast(null), 3500);
                } finally {
                  setPinBusy(false);
                }
              }}
            >
              Stage version
            </Button>
          </div>

          {pinError && <p role="status" className="text-muted">{pinError.message}</p>}
          <Table>
            <THead>
              <TH>Tenant</TH>
              <TH>Handler</TH>
              <TH>Active minimum</TH>
              <TH>Staged minimum</TH>
              <TH className="text-right">Actions</TH>
            </THead>
            <tbody>
              {(!pins || pins.length === 0) && (
                <Empty colSpan={5}>
                  No worker version rollouts.
                </Empty>
              )}
              {pins?.map((p) => (
                <TR key={`${p.tenant_id}/${p.handler_name}`}>
                  <TD>
                    <Id value={p.tenant_id} copy className="!text-muted" />
                  </TD>
                  <TD>
                    <Badge tone="neutral">{p.handler_name}</Badge>
                  </TD>
                  <TD className="font-mono">{p.active_min_version ?? "None"}</TD>
                  <TD className="font-mono">{p.staged_min_version ?? "None"}</TD>
                  <TD className="text-right">
                    <div className="flex flex-wrap justify-end gap-2">
                      {p.staged_min_version && p.revision && (
                        <>
                          <Button size="sm" disabled={pinBusy} onClick={() => void changeRollout(p, "promote")}>
                            Promote
                          </Button>
                          <Button size="sm" disabled={pinBusy} onClick={() => void changeRollout(p, "discard")}>
                            Discard stage
                          </Button>
                        </>
                      )}
                      {p.can_rollback && p.revision && (
                        <Button size="sm" disabled={pinBusy} onClick={() => void changeRollout(p, "rollback")}>
                          Roll back to {p.previous_min_version ?? "no minimum"}
                        </Button>
                      )}
                      {p.active_min_version && !p.staged_min_version && (
                        <Button variant="danger" size="sm" disabled={pinBusy} onClick={() => void changeRollout(p, "delete")}>
                          Delete pin
                        </Button>
                      )}
                    </div>
                  </TD>
                </TR>
              ))}
            </tbody>
          </Table>
        </div>
      </Section>
    </div>
  );
}
