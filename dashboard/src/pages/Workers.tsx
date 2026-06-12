import { useState, useCallback } from "react";
import { usePolling } from "../hooks/usePolling";
import { usePageTitle } from "../hooks/usePageTitle";
import {
  listWorkers,
  listHandlers,
  type WorkerInfo,
  type HandlerCatalog,
} from "../api";
import { PageHeader } from "../components/ui/PageHeader";
import { PageMeta } from "../components/ui/PageMeta";
import { Section } from "../components/ui/Section";
import { Glossary, type GlossaryItem } from "../components/ui/Glossary";
import { Badge } from "../components/ui/Badge";
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
  const [includeStale, setIncludeStale] = useState(false);

  const workersFetcher = useCallback(
    (signal?: AbortSignal) =>
      listWorkers(
        { include_stale: includeStale ? "true" : undefined },
        signal,
      ),
    [includeStale],
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

  return (
    <div className="space-y-12">
      <PageHeader
        eyebrow="Operator"
        title="Workers"
        description="Every external worker that has polled the engine, with liveness, served handlers, and in-flight load. The answer to 'is my worker alive and what can it run?' — without grepping logs."
        actions={<PageMeta updatedAt={updatedAt} onRefresh={refresh} />}
      />

      <Glossary items={PAGE_GLOSSARY} />

      {error && <div className="notice notice-warn">{error.message}</div>}

      {loading && !workers && <SkeletonTable rows={5} cols={7} />}

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
            </THead>
            <tbody>
              {workers.length === 0 && (
                <Empty colSpan={7}>
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
    </div>
  );
}
