import { useState, useCallback, useEffect, useRef } from "react";
import { usePolling } from "../hooks/usePolling";
import { listWorkerTasks, type WorkerTask } from "../api";
import { PageHeader } from "../components/ui/PageHeader";
import { PageMeta } from "../components/ui/PageMeta";
import { Section } from "../components/ui/Section";
import { Glossary, type GlossaryItem } from "../components/ui/Glossary";
import { Badge, TASK_TONE } from "../components/ui/Badge";
import { Button } from "../components/ui/Button";
import { Input, Select } from "../components/ui/Input";
import { Table, THead, TH, TR, TD, Empty } from "../components/ui/Table";
import { Id } from "../components/ui/Mono";
import { Relative } from "../components/ui/Relative";
import { SkeletonTable } from "../components/ui/Skeleton";
import { IconCheck, IconCopy } from "../components/ui/Icons";

const PAGE_GLOSSARY: GlossaryItem[] = [
  {
    term: "Worker task",
    definition:
      "A single step of an execution, dispatched to exactly one worker process. One execution produces many tasks — one per block, plus one per retry attempt.",
  },
  {
    term: "Handler",
    definition:
      "The code registered in your worker to execute this block. The engine finds a worker that has registered this handler name and routes the task there.",
  },
  {
    term: "Block",
    definition:
      "A step inside the sequence definition. The block_id identifies which step this task is for — e.g. 'send_email', 'charge_card'.",
  },
  {
    term: "Attempt",
    definition:
      "Retry counter starting at 1. If a task fails and the retry policy allows another attempt, a new task with attempt+1 is created. Max attempt is set in the sequence.",
  },
  {
    term: "pending",
    definition:
      "Created but not yet claimed by any worker. The first available worker with the matching handler will pick it up.",
  },
  {
    term: "claimed",
    definition:
      "A worker has taken ownership and is executing it. If the worker dies without completing, the task is reclaimed by another worker after the timeout.",
  },
  {
    term: "completed",
    definition:
      "Terminal success. Output payload is stored and the execution moves to the next block.",
  },
  {
    term: "failed",
    definition:
      "Terminal failure for this attempt. May produce a retry task; if the retry budget is exhausted, the whole execution fails.",
  },
  {
    term: "Worker",
    definition:
      "The ID of the process that claimed this task. Cross-reference with Operations → Cluster nodes to see if that worker is still alive.",
  },
  {
    term: "Timeout",
    definition:
      "Max milliseconds the handler is allowed to run before the engine reclaims the task. Set per-block; 0 or empty means no timeout.",
  },
  {
    term: "Params / Context / Output",
    definition:
      "Three JSON payloads attached to a task. Params are the inputs your handler receives. Context is engine metadata (trace IDs, execution id). Output is what your handler returned.",
  },
  {
    term: "retryable / permanent",
    definition:
      "Error classification. Retryable errors produce another attempt (if the retry budget allows). Permanent errors end the execution immediately — no more retries.",
  },
];

export default function Tasks() {
  const [stateFilter, setStateFilter] = useState("");
  const [handlerFilter, setHandlerFilter] = useState("");
  const [selected, setSelected] = useState<WorkerTask | null>(null);
  const [copiedError, setCopiedError] = useState(false);
  const didRestore = useRef(false);

  const fetcher = useCallback(
    () =>
      listWorkerTasks({
        state: stateFilter || undefined,
        handler_name: handlerFilter || undefined,
        limit: "100",
      }),
    [stateFilter, handlerFilter],
  );
  const { data: tasks, loading, updatedAt, refresh, error } =
    usePolling<WorkerTask[]>(fetcher);

  const hasFilters = Boolean(stateFilter || handlerFilter);

  // Restore selection from URL on first data load
  useEffect(() => {
    if (!tasks || didRestore.current) return;
    didRestore.current = true;
    const params = new URLSearchParams(window.location.search);
    const taskId = params.get("task");
    if (taskId) {
      const found = tasks.find((t) => t.id === taskId);
      if (found) setSelected(found);
    }
  }, [tasks]);

  // Sync selection to URL
  useEffect(() => {
    const url = new URL(window.location.href);
    if (selected) {
      url.searchParams.set("task", selected.id);
    } else {
      url.searchParams.delete("task");
    }
    window.history.replaceState({}, "", url);
  }, [selected]);

  return (
    <div className="space-y-12">
      <PageHeader
        eyebrow="Operator"
        title="Worker Tasks"
        description="The finest unit of work the engine tracks — one task is one attempt of one block for one execution. This is where you see exactly what each worker is doing right now."
        actions={<PageMeta updatedAt={updatedAt} onRefresh={refresh} />}
      />

      <Glossary items={PAGE_GLOSSARY} />

      <Section
        eyebrow="Filter"
        title="Narrow the task stream"
        description={
          <>
            State filter isolates tasks by lifecycle stage. Handler filter
            accepts a prefix — useful to see every task running a specific
            bit of your code. Clear both to see the most recent 100 tasks
            across the whole cluster.
          </>
        }
      >
        <div className="grid grid-cols-1 md:grid-cols-[200px_1fr] gap-3">
          <div>
            <label className="field-label block mb-1.5">State</label>
            <Select
              value={stateFilter}
              onChange={(e) => setStateFilter(e.target.value)}
              className="w-full"
            >
              <option value="">All states</option>
              <option value="pending">pending</option>
              <option value="claimed">claimed</option>
              <option value="completed">completed</option>
              <option value="failed">failed</option>
            </Select>
          </div>
          <div>
            <label className="field-label block mb-1.5">Handler name</label>
            <Input
              type="text"
              placeholder="e.g. send_email"
              value={handlerFilter}
              onChange={(e) => setHandlerFilter(e.target.value)}
            />
          </div>
        </div>
      </Section>

      {error && <div className="notice notice-warn">{error.message}</div>}

      {loading && !tasks && <SkeletonTable rows={8} cols={6} />}

      {tasks && (
        <Section
          eyebrow="Task stream"
          title="Dispatched work"
          description={
            <>
              Each row is a single attempt of a single block. Click a row
              to open its full detail on the right — inputs, outputs, error
              messages, timings. High{" "}
              <strong className="text-ink">Attempt</strong> numbers indicate
              flaky handlers and are a good signal to investigate.
            </>
          }
          meta={
            <span>
              <span className="text-faint">SHOWING</span>{" "}
              <span className="text-ink-dim">{tasks.length}</span>
              <span className="text-faint"> / LIMIT 100</span>
            </span>
          }
        >
          <Table>
            <THead>
              <TH>State</TH>
              <TH>Handler</TH>
              <TH>Block</TH>
              <TH>Worker</TH>
              <TH className="text-right">Attempt</TH>
              <TH>Created</TH>
            </THead>
              <tbody>
                {tasks.map((t) => (
                  <TR
                    key={t.id}
                    onClick={() =>
                      setSelected(selected?.id === t.id ? null : t)
                    }
                    active={selected?.id === t.id}
                    className="cursor-pointer"
                  >
                    <TD className="align-top">
                      <Badge
                        tone={TASK_TONE[t.state] ?? "dim"}
                        dot
                        live={t.state === "claimed"}
                      >
                        {t.state}
                      </Badge>
                    </TD>
                    <TD className="font-mono text-[12px] align-top">
                      {t.handler_name}
                    </TD>
                    <TD className="font-mono text-[12px] text-muted align-top">
                      {t.block_id}
                      {t.state === "failed" && t.error_message && (
                        <div
                          className="mt-1 text-warn text-[11px] font-mono line-clamp-2 max-w-[60ch]"
                          title={t.error_message}
                        >
                          {t.error_message}
                        </div>
                      )}
                    </TD>
                    <TD
                      className="font-mono text-[12px] text-muted align-top"
                      title={t.worker_id ?? "Not yet claimed"}
                    >
                      {t.worker_id ?? "—"}
                    </TD>
                    <TD
                      className="text-right tabular align-top"
                      title="1 = first try"
                    >
                      {t.attempt}
                    </TD>
                    <TD className="align-top">
                      <Relative at={t.created_at} />
                    </TD>
                  </TR>
                ))}
                {tasks.length === 0 && (
                  <Empty colSpan={99}>
                    {hasFilters
                      ? "No tasks match these filters — try clearing one."
                      : "No tasks yet. Start a sequence and tasks will appear here as workers claim them."}
                  </Empty>
                )}
              </tbody>
            </Table>
        </Section>
      )}

      {selected && (
        <Section
          eyebrow="Task detail"
          title="Full record"
          description={
            <>
              Everything the engine knows about this attempt. The three
              JSON panels (<strong className="text-ink">Params</strong>,{" "}
              <strong className="text-ink">Context</strong>,{" "}
              <strong className="text-ink">Output</strong>) are the full
              payloads — what the handler received and what it returned.
            </>
          }
          meta={
            <>
              <Id value={selected.id} short={false} copy />
              <Button
                variant="ghost"
                size="sm"
                onClick={() => setSelected(null)}
              >
                Close
              </Button>
            </>
          }
        >
          <div>
            <div className="grid grid-cols-2 md:grid-cols-3 gap-x-8 gap-y-6">
              <Field label="Execution" value={selected.instance_id} mono copy />
              <Field label="Handler" value={selected.handler_name} mono />
              <Field label="Block" value={selected.block_id} mono />
              <Field label="State" value={selected.state} />
              <Field label="Attempt" value={String(selected.attempt)} />
              <Field label="Worker" value={selected.worker_id ?? "—"} mono />
              <Field label="Queue" value={selected.queue_name ?? "—"} mono />
              <Field
                label="Timeout"
                value={selected.timeout_ms ? `${selected.timeout_ms} ms` : "—"}
              />
              <Field
                label="Created"
                value={new Date(selected.created_at).toLocaleString()}
              />
              <Field
                label="Claimed"
                value={
                  selected.claimed_at
                    ? new Date(selected.claimed_at).toLocaleString()
                    : "—"
                }
              />
              <Field
                label="Completed"
                value={
                  selected.completed_at
                    ? new Date(selected.completed_at).toLocaleString()
                    : "—"
                }
              />
            </div>

            {selected.error_message && (
              <div className="mt-6">
                <div className="field-label mb-1.5">Error</div>
                <p className="annotation mb-2">
                  The last error the handler raised on this attempt.
                  Retryable errors produce another attempt; permanent ones
                  end the execution immediately.
                </p>
                <div className="notice notice-warn">
                  <span className="flex-1">{selected.error_message}</span>
                  {selected.error_retryable !== null && (
                    <span className="text-muted">
                      ({selected.error_retryable ? "retryable" : "permanent"})
                    </span>
                  )}
                  <button
                    type="button"
                    onClick={async () => {
                      try {
                        await navigator.clipboard.writeText(selected.error_message!);
                        setCopiedError(true);
                        window.setTimeout(() => setCopiedError(false), 1200);
                      } catch {
                        /* clipboard blocked */
                      }
                    }}
                    title={copiedError ? "Copied" : "Copy error"}
                    className="inline-flex items-center gap-1 text-[11px] text-fg-dim hover:text-fg transition-colors cursor-copy ml-auto"
                  >
                    {copiedError ? (
                      <IconCheck size={12} className="text-ok" />
                    ) : (
                      <IconCopy size={12} className="text-faint" />
                    )}
                    <span>{copiedError ? "Copied" : "Copy"}</span>
                  </button>
                </div>
              </div>
            )}

            <JsonSection
              label="Params"
              hint="Inputs passed to your handler function."
              data={selected.params}
            />
            <JsonSection
              label="Context"
              hint="Engine metadata — execution ID, trace IDs, deadlines, tenancy."
              data={selected.context}
            />
            {selected.output && (
              <JsonSection
                label="Output"
                hint="The value your handler returned — consumed by the next block in the sequence."
                data={selected.output}
              />
            )}
          </div>
        </Section>
      )}
    </div>
  );
}

function Field({
  label,
  value,
  mono,
  copy,
}: {
  label: string;
  value: string;
  mono?: boolean;
  copy?: boolean;
}) {
  return (
    <div className="min-w-0">
      <div className="field-label mb-1">{label}</div>
      {copy && value !== "—" ? (
        <Id value={value} short={false} copy className="!text-[12px]" />
      ) : (
        <div
          className={`truncate text-[13px] ${
            mono ? "font-mono text-[12px]" : ""
          }`}
          title={value}
        >
          {value}
        </div>
      )}
    </div>
  );
}

function JsonSection({
  label,
  hint,
  data,
}: {
  label: string;
  hint?: string;
  data: unknown;
}) {
  return (
    <div className="mt-6">
      <div className="field-label mb-1">{label}</div>
      {hint && <p className="annotation mb-2">{hint}</p>}
      <pre className="well max-h-56">
        {JSON.stringify(data, null, 2)}
      </pre>
    </div>
  );
}
