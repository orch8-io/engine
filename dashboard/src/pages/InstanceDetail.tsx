import { useCallback, useEffect, useMemo, useState } from "react";
import { Link, useParams } from "react-router-dom";
import {
  getInstance,
  getExecutionTree,
  getInstanceOutputs,
  listWorkerTasks,
  sendSignal,
  retryInstance,
  streamInstance,
  ApiRequestError,
  type BlockType,
  type ExecutionNode,
  type NodeState,
  type TaskInstance,
  type BlockOutput,
  type SignalType,
  type WorkerTask,
} from "../api";
import { PageHeader } from "../components/ui/PageHeader";
import { Section } from "../components/ui/Section";
import { Glossary, type GlossaryItem } from "../components/ui/Glossary";
import { Badge, INSTANCE_TONE, NODE_TONE } from "../components/ui/Badge";
import { Button } from "../components/ui/Button";
import { Input } from "../components/ui/Input";
import { StatusDot } from "../components/ui/StatusDot";
import { SkeletonLine } from "../components/ui/Skeleton";
import {
  IconChevronDown,
  IconChevronRight,
  IconPause,
  IconPlay,
  IconStop,
  IconRetry,
  IconRefresh,
  IconSend,
} from "../components/ui/Icons";

const NODE_BORDER: Record<NodeState, string> = {
  pending: "border-rule",
  running: "border-live",
  waiting: "border-hold",
  completed: "border-ok",
  failed: "border-warn",
  cancelled: "border-rule",
  skipped: "border-rule",
};

const PAGE_GLOSSARY: GlossaryItem[] = [
  {
    term: "Execution tree",
    definition:
      "The actual run graph. Every block the engine attempted for this execution, nested by parent/child relationship. Branches show parallel fan-out.",
  },
  {
    term: "Block",
    definition:
      "One step of the sequence. The tree shows block_id, type, state, and duration for each.",
  },
  {
    term: "Branch",
    definition:
      "A parallel lane inside a fan-out block. branch_index differentiates siblings running concurrently.",
  },
  {
    term: "Node state",
    definition:
      "Lifecycle of a single block: pending → running → completed / failed / waiting / cancelled / skipped. The left-border colour mirrors the state.",
  },
  {
    term: "Pause / Resume",
    definition:
      "Soft controls. Pause freezes progress and holds the execution's state; Resume picks up where it left off. Only valid for non-terminal executions.",
  },
  {
    term: "Cancel",
    definition:
      "Hard stop. The execution moves to terminal 'cancelled' — no retry, no DLQ, no resume. Irreversible.",
  },
  {
    term: "Retry (instance)",
    definition:
      "Only valid for failed executions. Re-queues from the failed block; earlier successful blocks are not re-run.",
  },
  {
    term: "Custom signal",
    definition:
      "A named message your sequence code is waiting for (block type 'wait_signal'). Sends a payload-less signal by name.",
  },
  {
    term: "Live",
    definition:
      "When enabled, the page subscribes to server-sent events and refreshes automatically on every state or output change. Disable for a frozen snapshot.",
  },
  {
    term: "Context",
    definition:
      "Key/value state carried across the execution. Blocks can read and mutate it; it survives retries and pauses.",
  },
];

interface TreeNode extends ExecutionNode {
  children: TreeNode[];
}

function buildTree(nodes: ExecutionNode[]): TreeNode[] {
  const by_id = new Map<string, TreeNode>();
  for (const n of nodes) by_id.set(n.id, { ...n, children: [] });

  const roots: TreeNode[] = [];
  for (const n of by_id.values()) {
    if (n.parent_id && by_id.has(n.parent_id)) {
      by_id.get(n.parent_id)!.children.push(n);
    } else {
      roots.push(n);
    }
  }

  const sortChildren = (t: TreeNode) => {
    t.children.sort((a, b) => {
      const ai = a.branch_index ?? 0;
      const bi = b.branch_index ?? 0;
      if (ai !== bi) return ai - bi;
      return (a.started_at ?? "").localeCompare(b.started_at ?? "");
    });
    for (const c of t.children) sortChildren(c);
  };
  roots.forEach(sortChildren);
  return roots;
}

function useNow(intervalMs: number) {
  const [now, setNow] = useState(Date.now());
  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), intervalMs);
    return () => clearInterval(id);
  }, [intervalMs]);
  return now;
}

function formatDuration(node: ExecutionNode, now: number): string {
  if (!node.started_at) return "—";
  const start = new Date(node.started_at).getTime();
  const end = node.completed_at ? new Date(node.completed_at).getTime() : now;
  const ms = end - start;
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)}s`;
  return `${(ms / 60_000).toFixed(1)}m`;
}

function BlockTypeBadge({ type }: { type: BlockType }) {
  return (
    <span className="inline-block border border-rule text-muted text-[10px] font-mono uppercase tracking-[0.1em] px-1.5 py-[1px]">
      {type}
    </span>
  );
}

function TreeNodeView({
  node,
  outputs,
  tasksByBlock,
  depth,
  now,
}: {
  node: TreeNode;
  outputs: Map<string, BlockOutput>;
  tasksByBlock: Map<string, WorkerTask[]>;
  depth: number;
  now: number;
}) {
  const [expanded, setExpanded] = useState(true);
  const output = outputs.get(node.block_id);
  const blockTasks = tasksByBlock.get(node.block_id) ?? [];
  const failedTasks = blockTasks.filter((t) => t.state === "failed" && t.error_message);
  const hasChildren = node.children.length > 0;
  const collapsible = hasChildren || !!output || failedTasks.length > 0;

  return (
    <div>
      <div
        className={`flex items-start gap-2 py-1.5 pl-2 border-l-2 ${
          NODE_BORDER[node.state]
        } ${depth > 0 ? "ml-4" : ""}`}
      >
        <button
          onClick={() => setExpanded((e) => !e)}
          disabled={!collapsible}
          className={`text-muted hover:text-ink w-4 shrink-0 ${
            !collapsible ? "opacity-0 cursor-default" : ""
          }`}
          aria-label={expanded ? "collapse" : "expand"}
        >
          {expanded ? <IconChevronDown size={12} /> : <IconChevronRight size={12} />}
        </button>
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 flex-wrap">
            <span className="font-mono text-[13px] truncate">{node.block_id}</span>
            <BlockTypeBadge type={node.block_type} />
            <Badge
              tone={NODE_TONE[node.state] ?? "dim"}
              dot
              live={node.state === "running"}
            >
              {node.state}
            </Badge>
            {node.branch_index !== null && (
              <span
                className="text-[11px] text-muted font-mono"
                title="Index inside a parallel fan-out"
              >
                branch {node.branch_index}
              </span>
            )}
            <span
              className="text-[11px] text-faint font-mono tabular ml-auto"
              title="Time elapsed between started_at and completed_at"
            >
              {formatDuration(node, now)}
            </span>
          </div>
          {expanded && failedTasks.length > 0 && (
            <div className="mt-1.5 space-y-1.5">
              {failedTasks.map((t) => (
                <div
                  key={t.id}
                  className="border-l-2 border-warn pl-2.5 py-1 text-[12px] font-mono text-warn"
                >
                  <div className="flex items-baseline gap-2 mb-1 flex-wrap">
                    <span className="eyebrow text-warn">attempt {t.attempt}</span>
                    {t.error_retryable !== null && (
                      <span className="text-[10px] font-mono uppercase tracking-wider text-muted">
                        {t.error_retryable ? "retryable" : "permanent"}
                      </span>
                    )}
                    {t.handler_name && (
                      <span className="text-muted">· {t.handler_name}</span>
                    )}
                    {t.completed_at && (
                      <span className="text-faint ml-auto text-[10px]">
                        {new Date(t.completed_at).toLocaleTimeString()}
                      </span>
                    )}
                  </div>
                  <div className="whitespace-pre-wrap break-all text-ink-dim">
                    {t.error_message}
                  </div>
                </div>
              ))}
            </div>
          )}
          {expanded && output !== undefined && output.output !== null && (
            <pre
              className="mt-1.5 well max-h-40"
              title="Output produced by this block"
            >
              {JSON.stringify(output.output, null, 2)}
            </pre>
          )}
        </div>
      </div>
      {expanded && hasChildren && depth < 20 && (
        <div>
          {node.children.map((c) => (
            <TreeNodeView
              key={c.id}
              node={c}
              outputs={outputs}
              tasksByBlock={tasksByBlock}
              depth={depth + 1}
              now={now}
            />
          ))}
        </div>
      )}
    </div>
  );
}

function LoadingSkeleton() {
  return (
    <div className="space-y-12">
      <Section eyebrow="Loading" title="Fetching execution…">
        <div className="grid grid-cols-2 md:grid-cols-4 gap-x-8 gap-y-6">
          {Array.from({ length: 4 }).map((_, i) => (
            <div key={i} className="space-y-2">
              <SkeletonLine width={64} height={8} />
              <SkeletonLine width="80%" height={14} />
            </div>
          ))}
        </div>
      </Section>

      <Section eyebrow="Controls" title="Operator actions">
        <div className="flex gap-2 flex-wrap items-center">
          <SkeletonLine width={80} height={32} />
          <SkeletonLine width={80} height={32} />
          <SkeletonLine width={80} height={32} />
          <SkeletonLine width={80} height={32} />
          <div className="flex gap-1.5 ml-auto items-center">
            <SkeletonLine width={224} height={32} />
            <SkeletonLine width={80} height={32} />
          </div>
        </div>
      </Section>

      <Section eyebrow="Execution tree" title="What actually ran">
        <div className="space-y-2">
          {Array.from({ length: 6 }).map((_, i) => (
            <SkeletonLine key={i} width={`${50 + (i % 4) * 12}%`} height={24} />
          ))}
        </div>
      </Section>
    </div>
  );
}

export default function InstanceDetail() {
  const { id } = useParams<{ id: string }>();
  const [instance, setInstance] = useState<TaskInstance | null>(null);
  const [tree, setTree] = useState<ExecutionNode[]>([]);
  const [outputs, setOutputs] = useState<BlockOutput[]>([]);
  const [tasks, setTasks] = useState<WorkerTask[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [notFound, setNotFound] = useState(false);
  const [initialized, setInitialized] = useState(false);
  const [toast, setToast] = useState<string | null>(null);
  const [customSignal, setCustomSignal] = useState("");
  const [live, setLive] = useState(true);

  const [busyPause, setBusyPause] = useState(false);
  const [busyResume, setBusyResume] = useState(false);
  const [busyCancel, setBusyCancel] = useState(false);
  const [busyRetry, setBusyRetry] = useState(false);
  const [busySignal, setBusySignal] = useState(false);

  const refresh = useCallback(async () => {
    if (!id) return;
    try {
      const [inst, t, o, failed] = await Promise.all([
        getInstance(id),
        getExecutionTree(id),
        getInstanceOutputs(id).catch(() => [] as BlockOutput[]),
        // The tasks endpoint doesn't support filtering by instance_id, so we
        // fetch a batch of failed tasks and filter client-side.
        listWorkerTasks({ state: "failed", limit: "50" }).catch(() => [] as WorkerTask[]),
      ]);
      setInstance(inst);
      setTree(t);
      setOutputs(o);
      setTasks(failed.filter((t2) => t2.instance_id === id));
      setError(null);
      setNotFound(false);
    } catch (e) {
      if (e instanceof ApiRequestError && e.status === 404) {
        setNotFound(true);
      } else {
        setError(e instanceof Error ? e.message : String(e));
      }
    } finally {
      setInitialized(true);
    }
  }, [id]);

  useEffect(() => {
    refresh();
  }, [refresh]);

  useEffect(() => {
    if (!id || !live || !instance) return;
    if (["completed", "failed", "cancelled"].includes(instance.state)) return;

    const dispose = streamInstance(id, (ev) => {
      if (ev.kind === "state" || ev.kind === "output" || ev.kind === "done") {
        refresh();
      }
    });
    return dispose;
  }, [id, live, instance, refresh]);

  const now = useNow(1000);

  const outputsByBlock = useMemo(() => {
    const m = new Map<string, BlockOutput>();
    for (const o of outputs) m.set(o.block_id, o);
    return m;
  }, [outputs]);

  const tasksByBlock = useMemo(() => {
    const m = new Map<string, WorkerTask[]>();
    for (const t of tasks) {
      const arr = m.get(t.block_id) ?? [];
      arr.push(t);
      m.set(t.block_id, arr);
    }
    for (const arr of m.values()) {
      arr.sort((a, b) => (b.completed_at ?? "").localeCompare(a.completed_at ?? ""));
    }
    return m;
  }, [tasks]);

  const failureSummary = useMemo(() => {
    const failed = tasks.filter((t) => t.state === "failed" && t.error_message);
    return failed;
  }, [tasks]);

  const roots = useMemo(() => buildTree(tree), [tree]);
  const counts = useMemo(() => {
    const c: Record<NodeState, number> = {
      pending: 0,
      running: 0,
      waiting: 0,
      completed: 0,
      failed: 0,
      cancelled: 0,
      skipped: 0,
    };
    for (const n of tree) c[n.state]++;
    return c;
  }, [tree]);

  const doSignal = async (
    signal_type: SignalType,
    label: string,
    setBusyAction: (v: boolean) => void
  ) => {
    if (!id) return;
    setBusyAction(true);
    try {
      await sendSignal(id, signal_type);
      setToast(`Sent ${label}`);
      await refresh();
    } catch (e) {
      setToast(`Failed: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      setBusyAction(false);
      setTimeout(() => setToast(null), 2500);
    }
  };

  const doRetry = async () => {
    if (!id) return;
    setBusyRetry(true);
    try {
      await retryInstance(id);
      setToast("Retry scheduled");
      await refresh();
    } catch (e) {
      setToast(`Failed: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      setBusyRetry(false);
      setTimeout(() => setToast(null), 2500);
    }
  };

  if (!id)
    return <div className="notice notice-warn">Missing execution id</div>;

  const terminal = !!instance && ["completed", "cancelled", "failed"].includes(instance.state);
  const pausable = !!instance && ["scheduled", "running", "waiting"].includes(instance.state);
  const resumable = !!instance && instance.state === "paused";

  return (
    <div className="space-y-12">
      <PageHeader
        eyebrow={
          <Link
            to="/instances"
            className="hover:text-ink transition-colors"
          >
            ← Executions
          </Link>
        }
        title={
          <span className="font-mono text-[28px] tracking-tight">
            {id.slice(0, 8)}…
          </span>
        }
        description={
          <span className="font-mono text-[11px] text-faint tracking-wider">
            {id}
          </span>
        }
        actions={
          instance && (
            <Badge
              tone={INSTANCE_TONE[instance.state] ?? "dim"}
              dot
              live={instance.state === "running"}
            >
              {instance.state}
            </Badge>
          )
        }
      />

      <Glossary items={PAGE_GLOSSARY} />

      {!initialized ? (
        <LoadingSkeleton />
      ) : (
        <>
          {error && <div className="notice notice-warn">{error}</div>}
          {toast && <div className="notice notice-ok">{toast}</div>}

          {notFound && (
            <Section
              eyebrow="Not found"
              title="Execution not found"
              description="The requested execution does not exist or may have been deleted."
            >
              <div className="text-muted text-[13px] py-6 text-center font-mono">
                No execution with id <span className="text-ink">{id}</span>
              </div>
            </Section>
          )}

          {failureSummary.length > 0 && (
            <Section
              eyebrow="Failure reasons"
              title="What went wrong"
              description={
                <>
                  Every task that failed for this execution, most recent first.
                  <strong className="text-ink"> Retryable</strong> errors produced
                  another attempt; <strong className="text-ink">permanent</strong>{" "}
                  errors ended the execution immediately.
                </>
              }
              meta={
                <span>
                  <span className="text-faint">FAILED</span>{" "}
                  <span className="text-warn">{failureSummary.length}</span>
                </span>
              }
            >
              <ul className="border-l-2 border-warn divide-y divide-rule-faint">
                {failureSummary.map((t) => (
                  <li key={t.id} className="px-4 py-3 space-y-1">
                    <div className="flex items-baseline gap-2 flex-wrap">
                      <span className="font-mono text-[12px] text-ink">
                        {t.block_id}
                      </span>
                      <span className="text-faint">·</span>
                      <span className="font-mono text-[12px] text-muted">
                        {t.handler_name}
                      </span>
                      <span className="text-faint">·</span>
                      <span className="text-[11px] font-mono uppercase tracking-wider text-muted">
                        attempt {t.attempt}
                      </span>
                      {t.error_retryable !== null && (
                        <Badge tone={t.error_retryable ? "hold" : "warn"}>
                          {t.error_retryable ? "retryable" : "permanent"}
                        </Badge>
                      )}
                      {t.completed_at && (
                        <span className="ml-auto text-[11px] font-mono text-faint">
                          {new Date(t.completed_at).toLocaleString()}
                        </span>
                      )}
                    </div>
                    <pre className="text-[12px] font-mono text-warn whitespace-pre-wrap break-all">
                      {t.error_message}
                    </pre>
                  </li>
                ))}
              </ul>
            </Section>
          )}

          {instance && (
            <Section
              eyebrow="Metadata"
              title="Execution identity"
              description="The immutable identity and key timestamps of this execution. Sequence is clickable — it opens the definition this execution is running."
            >
              <div className="grid grid-cols-2 md:grid-cols-4 gap-x-8 gap-y-6">
                <Stat label="Namespace" mono hint="Environment isolation">
                  {instance.namespace}
                </Stat>
                <Stat label="Tenant" mono hint="Business-level isolation">
                  {instance.tenant_id}
                </Stat>
                <Stat label="Sequence" hint="The definition being executed">
                  <Link
                    to={`/sequences/${instance.sequence_id}`}
                    className="font-mono text-[12px] text-signal hover:underline"
                  >
                    {instance.sequence_id.slice(0, 8)}…
                  </Link>
                </Stat>
                <Stat
                  label="Priority"
                  mono
                  hint="Higher priority executions are dispatched first"
                >
                  {instance.priority}
                </Stat>
                <Stat label="Created" hint="When this execution was scheduled">
                  <span className="text-[13px] tabular">
                    {new Date(instance.created_at).toLocaleString()}
                  </span>
                </Stat>
                <Stat label="Updated" hint="Most recent state change">
                  <span className="text-[13px] tabular">
                    {new Date(instance.updated_at).toLocaleString()}
                  </span>
                </Stat>
                <Stat
                  label="Next fire"
                  hint="When the engine plans to wake this execution next"
                >
                  <span className="text-[13px] tabular">
                    {instance.next_fire_at
                      ? new Date(instance.next_fire_at).toLocaleString()
                      : "—"}
                  </span>
                </Stat>
              </div>
            </Section>
          )}

          <Section
            eyebrow="Controls"
            title="Operator actions"
            description={
              <>
                Buttons are enabled only when the current state allows the action —
                hover any disabled button for the reason. Destructive actions
                (<strong className="text-ink">Cancel</strong>) ask for
                confirmation. The <strong className="text-ink">Live</strong>{" "}
                toggle subscribes to server-sent events and refreshes this page
                on every state change.
              </>
            }
            meta={
              <>
                <label className="flex items-center gap-1.5 cursor-pointer">
                  <input
                    type="checkbox"
                    checked={live}
                    onChange={(e) => setLive(e.target.checked)}
                    className="accent-signal"
                  />
                  <StatusDot tone={live ? "live" : "dim"} live={live} />
                  <span className="text-ink-dim">Live</span>
                </label>
                <Button variant="ghost" size="sm" onClick={refresh}>
                  <IconRefresh size={13} /> Refresh
                </Button>
              </>
            }
          >
            <div>
              <div className="flex gap-2 flex-wrap items-center">
                <Button
                  size="sm"
                  disabled={busyPause || !pausable}
                  title={
                    !instance
                      ? ""
                      : pausable
                        ? "Pause progress; can be resumed later"
                        : `Cannot pause — already ${instance.state}`
                  }
                  onClick={() => doSignal("pause", "pause", setBusyPause)}
                >
                  <IconPause size={13} /> Pause
                </Button>
                <Button
                  size="sm"
                  disabled={busyResume || !resumable}
                  title={
                    !instance
                      ? ""
                      : resumable
                        ? "Resume from where it was paused"
                        : `Cannot resume — current state: ${instance.state}`
                  }
                  onClick={() => doSignal("resume", "resume", setBusyResume)}
                >
                  <IconPlay size={13} /> Resume
                </Button>
                <Button
                  size="sm"
                  variant="danger"
                  disabled={busyCancel || !instance || terminal}
                  title="Terminate this execution — irreversible"
                  onClick={() => {
                    if (
                      confirm(
                        "Cancel this execution?\n\nIt will move to terminal 'cancelled' — no retry, no DLQ, no resume.",
                      )
                    )
                      doSignal("cancel", "cancel", setBusyCancel);
                  }}
                >
                  <IconStop size={13} /> Cancel
                </Button>
                <Button
                  size="sm"
                  variant="primary"
                  disabled={busyRetry || !instance || instance.state !== "failed"}
                  title="Re-queue from the failed block — earlier blocks are not re-run"
                  onClick={doRetry}
                >
                  <IconRetry size={13} /> Retry
                </Button>

                <div className="flex gap-1.5 ml-auto items-center">
                  <Input
                    type="text"
                    value={customSignal}
                    onChange={(e) => setCustomSignal(e.target.value)}
                    placeholder="custom signal name"
                    className="w-56 font-mono"
                  />
                  <Button
                    size="sm"
                    disabled={busySignal || !customSignal || !instance || terminal}
                    title="Send a named signal to a wait_signal block"
                    onClick={() => {
                      doSignal({ Custom: customSignal }, `signal "${customSignal}"`, setBusySignal);
                      setCustomSignal("");
                    }}
                  >
                    <IconSend size={13} /> Send
                  </Button>
                </div>
              </div>
              <p className="annotation mt-3">
                Custom signals unblock any <code className="font-mono">wait_signal</code>{" "}
                block that matches the name. Use for human-in-the-loop approvals
                or external event waiting.
              </p>
            </div>
          </Section>

          <Section
            eyebrow="Execution tree"
            title="What actually ran"
            description={
              <>
                Every block the engine touched, indented by parent/child. The
                coloured left border mirrors the state. Click the chevron to
                fold — blocks with outputs or failures can be opened; others
                have none to show. Duration is measured from{" "}
                <code className="font-mono">started_at</code> to{" "}
                <code className="font-mono">completed_at</code>.
              </>
            }
            meta={
              <>
                <span>
                  <span className="text-faint">NODES</span>{" "}
                  <span className="text-ink-dim">{tree.length}</span>
                </span>
                {(Object.keys(counts) as NodeState[])
                  .filter((s) => counts[s] > 0)
                  .map((s) => (
                    <Badge key={s} tone={NODE_TONE[s] ?? "dim"}>
                      {s} {counts[s]}
                    </Badge>
                  ))}
              </>
            }
          >
            <div>
              {roots.length === 0 ? (
                <div className="text-muted text-[13px] py-6 text-center font-mono">
                  No execution nodes yet — the engine hasn't started this
                  execution, or the tree hasn't been persisted.
                </div>
              ) : (
                roots.map((r) => (
                  <TreeNodeView
                    key={r.id}
                    node={r}
                    outputs={outputsByBlock}
                    tasksByBlock={tasksByBlock}
                    depth={0}
                    now={now}
                  />
                ))
              )}
            </div>
          </Section>

          {instance && Object.keys(instance.context).length > 0 && (
            <Section
              eyebrow="Context"
              title="Shared execution state"
              description="Mutable key/value state carried through the whole execution. Blocks read from and write to it; its value here is the current snapshot."
            >
              <pre className="well max-h-96">
                {JSON.stringify(instance.context, null, 2)}
              </pre>
            </Section>
          )}
        </>
      )}
    </div>
  );
}

function Stat({
  label,
  children,
  mono,
  hint,
}: {
  label: string;
  children: React.ReactNode;
  mono?: boolean;
  hint?: string;
}) {
  return (
    <div className="min-w-0">
      <div className="field-label mb-1">{label}</div>
      <div
        className={`truncate ${mono ? "font-mono text-[12px]" : "text-[13px]"}`}
      >
        {children}
      </div>
      {hint && <p className="annotation mt-0.5 text-[11px]">{hint}</p>}
    </div>
  );
}
