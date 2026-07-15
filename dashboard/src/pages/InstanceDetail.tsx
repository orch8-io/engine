import { useCallback, useEffect, useMemo, useState } from "react";
import { Link, useParams, useNavigate } from "react-router-dom";
import { usePageTitle } from "../hooks/usePageTitle";
import {
  getInstance,
  getInstanceChildren,
  getInstanceLogs,
  getExecutionTree,
  getInstanceOutputs,
  listWorkerTasks,
  sendSignal,
  retryInstance,
  streamInstance,
  getInstanceTimeline,
  listInstanceArtifacts,
  getArtifactUrl,
  getInstanceAudit,
  listCheckpoints,
  saveCheckpoint,
  pruneCheckpoints,
  forkInstance,
  resumeFromBlock,
  patchInstanceContext,
  injectBlocks,
  ApiRequestError,
  type BlockType,
  type ExecutionNode,
  type NodeState,
  type TaskInstance,
  type BlockOutput,
  type SignalType,
  type WorkerTask,
  type StepLog,
  type TimelineEntry,
  type ArtifactRef,
  type AuditEntry,
  type Checkpoint,
} from "../api";
import { PageHeader } from "../components/ui/PageHeader";
import { Section } from "../components/ui/Section";
import { Glossary, type GlossaryItem } from "../components/ui/Glossary";
import { Badge } from "../components/ui/Badge";
import { INSTANCE_TONE, NODE_TONE } from "../components/ui/badgeTones";
import { Button } from "../components/ui/Button";
import { Table, THead, TH, TR, TD, Empty } from "../components/ui/Table";
import { Relative } from "../components/ui/Relative";
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

type DetailTab = "tree" | "timeline" | "artifacts" | "audit";

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

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
  onResumeFrom,
}: {
  node: TreeNode;
  outputs: Map<string, BlockOutput>;
  tasksByBlock: Map<string, WorkerTask[]>;
  depth: number;
  now: number;
  onResumeFrom?: (blockId: string) => void;
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
            {node.state === "failed" && onResumeFrom && (
              <Button
                size="sm"
                variant="primary"
                onClick={(e) => {
                  e.stopPropagation();
                  onResumeFrom(node.block_id);
                }}
                title="Resume execution from this failed block"
              >
                <IconRetry size={11} /> Resume from here
              </Button>
            )}
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
              onResumeFrom={onResumeFrom}
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
  usePageTitle("Execution");
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const [instance, setInstance] = useState<TaskInstance | null>(null);
  const [tree, setTree] = useState<ExecutionNode[]>([]);
  const [outputs, setOutputs] = useState<BlockOutput[]>([]);
  const [tasks, setTasks] = useState<WorkerTask[]>([]);
  const [children, setChildren] = useState<TaskInstance[]>([]);
  const [logs, setLogs] = useState<StepLog[]>([]);
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

  const [activeTab, setActiveTab] = useState<DetailTab>("tree");
  const [timeline, setTimeline] = useState<TimelineEntry[]>([]);
  const [artifacts, setArtifacts] = useState<ArtifactRef[]>([]);
  const [audit, setAudit] = useState<AuditEntry[]>([]);
  const [checkpoints, setCheckpoints] = useState<Checkpoint[]>([]);
  const [busyCheckpointSave, setBusyCheckpointSave] = useState(false);
  const [busyCheckpointPrune, setBusyCheckpointPrune] = useState(false);

  const [forkOpen, setForkOpen] = useState(false);
  const [forkDryRun, setForkDryRun] = useState(false);
  const [forkContextPatch, setForkContextPatch] = useState("");
  const [busyFork, setBusyFork] = useState(false);

  const [editContextOpen, setEditContextOpen] = useState(false);
  const [editContextJson, setEditContextJson] = useState("");
  const [busyEditContext, setBusyEditContext] = useState(false);

  const [injectOpen, setInjectOpen] = useState(false);
  const [injectParentBlockId, setInjectParentBlockId] = useState("");
  const [injectBlocksJson, setInjectBlocksJson] = useState("[]");
  const [busyInject, setBusyInject] = useState(false);

  const refresh = useCallback(async () => {
    if (!id) return;
    try {
      const inst = await getInstance(id);
      const [t, o, failed, kids, logsData] = await Promise.all([
        getExecutionTree(id),
        getInstanceOutputs(id).catch(() => [] as BlockOutput[]),
        // The tasks endpoint doesn't support filtering by instance_id, so we
        // fetch a batch of failed tasks for the same tenant and filter client-side.
        listWorkerTasks({ state: "failed", tenant_id: inst.tenant_id, limit: "200" }).catch(
          () => [] as WorkerTask[],
        ),
        getInstanceChildren(id).catch(() => [] as TaskInstance[]),
        getInstanceLogs(id).catch(() => [] as StepLog[]),
      ]);
      setInstance(inst);
      setTree(t);
      setOutputs(o);
      setTasks(failed.filter((t2) => t2.instance_id === id));
      setChildren(kids);
      setLogs(logsData);
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
    if (!id || !initialized) return;
    if (activeTab === "timeline") {
      getInstanceTimeline(id).then(setTimeline).catch(() => setTimeline([]));
    } else if (activeTab === "artifacts") {
      listInstanceArtifacts(id).then(setArtifacts).catch(() => setArtifacts([]));
    } else if (activeTab === "audit") {
      getInstanceAudit(id).then(setAudit).catch(() => setAudit([]));
    }
  }, [id, activeTab, initialized]);

  useEffect(() => {
    if (!id || !initialized) return;
    listCheckpoints(id).then(setCheckpoints).catch(() => setCheckpoints([]));
  }, [id, initialized]);

  useEffect(() => {
    if (!id || !live || !instance) return;
    if (["completed", "failed", "cancelled"].includes(instance.state)) return;

    const dispose = streamInstance(id, (ev) => {
      if (ev.kind === "error") {
        // SSE stream errors are usually transient; keep polling via refresh.
        refresh();
      } else if (ev.kind === "state" || ev.kind === "output" || ev.kind === "done") {
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

  // SLA breach sentinels are recorded as block outputs with reserved
  // `_sla:*` ids by the alert-only scheduler sweep. Surface them as a badge.
  const slaBreaches = useMemo(
    () => outputs.filter((o) => o.block_id.startsWith("_sla:")),
    [outputs],
  );

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

  const doResumeFrom = async (blockId: string) => {
    if (!id) return;
    try {
      await resumeFromBlock(id, blockId);
      setToast(`Resuming from ${blockId}`);
      await refresh();
    } catch (e) {
      setToast(`Failed: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      setTimeout(() => setToast(null), 2500);
    }
  };

  const doFork = async () => {
    if (!id) return;
    setBusyFork(true);
    try {
      let contextPatch: Record<string, unknown> | undefined;
      if (forkContextPatch.trim()) {
        contextPatch = JSON.parse(forkContextPatch);
      }
      const res = await forkInstance(id, { dry_run: forkDryRun, context_patch: contextPatch });
      if (forkDryRun) {
        setToast("Dry run complete — no instance created");
      } else {
        setForkOpen(false);
        navigate(`/instances/${res.id}`);
        return;
      }
    } catch (e) {
      setToast(`Failed: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      setBusyFork(false);
      setTimeout(() => setToast(null), 2500);
    }
  };

  const doEditContext = async () => {
    if (!id) return;
    setBusyEditContext(true);
    try {
      const patch = JSON.parse(editContextJson);
      await patchInstanceContext(id, patch);
      setToast("Context updated");
      setEditContextOpen(false);
      await refresh();
    } catch (e) {
      setToast(`Failed: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      setBusyEditContext(false);
      setTimeout(() => setToast(null), 2500);
    }
  };

  const doInjectBlocks = async () => {
    if (!id) return;
    setBusyInject(true);
    try {
      const blocks = JSON.parse(injectBlocksJson);
      await injectBlocks(id, { parent_block_id: injectParentBlockId, blocks });
      setToast("Blocks injected");
      setInjectOpen(false);
      await refresh();
    } catch (e) {
      setToast(`Failed: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      setBusyInject(false);
      setTimeout(() => setToast(null), 2500);
    }
  };

  const doSaveCheckpoint = async () => {
    if (!id) return;
    setBusyCheckpointSave(true);
    try {
      await saveCheckpoint(id);
      setToast("Checkpoint saved");
      const cp = await listCheckpoints(id).catch(() => [] as Checkpoint[]);
      setCheckpoints(cp);
    } catch (e) {
      setToast(`Failed: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      setBusyCheckpointSave(false);
      setTimeout(() => setToast(null), 2500);
    }
  };

  const doPruneCheckpoints = async () => {
    if (!id) return;
    setBusyCheckpointPrune(true);
    try {
      await pruneCheckpoints(id);
      setToast("Checkpoints pruned");
      const cp = await listCheckpoints(id).catch(() => [] as Checkpoint[]);
      setCheckpoints(cp);
    } catch (e) {
      setToast(`Failed: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      setBusyCheckpointPrune(false);
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
          <span className="flex items-center gap-2">
            <Link to="/instances" className="hover:text-ink transition-colors">
              ← Executions
            </Link>
            {instance?.parent_instance_id && (
              <>
                <span className="text-faint">/</span>
                <Link
                  to={`/instances/${instance.parent_instance_id}`}
                  className="font-mono text-signal hover:text-ink transition-colors"
                  title="This is a child execution — open its parent"
                >
                  ↑ parent {instance.parent_instance_id.slice(0, 8)}…
                </Link>
              </>
            )}
          </span>
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
            <div className="flex items-center gap-2">
              {slaBreaches.length > 0 && (
                <span title="This execution breached an SLA bound (alert-only)">
                  <Badge tone="warn">SLA breached</Badge>
                </span>
              )}
              <Badge
                tone={INSTANCE_TONE[instance.state] ?? "dim"}
                dot
                live={instance.state === "running"}
              >
                {instance.state}
              </Badge>
            </div>
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

          {slaBreaches.length > 0 && (
            <Section
              eyebrow="SLA"
              title="SLA breach (alert-only)"
              description="This execution exceeded an SLA bound declared on its sequence. The engine emitted an instance.sla_breached webhook and incremented orch8_sla_breached_total — the execution was not paused or failed."
            >
              <ul className="space-y-1.5">
                {slaBreaches.map((o) => {
                  const d = (o.output ?? {}) as Record<string, unknown>;
                  const kind = String(d._sla_breach ?? o.block_id);
                  return (
                    <li key={o.block_id} className="flex items-baseline gap-3 text-[12px] font-mono">
                      <Badge tone="warn">{kind}</Badge>
                      <span className="text-faint">
                        alerted {d._alerted_at ? new Date(String(d._alerted_at)).toLocaleString() : "—"}
                      </span>
                    </li>
                  );
                })}
              </ul>
            </Section>
          )}

          {children.length > 0 && (
            <Section
              eyebrow="Sub-executions"
              title={`Child executions (${children.length})`}
              description="Instances this execution spawned as sub-sequences. Click a row to open it."
            >
              <table className="w-full text-[13px]">
                <thead>
                  <tr className="text-faint text-[11px] uppercase tracking-wider text-left">
                    <th className="py-1.5 font-medium">State</th>
                    <th className="py-1.5 font-medium">ID</th>
                    <th className="py-1.5 font-medium">Namespace</th>
                    <th className="py-1.5 font-medium">Updated</th>
                  </tr>
                </thead>
                <tbody>
                  {children.map((c) => (
                    <tr
                      key={c.id}
                      onClick={() => navigate(`/instances/${c.id}`)}
                      className="cursor-pointer border-t border-hairline hover:bg-surface"
                    >
                      <td className="py-2">
                        <Badge tone={INSTANCE_TONE[c.state] ?? "dim"} dot>
                          {c.state}
                        </Badge>
                      </td>
                      <td className="py-2 font-mono text-[12px]">{c.id.slice(0, 8)}…</td>
                      <td className="py-2 font-mono text-[12px] text-muted">{c.namespace}</td>
                      <td className="py-2">
                        <Relative at={c.updated_at} />
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </Section>
          )}

          {logs.length > 0 && (
            <Section
              eyebrow="Logs"
              title={`Step logs (${logs.length})`}
              description="Log lines captured per step — in-process handler logs (scoped to the step span) and any logs an external worker attached on complete/fail. Oldest first."
            >
              <div className="font-mono text-[12px] space-y-0.5">
                {logs.map((l, i) => (
                  <div key={`${l.block_id}-${i}`} className="flex items-baseline gap-2">
                    <span className="text-faint tabular shrink-0">
                      {new Date(l.ts).toLocaleTimeString()}
                    </span>
                    <span
                      className={`shrink-0 uppercase text-[10px] tracking-wider w-10 ${
                        l.level === "error"
                          ? "text-warn"
                          : l.level === "warn"
                            ? "text-hold"
                            : "text-muted"
                      }`}
                    >
                      {l.level}
                    </span>
                    <span className="text-faint shrink-0">{l.block_id}</span>
                    <span className="text-ink-dim break-all">{l.message}</span>
                  </div>
                ))}
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
                {instance.session_id && (
                  <Stat label="Session" hint="Parent session grouping" mono>
                    {instance.session_id.slice(0, 8)}…
                  </Stat>
                )}
                {instance.parent_instance_id && (
                  <Stat label="Parent" hint="Parent execution (sub-sequence)">
                    <Link
                      to={`/instances/${instance.parent_instance_id}`}
                      className="font-mono text-[12px] text-signal hover:underline"
                    >
                      {instance.parent_instance_id.slice(0, 8)}…
                    </Link>
                  </Stat>
                )}
                {instance.concurrency_key && (
                  <Stat label="Concurrency" hint="Key limiting parallel runs" mono>
                    {instance.concurrency_key}
                  </Stat>
                )}
                {instance.idempotency_key && (
                  <Stat label="Idempotency" hint="Deduplication key" mono>
                    {instance.idempotency_key}
                  </Stat>
                )}
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
                <Button
                  size="sm"
                  disabled={busyFork || !instance}
                  title="Fork this execution into a new instance"
                  onClick={() => {
                    setForkContextPatch("");
                    setForkDryRun(false);
                    setForkOpen(!forkOpen);
                  }}
                >
                  Fork
                </Button>
                <Button
                  size="sm"
                  disabled={!instance}
                  title="Edit the shared context of this execution"
                  onClick={() => {
                    setEditContextJson(JSON.stringify(instance?.context ?? {}, null, 2));
                    setEditContextOpen(!editContextOpen);
                  }}
                >
                  Edit context
                </Button>
                <Button
                  size="sm"
                  disabled={!instance || terminal}
                  title="Inject blocks into the execution graph"
                  onClick={() => {
                    setInjectParentBlockId("");
                    setInjectBlocksJson("[]");
                    setInjectOpen(!injectOpen);
                  }}
                >
                  Inject blocks
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

          {forkOpen && (
            <Section eyebrow="Fork" title="Fork this execution">
              <div className="space-y-4">
                <label className="flex items-center gap-2 text-[13px]">
                  <input
                    type="checkbox"
                    checked={forkDryRun}
                    onChange={(e) => setForkDryRun(e.target.checked)}
                    className="accent-signal"
                  />
                  Dry run
                </label>
                <div>
                  <div className="field-label mb-1">Context patch (optional JSON)</div>
                  <textarea
                    value={forkContextPatch}
                    onChange={(e) => setForkContextPatch(e.target.value)}
                    rows={6}
                    spellCheck={false}
                    placeholder='{"key": "value"}'
                    className="w-full bg-sunken border border-rule px-2.5 py-2 text-[12px] font-mono text-ink placeholder:text-faint focus:border-signal focus:outline-none"
                  />
                </div>
                <div className="flex justify-end gap-2">
                  <Button size="sm" variant="ghost" onClick={() => setForkOpen(false)}>Cancel</Button>
                  <Button size="sm" variant="primary" disabled={busyFork} onClick={doFork}>
                    {busyFork ? "Forking…" : forkDryRun ? "Dry run" : "Fork"}
                  </Button>
                </div>
              </div>
            </Section>
          )}

          {editContextOpen && (
            <Section eyebrow="Edit context" title="Patch execution context">
              <div className="space-y-4">
                <div>
                  <div className="field-label mb-1">context (JSON)</div>
                  <textarea
                    value={editContextJson}
                    onChange={(e) => setEditContextJson(e.target.value)}
                    rows={12}
                    spellCheck={false}
                    className="w-full bg-sunken border border-rule px-2.5 py-2 text-[12px] font-mono text-ink placeholder:text-faint focus:border-signal focus:outline-none"
                  />
                </div>
                <div className="flex justify-end gap-2">
                  <Button size="sm" variant="ghost" onClick={() => setEditContextOpen(false)}>Cancel</Button>
                  <Button size="sm" variant="primary" disabled={busyEditContext} onClick={doEditContext}>
                    {busyEditContext ? "Saving…" : "Save"}
                  </Button>
                </div>
              </div>
            </Section>
          )}

          {injectOpen && (
            <Section eyebrow="Inject blocks" title="Inject blocks into execution">
              <div className="space-y-4">
                <div>
                  <div className="field-label mb-1">Parent block ID (optional)</div>
                  <Input
                    type="text"
                    value={injectParentBlockId}
                    onChange={(e) => setInjectParentBlockId(e.target.value)}
                    placeholder="leave empty for root"
                    className="w-full font-mono"
                  />
                </div>
                <div>
                  <div className="field-label mb-1">Blocks (JSON array)</div>
                  <textarea
                    value={injectBlocksJson}
                    onChange={(e) => setInjectBlocksJson(e.target.value)}
                    rows={10}
                    spellCheck={false}
                    className="w-full bg-sunken border border-rule px-2.5 py-2 text-[12px] font-mono text-ink placeholder:text-faint focus:border-signal focus:outline-none"
                  />
                </div>
                <div className="flex justify-end gap-2">
                  <Button size="sm" variant="ghost" onClick={() => setInjectOpen(false)}>Cancel</Button>
                  <Button size="sm" variant="primary" disabled={busyInject} onClick={doInjectBlocks}>
                    {busyInject ? "Injecting…" : "Inject"}
                  </Button>
                </div>
              </div>
            </Section>
          )}

          <Section
            eyebrow="Checkpoints"
            title="Execution checkpoints"
            description="Snapshots of execution state that can be saved and pruned."
            meta={
              <span>
                <span className="text-faint">COUNT</span>{" "}
                <span className="text-ink-dim">{checkpoints.length}</span>
              </span>
            }
          >
            <div className="flex items-center gap-2">
              <Button size="sm" disabled={busyCheckpointSave || !instance} onClick={doSaveCheckpoint}>
                {busyCheckpointSave ? "Saving…" : "Save checkpoint"}
              </Button>
              <Button
                size="sm"
                variant="danger"
                disabled={busyCheckpointPrune || checkpoints.length === 0}
                onClick={() => {
                  if (confirm("Prune all checkpoints for this execution?")) doPruneCheckpoints();
                }}
              >
                {busyCheckpointPrune ? "Pruning…" : "Prune"}
              </Button>
            </div>
          </Section>

          <Section
            eyebrow="Data"
            title="Execution data"
            description="Switch between views of the execution's runtime data."
          >
            <div>
              <div className="flex gap-0 border-b border-rule mb-6">
                {(["tree", "timeline", "artifacts", "audit"] as DetailTab[]).map((tab) => (
                  <button
                    key={tab}
                    onClick={() => setActiveTab(tab)}
                    className={`px-4 py-2 text-[12px] font-medium uppercase tracking-wider border-b-2 transition-colors ${
                      activeTab === tab
                        ? "border-signal text-ink"
                        : "border-transparent text-muted hover:text-ink"
                    }`}
                  >
                    {tab === "tree" ? "Execution tree" : tab}
                  </button>
                ))}
              </div>

              {activeTab === "tree" && (
                <div>
                  <div className="flex items-center gap-2 mb-4 flex-wrap">
                    <span>
                      <span className="text-faint text-[11px] uppercase tracking-wider">NODES</span>{" "}
                      <span className="text-ink-dim text-[13px]">{tree.length}</span>
                    </span>
                    {(Object.keys(counts) as NodeState[])
                      .filter((s) => counts[s] > 0)
                      .map((s) => (
                        <Badge key={s} tone={NODE_TONE[s] ?? "dim"}>
                          {s} {counts[s]}
                        </Badge>
                      ))}
                  </div>
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
                        onResumeFrom={instance?.state === "failed" ? doResumeFrom : undefined}
                      />
                    ))
                  )}
                </div>
              )}

              {activeTab === "timeline" && (
                <Table>
                  <THead>
                    <TH>Block ID</TH>
                    <TH>Type</TH>
                    <TH>State</TH>
                    <TH>Started</TH>
                    <TH>Completed</TH>
                    <TH className="text-right">Duration</TH>
                  </THead>
                  <tbody>
                    {timeline.length === 0 ? (
                      <Empty colSpan={6}>No timeline entries</Empty>
                    ) : (
                      timeline.map((t, i) => (
                        <TR key={`${t.block_id}-${i}`}>
                          <TD className="font-mono text-[12px]">{t.block_id}</TD>
                          <TD>
                            <span className="inline-block border border-rule text-muted text-[10px] font-mono uppercase tracking-[0.1em] px-1.5 py-[1px]">
                              {t.block_type}
                            </span>
                          </TD>
                          <TD>
                            <Badge tone={NODE_TONE[t.state as NodeState] ?? "dim"} dot>
                              {t.state}
                            </Badge>
                          </TD>
                          <TD className="text-[12px] tabular">
                            {t.started_at ? new Date(t.started_at).toLocaleString() : "—"}
                          </TD>
                          <TD className="text-[12px] tabular">
                            {t.completed_at ? new Date(t.completed_at).toLocaleString() : "—"}
                          </TD>
                          <TD className="text-right font-mono text-[12px] tabular">
                            {t.duration_ms !== null ? `${t.duration_ms}ms` : "—"}
                          </TD>
                        </TR>
                      ))
                    )}
                  </tbody>
                </Table>
              )}

              {activeTab === "artifacts" && (
                <Table>
                  <THead>
                    <TH>Key</TH>
                    <TH>Size</TH>
                    <TH>Content type</TH>
                    <TH>Created</TH>
                    <TH></TH>
                  </THead>
                  <tbody>
                    {artifacts.length === 0 ? (
                      <Empty colSpan={5}>No artifacts</Empty>
                    ) : (
                      artifacts.map((a) => (
                        <TR key={a.key}>
                          <TD className="font-mono text-[12px]">{a.key}</TD>
                          <TD className="font-mono text-[12px] text-muted">{formatBytes(a.size)}</TD>
                          <TD className="text-[12px] text-muted">{a.content_type ?? "—"}</TD>
                          <TD className="text-[12px] tabular">
                            {new Date(a.created_at).toLocaleString()}
                          </TD>
                          <TD>
                            <a
                              href={getArtifactUrl(a.key)}
                              target="_blank"
                              rel="noopener noreferrer"
                              className="text-signal text-[12px] hover:underline"
                            >
                              Download
                            </a>
                          </TD>
                        </TR>
                      ))
                    )}
                  </tbody>
                </Table>
              )}

              {activeTab === "audit" && (
                <Table>
                  <THead>
                    <TH>Action</TH>
                    <TH>Actor</TH>
                    <TH>Detail</TH>
                    <TH>Created</TH>
                  </THead>
                  <tbody>
                    {audit.length === 0 ? (
                      <Empty colSpan={4}>No audit entries</Empty>
                    ) : (
                      audit.map((a, i) => (
                        <TR key={`${a.action}-${i}`}>
                          <TD className="font-mono text-[12px]">{a.action}</TD>
                          <TD className="font-mono text-[12px] text-muted">{a.actor ?? "—"}</TD>
                          <TD>
                            <pre className="text-[11px] font-mono text-muted max-w-md truncate">
                              {JSON.stringify(a.detail)}
                            </pre>
                          </TD>
                          <TD className="text-[12px] tabular">
                            {new Date(a.created_at).toLocaleString()}
                          </TD>
                        </TR>
                      ))
                    )}
                  </tbody>
                </Table>
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
