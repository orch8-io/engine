import { useCallback, useEffect, useMemo, useState } from "react";
import { Link, useParams } from "react-router-dom";
import {
  getInstance,
  getExecutionTree,
  getInstanceOutputs,
  sendSignal,
  retryInstance,
  streamInstance,
  type BlockType,
  type ExecutionNode,
  type InstanceState,
  type NodeState,
  type TaskInstance,
  type BlockOutput,
  type SignalType,
} from "../api";

const NODE_STATE_COLOR: Record<NodeState, string> = {
  pending: "text-muted",
  running: "text-accent",
  waiting: "text-warning",
  completed: "text-success",
  failed: "text-danger",
  cancelled: "text-muted",
  skipped: "text-muted",
};

const NODE_STATE_BORDER: Record<NodeState, string> = {
  pending: "border-border",
  running: "border-accent",
  waiting: "border-warning",
  completed: "border-success",
  failed: "border-danger",
  cancelled: "border-muted",
  skipped: "border-muted",
};

const INSTANCE_STATE_COLOR: Record<InstanceState, string> = {
  scheduled: "text-muted",
  running: "text-accent",
  waiting: "text-warning",
  paused: "text-warning",
  completed: "text-success",
  failed: "text-danger",
  cancelled: "text-muted",
};

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

function formatDuration(node: ExecutionNode): string {
  if (!node.started_at) return "-";
  const start = new Date(node.started_at).getTime();
  const end = node.completed_at ? new Date(node.completed_at).getTime() : Date.now();
  const ms = end - start;
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)}s`;
  return `${(ms / 60_000).toFixed(1)}m`;
}

function BlockTypeBadge({ type }: { type: BlockType }) {
  return (
    <span className="inline-block bg-secondary/60 text-muted text-xs font-mono px-1.5 py-0.5 rounded">
      {type}
    </span>
  );
}

function TreeNodeView({
  node,
  outputs,
  depth,
}: {
  node: TreeNode;
  outputs: Map<string, BlockOutput>;
  depth: number;
}) {
  const [expanded, setExpanded] = useState(true);
  const output = outputs.get(node.block_id);
  const hasChildren = node.children.length > 0;

  return (
    <div>
      <div
        className={`flex items-start gap-2 py-1.5 pl-2 border-l-2 ${
          NODE_STATE_BORDER[node.state]
        } ${depth > 0 ? "ml-4" : ""}`}
      >
        <button
          onClick={() => setExpanded((e) => !e)}
          disabled={!hasChildren && !output}
          className={`text-muted hover:text-foreground text-xs w-4 ${
            !hasChildren && !output ? "opacity-0 cursor-default" : ""
          }`}
        >
          {expanded ? "▼" : "▶"}
        </button>
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 flex-wrap">
            <span className="font-mono text-sm truncate">{node.block_id}</span>
            <BlockTypeBadge type={node.block_type} />
            <span className={`text-xs font-medium ${NODE_STATE_COLOR[node.state]}`}>
              {node.state}
            </span>
            {node.branch_index !== null && (
              <span className="text-xs text-muted">branch {node.branch_index}</span>
            )}
            <span className="text-xs text-muted ml-auto">{formatDuration(node)}</span>
          </div>
          {expanded && output !== undefined && output.output !== null && (
            <pre className="mt-1 bg-secondary/40 rounded p-2 text-xs font-mono overflow-x-auto max-h-40">
              {JSON.stringify(output.output, null, 2)}
            </pre>
          )}
        </div>
      </div>
      {expanded && hasChildren && (
        <div>
          {node.children.map((c) => (
            <TreeNodeView key={c.id} node={c} outputs={outputs} depth={depth + 1} />
          ))}
        </div>
      )}
    </div>
  );
}

export default function InstanceDetail() {
  const { id } = useParams<{ id: string }>();
  const [instance, setInstance] = useState<TaskInstance | null>(null);
  const [tree, setTree] = useState<ExecutionNode[]>([]);
  const [outputs, setOutputs] = useState<BlockOutput[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [toast, setToast] = useState<string | null>(null);
  const [customSignal, setCustomSignal] = useState("");
  const [live, setLive] = useState(true);

  const refresh = useCallback(async () => {
    if (!id) return;
    try {
      const [inst, t, o] = await Promise.all([
        getInstance(id),
        getExecutionTree(id),
        getInstanceOutputs(id).catch(() => [] as BlockOutput[]),
      ]);
      setInstance(inst);
      setTree(t);
      setOutputs(o);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }, [id]);

  useEffect(() => {
    refresh();
  }, [refresh]);

  // Live SSE stream — re-fetch on every state/output event.
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

  const outputsByBlock = useMemo(() => {
    const m = new Map<string, BlockOutput>();
    for (const o of outputs) m.set(o.block_id, o);
    return m;
  }, [outputs]);

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

  const doSignal = async (signal_type: SignalType, label: string) => {
    if (!id) return;
    setBusy(true);
    try {
      await sendSignal(id, signal_type);
      setToast(`Sent ${label}`);
      await refresh();
    } catch (e) {
      setToast(`Failed: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      setBusy(false);
      setTimeout(() => setToast(null), 2500);
    }
  };

  const doRetry = async () => {
    if (!id) return;
    setBusy(true);
    try {
      await retryInstance(id);
      setToast("Retry scheduled");
      await refresh();
    } catch (e) {
      setToast(`Failed: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      setBusy(false);
      setTimeout(() => setToast(null), 2500);
    }
  };

  if (!id) return <div className="text-danger">Missing instance id</div>;

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-3">
        <Link to="/instances" className="text-muted hover:text-foreground text-sm">
          ← Instances
        </Link>
        <span className="text-muted">/</span>
        <span className="font-mono text-sm">{id}</span>
      </div>

      {error && (
        <div className="rounded border border-danger/40 bg-danger/10 text-danger p-3 text-sm">
          {error}
        </div>
      )}
      {toast && (
        <div className="rounded border border-accent/40 bg-accent/10 text-accent p-3 text-sm">
          {toast}
        </div>
      )}

      {instance && (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 bg-card border border-border rounded-lg p-4">
          <Stat label="State">
            <span className={`font-semibold ${INSTANCE_STATE_COLOR[instance.state]}`}>
              {instance.state}
            </span>
          </Stat>
          <Stat label="Namespace">
            <span className="font-mono text-sm">{instance.namespace}</span>
          </Stat>
          <Stat label="Tenant">
            <span className="font-mono text-sm">{instance.tenant_id}</span>
          </Stat>
          <Stat label="Sequence">
            <Link
              to={`/sequences/${instance.sequence_id}`}
              className="font-mono text-xs text-primary hover:underline"
            >
              {instance.sequence_id.slice(0, 8)}…
            </Link>
          </Stat>
          <Stat label="Created">
            <span className="text-sm">{new Date(instance.created_at).toLocaleString()}</span>
          </Stat>
          <Stat label="Updated">
            <span className="text-sm">{new Date(instance.updated_at).toLocaleString()}</span>
          </Stat>
          <Stat label="Next fire">
            <span className="text-sm">
              {instance.next_fire_at ? new Date(instance.next_fire_at).toLocaleString() : "-"}
            </span>
          </Stat>
          <Stat label="Priority">
            <span className="text-sm">{instance.priority}</span>
          </Stat>
        </div>
      )}

      <div className="flex gap-2 flex-wrap items-center">
        <button
          disabled={busy || !instance || instance.state === "paused" || instance.state === "completed"}
          onClick={() => doSignal("pause", "pause")}
          className="bg-card border border-border rounded px-3 py-1.5 text-sm hover:bg-secondary/50 disabled:opacity-50"
        >
          Pause
        </button>
        <button
          disabled={busy || !instance || instance.state !== "paused"}
          onClick={() => doSignal("resume", "resume")}
          className="bg-card border border-border rounded px-3 py-1.5 text-sm hover:bg-secondary/50 disabled:opacity-50"
        >
          Resume
        </button>
        <button
          disabled={busy || !instance || ["completed", "cancelled", "failed"].includes(instance.state)}
          onClick={() => {
            if (confirm("Cancel this instance?")) doSignal("cancel", "cancel");
          }}
          className="bg-card border border-danger/40 text-danger rounded px-3 py-1.5 text-sm hover:bg-danger/10 disabled:opacity-50"
        >
          Cancel
        </button>
        <button
          disabled={busy || !instance || instance.state !== "failed"}
          onClick={doRetry}
          className="bg-card border border-accent/40 text-accent rounded px-3 py-1.5 text-sm hover:bg-accent/10 disabled:opacity-50"
        >
          Retry
        </button>

        <div className="flex gap-1 ml-auto items-center">
          <input
            type="text"
            value={customSignal}
            onChange={(e) => setCustomSignal(e.target.value)}
            placeholder="custom signal name"
            className="bg-card border border-border rounded px-3 py-1.5 text-sm w-48 text-foreground"
          />
          <button
            disabled={busy || !customSignal || !instance || ["completed", "cancelled", "failed"].includes(instance.state)}
            onClick={() => {
              doSignal({ Custom: customSignal }, `signal "${customSignal}"`);
              setCustomSignal("");
            }}
            className="bg-card border border-border rounded px-3 py-1.5 text-sm hover:bg-secondary/50 disabled:opacity-50"
          >
            Send
          </button>
        </div>

        <label className="flex items-center gap-2 text-sm text-muted ml-2">
          <input
            type="checkbox"
            checked={live}
            onChange={(e) => setLive(e.target.checked)}
          />
          Live
        </label>
        <button
          onClick={refresh}
          className="text-sm text-muted hover:text-foreground px-2"
        >
          Refresh
        </button>
      </div>

      <section className="space-y-3">
        <div className="flex items-baseline gap-3">
          <h2 className="text-lg font-semibold">Execution tree</h2>
          <span className="text-xs text-muted">{tree.length} nodes</span>
          <div className="flex gap-3 text-xs ml-auto">
            {(Object.keys(counts) as NodeState[])
              .filter((s) => counts[s] > 0)
              .map((s) => (
                <span key={s} className={NODE_STATE_COLOR[s]}>
                  {s}: {counts[s]}
                </span>
              ))}
          </div>
        </div>
        <div className="bg-card border border-border rounded-lg p-3">
          {roots.length === 0 ? (
            <div className="text-muted text-sm py-4 text-center">No execution nodes yet</div>
          ) : (
            roots.map((r) => (
              <TreeNodeView key={r.id} node={r} outputs={outputsByBlock} depth={0} />
            ))
          )}
        </div>
      </section>

      {instance && Object.keys(instance.context).length > 0 && (
        <section className="space-y-2">
          <h2 className="text-lg font-semibold">Context</h2>
          <pre className="bg-secondary/50 rounded p-3 text-xs font-mono overflow-x-auto max-h-96">
            {JSON.stringify(instance.context, null, 2)}
          </pre>
        </section>
      )}
    </div>
  );
}

function Stat({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div>
      <div className="text-muted text-xs mb-0.5">{label}</div>
      <div>{children}</div>
    </div>
  );
}
