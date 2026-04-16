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
  type NodeState,
  type TaskInstance,
  type BlockOutput,
  type SignalType,
} from "../api";
import { PageHeader } from "../components/ui/PageHeader";
import { Panel, PanelBody, PanelHeader } from "../components/ui/Panel";
import { Badge, INSTANCE_TONE, NODE_TONE } from "../components/ui/Badge";
import { Button } from "../components/ui/Button";
import { Input } from "../components/ui/Input";
import { StatusDot } from "../components/ui/StatusDot";
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
  pending: "border-hairline",
  running: "border-live",
  waiting: "border-hold",
  completed: "border-ok",
  failed: "border-warn",
  cancelled: "border-hairline",
  skipped: "border-hairline",
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
  if (!node.started_at) return "—";
  const start = new Date(node.started_at).getTime();
  const end = node.completed_at ? new Date(node.completed_at).getTime() : Date.now();
  const ms = end - start;
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)}s`;
  return `${(ms / 60_000).toFixed(1)}m`;
}

function BlockTypeBadge({ type }: { type: BlockType }) {
  return (
    <span className="inline-block bg-sunken border border-hairline text-muted text-[11px] font-mono px-1.5 py-0.5 rounded-sm">
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
  const collapsible = hasChildren || !!output;

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
          className={`text-muted hover:text-fg w-4 shrink-0 ${
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
              <span className="text-[11px] text-muted font-mono">
                branch {node.branch_index}
              </span>
            )}
            <span className="text-[11px] text-faint font-mono tabular ml-auto">
              {formatDuration(node)}
            </span>
          </div>
          {expanded && output !== undefined && output.output !== null && (
            <pre className="mt-1.5 bg-sunken border border-hairline rounded-sm p-2 text-[12px] font-mono text-fg-dim overflow-x-auto max-h-40">
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

  if (!id)
    return (
      <div className="rounded-md border border-warn/40 bg-warn/10 text-warn p-3 text-[13px]">
        Missing instance id
      </div>
    );

  const terminal = instance && ["completed", "cancelled", "failed"].includes(instance.state);

  return (
    <div className="space-y-6">
      <PageHeader
        eyebrow={
          <Link
            to="/instances"
            className="hover:text-fg transition-colors"
          >
            ← Instances
          </Link>
        }
        title={
          <span className="font-mono text-[20px] tracking-tight">
            {id.slice(0, 8)}…
          </span>
        }
        description={
          <span className="font-mono text-[11px] text-faint">{id}</span>
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

      {error && (
        <div className="rounded-md border border-warn/40 bg-warn/10 text-warn p-3 text-[13px]">
          {error}
        </div>
      )}
      {toast && (
        <div className="rounded-md border border-signal/40 bg-signal/10 text-signal p-3 text-[13px]">
          {toast}
        </div>
      )}

      {instance && (
        <Panel>
          <PanelBody>
            <div className="grid grid-cols-2 md:grid-cols-4 gap-5">
              <Stat label="Namespace" mono>
                {instance.namespace}
              </Stat>
              <Stat label="Tenant" mono>
                {instance.tenant_id}
              </Stat>
              <Stat label="Sequence">
                <Link
                  to={`/sequences/${instance.sequence_id}`}
                  className="font-mono text-[12px] text-signal hover:underline"
                >
                  {instance.sequence_id.slice(0, 8)}…
                </Link>
              </Stat>
              <Stat label="Priority" mono>
                {instance.priority}
              </Stat>
              <Stat label="Created">
                <span className="text-[13px] tabular">
                  {new Date(instance.created_at).toLocaleString()}
                </span>
              </Stat>
              <Stat label="Updated">
                <span className="text-[13px] tabular">
                  {new Date(instance.updated_at).toLocaleString()}
                </span>
              </Stat>
              <Stat label="Next fire">
                <span className="text-[13px] tabular">
                  {instance.next_fire_at
                    ? new Date(instance.next_fire_at).toLocaleString()
                    : "—"}
                </span>
              </Stat>
            </div>
          </PanelBody>
        </Panel>
      )}

      <Panel>
        <PanelHeader>
          <span className="eyebrow">Controls</span>
          <label className="ml-auto flex items-center gap-1.5 text-[12px] text-muted cursor-pointer">
            <input
              type="checkbox"
              checked={live}
              onChange={(e) => setLive(e.target.checked)}
              className="accent-signal"
            />
            <StatusDot tone={live ? "live" : "dim"} live={live} />
            Live
          </label>
          <Button variant="ghost" size="sm" onClick={refresh}>
            <IconRefresh size={13} /> Refresh
          </Button>
        </PanelHeader>
        <PanelBody>
          <div className="flex gap-2 flex-wrap items-center">
            <Button
              size="sm"
              disabled={
                busy ||
                !instance ||
                instance.state === "paused" ||
                instance.state === "completed"
              }
              onClick={() => doSignal("pause", "pause")}
            >
              <IconPause size={13} /> Pause
            </Button>
            <Button
              size="sm"
              disabled={busy || !instance || instance.state !== "paused"}
              onClick={() => doSignal("resume", "resume")}
            >
              <IconPlay size={13} /> Resume
            </Button>
            <Button
              size="sm"
              variant="danger"
              disabled={busy || !instance || terminal}
              onClick={() => {
                if (confirm("Cancel this instance?")) doSignal("cancel", "cancel");
              }}
            >
              <IconStop size={13} /> Cancel
            </Button>
            <Button
              size="sm"
              variant="primary"
              disabled={busy || !instance || instance.state !== "failed"}
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
                className="w-56"
              />
              <Button
                size="sm"
                disabled={busy || !customSignal || !instance || terminal}
                onClick={() => {
                  doSignal({ Custom: customSignal }, `signal "${customSignal}"`);
                  setCustomSignal("");
                }}
              >
                <IconSend size={13} /> Send
              </Button>
            </div>
          </div>
        </PanelBody>
      </Panel>

      <Panel>
        <PanelHeader>
          <span className="eyebrow">Execution tree</span>
          <span className="font-mono text-[11px] text-faint tabular">
            {tree.length} NODES
          </span>
          <div className="ml-auto flex gap-2 flex-wrap">
            {(Object.keys(counts) as NodeState[])
              .filter((s) => counts[s] > 0)
              .map((s) => (
                <Badge key={s} tone={NODE_TONE[s] ?? "dim"}>
                  {s} {counts[s]}
                </Badge>
              ))}
          </div>
        </PanelHeader>
        <PanelBody>
          {roots.length === 0 ? (
            <div className="text-muted text-[13px] py-6 text-center">
              No execution nodes yet
            </div>
          ) : (
            roots.map((r) => (
              <TreeNodeView
                key={r.id}
                node={r}
                outputs={outputsByBlock}
                depth={0}
              />
            ))
          )}
        </PanelBody>
      </Panel>

      {instance && Object.keys(instance.context).length > 0 && (
        <Panel>
          <PanelHeader>
            <span className="eyebrow">Context</span>
          </PanelHeader>
          <PanelBody>
            <pre className="bg-sunken border border-hairline rounded-sm p-3 text-[12px] font-mono text-fg-dim overflow-x-auto max-h-96">
              {JSON.stringify(instance.context, null, 2)}
            </pre>
          </PanelBody>
        </Panel>
      )}
    </div>
  );
}

function Stat({
  label,
  children,
  mono,
}: {
  label: string;
  children: React.ReactNode;
  mono?: boolean;
}) {
  return (
    <div className="min-w-0">
      <div className="eyebrow mb-1">{label}</div>
      <div className={`truncate ${mono ? "font-mono text-[12px]" : "text-[13px]"}`}>
        {children}
      </div>
    </div>
  );
}
