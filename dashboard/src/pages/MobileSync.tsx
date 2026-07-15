import { useCallback, useEffect, useState } from "react";
import { usePolling } from "../hooks/usePolling";
import { usePageTitle } from "../hooks/usePageTitle";
import {
  listMobileStatus,
  listMobileApprovals,
  listMobileDevices,
  resolveMobileApproval,
  sendMobileCommand,
  type MobileStatusResponse,
  type MobileApprovalsResponse,
  type MobileDevicesResponse,
  type MobileApproval,
  type MobileDeviceInfo,
  type MobileInstanceStatus,
  type MobileStepEntry,
} from "../api";
import { PageHeader } from "../components/ui/PageHeader";
import { PageMeta } from "../components/ui/PageMeta";
import { Section } from "../components/ui/Section";
import { Button } from "../components/ui/Button";
import { Badge } from "../components/ui/Badge";
import { INSTANCE_TONE } from "../components/ui/badgeTones";
import { Table, THead, TH, TR, TD, Empty } from "../components/ui/Table";
import { Id } from "../components/ui/Mono";
import { Relative } from "../components/ui/Relative";
import { Metric, MetricRow } from "../components/ui/Metric";
import { StatusDot } from "../components/ui/StatusDot";
import { SkeletonTable } from "../components/ui/Skeleton";

export default function MobileSync() {
  usePageTitle("Mobile Sync");
  const [busyId, setBusyId] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [expandedInstances, setExpandedInstances] = useState<Set<string>>(new Set());
  const [filterDevice, setFilterDevice] = useState<string>("");
  const [filterState, setFilterState] = useState<string>("");

  const statusFetcher = useCallback(
    (signal?: AbortSignal) => listMobileStatus({ limit: "200" }, signal),
    [],
  );
  const approvalsFetcher = useCallback(
    (signal?: AbortSignal) => listMobileApprovals({ state: "pending", limit: "100" }, signal),
    [],
  );
  const devicesFetcher = useCallback(
    (signal?: AbortSignal) => listMobileDevices({ limit: "200" }, signal),
    [],
  );

  const { data: statusData, loading: statusLoading, updatedAt, refresh: refreshStatus } =
    usePolling<MobileStatusResponse>(statusFetcher, 3000);
  const { data: approvalsData, loading: approvalsLoading, refresh: refreshApprovals } =
    usePolling<MobileApprovalsResponse>(approvalsFetcher, 3000);
  const { data: devicesData, refresh: refreshDevices } =
    usePolling<MobileDevicesResponse>(devicesFetcher, 10000);

  const statuses = statusData?.items ?? [];
  const statusTotal = statusData?.total ?? 0;
  const approvals = approvalsData?.items ?? [];
  const devices = devicesData?.items ?? [];

  const deviceMap = new Map(devices.map((d) => [d.device_id, d]));
  const deviceIds = [...new Set([
    ...devices.map((d) => d.device_id),
    ...statuses.map((s) => s.device_id),
  ])];

  const filteredStatuses = statuses.filter((s) => {
    if (filterDevice && s.device_id !== filterDevice) return false;
    if (filterState && s.state.toLowerCase() !== filterState) return false;
    return true;
  });

  const activeDevices = devices.filter((d) => {
    if (!d.last_sync_at) return false;
    return Date.now() - new Date(d.last_sync_at).getTime() < 5 * 60 * 1000;
  });
  const staleDevices = devices.filter((d) => {
    if (!d.last_sync_at) return true;
    return Date.now() - new Date(d.last_sync_at).getTime() >= 5 * 60 * 1000;
  });
  const runningInstances = statuses.filter(
    (s) => !["completed", "failed", "cancelled"].includes(s.state.toLowerCase()),
  );

  async function refresh() {
    await Promise.all([refreshStatus(), refreshApprovals(), refreshDevices()]);
  }

  async function resolve(item: MobileApproval, value: string) {
    setBusyId(item.id);
    setError(null);
    try {
      await resolveMobileApproval(item.id, { value });
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusyId(null);
    }
  }

  function toggleExpand(key: string) {
    setExpandedInstances((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  }

  return (
    <div className="space-y-12">
      <PageHeader
        eyebrow="Mobile"
        title="Mobile Sync"
        description="Real-time view of workflows running on mobile devices. Approve or reject human-in-the-loop steps, start new workflows with initial state, and push sequence updates with different rollout policies."
        actions={<PageMeta updatedAt={updatedAt} onRefresh={refresh} />}
      />

      {error && <div className="notice notice-warn">{error}</div>}

      {/* KPI Stat Cards */}
      <MetricRow cols={4}>
        <Metric
          label="Connected Devices"
          value={activeDevices.length}
          unit={`of ${devices.length}`}
          tone={activeDevices.length > 0 ? "ok" : "muted"}
          caption={devices.length > 0
            ? `${devices.filter((d) => d.platform === "ios").length} iOS, ${devices.filter((d) => d.platform === "android").length} Android`
            : "No devices registered"}
        />
        <Metric
          label="Pending Approvals"
          value={approvals.length}
          tone={approvals.length > 0 ? "warn" : "muted"}
          caption={approvals.length > 0 ? "Waiting for human decision" : "All clear"}
        />
        <Metric
          label="Running Instances"
          value={runningInstances.length}
          tone={runningInstances.length > 0 ? "live" : "muted"}
          caption={`${statuses.filter((s) => s.state.toLowerCase() === "waiting").length} waiting, ${statuses.filter((s) => s.state.toLowerCase() === "running").length} executing`}
        />
        <Metric
          label="Stale Devices"
          value={staleDevices.length}
          tone={staleDevices.length > 0 ? "warn" : "ok"}
          caption={staleDevices.length > 0 ? "No sync in 5+ minutes" : "All devices healthy"}
        />
      </MetricRow>

      {/* Pending Approvals */}
      <Section
        eyebrow="Approvals"
        title={approvals.length > 0 ? `${approvals.length} pending` : "No pending approvals"}
        description="Workflow steps blocked on human decision. Resolution sends a command to the device to continue execution."
        meta={
          approvals.length > 0 ? (
            <span>
              <span className="text-faint">PENDING</span>{" "}
              <span className="text-ink-dim">{approvals.length}</span>
            </span>
          ) : undefined
        }
      >
        {approvalsLoading && !approvalsData ? (
          <SkeletonTable rows={3} cols={7} />
        ) : (
          <Table>
            <THead>
              <TH>Device</TH>
              <TH>Instance</TH>
              <TH>Sequence</TH>
              <TH>Prompt</TH>
              <TH>Timeout</TH>
              <TH>Created</TH>
              <TH>Respond</TH>
            </THead>
            <tbody>
              {approvals.map((item) => {
                const choices = parseChoices(item.choices);
                const device = deviceMap.get(item.device_id);
                return (
                  <TR key={item.id}>
                    <TD>
                      <DeviceCell deviceId={item.device_id} device={device} />
                    </TD>
                    <TD>
                      <Id value={item.instance_id} />
                    </TD>
                    <TD className="text-[13px]">{item.sequence_name ?? "—"}</TD>
                    <TD>
                      <div className="max-w-[36ch]">
                        <span className="block truncate text-[13px]" title={item.prompt ?? undefined}>
                          {item.prompt ?? "—"}
                        </span>
                        {item.store_as && (
                          <span className="text-[10px] text-faint font-mono">
                            stores as: {item.store_as}
                          </span>
                        )}
                        {item.metadata && (
                          <details className="mt-0.5">
                            <summary className="text-[10px] text-faint cursor-pointer hover:text-muted">
                              metadata
                            </summary>
                            <pre className="text-[10px] text-muted font-mono mt-0.5 max-h-16 overflow-auto whitespace-pre-wrap">
                              {formatJson(item.metadata)}
                            </pre>
                          </details>
                        )}
                      </div>
                    </TD>
                    <TD>
                      <ApprovalTimeout
                        createdAt={item.created_at}
                        timeoutSecs={item.timeout_secs}
                      />
                    </TD>
                    <TD>
                      <Relative at={item.created_at} />
                    </TD>
                    <TD>
                      <div className="flex flex-wrap gap-2">
                        {choices.length > 0 ? (
                          choices.map((c) => (
                            <Button
                              key={c.value}
                              variant="ghost"
                              size="sm"
                              disabled={busyId === item.id}
                              onClick={() => resolve(item, c.value)}
                            >
                              {c.label}
                            </Button>
                          ))
                        ) : (
                          <>
                            <Button
                              variant="primary"
                              size="sm"
                              disabled={busyId === item.id}
                              onClick={() => resolve(item, "approved")}
                            >
                              Approve
                            </Button>
                            <Button
                              variant="danger"
                              size="sm"
                              disabled={busyId === item.id}
                              onClick={() => resolve(item, "rejected")}
                            >
                              Reject
                            </Button>
                          </>
                        )}
                      </div>
                    </TD>
                  </TR>
                );
              })}
              {approvals.length === 0 && (
                <Empty colSpan={99}>No pending approvals</Empty>
              )}
            </tbody>
          </Table>
        )}
      </Section>

      {/* Start Workflow */}
      <StartWorkflowPanel
        deviceIds={deviceIds}
        deviceMap={deviceMap}
        onStarted={refresh}
        onError={(msg) => setError(msg)}
      />

      {/* Instance Status */}
      <Section
        eyebrow="Instance Status"
        title={`${statuses.length} instances across ${deviceIds.length} device${deviceIds.length !== 1 ? "s" : ""}`}
        description="Latest status reported by mobile devices. Click a row to expand execution steps. Use Update to push a new sequence version with a rollout policy."
        meta={
          <div className="flex items-center gap-4">
            {statusTotal > statuses.length && (
              <span className="text-warn">
                Showing {statuses.length} of {statusTotal}
              </span>
            )}
            <span>
              <span className="text-faint">DEVICES</span>{" "}
              <span className="text-ink-dim">{deviceIds.length}</span>
            </span>
          </div>
        }
      >
        {/* Filters */}
        <div className="flex gap-3 mb-4">
          <select
            value={filterDevice}
            onChange={(e) => setFilterDevice(e.target.value)}
            className="rounded bg-surface border border-hairline px-2 py-1 text-[12px] font-mono text-fg focus:outline-none focus:ring-1 focus:ring-signal/40"
          >
            <option value="">All devices</option>
            {deviceIds.map((id) => {
              const d = deviceMap.get(id);
              return (
                <option key={id} value={id}>
                  {shortDevice(id)}{d ? ` (${d.platform})` : ""}
                </option>
              );
            })}
          </select>
          <select
            value={filterState}
            onChange={(e) => setFilterState(e.target.value)}
            className="rounded bg-surface border border-hairline px-2 py-1 text-[12px] font-mono text-fg focus:outline-none focus:ring-1 focus:ring-signal/40"
          >
            <option value="">All states</option>
            <option value="scheduled">Scheduled</option>
            <option value="running">Running</option>
            <option value="waiting">Waiting</option>
            <option value="completed">Completed</option>
            <option value="failed">Failed</option>
            <option value="cancelled">Cancelled</option>
          </select>
          {(filterDevice || filterState) && (
            <Button
              variant="ghost"
              size="sm"
              onClick={() => { setFilterDevice(""); setFilterState(""); }}
            >
              Clear filters
            </Button>
          )}
        </div>

        {statusLoading && !statusData ? (
          <SkeletonTable rows={5} cols={7} />
        ) : (
          <Table>
            <THead>
              <TH></TH>
              <TH>Device</TH>
              <TH>Instance</TH>
              <TH>Sequence</TH>
              <TH>State</TH>
              <TH>Current Step</TH>
              <TH>Progress</TH>
              <TH>Updated</TH>
              <TH>Actions</TH>
            </THead>
            <tbody>
              {filteredStatuses.map((s) => {
                const key = `${s.device_id}:${s.instance_id}`;
                const tone = INSTANCE_TONE[s.state.toLowerCase()] ?? "dim";
                const steps = parseSteps(s.steps);
                const expanded = expandedInstances.has(key);
                const device = deviceMap.get(s.device_id);
                return (
                  <>
                    <TR
                      key={key}
                      className="cursor-pointer"
                      onClick={() => steps.length > 0 && toggleExpand(key)}
                    >
                      <TD className="w-6 text-center text-muted">
                        {steps.length > 0 ? (expanded ? "▾" : "▸") : ""}
                      </TD>
                      <TD>
                        <DeviceCell deviceId={s.device_id} device={device} />
                      </TD>
                      <TD>
                        <Id value={s.instance_id} />
                      </TD>
                      <TD className="text-[13px]">{s.sequence_name ?? "—"}</TD>
                      <TD>
                        <Badge tone={tone} dot>
                          {s.state}
                        </Badge>
                      </TD>
                      <TD className="font-mono text-[12px] text-muted">
                        {s.current_step ?? "—"}
                      </TD>
                      <TD>
                        <StepProgressBar steps={steps} />
                      </TD>
                      <TD>
                        <Relative at={s.updated_at} />
                      </TD>
                      <TD>
                        <InstanceActions
                          status={s}
                          onAction={refresh}
                          onError={(msg) => setError(msg)}
                        />
                      </TD>
                    </TR>
                    {expanded && steps.length > 0 && (
                      <tr key={`${key}:steps`}>
                        <td colSpan={99} className="bg-surface-raised px-6 py-3">
                          {s.context_summary && (
                            <details className="mb-3">
                              <summary className="text-[11px] text-faint cursor-pointer hover:text-muted font-mono uppercase tracking-wider">
                                Context
                              </summary>
                              <pre className="mt-1.5 text-[11px] text-muted font-mono bg-sunken rounded px-3 py-2 max-h-32 overflow-auto whitespace-pre-wrap">
                                {formatJson(s.context_summary)}
                              </pre>
                            </details>
                          )}
                          <StepTimeline steps={steps} currentStep={s.current_step} />
                        </td>
                      </tr>
                    )}
                  </>
                );
              })}
              {filteredStatuses.length === 0 && statuses.length > 0 && (
                <Empty colSpan={99}>No instances match the current filters.</Empty>
              )}
              {statuses.length === 0 && (
                <Empty colSpan={99}>
                  No mobile instances reported yet.
                </Empty>
              )}
            </tbody>
          </Table>
        )}
      </Section>
    </div>
  );
}

/* ── Device Cell with health indicator ────────────────────────────── */

function DeviceCell({ deviceId, device }: { deviceId: string; device?: MobileDeviceInfo }) {
  const syncAge = device?.last_sync_at
    ? Date.now() - new Date(device.last_sync_at).getTime()
    : null;

  const healthTone: "ok" | "hold" | "warn" | "dim" =
    syncAge === null ? "dim"
    : syncAge < 60_000 ? "ok"
    : syncAge < 5 * 60_000 ? "hold"
    : "warn";

  const tooltip = device
    ? `${device.device_id}\n${device.platform} · v${device.app_version ?? "?"}\nLast sync: ${device.last_sync_at ? new Date(device.last_sync_at).toLocaleString() : "never"}\nStatus: ${device.active ? "active" : "inactive"}`
    : deviceId;

  return (
    <span className="inline-flex items-center gap-1.5" title={tooltip}>
      <StatusDot tone={healthTone} live={healthTone === "ok"} />
      <span className="font-mono text-[12px] text-muted">{shortDevice(deviceId)}</span>
    </span>
  );
}

/* ── Approval Timeout Countdown ───────────────────────────────────── */

function ApprovalTimeout({
  createdAt,
  timeoutSecs,
}: {
  createdAt: string;
  timeoutSecs: number | null;
}) {
  const [now, setNow] = useState(Date.now());

  useEffect(() => {
    if (!timeoutSecs) return;
    const id = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(id);
  }, [timeoutSecs]);

  if (!timeoutSecs) {
    return <span className="text-[12px] text-faint">—</span>;
  }

  const elapsed = (now - new Date(createdAt).getTime()) / 1000;
  const remaining = Math.max(0, timeoutSecs - elapsed);
  const pct = remaining / timeoutSecs;
  const expired = remaining <= 0;

  if (expired) {
    return <Badge tone="warn">Expired</Badge>;
  }

  const tone: "warn" | "hold" | "dim" = pct < 0.2 ? "warn" : pct < 0.5 ? "hold" : "dim";

  return (
    <Badge tone={tone}>
      {remaining < 60
        ? `${Math.ceil(remaining)}s left`
        : `${Math.ceil(remaining / 60)}m left`}
    </Badge>
  );
}

/* ── Step Progress Bar ────────────────────────────────────────────── */

const STEP_STATE_COLORS: Record<string, { bg: string; text: string; dot: string }> = {
  completed: { bg: "bg-emerald-500/10", text: "text-emerald-400", dot: "bg-emerald-400" },
  running: { bg: "bg-blue-500/10", text: "text-blue-400", dot: "bg-blue-400" },
  executing: { bg: "bg-blue-500/10", text: "text-blue-400", dot: "bg-blue-400" },
  waiting: { bg: "bg-amber-500/10", text: "text-amber-400", dot: "bg-amber-400" },
  failed: { bg: "bg-red-500/10", text: "text-red-400", dot: "bg-red-400" },
  cancelled: { bg: "bg-zinc-500/10", text: "text-zinc-400", dot: "bg-zinc-500" },
  skipped: { bg: "bg-zinc-500/10", text: "text-zinc-500", dot: "bg-zinc-600" },
  pending: { bg: "bg-zinc-500/5", text: "text-zinc-600", dot: "bg-zinc-700" },
};

function StepProgressBar({ steps }: { steps: MobileStepEntry[] }) {
  if (steps.length === 0) return <span className="text-faint text-[12px]">—</span>;
  const completed = steps.filter((s) => s.state === "completed").length;
  const running = steps.filter((s) => s.state === "running" || s.state === "executing" || s.state === "waiting").length;
  const total = steps.length;
  const pctDone = (completed / total) * 100;
  const pctActive = (running / total) * 100;

  return (
    <div className="flex items-center gap-2">
      <div className="h-1.5 w-20 rounded-full bg-zinc-800 overflow-hidden flex">
        {pctDone > 0 && (
          <div className="h-full bg-emerald-500 transition-all" style={{ width: `${pctDone}%` }} />
        )}
        {pctActive > 0 && (
          <div className="h-full bg-blue-500 animate-pulse transition-all" style={{ width: `${pctActive}%` }} />
        )}
      </div>
      <span className="text-[11px] text-muted tabular-nums">
        {completed}/{total}
      </span>
    </div>
  );
}

/* ── Step Timeline ────────────────────────────────────────────────── */

function StepTimeline({ steps, currentStep }: { steps: MobileStepEntry[]; currentStep: string | null }) {
  return (
    <div className="space-y-0">
      {steps.map((step, i) => {
        const colors = STEP_STATE_COLORS[step.state] ?? STEP_STATE_COLORS.pending;
        const isCurrent = step.block_id === currentStep;
        const isLast = i === steps.length - 1;
        return (
          <div key={`${step.block_id}-${i}`} className="flex items-stretch gap-3">
            <div className="flex flex-col items-center w-4 shrink-0">
              <div className={`w-2.5 h-2.5 rounded-full mt-1.5 shrink-0 ${colors.dot} ${step.state === "running" ? "animate-pulse ring-2 ring-blue-500/30" : ""} ${isCurrent ? "ring-2 ring-white/20" : ""}`} />
              {!isLast && <div className="w-px flex-1 bg-zinc-700/50 my-0.5" />}
            </div>
            <div className={`flex-1 pb-2 pt-0.5 ${step.state === "pending" ? "opacity-50" : ""}`}>
              <div className="flex items-center gap-2">
                <span className={`font-mono text-[12px] font-medium ${colors.text}`}>
                  {step.block_id}
                </span>
                <span className={`text-[10px] px-1.5 py-0.5 rounded ${colors.bg} ${colors.text}`}>
                  {step.state}
                </span>
                {step.block_type !== "step" && (
                  <span className="text-[10px] px-1.5 py-0.5 rounded bg-zinc-800 text-zinc-500">
                    {step.block_type}
                  </span>
                )}
              </div>
              {step.handler && (
                <span className="text-[11px] text-zinc-500 font-mono">{step.handler}</span>
              )}
              {(step.started_at || step.completed_at) && (
                <div className="text-[10px] text-zinc-600 mt-0.5 flex gap-3">
                  {step.started_at && <span>started <Relative at={step.started_at} /></span>}
                  {step.completed_at && <span>finished <Relative at={step.completed_at} /></span>}
                </div>
              )}
            </div>
          </div>
        );
      })}
    </div>
  );
}

/* ── Update Policies ──────────────────────────────────────────────── */

const UPDATE_POLICIES = [
  { value: "restart", label: "Restart", description: "Cancel the running instance and start from the first step with a fresh context", destructive: false },
  { value: "graceful", label: "Graceful", description: "Let the current step finish executing, then apply the new sequence version on the next step", destructive: false },
  { value: "fail", label: "Fail", description: "Mark the instance as failed immediately", destructive: true },
  { value: "cancel", label: "Cancel", description: "Cancel the instance immediately — no further steps execute", destructive: true },
  { value: "skip_executed", label: "Skip Executed", description: "Restart with the new sequence version but skip steps that already completed", destructive: false },
] as const;

/* ── Instance Actions ─────────────────────────────────────────────── */

function InstanceActions({
  status,
  onAction,
  onError,
}: {
  status: MobileInstanceStatus;
  onAction: () => void;
  onError: (msg: string) => void;
}) {
  const [open, setOpen] = useState(false);
  const [busy, setBusy] = useState(false);
  const [confirmPolicy, setConfirmPolicy] = useState<string | null>(null);
  const isTerminal = ["completed", "failed", "cancelled"].includes(status.state.toLowerCase());
  const isActive = ["scheduled", "running", "waiting"].includes(status.state.toLowerCase());

  async function handleAction(policy: string) {
    setBusy(true);
    try {
      await sendMobileCommand(status.device_id, "update_sequence", {
        instance_id: status.instance_id,
        sequence_name: status.sequence_name,
        policy,
      });
      onAction();
      setOpen(false);
      setConfirmPolicy(null);
    } catch (e) {
      onError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  async function handleCancel() {
    setBusy(true);
    try {
      await sendMobileCommand(status.device_id, "cancel_instance", {
        instance_id: status.instance_id,
      });
      onAction();
    } catch (e) {
      onError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  if (!open) {
    return (
      <div className="flex gap-1" onClick={(e) => e.stopPropagation()}>
        {isActive && (
          <Button
            variant="danger"
            size="sm"
            disabled={busy}
            onClick={() => setConfirmPolicy(confirmPolicy === "cancel_instance" ? null : "cancel_instance")}
          >
            {confirmPolicy === "cancel_instance" ? "Confirm?" : "Cancel"}
          </Button>
        )}
        {confirmPolicy === "cancel_instance" && (
          <>
            <Button variant="danger" size="sm" disabled={busy} onClick={handleCancel}>
              Yes
            </Button>
            <Button variant="ghost" size="sm" onClick={() => setConfirmPolicy(null)}>
              No
            </Button>
          </>
        )}
        {confirmPolicy !== "cancel_instance" && (
          <Button
            variant="ghost"
            size="sm"
            onClick={() => setOpen(true)}
          >
            {isTerminal ? "Restart" : "Update"}
          </Button>
        )}
      </div>
    );
  }

  return (
    <div className="flex flex-wrap gap-1" onClick={(e) => e.stopPropagation()}>
      {confirmPolicy && (
        <div className="flex items-center gap-1 mr-1">
          <span className="text-[10px] text-warn">Confirm {confirmPolicy}?</span>
          <Button variant="danger" size="sm" disabled={busy} onClick={() => handleAction(confirmPolicy)}>
            Yes
          </Button>
          <Button variant="ghost" size="sm" onClick={() => setConfirmPolicy(null)}>
            No
          </Button>
        </div>
      )}
      {!confirmPolicy && UPDATE_POLICIES.filter((p) => {
        if (isTerminal) return p.value === "restart" || p.value === "skip_executed";
        return true;
      }).map((p) => (
        <Button
          key={p.value}
          variant={p.destructive ? "danger" : "ghost"}
          size="sm"
          disabled={busy}
          title={p.description}
          onClick={() => {
            if (p.destructive) {
              setConfirmPolicy(p.value);
            } else {
              handleAction(p.value);
            }
          }}
        >
          {p.label}
        </Button>
      ))}
      <Button variant="ghost" size="sm" onClick={() => { setOpen(false); setConfirmPolicy(null); }}>
        ×
      </Button>
    </div>
  );
}

/* ── Start Workflow Panel ─────────────────────────────────────────── */

function StartWorkflowPanel({
  deviceIds,
  deviceMap,
  onStarted,
  onError,
}: {
  deviceIds: string[];
  deviceMap: Map<string, MobileDeviceInfo>;
  onStarted: () => void;
  onError: (msg: string) => void;
}) {
  const [open, setOpen] = useState(false);
  const [deviceId, setDeviceId] = useState("");
  const [sequenceName, setSequenceName] = useState("");
  const [inputJson, setInputJson] = useState('{\n  "user_id": "USR-42",\n  "feature_flags": ["beta_checkout"]\n}');
  const [busy, setBusy] = useState(false);

  async function handleStart() {
    if (!deviceId || !sequenceName) return;
    setBusy(true);
    try {
      JSON.parse(inputJson);
    } catch {
      onError("Invalid JSON in initial state input");
      setBusy(false);
      return;
    }
    try {
      await sendMobileCommand(deviceId, "start_workflow", {
        sequence_name: sequenceName,
        input: JSON.parse(inputJson),
      });
      onStarted();
      setSequenceName("");
      setInputJson('{\n  "user_id": "USR-42",\n  "feature_flags": ["beta_checkout"]\n}');
      setOpen(false);
    } catch (e) {
      onError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  if (!open) {
    return (
      <Section
        eyebrow="Commands"
        title="Start Workflow"
        description="Send a start_workflow command to a connected device. Pass initial state as JSON — the device receives it as the workflow's starting context."
      >
        <Button variant="primary" size="sm" onClick={() => setOpen(true)}>
          New Workflow
        </Button>
      </Section>
    );
  }

  return (
    <Section
      eyebrow="Commands"
      title="Start Workflow"
      description="Send a start_workflow command to a connected device. Pass initial state as JSON — the device receives it as the workflow's starting context."
    >
      <div className="grid grid-cols-1 gap-4 max-w-xl">
        <label className="block">
          <span className="text-[12px] uppercase tracking-wider text-faint mb-1 block">Device</span>
          <select
            value={deviceId}
            onChange={(e) => setDeviceId(e.target.value)}
            className="w-full rounded bg-surface-raised border border-white/10 px-3 py-2 text-sm text-ink font-mono focus:outline-none focus:ring-1 focus:ring-accent"
          >
            <option value="">Select device...</option>
            {deviceIds.map((id) => {
              const d = deviceMap.get(id);
              return (
                <option key={id} value={id}>
                  {id}{d ? ` (${d.platform} · v${d.app_version ?? "?"})` : ""}
                </option>
              );
            })}
          </select>
        </label>
        <label className="block">
          <span className="text-[12px] uppercase tracking-wider text-faint mb-1 block">Sequence Name</span>
          <input
            type="text"
            value={sequenceName}
            onChange={(e) => setSequenceName(e.target.value)}
            placeholder="e.g. onboarding-flow"
            list="sequence-suggestions"
            className="w-full rounded bg-surface-raised border border-white/10 px-3 py-2 text-sm text-ink font-mono focus:outline-none focus:ring-1 focus:ring-accent"
          />
          <datalist id="sequence-suggestions">
            {[...new Set(
              deviceIds
                .map((id) => deviceMap.get(id))
                .filter(Boolean)
                .flatMap(() => [])
            )].map((name) => (
              <option key={name} value={name} />
            ))}
            <option value="onboarding-flow" />
            <option value="payment-verification" />
            <option value="feature-access" />
          </datalist>
        </label>
        <label className="block">
          <span className="text-[12px] uppercase tracking-wider text-faint mb-1 block">Initial State (JSON)</span>
          <textarea
            value={inputJson}
            onChange={(e) => setInputJson(e.target.value)}
            rows={4}
            className="w-full rounded bg-surface-raised border border-white/10 px-3 py-2 text-sm text-ink font-mono focus:outline-none focus:ring-1 focus:ring-accent resize-y"
          />
        </label>
        <div className="flex gap-2">
          <Button
            variant="primary"
            size="sm"
            disabled={busy || !deviceId || !sequenceName}
            onClick={handleStart}
          >
            {busy ? "Sending..." : "Start Workflow"}
          </Button>
          <Button variant="ghost" size="sm" onClick={() => setOpen(false)}>
            Cancel
          </Button>
        </div>
      </div>
    </Section>
  );
}

/* ── Helpers ──────────────────────────────────────────────────────── */

function shortDevice(id: string): string {
  if (id.length <= 12) return id;
  return id.slice(0, 8) + "…";
}

function formatJson(raw: string): string {
  try {
    return JSON.stringify(JSON.parse(raw), null, 2);
  } catch {
    return raw;
  }
}

function parseSteps(raw: string | null): MobileStepEntry[] {
  if (!raw) return [];
  try {
    const parsed = JSON.parse(raw);
    if (Array.isArray(parsed)) {
      return parsed.map((s: Record<string, unknown>) => ({
        block_id: (s.block_id ?? s.name ?? "unknown") as string,
        block_type: (s.block_type ?? "step") as string,
        state: (s.state ?? "pending") as string,
        handler: (s.handler ?? null) as string | null,
        started_at: (s.started_at ?? null) as string | null,
        completed_at: (s.completed_at ?? null) as string | null,
      }));
    }
  } catch {
    // ignore
  }
  return [];
}

function parseChoices(raw: string | null): { label: string; value: string }[] {
  if (!raw) return [];
  try {
    const parsed = JSON.parse(raw);
    if (Array.isArray(parsed)) {
      return parsed.map((c) =>
        typeof c === "string"
          ? { label: c, value: c }
          : { label: c.label ?? c.value ?? String(c), value: c.value ?? c.label ?? String(c) },
      );
    }
  } catch {
    // ignore
  }
  return [];
}
