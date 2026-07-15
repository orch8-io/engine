import { NavLink, Outlet } from "react-router-dom";
import { Suspense, useCallback, useEffect, useState } from "react";
import {
  checkHealth,
  getEngineInfo,
  getWorkerTaskStats,
  listApprovals,
  type ApprovalsResponse,
  type EngineInfo,
} from "../api";
import { usePolling } from "../hooks/usePolling";
import { Wordmark } from "./ui/Brand";
import { StatusDot } from "./ui/StatusDot";
import { Badge } from "./ui/Badge";
import {
  IconHome,
  IconActivity,
  IconLayers,
  IconList,
  IconSliders,
  IconClock,
  IconZap,
  IconShield,
  IconCheckCircle,
  IconSession,
  IconPlugin,
  IconKey,
  IconDatabase,
  IconPhone,
  IconCost,
  IconWorkers,
} from "./ui/Icons";

const CONN_TONE = {
  online: "ok",
  offline: "warn",
  connecting: "hold",
} as const;

const NAV_GROUPS: Array<{
  label: string;
  items: Array<{ to: string; label: string; icon: typeof IconHome; end?: boolean }>;
}> = [
  { label: "Monitor", items: [
    { to: "/", label: "Overview", icon: IconHome, end: true },
    { to: "/instances", label: "Executions", icon: IconActivity },
    { to: "/approvals", label: "Approvals", icon: IconCheckCircle },
    { to: "/operations", label: "Operations", icon: IconShield },
  ] },
  { label: "Build & ship", items: [
    { to: "/sequences", label: "Sequences", icon: IconLayers },
    { to: "/releases", label: "Releases", icon: IconShield },
    { to: "/cron", label: "Cron", icon: IconClock },
    { to: "/triggers", label: "Triggers", icon: IconZap },
    { to: "/sessions", label: "Sessions", icon: IconSession },
  ] },
  { label: "Runtime", items: [
    { to: "/tasks", label: "Tasks", icon: IconList },
    { to: "/workers", label: "Workers", icon: IconWorkers },
    { to: "/queues", label: "Queues", icon: IconList },
    { to: "/pools", label: "Pools", icon: IconDatabase },
    { to: "/mobile", label: "Mobile", icon: IconPhone },
    { to: "/usage", label: "Usage", icon: IconCost },
  ] },
  { label: "Administration", items: [
    { to: "/plugins", label: "Plugins", icon: IconPlugin },
    { to: "/credentials", label: "Credentials", icon: IconKey },
    { to: "/rollback", label: "Rollback policies", icon: IconShield },
    { to: "/api-keys", label: "API keys", icon: IconKey },
    { to: "/settings", label: "Settings", icon: IconSliders },
  ] },
];

type ConnState = "connecting" | "online" | "offline";

export default function Layout() {
  const [conn, setConn] = useState<ConnState>("connecting");
  const [workers, setWorkers] = useState<number>(0);
  const [lastPoll, setLastPoll] = useState<string>("—");
  const [info, setInfo] = useState<EngineInfo | null>(null);

  // Deployment info is static for the life of the process — fetch once.
  useEffect(() => {
    getEngineInfo()
      .then(setInfo)
      .catch(() => setInfo(null));
  }, []);

  const approvalsFetcher = useCallback(
    (signal?: AbortSignal) => listApprovals({ limit: "1" }, signal),
    [],
  );
  const { data: approvalsData } = usePolling<ApprovalsResponse>(
    approvalsFetcher,
    10000,
  );
  const approvalCount = approvalsData?.total ?? 0;

  useEffect(() => {
    let cancelled = false;

    const tick = async () => {
      try {
        await checkHealth();
        const stats = await getWorkerTaskStats().catch(() => null);
        if (cancelled) return;
        setConn("online");
        setWorkers(stats?.active_workers.length ?? 0);
        setLastPoll(new Date().toLocaleTimeString());
      } catch {
        if (!cancelled) setConn("offline");
      }
    };
    tick();
    const id = setInterval(tick, 2000);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, []);

  return (
    <div className="min-h-screen grid grid-cols-1 grid-rows-[auto_1fr] lg:grid-cols-[220px_1fr] lg:grid-rows-[1fr_28px] bg-bg text-ink">
      {/* ── Sidebar ─────────────────────────────────────────── */}
      <aside className="border-b lg:row-span-2 lg:border-b-0 lg:border-r border-rule flex flex-col max-h-[46vh] lg:max-h-none">
        <div className="px-5 py-5 border-b border-rule flex items-baseline gap-2">
          <Wordmark />
        </div>

        <nav aria-label="Primary" className="flex-1 px-3 py-3 space-y-5 overflow-y-auto">
          {NAV_GROUPS.map((group) => <div key={group.label}>
            <div className="eyebrow px-5 pb-2">{group.label}</div>
            <div className="space-y-px">
              {group.items.map(({ to, label, icon: Icon, end }) => (
                <NavLink
                  key={to}
                  to={to}
                  end={end}
                  className={({ isActive }) =>
                    `relative flex items-center gap-3 pl-5 pr-3 min-h-11 text-[13px] transition-colors lg:min-h-10 ${
                      isActive
                        ? "text-ink before:absolute before:left-0 before:top-2 before:bottom-2 before:w-[2px] before:bg-signal"
                        : "text-muted hover:text-ink hover:bg-surface"
                    }`
                  }
                >
                  <Icon size={16} />
                  <span>{label}</span>
                  {to === "/approvals" && approvalCount > 0 && <Badge tone="hold" className="ml-auto">{approvalCount}</Badge>}
                </NavLink>
              ))}
            </div>
          </div>)}
        </nav>

        <div className="hidden lg:block p-5 border-t border-rule space-y-2">
          <div className="eyebrow">Engine</div>
          <div className="flex items-center gap-2">
            <StatusDot tone={CONN_TONE[conn]} live={conn === "online"} />
            <span className="text-[12px] text-ink-dim capitalize">{conn}</span>
          </div>
          <div className="text-[10px] font-mono uppercase tracking-[0.14em] text-faint">
            {workers} workers
          </div>
        </div>
      </aside>

      {/* ── Main ────────────────────────────────────────────── */}
      <main className="overflow-y-auto">
        {info?.env_label && (
          <div
            className="sticky top-0 z-20 h-6 flex items-center justify-center text-[10px] font-mono uppercase tracking-[0.2em] text-white"
            style={{ background: info.env_color || "#b45309" }}
            title={`Environment: ${info.env_label}`}
          >
            {info.env_label}
          </div>
        )}
        <div className="max-w-[1280px] mx-auto px-4 py-8 md:px-8 lg:px-12 lg:py-10 fade-in">
          <Suspense fallback={<div role="status" className="notice">Loading surface…</div>}>
            <Outlet />
          </Suspense>
        </div>
      </main>

      {/* ── Statusline ──────────────────────────────────────── */}
      <footer className="hidden lg:flex col-start-2 border-t border-rule px-12 items-center gap-6 text-[10px] font-mono text-muted tracking-[0.14em] uppercase">
        <span className="flex items-center gap-2">
          <StatusDot tone={CONN_TONE[conn]} live={conn === "online"} />
          <span className="text-ink-dim">{conn}</span>
        </span>
        <span className="text-faint">/</span>
        <span>
          WRK <span className="text-ink-dim ml-1">{workers}</span>
        </span>
        <span className="ml-auto">
          POLL <span className="text-ink-dim ml-1">{lastPoll}</span>
        </span>
      </footer>
    </div>
  );
}
