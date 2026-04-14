import { NavLink, Outlet } from "react-router-dom";
import { useCallback, useEffect, useState } from "react";
import {
  checkHealth,
  getWorkerTaskStats,
  listApprovals,
  type ApprovalsResponse,
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
} from "./ui/Icons";

const CONN_TONE = {
  online: "ok",
  offline: "warn",
  connecting: "hold",
} as const;

const NAV = [
  { to: "/", label: "Overview", icon: IconHome, end: true },
  { to: "/instances", label: "Executions", icon: IconActivity },
  { to: "/approvals", label: "Approvals", icon: IconCheckCircle },
  { to: "/sequences", label: "Sequences", icon: IconLayers },
  { to: "/tasks", label: "Tasks", icon: IconList },
  { to: "/cron", label: "Cron", icon: IconClock },
  { to: "/triggers", label: "Triggers", icon: IconZap },
  { to: "/operations", label: "Operations", icon: IconShield },
  { to: "/settings", label: "Settings", icon: IconSliders },
];

type ConnState = "connecting" | "online" | "offline";

export default function Layout() {
  const [conn, setConn] = useState<ConnState>("connecting");
  const [workers, setWorkers] = useState<number>(0);
  const [lastPoll, setLastPoll] = useState<string>("—");

  const approvalsFetcher = useCallback(
    () => listApprovals({ limit: "1" }),
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
    <div className="min-h-screen grid grid-cols-[220px_1fr] grid-rows-[1fr_28px] bg-bg text-ink">
      {/* ── Sidebar ─────────────────────────────────────────── */}
      <aside className="row-span-2 border-r border-rule flex flex-col">
        <div className="px-5 py-5 border-b border-rule flex items-baseline gap-2">
          <Wordmark />
        </div>

        <div className="px-5 pt-5 pb-3">
          <span className="eyebrow">Navigation</span>
        </div>

        <nav className="flex-1 px-3 space-y-px">
          {NAV.map(({ to, label, icon: Icon, end }) => (
            <NavLink
              key={to}
              to={to}
              end={end}
              className={({ isActive }) =>
                `relative flex items-center gap-3 pl-5 pr-3 h-9 text-[13px] transition-colors ${
                  isActive
                    ? "text-ink before:absolute before:left-0 before:top-2 before:bottom-2 before:w-[2px] before:bg-signal"
                    : "text-muted hover:text-ink"
                }`
              }
            >
              <Icon size={14} />
              <span>{label}</span>
              {to === "/approvals" && approvalCount > 0 && (
                <Badge tone="hold" className="ml-auto">
                  {approvalCount}
                </Badge>
              )}
            </NavLink>
          ))}
        </nav>

        <div className="p-5 border-t border-rule space-y-2">
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
        <div className="max-w-[1280px] mx-auto px-12 py-10 fade-in">
          <Outlet />
        </div>
      </main>

      {/* ── Statusline ──────────────────────────────────────── */}
      <footer className="col-start-2 border-t border-rule px-12 flex items-center gap-6 text-[10px] font-mono text-muted tracking-[0.14em] uppercase">
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
