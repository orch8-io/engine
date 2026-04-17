import { NavLink, Outlet } from "react-router-dom";
import { useEffect, useState } from "react";
import { checkHealth, getWorkerTaskStats } from "../api";
import { Wordmark } from "./ui/Brand";
import { StatusDot } from "./ui/StatusDot";
import {
  IconHome,
  IconActivity,
  IconLayers,
  IconList,
  IconSliders,
} from "./ui/Icons";

const CONN_TONE = {
  online: "ok",
  offline: "warn",
  connecting: "hold",
} as const;

const NAV = [
  { to: "/", label: "Overview", icon: IconHome, end: true },
  { to: "/instances", label: "Instances", icon: IconActivity },
  { to: "/sequences", label: "Sequences", icon: IconLayers },
  { to: "/tasks", label: "Tasks", icon: IconList },
  { to: "/settings", label: "Settings", icon: IconSliders },
];

type ConnState = "connecting" | "online" | "offline";

export default function Layout() {
  const [conn, setConn] = useState<ConnState>("connecting");
  const [workers, setWorkers] = useState<number>(0);
  const [lastPoll, setLastPoll] = useState<string>("—");

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
    const id = setInterval(tick, 8000);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, []);

  return (
    <div className="min-h-screen grid grid-cols-[216px_1fr] grid-rows-[1fr_32px] bg-bg text-fg">
      {/* ── Sidebar ─────────────────────────────────────────── */}
      <aside className="row-span-2 border-r border-hairline bg-surface flex flex-col">
        <div className="px-4 py-4 border-b border-hairline flex items-baseline gap-2">
          <Wordmark />
          <span className="eyebrow text-faint">Console</span>
        </div>

        <nav className="flex-1 p-2 space-y-0.5">
          {NAV.map(({ to, label, icon: Icon, end }) => (
            <NavLink
              key={to}
              to={to}
              end={end}
              className={({ isActive }) =>
                `relative flex items-center gap-2.5 pl-3 pr-2.5 h-8 rounded-sm text-[13px] transition-colors ${
                  isActive
                    ? "text-signal bg-signal/[0.07] before:absolute before:left-0 before:top-1.5 before:bottom-1.5 before:w-[2px] before:bg-signal before:rounded-full"
                    : "text-muted hover:text-fg hover:bg-raised/40"
                }`
              }
            >
              <Icon size={15} />
              <span>{label}</span>
            </NavLink>
          ))}
        </nav>

        <div className="p-3 border-t border-hairline">
          <div className="eyebrow mb-1.5">Engine</div>
          <div className="flex items-center gap-2">
            <StatusDot tone={CONN_TONE[conn]} live={conn === "online"} />
            <span className="text-[12px] text-fg-dim capitalize">{conn}</span>
          </div>
        </div>
      </aside>

      {/* ── Main ────────────────────────────────────────────── */}
      <main className="overflow-y-auto">
        <div className="max-w-[1320px] mx-auto px-10 py-9 fade-in">
          <Outlet />
        </div>
      </main>

      {/* ── Statusline ──────────────────────────────────────── */}
      <footer className="col-start-2 border-t border-hairline bg-surface px-4 flex items-center gap-5 text-[11px] font-mono text-muted tracking-wider uppercase">
        <span className="flex items-center gap-1.5">
          <StatusDot tone={CONN_TONE[conn]} live={conn === "online"} />
          <span className="text-fg-dim">{conn}</span>
        </span>
        <span className="text-faint">·</span>
        <span>
          WRK <span className="text-fg-dim">{workers}</span>
        </span>
        <span className="ml-auto">
          POLL <span className="text-fg-dim">{lastPoll}</span>
        </span>
      </footer>
    </div>
  );
}
