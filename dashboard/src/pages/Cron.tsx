import { useCallback, useState } from "react";
import {
  listCronSchedules,
  createCronSchedule,
  updateCronSchedule,
  deleteCronSchedule,
  cronNextFires,
  type CronSchedule,
} from "../api";
import { usePolling } from "../hooks/usePolling";
import { usePageTitle } from "../hooks/usePageTitle";
import { PageHeader } from "../components/ui/PageHeader";
import { PageMeta } from "../components/ui/PageMeta";
import { Section } from "../components/ui/Section";
import { Glossary, type GlossaryItem } from "../components/ui/Glossary";
import { Table, THead, TH, TR, TD, Empty } from "../components/ui/Table";
import { Button } from "../components/ui/Button";
import { CopyAsMenu } from "../components/CopyAsMenu";
import { createCronRequest } from "../lib/requests";
import { Input, FieldLabel } from "../components/ui/Input";
import { Badge } from "../components/ui/Badge";
import { Relative } from "../components/ui/Relative";
import {
  IconPlus,
  IconTrash,
  IconPause,
  IconPlay,
  IconChevronDown,
  IconChevronRight,
} from "../components/ui/Icons";

const PAGE_GLOSSARY: GlossaryItem[] = [
  {
    term: "Cron schedule",
    definition:
      "A recurring trigger that creates a fresh execution of a sequence on a schedule. Think of it as a standing order: every time the expression fires, the engine spins up one new execution.",
  },
  {
    term: "Expression",
    definition: (
      <>
        Standard five-field cron:{" "}
        <code className="font-mono text-ink">min hour day-of-month month day-of-week</code>.
        Examples: <code className="font-mono text-ink">*/5 * * * *</code> (every 5 min),{" "}
        <code className="font-mono text-ink">0 9 * * 1-5</code> (9 AM Mon–Fri),{" "}
        <code className="font-mono text-ink">0 0 1 * *</code> (1st of each month at midnight).
      </>
    ),
  },
  {
    term: "Timezone",
    definition:
      "IANA timezone name (e.g. UTC, Europe/London, America/New_York). The cron expression is interpreted in this timezone, which matters for DST transitions.",
  },
  {
    term: "Sequence",
    definition:
      "The workflow definition to be executed each time the cron fires. Create sequences on the Sequences page first, then paste the UUID here.",
  },
  {
    term: "Next fire",
    definition:
      "The next absolute wall-clock time (in the schedule's timezone) that this cron will trigger. Computed server-side from the expression.",
  },
  {
    term: "enabled / disabled",
    definition:
      "Disabled schedules are frozen — the engine stops evaluating them. Disabling is reversible and does not delete any execution history.",
  },
  {
    term: "Overlap policy",
    definition: (
      <>
        What happens when a fire is due while a previous run from the same
        schedule is still active. <code className="font-mono text-ink">allow</code>{" "}
        starts a concurrent run; <code className="font-mono text-ink">skip</code>{" "}
        counts the fire and moves on; <code className="font-mono text-ink">buffer_one</code>{" "}
        defers one fire until the previous finishes;{" "}
        <code className="font-mono text-ink">cancel_previous</code> cancels the
        in-flight run first.
      </>
    ),
  },
  {
    term: "Preview",
    definition:
      "Expand a row to see the next five fire times, computed server-side in the schedule's timezone with DST gaps and fall-back ambiguity resolved exactly as the engine will fire them.",
  },
];

export default function Cron() {
  usePageTitle("Cron");
  const fetcher = useCallback((signal?: AbortSignal) => listCronSchedules(undefined, signal), []);
  const { data, loading, updatedAt, refresh } =
    usePolling<CronSchedule[]>(fetcher);
  const [showForm, setShowForm] = useState(false);
  const [toast, setToast] = useState<string | null>(null);

  const flash = (msg: string) => {
    setToast(msg);
    setTimeout(() => setToast(null), 2500);
  };

  const toggle = async (c: CronSchedule) => {
    try {
      await updateCronSchedule(c.id, { enabled: !c.enabled });
      flash(`${c.enabled ? "Disabled" : "Enabled"} ${c.id.slice(0, 8)}`);
      refresh();
    } catch (e) {
      flash(`Failed: ${e instanceof Error ? e.message : String(e)}`);
    }
  };

  const remove = async (c: CronSchedule) => {
    if (
      !confirm(
        `Delete cron ${c.id.slice(0, 8)}…?\n\nThis removes the schedule permanently. Existing executions it has already spawned will keep running.`,
      )
    )
      return;
    try {
      await deleteCronSchedule(c.id);
      flash("Deleted");
      refresh();
    } catch (e) {
      flash(`Failed: ${e instanceof Error ? e.message : String(e)}`);
    }
  };

  const enabled = data?.filter((c) => c.enabled).length ?? 0;
  const total = data?.length ?? 0;

  return (
    <div className="space-y-12">
      <PageHeader
        eyebrow="Operator"
        title="Cron schedules"
        description="Standing orders that spawn a new execution of a sequence every time a cron expression fires. One schedule → one execution per tick."
        actions={
          <div className="flex items-center gap-2">
            <Button
              variant="primary"
              size="sm"
              onClick={() => setShowForm((v) => !v)}
            >
              <IconPlus size={13} /> {showForm ? "Close" : "New schedule"}
            </Button>
            <PageMeta updatedAt={updatedAt} onRefresh={refresh} />
          </div>
        }
      />

      <Glossary items={PAGE_GLOSSARY} />

      {toast && <div className="notice notice-ok">{toast}</div>}

      {showForm && (
        <CreateCronForm
          onCreated={() => {
            flash("Cron created");
            setShowForm(false);
            refresh();
          }}
          onError={(msg) => flash(msg)}
        />
      )}

      <Section
        eyebrow="Active schedules"
        title="Registered crons"
        description={
          <>
            The <strong className="text-ink">Next fire</strong> column shows
            exactly when each cron will next spawn an execution — verify it
            before relying on the schedule. Disabling a cron is the soft kill
            switch; deleting it is permanent.
          </>
        }
        meta={
          <>
            <span>
              <span className="text-faint">ENABLED</span>{" "}
              <span className="text-ok">{enabled}</span>
            </span>
            <span>
              <span className="text-faint">TOTAL</span>{" "}
              <span className="text-ink-dim">{total}</span>
            </span>
          </>
        }
      >
        {loading && !data && (
          <div className="text-muted text-[13px] font-mono">Loading…</div>
        )}

        {data && (
          <Table>
            <THead>
              <TH>Status</TH>
              <TH>Tenant / Namespace</TH>
              <TH>Sequence</TH>
              <TH>Expression</TH>
              <TH>Overlap</TH>
              <TH>Timezone</TH>
              <TH>Next fire</TH>
              <TH>Last fired</TH>
              <TH className="text-right">Actions</TH>
            </THead>
            <tbody>
              {data.map((c) => (
                <ScheduleRow
                  key={c.id}
                  c={c}
                  onToggle={() => toggle(c)}
                  onRemove={() => remove(c)}
                />
              ))}
              {data.length === 0 && (
                <Empty colSpan={99}>
                  No cron schedules yet. Click “New schedule” to create
                  one — you'll need a sequence UUID and a cron expression.
                </Empty>
              )}
            </tbody>
          </Table>
        )}
      </Section>
    </div>
  );
}

function ScheduleRow({
  c,
  onToggle,
  onRemove,
}: {
  c: CronSchedule;
  onToggle: () => void;
  onRemove: () => void;
}) {
  const [open, setOpen] = useState(false);
  const [fires, setFires] = useState<string[] | null>(null);
  const [previewError, setPreviewError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  const toggleOpen = async () => {
    const next = !open;
    setOpen(next);
    if (next && fires === null && !loading) {
      setLoading(true);
      try {
        const res = await cronNextFires(c.id, 5);
        setFires(res.fires);
      } catch (e) {
        setPreviewError(e instanceof Error ? e.message : String(e));
      } finally {
        setLoading(false);
      }
    }
  };

  return (
    <>
      <TR>
        <TD>
          <button
            onClick={toggleOpen}
            className="inline-flex items-center gap-1.5 text-left"
            title="Preview the next five fires"
          >
            <span className="text-muted">
              {open ? <IconChevronDown size={12} /> : <IconChevronRight size={12} />}
            </span>
            <Badge tone={c.enabled ? "ok" : "dim"} dot>
              {c.enabled ? "enabled" : "disabled"}
            </Badge>
          </button>
        </TD>
        <TD className="font-mono text-[12px]">
          {c.tenant_id}
          <span className="text-faint"> / </span>
          {c.namespace}
        </TD>
        <TD className="font-mono text-[12px] text-muted" title={c.sequence_id}>
          {c.sequence_id.slice(0, 8)}…
        </TD>
        <TD
          className="font-mono text-[12px]"
          title="min hour day-of-month month day-of-week"
        >
          {c.cron_expr}
        </TD>
        <TD>
          <span
            className="bg-sunken border border-rule text-muted text-[10px] font-mono uppercase tracking-wider px-1.5 py-0.5"
            title="Overlap policy — behavior when a fire is due while a previous run is still active"
          >
            {c.overlap_policy}
          </span>
          {c.skipped_fires > 0 && (
            <span
              className="ml-1.5 text-[11px] text-warn font-mono"
              title={
                c.last_skipped_at
                  ? `Last skipped ${new Date(c.last_skipped_at).toLocaleString()}`
                  : "Fires skipped under the skip policy"
              }
            >
              {c.skipped_fires} skipped
            </span>
          )}
        </TD>
        <TD className="font-mono text-[12px] text-muted">{c.timezone}</TD>
        <TD>
          {c.next_fire_at ? (
            <span className="tabular text-[12px]" title={`In ${c.timezone}`}>
              {new Date(c.next_fire_at).toLocaleString()}
            </span>
          ) : (
            <span
              className="text-faint"
              title="No next fire — cron is disabled or expression has no future matches"
            >
              —
            </span>
          )}
        </TD>
        <TD>
          {c.last_triggered_at ? (
            <Relative at={c.last_triggered_at} />
          ) : (
            <span className="text-faint" title="This cron has not fired yet">
              never
            </span>
          )}
        </TD>
        <TD className="text-right">
          <div className="inline-flex gap-1">
            <Button
              variant="ghost"
              size="sm"
              onClick={onToggle}
              title={
                c.enabled
                  ? "Pause this cron — reversible, no data loss"
                  : "Resume this cron"
              }
            >
              {c.enabled ? <IconPause size={13} /> : <IconPlay size={13} />}
            </Button>
            <Button
              variant="ghost"
              size="sm"
              onClick={onRemove}
              title="Delete this cron permanently"
            >
              <IconTrash size={13} />
            </Button>
          </div>
        </TD>
      </TR>
      {open && (
        <TR>
          <TD colSpan={99} className="bg-sunken/40">
            <div className="px-6 py-3">
              <div className="field-label mb-2">
                Next 5 fires · <span className="font-mono">{c.timezone}</span>
              </div>
              {loading && (
                <div className="text-muted text-[12px] font-mono">Computing…</div>
              )}
              {previewError && (
                <div className="notice notice-warn">{previewError}</div>
              )}
              {fires && fires.length === 0 && (
                <div className="text-faint text-[12px] font-mono">
                  No upcoming fires — the expression has no future matches.
                </div>
              )}
              {fires && fires.length > 0 && (
                <ol className="space-y-1">
                  {fires.map((f, i) => (
                    <li
                      key={f}
                      className="flex items-baseline gap-3 text-[12px] font-mono tabular"
                    >
                      <span className="text-faint w-5 text-right">{i + 1}.</span>
                      <span className="text-ink">
                        {new Date(f).toLocaleString(undefined, {
                          timeZone: c.timezone,
                        })}
                      </span>
                      <span className="text-faint">
                        ({new Date(f).toISOString()})
                      </span>
                    </li>
                  ))}
                </ol>
              )}
            </div>
          </TD>
        </TR>
      )}
    </>
  );
}

function CreateCronForm({
  onCreated,
  onError,
}: {
  onCreated: () => void;
  onError: (msg: string) => void;
}) {
  const [tenantId, setTenantId] = useState("tenant-a");
  const [namespace, setNamespace] = useState("prod");
  const [sequenceId, setSequenceId] = useState("");
  const [cronExpr, setCronExpr] = useState("*/5 * * * *");
  const [timezone, setTimezone] = useState("UTC");
  const [busy, setBusy] = useState(false);

  const submit = async () => {
    if (!sequenceId) {
      onError("sequence_id is required");
      return;
    }
    setBusy(true);
    try {
      await createCronSchedule({
        tenant_id: tenantId,
        namespace,
        sequence_id: sequenceId,
        cron_expr: cronExpr,
        timezone,
      });
      onCreated();
    } catch (e) {
      onError(`Failed: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      setBusy(false);
    }
  };

  return (
    <Section
      eyebrow="New cron schedule"
      title="Register a recurring trigger"
      description={
        <>
          Fill every field — the engine validates the cron expression and
          timezone at create time, so typos fail loudly instead of silently.
          The sequence UUID must already exist; copy it from the{" "}
          <strong className="text-ink">Sequences</strong> page.
        </>
      }
    >
      <div>
        <div className="grid grid-cols-2 md:grid-cols-3 gap-6">
          <div>
            <FieldLabel>Tenant</FieldLabel>
            <Input
              value={tenantId}
              onChange={(e) => setTenantId(e.target.value)}
              className="w-full"
            />
            <p className="annotation mt-1">
              Isolation group. Executions never cross tenants.
            </p>
          </div>
          <div>
            <FieldLabel>Namespace</FieldLabel>
            <Input
              value={namespace}
              onChange={(e) => setNamespace(e.target.value)}
              className="w-full"
            />
            <p className="annotation mt-1">
              Environment label — e.g. prod, staging, dev.
            </p>
          </div>
          <div>
            <FieldLabel>Sequence ID (UUID)</FieldLabel>
            <Input
              value={sequenceId}
              onChange={(e) => setSequenceId(e.target.value)}
              placeholder="00000000-0000-0000-0000-000000000000"
              className="w-full"
            />
            <p className="annotation mt-1">
              The workflow to run on each fire.
            </p>
          </div>
          <div>
            <FieldLabel>Cron expression</FieldLabel>
            <Input
              value={cronExpr}
              onChange={(e) => setCronExpr(e.target.value)}
              className="w-full font-mono"
            />
            <p className="annotation mt-1">
              <code className="font-mono">min hour dom mon dow</code> — e.g.{" "}
              <code className="font-mono">*/5 * * * *</code> = every 5 min.
            </p>
          </div>
          <div>
            <FieldLabel>Timezone</FieldLabel>
            <Input
              value={timezone}
              onChange={(e) => setTimezone(e.target.value)}
              className="w-full"
            />
            <p className="annotation mt-1">
              IANA name (UTC, Europe/London). DST is handled.
            </p>
          </div>
        </div>
        <div className="mt-6 flex justify-end gap-2">
          <CopyAsMenu
            spec={() =>
              sequenceId
                ? createCronRequest({
                    tenant_id: tenantId,
                    namespace,
                    sequence_id: sequenceId,
                    cron_expr: cronExpr,
                    timezone,
                  })
                : null
            }
            disabledReason={sequenceId ? undefined : "Enter a sequence ID first"}
          />
          <Button variant="primary" size="sm" disabled={busy} onClick={submit}>
            Create schedule
          </Button>
        </div>
      </div>
    </Section>
  );
}
