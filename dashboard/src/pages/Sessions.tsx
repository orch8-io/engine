import { useCallback, useState } from "react";
import { usePolling } from "../hooks/usePolling";
import { usePageTitle } from "../hooks/usePageTitle";
import {
  listSessions,
  createSession,
  updateSessionState,
  type Session,
  type SessionState,
} from "../api";
import { PageHeader } from "../components/ui/PageHeader";
import { PageMeta } from "../components/ui/PageMeta";
import { Section } from "../components/ui/Section";
import { Glossary, type GlossaryItem } from "../components/ui/Glossary";
import { Table, THead, TH, TR, TD, Empty } from "../components/ui/Table";
import { Button } from "../components/ui/Button";
import { Input, FieldLabel } from "../components/ui/Input";
import { Badge } from "../components/ui/Badge";
import { Relative } from "../components/ui/Relative";
import { Id } from "../components/ui/Mono";
import { IconPlus, IconPause, IconPlay } from "../components/ui/Icons";
import { SkeletonTable } from "../components/ui/Skeleton";

const SESSION_TONE: Record<SessionState, "ok" | "hold" | "dim" | "warn"> = {
  active: "ok",
  paused: "hold",
  completed: "dim",
  expired: "warn",
};

const PAGE_GLOSSARY: GlossaryItem[] = [
  {
    term: "Session",
    definition:
      "A named container that groups related instances and holds shared data across them. Think of it as a conversation thread or user journey.",
  },
  {
    term: "Session key",
    definition:
      "Human-readable identifier for a session, e.g. user:123:onboarding. Instances reference sessions by this key.",
  },
  {
    term: "Session data",
    definition:
      "JSON state shared across every instance in the session. Blocks can read and mutate it; changes are visible to subsequent blocks.",
  },
];

export default function Sessions() {
  usePageTitle("Sessions");
  const fetcher = useCallback((signal?: AbortSignal) => listSessions(signal), []);
  const { data, loading, updatedAt, refresh } = usePolling<Session[]>(fetcher, 5000);
  const [showForm, setShowForm] = useState(false);
  const [toast, setToast] = useState<string | null>(null);

  const flash = (msg: string) => {
    setToast(msg);
    setTimeout(() => setToast(null), 2500);
  };

  const enabled = data?.filter((s) => s.state === "active").length ?? 0;

  return (
    <div className="space-y-12">
      <PageHeader
        eyebrow="Operator"
        title="Sessions"
        description="Named containers that group related instances and share state across them. Sessions are the backbone of multi-step user journeys."
        actions={
          <div className="flex items-center gap-2">
            <Button variant="primary" size="sm" onClick={() => setShowForm((v) => !v)}>
              <IconPlus size={13} /> {showForm ? "Close" : "New session"}
            </Button>
            <PageMeta updatedAt={updatedAt} onRefresh={refresh} />
          </div>
        }
      />

      <Glossary items={PAGE_GLOSSARY} />

      {toast && <div className="notice notice-ok">{toast}</div>}

      {showForm && (
        <CreateSessionForm
          onCreated={() => {
            flash("Session created");
            setShowForm(false);
            refresh();
          }}
          onError={(msg) => flash(msg)}
        />
      )}

      {loading && !data && <SkeletonTable rows={6} cols={6} />}

      {data && (
        <Section
          eyebrow="Active sessions"
          title="Session registry"
          description="Every session currently tracked by the engine."
          meta={
            <>
              <span>
                <span className="text-faint">ACTIVE</span>{" "}
                <span className="text-ok">{enabled}</span>
              </span>
              <span>
                <span className="text-faint">TOTAL</span>{" "}
                <span className="text-ink-dim">{data.length}</span>
              </span>
            </>
          }
        >
          <Table>
            <THead>
              <TH>State</TH>
              <TH>Key</TH>
              <TH>Tenant</TH>
              <TH>Created</TH>
              <TH className="text-right">Actions</TH>
            </THead>
            <tbody>
              {data.map((s) => (
                <TR key={s.id}>
                  <TD>
                    <Badge tone={SESSION_TONE[s.state]} dot live={s.state === "active"}>
                      {s.state}
                    </Badge>
                  </TD>
                  <TD className="font-mono text-[12px] text-ink">{s.session_key}</TD>
                  <TD>
                    <Id value={s.tenant_id} copy className="!text-muted" />
                  </TD>
                  <TD>
                    <Relative at={s.created_at} />
                  </TD>
                  <TD className="text-right">
                    <div className="inline-flex gap-1">
                      {s.state === "active" && (
                        <Button
                          variant="ghost"
                          size="sm"
                          onClick={() => {
                            updateSessionState(s.id, { state: "paused" })
                              .then(() => refresh())
                              .catch((e) => flash(String(e)));
                          }}
                          title="Pause session"
                        >
                          <IconPause size={13} />
                        </Button>
                      )}
                      {s.state === "paused" && (
                        <Button
                          variant="ghost"
                          size="sm"
                          onClick={() => {
                            updateSessionState(s.id, { state: "active" })
                              .then(() => refresh())
                              .catch((e) => flash(String(e)));
                          }}
                          title="Resume session"
                        >
                          <IconPlay size={13} />
                        </Button>
                      )}
                    </div>
                  </TD>
                </TR>
              ))}
              {data.length === 0 && (
                <Empty colSpan={99}>
                  No sessions yet. Create one to group related executions.
                </Empty>
              )}
            </tbody>
          </Table>
        </Section>
      )}
    </div>
  );
}

function CreateSessionForm({
  onCreated,
  onError,
}: {
  onCreated: () => void;
  onError: (msg: string) => void;
}) {
  const [tenantId, setTenantId] = useState("tenant-a");
  const [key, setKey] = useState("");
  const [busy, setBusy] = useState(false);

  const submit = async () => {
    if (!key) {
      onError("session_key is required");
      return;
    }
    setBusy(true);
    try {
      await createSession({ tenant_id: tenantId, session_key: key });
      onCreated();
    } catch (e) {
      onError(`Failed: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      setBusy(false);
    }
  };

  return (
    <Section
      eyebrow="New session"
      title="Start a session"
      description="Sessions group related executions and share state across them."
    >
      <div>
        <div className="grid grid-cols-2 md:grid-cols-3 gap-6">
          <div>
            <FieldLabel>Tenant</FieldLabel>
            <Input value={tenantId} onChange={(e) => setTenantId(e.target.value)} className="w-full" />
          </div>
          <div>
            <FieldLabel>Session key</FieldLabel>
            <Input
              value={key}
              onChange={(e) => setKey(e.target.value)}
              placeholder="user:123:onboarding"
              className="w-full"
            />
          </div>
        </div>
        <div className="mt-6 flex justify-end">
          <Button variant="primary" size="sm" disabled={busy} onClick={submit}>
            Create session
          </Button>
        </div>
      </div>
    </Section>
  );
}
