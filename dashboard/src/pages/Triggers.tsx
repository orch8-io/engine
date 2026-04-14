import { useCallback, useState } from "react";
import {
  listTriggers,
  createTrigger,
  deleteTrigger,
  fireTrigger,
  type TriggerDef,
  type TriggerType,
} from "../api";
import { usePolling } from "../hooks/usePolling";
import { PageHeader } from "../components/ui/PageHeader";
import { PageMeta } from "../components/ui/PageMeta";
import { Section } from "../components/ui/Section";
import { Glossary, type GlossaryItem } from "../components/ui/Glossary";
import { Table, THead, TH, TR, TD, Empty } from "../components/ui/Table";
import { Button } from "../components/ui/Button";
import { Input, Select, FieldLabel } from "../components/ui/Input";
import { Badge } from "../components/ui/Badge";
import { Relative } from "../components/ui/Relative";
import { IconPlus, IconTrash, IconSend } from "../components/ui/Icons";

const PAGE_GLOSSARY: GlossaryItem[] = [
  {
    term: "Trigger",
    definition:
      "A named endpoint that, when invoked, spawns a new execution of a sequence. Triggers are the bridge between external events and your workflows.",
  },
  {
    term: "Slug",
    definition:
      "The trigger's public identifier — used in the URL for webhooks and as the key for manual firing. Must be unique per tenant.",
  },
  {
    term: "webhook",
    definition: (
      <>
        HTTP endpoint. External systems POST to{" "}
        <code className="font-mono text-ink">/triggers/{"{slug}"}/fire</code>{" "}
        with a JSON payload to kick off an execution.
      </>
    ),
  },
  {
    term: "event",
    definition:
      "Fires on an internal engine event — e.g. another execution completing. Used for chaining workflows.",
  },
  {
    term: "nats",
    definition:
      "Listens on a NATS subject. Every matching message spawns an execution.",
  },
  {
    term: "file_watch",
    definition:
      "Fires when a file appears or changes in a watched directory. Useful for batch ingestion pipelines.",
  },
  {
    term: "Secret",
    definition:
      "Optional shared secret. Callers must present it in the x-trigger-secret header — requests without the secret are rejected. Leave empty for open triggers (not recommended in prod).",
  },
  {
    term: "Version",
    definition:
      "Pins the trigger to a specific sequence version. Leave blank to always use the head — usually what you want during active development.",
  },
  {
    term: "Payload",
    definition:
      "The JSON body posted when firing. It becomes the input of the first block of the spawned execution.",
  },
];

export default function Triggers() {
  const fetcher = useCallback(() => listTriggers(), []);
  const { data, loading, updatedAt, refresh } =
    usePolling<TriggerDef[]>(fetcher);
  const [showForm, setShowForm] = useState(false);
  const [toast, setToast] = useState<string | null>(null);
  const [fireTarget, setFireTarget] = useState<TriggerDef | null>(null);

  const flash = (msg: string) => {
    setToast(msg);
    setTimeout(() => setToast(null), 2500);
  };

  const remove = async (t: TriggerDef) => {
    if (
      !confirm(
        `Delete trigger "${t.slug}"?\n\nFuture requests to this slug will 404. Existing executions keep running.`,
      )
    )
      return;
    try {
      await deleteTrigger(t.slug);
      flash("Deleted");
      refresh();
    } catch (e) {
      flash(`Failed: ${e instanceof Error ? e.message : String(e)}`);
    }
  };

  const enabled = data?.filter((t) => t.enabled).length ?? 0;

  return (
    <div className="space-y-12">
      <PageHeader
        eyebrow="Operator"
        title="Triggers"
        description="Endpoints that turn external events into executions. One trigger = one named door into the engine, wired to a specific sequence."
        actions={
          <div className="flex items-center gap-2">
            <Button
              variant="primary"
              size="sm"
              onClick={() => setShowForm((v) => !v)}
            >
              <IconPlus size={13} /> {showForm ? "Close" : "New trigger"}
            </Button>
            <PageMeta updatedAt={updatedAt} onRefresh={refresh} />
          </div>
        }
      />

      <Glossary items={PAGE_GLOSSARY} />

      {toast && <div className="notice notice-ok">{toast}</div>}

      {showForm && (
        <CreateTriggerForm
          onCreated={() => {
            flash("Trigger created");
            setShowForm(false);
            refresh();
          }}
          onError={(msg) => flash(msg)}
        />
      )}

      {fireTarget && (
        <FireTriggerPanel
          trigger={fireTarget}
          onClose={() => setFireTarget(null)}
          onFired={(id) => {
            flash(`Fired: execution ${id.slice(0, 8)}…`);
            setFireTarget(null);
            refresh();
          }}
          onError={(msg) => flash(msg)}
        />
      )}

      {loading && !data && (
        <div className="text-muted text-[13px] font-mono">Loading…</div>
      )}

      {data && (
        <Section
          eyebrow="Registered triggers"
          title="Event doors"
          description={
            <>
              Use the <strong className="text-ink">send</strong> button to
              fire a trigger manually — useful for testing wiring without
              involving the external system. Deleting a trigger only removes
              the endpoint; any execution it previously spawned keeps running.
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
                <span className="text-ink-dim">{data.length}</span>
              </span>
            </>
          }
        >
          <Table>
            <THead>
              <TH>Status</TH>
              <TH>Slug</TH>
              <TH>Type</TH>
              <TH>Tenant / Namespace</TH>
              <TH>Sequence</TH>
              <TH>Version</TH>
              <TH>Created</TH>
              <TH className="text-right">Actions</TH>
            </THead>
            <tbody>
              {data.map((t) => (
                <TR key={t.slug}>
                  <TD>
                    <Badge tone={t.enabled ? "ok" : "dim"} dot>
                      {t.enabled ? "enabled" : "disabled"}
                    </Badge>
                  </TD>
                  <TD className="font-mono text-[12px]">{t.slug}</TD>
                  <TD>
                    <Badge tone="dim">{t.trigger_type}</Badge>
                  </TD>
                  <TD className="font-mono text-[12px]">
                    {t.tenant_id}
                    <span className="text-faint"> / </span>
                    {t.namespace}
                  </TD>
                  <TD className="font-mono text-[12px]">
                    {t.sequence_name}
                  </TD>
                  <TD
                    className="font-mono text-[12px] text-muted tabular"
                    title={
                      t.version
                        ? `Pinned to v${t.version}`
                        : "Uses the newest version of the sequence"
                    }
                  >
                    {t.version ?? "latest"}
                  </TD>
                  <TD>
                    <Relative at={t.created_at} />
                  </TD>
                  <TD className="text-right">
                    <div className="inline-flex gap-1">
                      <Button
                        variant="ghost"
                        size="sm"
                        onClick={() => setFireTarget(t)}
                        title="Fire this trigger manually with a custom payload"
                        disabled={!t.enabled}
                      >
                        <IconSend size={13} />
                      </Button>
                      <Button
                        variant="ghost"
                        size="sm"
                        onClick={() => remove(t)}
                        title="Delete this trigger"
                      >
                        <IconTrash size={13} />
                      </Button>
                    </div>
                  </TD>
                </TR>
              ))}
              {data.length === 0 && (
                <Empty colSpan={99}>
                  No triggers yet. Create one to let external systems
                  start executions.
                </Empty>
              )}
            </tbody>
          </Table>
        </Section>
      )}
    </div>
  );
}

function CreateTriggerForm({
  onCreated,
  onError,
}: {
  onCreated: () => void;
  onError: (msg: string) => void;
}) {
  const [slug, setSlug] = useState("");
  const [sequenceName, setSequenceName] = useState("");
  const [tenantId, setTenantId] = useState("tenant-a");
  const [namespace, setNamespace] = useState("prod");
  const [type, setType] = useState<TriggerType>("webhook");
  const [secret, setSecret] = useState("");
  const [busy, setBusy] = useState(false);

  const submit = async () => {
    if (!slug || !sequenceName) {
      onError("slug and sequence_name are required");
      return;
    }
    setBusy(true);
    try {
      await createTrigger({
        slug,
        sequence_name: sequenceName,
        tenant_id: tenantId,
        namespace,
        trigger_type: type,
        ...(secret ? { secret } : {}),
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
      eyebrow="New trigger"
      title="Wire an event source to a sequence"
      description={
        <>
          The slug determines the public URL (for webhooks). The sequence
          name must already be deployed. Providing a secret is strongly
          recommended for webhooks — it's checked on every fire.
        </>
      }
    >
      <div>
        <div className="grid grid-cols-2 md:grid-cols-3 gap-6">
          <div>
            <FieldLabel>Slug</FieldLabel>
            <Input
              value={slug}
              onChange={(e) => setSlug(e.target.value)}
              placeholder="my-webhook"
              className="w-full"
            />
            <p className="annotation mt-1">
              Lowercase, hyphenated. Becomes part of the URL.
            </p>
          </div>
          <div>
            <FieldLabel>Sequence name</FieldLabel>
            <Input
              value={sequenceName}
              onChange={(e) => setSequenceName(e.target.value)}
              placeholder="order-fulfillment"
              className="w-full"
            />
            <p className="annotation mt-1">Must be an already-deployed sequence.</p>
          </div>
          <div>
            <FieldLabel>Type</FieldLabel>
            <Select
              value={type}
              onChange={(e) => setType(e.target.value as TriggerType)}
              className="w-full"
            >
              <option value="webhook">webhook (HTTP)</option>
              <option value="event">event (internal)</option>
              <option value="nats">nats (subject)</option>
              <option value="file_watch">file_watch (dir)</option>
            </Select>
            <p className="annotation mt-1">
              Transport the trigger listens on.
            </p>
          </div>
          <div>
            <FieldLabel>Tenant</FieldLabel>
            <Input
              value={tenantId}
              onChange={(e) => setTenantId(e.target.value)}
              className="w-full"
            />
            <p className="annotation mt-1">
              Must match the sequence's tenant.
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
              Environment — prod, staging, etc.
            </p>
          </div>
          <div>
            <FieldLabel>Secret (optional)</FieldLabel>
            <Input
              value={secret}
              onChange={(e) => setSecret(e.target.value)}
              type="password"
              placeholder="shared token"
              className="w-full"
            />
            <p className="annotation mt-1">
              Callers must send via x-trigger-secret header.
            </p>
          </div>
        </div>
        <div className="mt-6 flex justify-end">
          <Button variant="primary" size="sm" disabled={busy} onClick={submit}>
            Create trigger
          </Button>
        </div>
      </div>
    </Section>
  );
}

function FireTriggerPanel({
  trigger,
  onClose,
  onFired,
  onError,
}: {
  trigger: TriggerDef;
  onClose: () => void;
  onFired: (instanceId: string) => void;
  onError: (msg: string) => void;
}) {
  const [payload, setPayload] = useState("{}");
  const [secret, setSecret] = useState("");
  const [busy, setBusy] = useState(false);

  const submit = async () => {
    let parsed: Record<string, unknown>;
    try {
      parsed = JSON.parse(payload);
    } catch {
      onError("Payload is not valid JSON");
      return;
    }
    setBusy(true);
    try {
      const res = await fireTrigger(trigger.slug, parsed, secret || undefined);
      onFired(res.instance_id);
    } catch (e) {
      onError(`Failed: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      setBusy(false);
    }
  };

  return (
    <Section
      eyebrow="Fire trigger"
      title={`Send a test payload to "${trigger.slug}"`}
      description={
        <>
          This spawns a real execution with your payload as the input of
          the first block. Use it to smoke-test wiring end-to-end without
          involving the external system.
        </>
      }
      meta={
        <Button variant="ghost" size="sm" onClick={onClose}>
          Close
        </Button>
      }
    >
      <div>
        <div className="grid grid-cols-1 gap-6">
          <div>
            <FieldLabel>Payload (JSON)</FieldLabel>
            <p className="annotation mb-1">
              Becomes the input of the first block of the spawned execution.
            </p>
            <textarea
              value={payload}
              onChange={(e) => setPayload(e.target.value)}
              rows={6}
              className="w-full bg-sunken border border-rule px-2.5 py-2 text-[12px] font-mono text-ink placeholder:text-faint focus:border-signal focus:outline-none"
            />
          </div>
          {trigger.secret !== null && (
            <div>
              <FieldLabel>x-trigger-secret</FieldLabel>
              <p className="annotation mb-1">
                This trigger requires a secret. Without it the fire is rejected.
              </p>
              <Input
                value={secret}
                onChange={(e) => setSecret(e.target.value)}
                type="password"
                className="w-full"
              />
            </div>
          )}
        </div>
        <div className="mt-6 flex justify-end">
          <Button variant="primary" size="sm" disabled={busy} onClick={submit}>
            <IconSend size={13} /> Fire trigger
          </Button>
        </div>
      </div>
    </Section>
  );
}
