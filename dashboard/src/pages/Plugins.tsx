import { useCallback, useState } from "react";
import { usePolling } from "../hooks/usePolling";
import { usePageTitle } from "../hooks/usePageTitle";
import {
  listPlugins,
  createPlugin,
  deletePlugin,
  type PluginDef,
  type PluginType,
} from "../api";
import { PageHeader } from "../components/ui/PageHeader";
import { PageMeta } from "../components/ui/PageMeta";
import { Section } from "../components/ui/Section";
import { Glossary, type GlossaryItem } from "../components/ui/Glossary";
import { Table, THead, TH, TR, TD, Empty } from "../components/ui/Table";
import { Button } from "../components/ui/Button";
import { Input, Select, FieldLabel } from "../components/ui/Input";
import { Badge } from "../components/ui/Badge";
import { Relative } from "../components/ui/Relative";
import { IconPlus, IconTrash } from "../components/ui/Icons";
import { SkeletonTable } from "../components/ui/Skeleton";

const PAGE_GLOSSARY: GlossaryItem[] = [
  {
    term: "Plugin",
    definition:
      "An external implementation of a handler. Plugins let you extend the engine without rebuilding it — write WASM or gRPC services and register them here.",
  },
  {
    term: "WASM",
    definition: "WebAssembly module. Sandboxed, fast, and language-agnostic.",
  },
  {
    term: "gRPC",
    definition: "Remote procedure call over HTTP/2. Use for services that run on another host or in another language.",
  },
];

export default function Plugins() {
  usePageTitle("Plugins");
  const fetcher = useCallback((signal?: AbortSignal) => listPlugins(signal), []);
  const { data, loading, updatedAt, refresh } = usePolling<PluginDef[]>(fetcher, 5000);
  const [showForm, setShowForm] = useState(false);
  const [toast, setToast] = useState<string | null>(null);

  const flash = (msg: string) => {
    setToast(msg);
    setTimeout(() => setToast(null), 2500);
  };

  const enabled = data?.filter((p) => p.enabled).length ?? 0;

  return (
    <div className="space-y-12">
      <PageHeader
        eyebrow="Operator"
        title="Plugins"
        description="External handler implementations registered with the engine. WASM modules and gRPC services extend what your sequences can do."
        actions={
          <div className="flex items-center gap-2">
            <Button variant="primary" size="sm" onClick={() => setShowForm((v) => !v)}>
              <IconPlus size={13} /> {showForm ? "Close" : "New plugin"}
            </Button>
            <PageMeta updatedAt={updatedAt} onRefresh={refresh} />
          </div>
        }
      />

      <Glossary items={PAGE_GLOSSARY} />

      {toast && <div className="notice notice-ok">{toast}</div>}

      {showForm && (
        <CreatePluginForm
          onCreated={() => {
            flash("Plugin created");
            setShowForm(false);
            refresh();
          }}
          onError={(msg) => flash(msg)}
        />
      )}

      {loading && !data && <SkeletonTable rows={6} cols={6} />}

      {data && (
        <Section
          eyebrow="Registry"
          title="Installed plugins"
          description="Every plugin registered against this engine."
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
              <TH>Name</TH>
              <TH>Type</TH>
              <TH>Source</TH>
              <TH>Tenant</TH>
              <TH>Created</TH>
              <TH className="text-right">Actions</TH>
            </THead>
            <tbody>
              {data.map((p) => (
                <TR key={p.name}>
                  <TD>
                    <Badge tone={p.enabled ? "ok" : "dim"} dot>
                      {p.enabled ? "enabled" : "disabled"}
                    </Badge>
                  </TD>
                  <TD className="font-mono text-[12px] text-ink">{p.name}</TD>
                  <TD>
                    <Badge tone="dim">{p.plugin_type}</Badge>
                  </TD>
                  <TD className="font-mono text-[12px] text-muted">{p.source}</TD>
                  <TD className="font-mono text-[12px] text-muted">
                    {p.tenant_id || "global"}
                  </TD>
                  <TD>
                    <Relative at={p.created_at} />
                  </TD>
                  <TD className="text-right">
                    <Button
                      variant="ghost"
                      size="sm"
                      onClick={() => {
                        if (!confirm(`Delete plugin "${p.name}"?`)) return;
                        deletePlugin(p.name)
                          .then(() => {
                            flash("Deleted");
                            refresh();
                          })
                          .catch((e) => flash(String(e)));
                      }}
                      title="Delete plugin"
                    >
                      <IconTrash size={13} />
                    </Button>
                  </TD>
                </TR>
              ))}
              {data.length === 0 && (
                <Empty colSpan={99}>
                  No plugins installed. Register a WASM module or gRPC service to extend the engine.
                </Empty>
              )}
            </tbody>
          </Table>
        </Section>
      )}
    </div>
  );
}

function CreatePluginForm({
  onCreated,
  onError,
}: {
  onCreated: () => void;
  onError: (msg: string) => void;
}) {
  const [name, setName] = useState("");
  const [type, setType] = useState<PluginType>("wasm");
  const [source, setSource] = useState("");
  const [tenantId, setTenantId] = useState("");
  const [busy, setBusy] = useState(false);

  const submit = async () => {
    if (!name || !source) {
      onError("name and source are required");
      return;
    }
    setBusy(true);
    try {
      await createPlugin({ name, plugin_type: type, source, tenant_id: tenantId || undefined });
      onCreated();
    } catch (e) {
      onError(`Failed: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      setBusy(false);
    }
  };

  return (
    <Section eyebrow="New plugin" title="Register an external handler">
      <div>
        <div className="grid grid-cols-2 md:grid-cols-3 gap-6">
          <div>
            <FieldLabel>Name</FieldLabel>
            <Input value={name} onChange={(e) => setName(e.target.value)} placeholder="my-plugin" className="w-full" />
          </div>
          <div>
            <FieldLabel>Type</FieldLabel>
            <Select value={type} onChange={(e) => setType(e.target.value as PluginType)} className="w-full">
              <option value="wasm">wasm</option>
              <option value="grpc">grpc</option>
            </Select>
          </div>
          <div>
            <FieldLabel>Source</FieldLabel>
            <Input
              value={source}
              onChange={(e) => setSource(e.target.value)}
              placeholder="/path/to/plugin.wasm"
              className="w-full font-mono"
            />
          </div>
          <div>
            <FieldLabel>Tenant (optional)</FieldLabel>
            <Input
              value={tenantId}
              onChange={(e) => setTenantId(e.target.value)}
              placeholder="global if empty"
              className="w-full"
            />
          </div>
        </div>
        <div className="mt-6 flex justify-end">
          <Button variant="primary" size="sm" disabled={busy} onClick={submit}>
            Create plugin
          </Button>
        </div>
      </div>
    </Section>
  );
}
