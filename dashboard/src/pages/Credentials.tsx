import { useCallback, useState } from "react";
import { usePolling } from "../hooks/usePolling";
import { usePageTitle } from "../hooks/usePageTitle";
import {
  listCredentials,
  createCredential,
  deleteCredential,
  type CredentialDef,
  type CredentialKind,
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
    term: "Credential",
    definition:
      "A named secret referenced by sequences via credentials://<id>. Rotating a credential updates every sequence that uses it — no redeploy needed.",
  },
  {
    term: "api_key",
    definition: "A single opaque token (Bearer, API key, PAT).",
  },
  {
    term: "oauth2",
    definition: "Access/refresh token pair. The engine refreshes it automatically before expiry.",
  },
  {
    term: "basic",
    definition: "Username + password pair.",
  },
];

export default function Credentials() {
  usePageTitle("Credentials");
  const fetcher = useCallback((signal?: AbortSignal) => listCredentials(signal), []);
  const { data, loading, updatedAt, refresh } = usePolling<CredentialDef[]>(fetcher, 5000);
  const [showForm, setShowForm] = useState(false);
  const [toast, setToast] = useState<string | null>(null);

  const flash = (msg: string) => {
    setToast(msg);
    setTimeout(() => setToast(null), 2500);
  };

  const enabled = data?.filter((c) => c.enabled).length ?? 0;

  return (
    <div className="space-y-12">
      <PageHeader
        eyebrow="Operator"
        title="Credentials"
        description="Named secrets referenced by sequences via the credentials:// URI scheme. Rotate once, update everywhere."
        actions={
          <div className="flex items-center gap-2">
            <Button variant="primary" size="sm" onClick={() => setShowForm((v) => !v)}>
              <IconPlus size={13} /> {showForm ? "Close" : "New credential"}
            </Button>
            <PageMeta updatedAt={updatedAt} onRefresh={refresh} />
          </div>
        }
      />

      <Glossary items={PAGE_GLOSSARY} />

      {toast && <div className="notice notice-ok">{toast}</div>}

      {showForm && (
        <CreateCredentialForm
          onCreated={() => {
            flash("Credential created");
            setShowForm(false);
            refresh();
          }}
          onError={(msg) => flash(msg)}
        />
      )}

      {loading && !data && <SkeletonTable rows={6} cols={7} />}

      {data && (
        <Section
          eyebrow="Registry"
          title="Stored credentials"
          description="Every credential the engine can resolve for sequences."
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
              <TH>ID</TH>
              <TH>Name</TH>
              <TH>Kind</TH>
              <TH>Tenant</TH>
              <TH>Expires</TH>
              <TH className="text-right">Actions</TH>
            </THead>
            <tbody>
              {data.map((c) => (
                <TR key={c.id}>
                  <TD>
                    <Badge tone={c.enabled ? "ok" : "dim"} dot>
                      {c.enabled ? "enabled" : "disabled"}
                    </Badge>
                  </TD>
                  <TD className="font-mono text-[12px] text-ink">{c.id}</TD>
                  <TD className="font-mono text-[12px] text-muted">{c.name}</TD>
                  <TD>
                    <Badge tone="dim">{c.kind}</Badge>
                  </TD>
                  <TD className="font-mono text-[12px] text-muted">
                    {c.tenant_id || "global"}
                  </TD>
                  <TD>
                    {c.expires_at ? (
                      <Relative at={c.expires_at} />
                    ) : (
                      <span className="text-faint">never</span>
                    )}
                  </TD>
                  <TD className="text-right">
                    <Button
                      variant="ghost"
                      size="sm"
                      onClick={() => {
                        if (!confirm(`Delete credential "${c.id}"?`)) return;
                        deleteCredential(c.id)
                          .then(() => {
                            flash("Deleted");
                            refresh();
                          })
                          .catch((e) => flash(String(e)));
                      }}
                      title="Delete credential"
                    >
                      <IconTrash size={13} />
                    </Button>
                  </TD>
                </TR>
              ))}
              {data.length === 0 && (
                <Empty colSpan={99}>
                  No credentials stored. Create one so sequences can reference secrets safely.
                </Empty>
              )}
            </tbody>
          </Table>
        </Section>
      )}
    </div>
  );
}

function CreateCredentialForm({
  onCreated,
  onError,
}: {
  onCreated: () => void;
  onError: (msg: string) => void;
}) {
  const [id, setId] = useState("");
  const [name, setName] = useState("");
  const [kind, setKind] = useState<CredentialKind>("api_key");
  const [value, setValue] = useState("");
  const [tenantId, setTenantId] = useState("");
  const [busy, setBusy] = useState(false);

  const submit = async () => {
    if (!id || !name || !value) {
      onError("id, name and value are required");
      return;
    }
    setBusy(true);
    try {
      await createCredential({ id, name, kind, value, tenant_id: tenantId || undefined });
      onCreated();
    } catch (e) {
      onError(`Failed: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      setBusy(false);
    }
  };

  return (
    <Section eyebrow="New credential" title="Store a secret">
      <div>
        <div className="grid grid-cols-2 md:grid-cols-3 gap-6">
          <div>
            <FieldLabel>ID</FieldLabel>
            <Input
              value={id}
              onChange={(e) => setId(e.target.value)}
              placeholder="stripe-prod"
              className="w-full font-mono"
            />
            <p className="annotation mt-1">Used in credentials:// references.</p>
          </div>
          <div>
            <FieldLabel>Name</FieldLabel>
            <Input value={name} onChange={(e) => setName(e.target.value)} placeholder="Stripe Production" className="w-full" />
          </div>
          <div>
            <FieldLabel>Kind</FieldLabel>
            <Select value={kind} onChange={(e) => setKind(e.target.value as CredentialKind)} className="w-full">
              <option value="api_key">api_key</option>
              <option value="oauth2">oauth2</option>
              <option value="basic">basic</option>
            </Select>
          </div>
          <div>
            <FieldLabel>Value (JSON)</FieldLabel>
            <Input
              value={value}
              onChange={(e) => setValue(e.target.value)}
              placeholder='{"token":"sk_live_xxx"}'
              className="w-full font-mono"
            />
          </div>
          <div>
            <FieldLabel>Tenant (optional)</FieldLabel>
            <Input value={tenantId} onChange={(e) => setTenantId(e.target.value)} placeholder="global if empty" className="w-full" />
          </div>
        </div>
        <div className="mt-6 flex justify-end">
          <Button variant="primary" size="sm" disabled={busy} onClick={submit}>
            Store credential
          </Button>
        </div>
      </div>
    </Section>
  );
}
