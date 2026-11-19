import { useState } from "react";
import { connectionInfo, createWorkerEnrollment } from "../api";
import { Button } from "./ui/Button";
import { FieldLabel, Input } from "./ui/Input";
import { Section } from "./ui/Section";

export function WorkerEnrollment() {
  const { tenantId, baseUrl } = connectionInfo();
  const [open, setOpen] = useState(false);
  const [tenant, setTenant] = useState(tenantId ?? "default");
  const [name, setName] = useState("");
  const [issued, setIssued] = useState<{ secret: string; tenant: string; baseUrl: string } | null>(null);
  const [status, setStatus] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const enroll = async () => {
    const workerName = name.trim();
    const tenantName = tenant.trim();
    if (!workerName || !tenantName) return;
    setBusy(true);
    setStatus(null);
    try {
      const expiresAt = new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString();
      const credential = await createWorkerEnrollment(tenantName, workerName, expiresAt);
      setIssued({ secret: credential.secret, tenant: tenantName, baseUrl });
    } catch (error) {
      setStatus(error instanceof Error ? error.message : String(error));
    } finally {
      setBusy(false);
    }
  };

  if (!open) {
    return <Button size="sm" onClick={() => setOpen(true)}>Enroll a worker</Button>;
  }

  return (
    <Section title="Enroll a worker" description="Create a tenant-scoped worker key that expires in 24 hours. Enrollment requires the root API key.">
      {issued ? (
        <div className="space-y-3">
          <p>Copy the key now. The engine displays it once.</p>
          <pre className="overflow-auto select-all">{issued.secret}</pre>
          <dl className="grid grid-cols-[max-content_1fr] gap-x-3 gap-y-1">
            <dt>ORCH8_URL</dt><dd className="font-mono">{issued.baseUrl.replace(/\/+$/, "").replace(/\/api\/v1$/, "")}/api/v1</dd>
            <dt>ORCH8_TENANT_ID</dt><dd className="font-mono">{issued.tenant}</dd>
          </dl>
          <p className="text-muted">Provide the key as ORCH8_API_KEY on the worker host. Refresh the fleet after starting the worker.</p>
          <Button size="sm" onClick={() => { setIssued(null); setOpen(false); }}>Hide key and finish</Button>
        </div>
      ) : (
        <form className="flex flex-wrap items-end gap-3" onSubmit={(event) => { event.preventDefault(); void enroll(); }}>
          {status && <p role="status" className="w-full text-muted">{status}</p>}
          <div>
            <FieldLabel htmlFor="enrollment-tenant">Tenant</FieldLabel>
            <Input id="enrollment-tenant" value={tenant} onChange={(event) => setTenant(event.target.value)} required disabled={busy} />
          </div>
          <div>
            <FieldLabel htmlFor="enrollment-name">Key name</FieldLabel>
            <Input id="enrollment-name" value={name} onChange={(event) => setName(event.target.value)} required disabled={busy} />
          </div>
          <Button type="submit" size="sm" disabled={busy}>Create worker key</Button>
          <Button type="button" size="sm" disabled={busy} onClick={() => { setStatus(null); setOpen(false); }}>Cancel</Button>
        </form>
      )}
    </Section>
  );
}
