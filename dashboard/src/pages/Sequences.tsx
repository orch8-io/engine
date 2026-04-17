import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { listSequenceVersions, type SequenceDefinition } from "../api";
import { PageHeader } from "../components/ui/PageHeader";
import { Panel, PanelBody } from "../components/ui/Panel";
import { Badge } from "../components/ui/Badge";
import { Button } from "../components/ui/Button";
import { Input, FieldLabel } from "../components/ui/Input";
import { Table, THead, TH, TR, TD, Empty } from "../components/ui/Table";
import { Id } from "../components/ui/Mono";
import { Relative } from "../components/ui/Relative";
import { SkeletonTable } from "../components/ui/Skeleton";

export default function Sequences() {
  const navigate = useNavigate();
  const [tenant, setTenant] = useState("");
  const [namespace, setNamespace] = useState("default");
  const [name, setName] = useState("");
  const [versions, setVersions] = useState<SequenceDefinition[] | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const search = async () => {
    if (!tenant || !name) {
      setError("tenant_id and name are required");
      return;
    }
    setLoading(true);
    setError(null);
    try {
      const v = await listSequenceVersions({
        tenant_id: tenant,
        namespace,
        name,
      });
      setVersions(v);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="space-y-6">
      <PageHeader
        eyebrow="Operator"
        title="Sequences"
        description="Sequences are keyed by (tenant, namespace, name, version). Pick a name to see all deployed versions."
      />

      <Panel>
        <PanelBody>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
            <div>
              <FieldLabel>Tenant ID</FieldLabel>
              <Input
                type="text"
                placeholder="tenant_id"
                value={tenant}
                onChange={(e) => setTenant(e.target.value)}
                className="w-full"
              />
            </div>
            <div>
              <FieldLabel>Namespace</FieldLabel>
              <Input
                type="text"
                placeholder="namespace"
                value={namespace}
                onChange={(e) => setNamespace(e.target.value)}
                className="w-full"
              />
            </div>
            <div>
              <FieldLabel>Name</FieldLabel>
              <Input
                type="text"
                placeholder="sequence name"
                value={name}
                onChange={(e) => setName(e.target.value)}
                onKeyDown={(e) => e.key === "Enter" && search()}
                className="w-full"
              />
            </div>
          </div>
          <div className="mt-4">
            <Button variant="primary" onClick={search} disabled={loading}>
              {loading ? "Loading…" : "List versions"}
            </Button>
          </div>
        </PanelBody>
      </Panel>

      {error && (
        <div className="rounded-md border border-warn/40 bg-warn/10 text-warn p-3 text-[13px]">
          {error}
        </div>
      )}

      {loading && <SkeletonTable rows={4} cols={5} />}

      {versions && versions.length === 0 && !loading && (
        <Panel>
          <PanelBody>
            <div className="py-6 text-center text-muted text-[13px]">
              No versions found for{" "}
              <span className="font-mono text-fg-dim">
                {tenant}/{namespace}/{name}
              </span>
              .
            </div>
          </PanelBody>
        </Panel>
      )}

      {versions && versions.length > 0 && (
        <Panel>
          <PanelBody padded={false}>
            <Table>
              <THead>
                <TH className="pl-4">Version</TH>
                <TH>ID</TH>
                <TH className="text-right">Blocks</TH>
                <TH>Status</TH>
                <TH className="pr-4">Created</TH>
              </THead>
              <tbody>
                {versions.map((v) => (
                  <TR
                    key={v.id}
                    onClick={() => navigate(`/sequences/${v.id}`)}
                    className="cursor-pointer"
                  >
                    <TD className="pl-4 font-mono text-[12px] tabular">
                      v{v.version}
                    </TD>
                    <TD>
                      <Id value={v.id} copy />
                    </TD>
                    <TD className="text-right tabular">{v.blocks.length}</TD>
                    <TD>
                      {v.deprecated ? (
                        <Badge tone="hold">deprecated</Badge>
                      ) : (
                        <Badge tone="ok">active</Badge>
                      )}
                    </TD>
                    <TD className="pr-4">
                      <Relative at={v.created_at} />
                    </TD>
                  </TR>
                ))}
                {versions.length === 0 && <Empty>No versions found.</Empty>}
              </tbody>
            </Table>
          </PanelBody>
        </Panel>
      )}
    </div>
  );
}
