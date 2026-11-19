import { useCallback, useState } from "react";
import { Link } from "react-router-dom";
import { getEffectReviewQueue, settleUnknownEffect, type EffectReviewItem } from "../api";
import { usePolling } from "../hooks/usePolling";
import { Button } from "./ui/Button";
import { FieldLabel, Input, Select } from "./ui/Input";
import { Section } from "./ui/Section";
import { Empty, Table, THead, TH, TR, TD } from "./ui/Table";

export function EffectReviewQueue({ tenant }: { tenant: string }) {
  return <TenantEffectReviewQueue key={tenant} tenant={tenant} />;
}

function TenantEffectReviewQueue({ tenant }: { tenant: string }) {
  const [cursors, setCursors] = useState<string[]>([]);
  const after = cursors.at(-1);
  const page = cursors.length + 1;
  return <EffectReviewPage key={after ?? ""} tenant={tenant} after={after} page={page}
    previous={() => setCursors((current) => current.slice(0, -1))}
    next={(cursor) => setCursors((current) => [...current, cursor])} />;
}

function EffectReviewPage({ tenant, after, page, previous, next }: {
  tenant: string;
  after?: string;
  page: number;
  previous: () => void;
  next: (cursor: string) => void;
}) {
  const fetchQueue = useCallback((signal?: AbortSignal) => getEffectReviewQueue(tenant, signal, after), [tenant, after]);
  const { data, error, loading, refresh } = usePolling(fetchQueue, 10000);
  const [selected, setSelected] = useState<EffectReviewItem | null>(null);
  const [decision, setDecision] = useState<"verified" | "abandoned">("verified");
  const [receipt, setReceipt] = useState("");
  const [busy, setBusy] = useState(false);
  const [status, setStatus] = useState<string | null>(null);

  const settle = async () => {
    if (!selected || (decision === "verified" && !receipt.trim())) return;
    setBusy(true);
    setStatus(null);
    try {
      await settleUnknownEffect(selected.id, tenant, decision, receipt.trim() || undefined);
      setStatus("Effect resolution recorded.");
      setSelected(null);
      refresh();
    } catch (error) {
      setStatus(error instanceof Error ? error.message : String(error));
    } finally {
      setBusy(false);
    }
  };

  return (
    <Section title="Unknown effects" description="Review provider evidence before recording the outcome of an uncertain side effect.">
      {error && <p role="status" className="text-muted">{error.message}</p>}
      {status && <p role="status">{status}</p>}
      <Table>
        <THead><TH>Execution</TH><TH>Step</TH><TH>Effect</TH><TH>Last updated</TH><TH>Action</TH></THead>
        <tbody>
          {data?.items.map((item) => (
            <TR key={item.id}>
              <TD><Link className="text-signal underline" to={`/instances/${item.instance_id}`}>{item.instance_id.slice(0, 8)}</Link></TD>
              <TD>{item.block_id}</TD>
              <TD>{item.kind}</TD>
              <TD>{new Date(item.updated_at).toLocaleString()}</TD>
              <TD><Button size="sm" disabled={busy} onClick={() => { setSelected(item); setReceipt(""); setDecision("verified"); }}>Review</Button></TD>
            </TR>
          ))}
          {!data?.items.length && <Empty colSpan={5}>{loading ? "Loading review queue…" : "No unknown effects."}</Empty>}
        </tbody>
      </Table>
      {(page > 1 || data?.next_cursor) && (
        <nav aria-label="Effect review pages" className="mt-3 flex items-center gap-2">
          <Button size="sm" disabled={page === 1 || busy} onClick={previous}>Previous</Button>
          <span>Page {page}</span>
          <Button size="sm" disabled={!data?.next_cursor || busy}
            onClick={() => { if (data?.next_cursor) next(data.next_cursor); }}>Next</Button>
        </nav>
      )}
      {selected && (
        <form className="mt-4 flex flex-wrap items-end gap-3" onSubmit={(event) => { event.preventDefault(); void settle(); }}>
          <p className="w-full">Record the reviewed outcome for {selected.block_id}.</p>
          <div>
            <FieldLabel htmlFor="effect-decision">Outcome</FieldLabel>
            <Select id="effect-decision" value={decision} onChange={(event) => setDecision(event.target.value === "verified" ? "verified" : "abandoned")}>
              <option value="verified">Verified with the provider</option>
              <option value="abandoned">Abandoned after review</option>
            </Select>
          </div>
          <div>
            <FieldLabel htmlFor="effect-receipt">Provider receipt</FieldLabel>
            <Input id="effect-receipt" value={receipt} onChange={(event) => setReceipt(event.target.value)} required={decision === "verified"} />
          </div>
          <Button type="submit" size="sm" disabled={busy}>Record resolution</Button>
          <Button type="button" size="sm" disabled={busy} onClick={() => setSelected(null)}>Cancel</Button>
        </form>
      )}
    </Section>
  );
}
