import { useCallback, useMemo, useState } from "react";
import { getUsage, type UsageResponse } from "../api";
import { usePolling } from "../hooks/usePolling";
import { usePageTitle } from "../hooks/usePageTitle";
import { useDebounce } from "../hooks/useDebounce";
import { PageHeader } from "../components/ui/PageHeader";
import { PageMeta } from "../components/ui/PageMeta";
import { Section } from "../components/ui/Section";
import { Glossary, type GlossaryItem } from "../components/ui/Glossary";
import { Metric, MetricRow } from "../components/ui/Metric";
import { Table, THead, TH, TR, TD, Empty } from "../components/ui/Table";
import { Input } from "../components/ui/Input";
import { SkeletonTable } from "../components/ui/Skeleton";
import { fmtCount, fmtUsd } from "../lib/fmt";

const EST_TOOLTIP = "Estimated from list prices";

const PAGE_GLOSSARY: GlossaryItem[] = [
  {
    term: "Usage event",
    definition:
      "One token-consuming LLM call captured by the engine (llm_call / agent blocks). Each row below aggregates every event for one (kind, model) pair inside the window.",
  },
  {
    term: "Kind",
    definition:
      "What produced the tokens — the block/handler kind that made the call (e.g. llm_call, agent).",
  },
  {
    term: "Tokens",
    definition:
      "Input tokens are what you sent (prompt + context); output tokens are what the model generated. Providers bill the two at different rates.",
  },
  {
    term: "Est. cost",
    definition:
      "input_tokens × list input price + output_tokens × list output price, from a static per-model pricing table. An estimate — negotiated rates, caching discounts, and provider-side rounding are not reflected. Models missing from the table show — and are excluded from the total.",
  },
  {
    term: "Window",
    definition:
      "The reporting period. Defaults to the last 30 days, ending now.",
  },
  {
    term: "Tenant",
    definition:
      "Usage is tenant-scoped. A tenant-scoped API key always reports on its own tenant; an unscoped (admin) key picks one here.",
  },
];

export default function Usage() {
  usePageTitle("Usage");
  const [tenant, setTenant] = useState("tenant-a");
  const debouncedTenant = useDebounce(tenant, 400);

  const fetcher = useCallback(
    (signal?: AbortSignal) =>
      getUsage({ tenant: debouncedTenant || undefined }, signal),
    [debouncedTenant],
  );
  const { data, loading, error, updatedAt, refresh } = usePolling<UsageResponse>(
    fetcher,
    10000,
  );

  const totals = useMemo(() => {
    const entries = data?.usage ?? [];
    return {
      events: entries.reduce((s, u) => s + u.events, 0),
      input: entries.reduce((s, u) => s + u.input_tokens, 0),
      output: entries.reduce((s, u) => s + u.output_tokens, 0),
    };
  }, [data]);

  // Biggest spender first; unknown-cost models sink to the bottom.
  const rows = useMemo(
    () =>
      [...(data?.usage ?? [])].sort(
        (a, b) => (b.cost_usd ?? -1) - (a.cost_usd ?? -1),
      ),
    [data],
  );

  return (
    <div className="space-y-12">
      <PageHeader
        eyebrow="Operator"
        title="Usage & cost"
        description="LLM token consumption captured by the engine, aggregated by (kind, model) over the window, with estimated USD costs from list prices. The first question every team running agents asks — answered per tenant."
        actions={<PageMeta updatedAt={updatedAt} onRefresh={refresh} />}
      />

      <Glossary items={PAGE_GLOSSARY} />

      <Section
        eyebrow="Filter"
        title="Pick a tenant"
        description="Usage is always tenant-scoped. With a tenant-scoped API key this field is ignored — the engine locks reporting to the key's own tenant."
      >
        <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
          <div>
            <label className="field-label block mb-1.5">Tenant ID</label>
            <Input
              type="text"
              placeholder="tenant-a"
              value={tenant}
              onChange={(e) => setTenant(e.target.value)}
            />
          </div>
        </div>
      </Section>

      {error && <div className="notice notice-warn">{error.message}</div>}

      {loading && !data && <SkeletonTable rows={4} cols={6} />}

      {data && (
        <>
          <Section
            eyebrow="Spend"
            title="Window totals"
            description={
              <>
                Spend is an <strong className="text-ink">estimate from list
                prices</strong> — negotiated rates and caching discounts are
                not reflected. Models missing from the pricing table are
                excluded from the total.
              </>
            }
            meta={
              <span>
                <span className="text-faint">WINDOW</span>{" "}
                <span className="text-ink-dim">
                  {new Date(data.start).toLocaleDateString()} –{" "}
                  {new Date(data.end).toLocaleDateString()}
                </span>
              </span>
            }
          >
            <MetricRow cols={4}>
              <Metric
                label="Estimated spend"
                value={fmtUsd(data.total_cost_usd)}
                unit="USD · est."
                tone="signal"
                caption="Sum over every priced (kind, model) aggregate in the window."
                annotation={EST_TOOLTIP}
              />
              <Metric
                label="Events"
                value={fmtCount(totals.events)}
                unit="calls"
                caption="Token-consuming LLM calls captured in the window."
              />
              <Metric
                label="Input tokens"
                value={fmtCount(totals.input)}
                unit="tokens"
                caption="Prompt + context sent to providers."
              />
              <Metric
                label="Output tokens"
                value={fmtCount(totals.output)}
                unit="tokens"
                caption="Generated by providers — usually the pricier side."
              />
            </MetricRow>
          </Section>

          <Section
            eyebrow="Breakdown"
            title="By kind and model"
            description={
              <>
                One row per (kind, model) aggregate, biggest estimated spend
                first. A <span className="font-mono">—</span> cost means the
                model has no entry in the pricing table, so it can't be
                priced and is excluded from the total.
              </>
            }
            meta={
              <span>
                <span className="text-faint">ROWS</span>{" "}
                <span className="text-ink-dim">{rows.length}</span>
              </span>
            }
          >
            <Table>
              <THead>
                <TH>Kind</TH>
                <TH>Model</TH>
                <TH className="text-right">Events</TH>
                <TH className="text-right">Input tokens</TH>
                <TH className="text-right">Output tokens</TH>
                <TH className="text-right">Est. cost</TH>
              </THead>
              <tbody>
                {rows.map((u) => (
                  <TR key={`${u.kind}\0${u.model}`}>
                    <TD className="font-mono text-[12px]">{u.kind}</TD>
                    <TD className="font-mono text-[12px] text-ink">{u.model}</TD>
                    <TD className="text-right tabular">{fmtCount(u.events)}</TD>
                    <TD className="text-right tabular">{fmtCount(u.input_tokens)}</TD>
                    <TD className="text-right tabular">{fmtCount(u.output_tokens)}</TD>
                    <TD className="text-right tabular">
                      {u.cost_usd === null ? (
                        <span
                          className="text-faint"
                          title="Unknown model — no entry in the pricing table; excluded from the total"
                        >
                          —
                        </span>
                      ) : (
                        <span title={EST_TOOLTIP}>
                          {fmtUsd(u.cost_usd)}
                          <span className="text-faint text-[11px]"> est.</span>
                        </span>
                      )}
                    </TD>
                  </TR>
                ))}
                {rows.length === 0 && (
                  <Empty colSpan={99}>
                    No usage events in this window. Run a sequence with an
                    llm_call or agent block and token usage shows up here.
                  </Empty>
                )}
              </tbody>
            </Table>
          </Section>
        </>
      )}
    </div>
  );
}
