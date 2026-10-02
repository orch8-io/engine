"use client";

import { useRouter } from "next/navigation";
import { useState } from "react";
import { Orch8Approvals, Orch8Badge, Orch8Builder, Orch8Runs, Orch8RunTimeline } from "@orch8/embed/react";
import { tokenProvider } from "@/lib/token-provider";

interface Common {
  /** Browser-facing engine origin (or "/mock"). Comes from the server config. */
  baseUrl: string;
  vendor: string;
}

// Your brand, applied on top of the server theme (GET /api/v1/embed/theme).
const brand = { accent: "#0f766e", radius: "10px", "dark-accent": "#5eead4" };

export function RunsWidget({ baseUrl, vendor, startSequence }: Common & { startSequence?: string }) {
  const router = useRouter();
  return (
    <Orch8Runs
      baseUrl={baseUrl}
      vendor={vendor}
      tokenProvider={tokenProvider}
      theme={brand}
      startSequence={startSequence}
      onRunSelect={({ id }) => router.push(`/runs/${encodeURIComponent(id)}`)}
      onRunStarted={({ id }) => router.push(`/runs/${encodeURIComponent(id)}`)}
    />
  );
}

export function TimelineWidget({ baseUrl, vendor, runId }: Common & { runId: string }) {
  return <Orch8RunTimeline baseUrl={baseUrl} vendor={vendor} tokenProvider={tokenProvider} theme={brand} runId={runId} />;
}

export function ApprovalsWidget({ baseUrl, vendor }: Common) {
  const [last, setLast] = useState<string | null>(null);
  return (
    <>
      <Orch8Approvals
        baseUrl={baseUrl}
        vendor={vendor}
        tokenProvider={tokenProvider}
        theme={brand}
        onApprovalResolved={({ choice, instance_id }) => setLast(`Recorded “${choice}” for run ${instance_id}.`)}
      />
      {last ? <p className="note" role="status">{last}</p> : null}
    </>
  );
}

export function BuilderWidget({ baseUrl, vendor, sequence }: Common & { sequence: string }) {
  return <Orch8Builder baseUrl={baseUrl} vendor={vendor} tokenProvider={tokenProvider} theme={brand} sequence={sequence} />;
}

export function BadgeWidget({ vendor }: { vendor: string }) {
  return <Orch8Badge vendor={vendor} />;
}
