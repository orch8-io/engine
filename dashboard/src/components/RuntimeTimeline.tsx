import { useCallback } from "react";
import { getInstanceDiagnosis, getRuntimeTimeline, type RuntimeTimelineEvent } from "../api";
import { usePolling } from "../hooks/usePolling";
import { Badge } from "./ui/Badge";
import { Button } from "./ui/Button";
import { Section } from "./ui/Section";
import { Empty, Table, THead, TH, TR, TD } from "./ui/Table";

function describe(event: RuntimeTimelineEvent): [string, string] {
  switch (event.kind) {
    case "runtime_owner":
      return ["Runtime owner", `${event.runtime_id} · epoch ${event.epoch}`];
    case "task_queued":
      return ["Task queued", event.block_id];
    case "worker_attempt":
      return [event.event.replaceAll("_", " "), [event.block_id, event.worker_id, `claim ${event.claim_epoch}`].filter(Boolean).join(" · ")];
    case "task_terminal":
      return [event.state, event.block_id];
  }
}

export function WaitReason({ instanceId }: { instanceId: string }) {
  const fetchDiagnosis = useCallback((signal?: AbortSignal) => getInstanceDiagnosis(instanceId, signal), [instanceId]);
  const { data, error } = usePolling(fetchDiagnosis, 5000);
  const finding = data?.diagnoses[0];

  return (
    <Section title="Wait reason">
      {error && <p role="status" className="text-muted">{error.message}</p>}
      {finding ? (
        <div className="space-y-2">
          <Badge tone="dim">{finding.code}</Badge>
          <p>{finding.summary}</p>
          {!!finding.remediation?.length && (
            <ul className="list-disc pl-5 text-muted">
              {finding.remediation.map((action, index) => (
                <li key={index}>{action.summary}{action.side_effect_risk ? " (may repeat an external effect)" : ""}</li>
              ))}
            </ul>
          )}
        </div>
      ) : data ? <p className="text-muted">No wait reason has been reported.</p> : null}
    </Section>
  );
}

export function RuntimeTimeline({ instanceId }: { instanceId: string }) {
  const fetchTimeline = useCallback((signal?: AbortSignal) => getRuntimeTimeline(instanceId, signal), [instanceId]);
  const { data, error, loading } = usePolling(fetchTimeline, 10000);

  const download = () => {
    if (!data) return;
    const href = URL.createObjectURL(new Blob([JSON.stringify(data, null, 2)], { type: "application/json" }));
    const anchor = document.createElement("a");
    anchor.href = href;
    anchor.download = `orch8-runtime-timeline-${instanceId}.json`;
    anchor.click();
    setTimeout(() => URL.revokeObjectURL(href), 1000);
  };

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-end">
        <Button size="sm" disabled={!data} onClick={download}>Download redacted trace</Button>
      </div>
      {error && <p role="status" className="text-muted">{error.message}</p>}
      {data?.truncated && <p className="text-muted">This trace reached the event limit.</p>}
      <Table>
        <THead><TH>Time</TH><TH>Event</TH><TH>Worker or step</TH></THead>
        <tbody>
          {data?.events.map((event, index) => {
            const [title, details] = describe(event);
            return (
              <TR key={index}>
                <TD className="tabular">{new Date(event.at).toLocaleString()}</TD>
                <TD>{title}</TD>
                <TD className="font-mono">{details}</TD>
              </TR>
            );
          })}
          {!data?.events.length && <Empty colSpan={3}>{loading ? "Loading runtime events…" : "No runtime events have been recorded."}</Empty>}
        </tbody>
      </Table>
    </div>
  );
}
