import { useCallback, useState } from "react";
import { Link } from "react-router-dom";
import { usePolling } from "../hooks/usePolling";
import {
  listApprovals,
  sendHumanInputSignal,
  type ApprovalItem,
  type ApprovalsResponse,
  type HumanChoice,
} from "../api";
import { PageHeader } from "../components/ui/PageHeader";
import { PageMeta } from "../components/ui/PageMeta";
import { Section } from "../components/ui/Section";
import { Glossary, type GlossaryItem } from "../components/ui/Glossary";
import { Button } from "../components/ui/Button";
import { Table, THead, TH, TR, TD, Empty } from "../components/ui/Table";
import { Id } from "../components/ui/Mono";
import { Relative } from "../components/ui/Relative";
import { SkeletonTable } from "../components/ui/Skeleton";

const PAGE_GLOSSARY: GlossaryItem[] = [
  {
    term: "Approval",
    definition:
      "An execution paused on a step that declares wait_for_input. The engine holds the block until an operator sends the human_input:{block_id} signal with a value matching one of the declared choices.",
  },
  {
    term: "Choice",
    definition:
      "One of the author-declared {label, value} pairs. label is the button text; value is what gets written to the instance's output for that block.",
  },
  {
    term: "Deadline",
    definition:
      "When this approval times out. On timeout, the step either fails or routes to the declared escalation_handler.",
  },
];

export default function Approvals() {
  const [busyKey, setBusyKey] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const fetcher = useCallback(() => listApprovals({ limit: "100" }), []);
  const { data, loading, updatedAt, refresh } =
    usePolling<ApprovalsResponse>(fetcher, 5000);

  const items = data?.items ?? [];

  async function respond(item: ApprovalItem, choice: HumanChoice) {
    const key = `${item.instance_id}:${item.block_id}`;
    setBusyKey(key);
    setError(null);
    try {
      await sendHumanInputSignal(item.instance_id, item.block_id, choice.value);
      refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusyKey(null);
    }
  }

  return (
    <div className="space-y-12">
      <PageHeader
        eyebrow="Operator"
        title="Approvals"
        description="Executions paused on a wait_for_input step. Send the decision signal to resume."
        actions={<PageMeta updatedAt={updatedAt} onRefresh={refresh} />}
      />

      <Glossary items={PAGE_GLOSSARY} />

      {error && <div className="notice notice-warn">{error}</div>}

      <Section
        eyebrow="Inbox"
        title={items.length > 0 ? `${items.length} waiting` : "No pending approvals"}
        description={
          <>
            Each row is one execution paused on a{" "}
            <strong className="text-ink">wait_for_input</strong> step. Click a
            choice to send the{" "}
            <strong className="text-ink">human_input:&#123;block_id&#125;</strong>{" "}
            signal — the execution resumes with that value as the block's
            output.
          </>
        }
        meta={
          items.length > 0 ? (
            <span>
              <span className="text-faint">WAITING</span>{" "}
              <span className="text-ink-dim">{items.length}</span>
            </span>
          ) : undefined
        }
      >
        {loading && !data ? (
          <SkeletonTable rows={4} cols={7} />
        ) : (
          <Table>
            <THead>
              <TH>Instance</TH>
              <TH>Sequence</TH>
              <TH>Block</TH>
              <TH>Prompt</TH>
              <TH>Waiting</TH>
              <TH>Deadline</TH>
              <TH>Respond</TH>
            </THead>
            <tbody>
              {items.map((item) => {
                const key = `${item.instance_id}:${item.block_id}`;
                return (
                  <TR key={key}>
                    <TD className="align-top">
                      <Link
                        to={`/instances/${item.instance_id}`}
                        className="hover:text-fg"
                      >
                        <Id value={item.instance_id} />
                      </Link>
                    </TD>
                    <TD className="align-top">
                      <Link
                        to={`/sequences/${item.sequence_id}`}
                        className="text-[13px] hover:text-fg"
                      >
                        {item.sequence_name}
                      </Link>
                    </TD>
                    <TD className="font-mono text-[12px] text-muted align-top">
                      {item.block_id}
                    </TD>
                    <TD className="align-top" title={item.prompt}>
                      <span className="block max-w-[32ch] truncate text-[13px]">
                        {item.prompt}
                      </span>
                    </TD>
                    <TD className="align-top">
                      <Relative at={item.waiting_since} />
                    </TD>
                    <TD className="align-top">
                      {item.deadline ? (
                        <Relative at={item.deadline} />
                      ) : (
                        <span className="text-muted text-[12px]">—</span>
                      )}
                    </TD>
                    <TD className="align-top">
                      <div className="flex flex-wrap gap-2">
                        {item.choices.map((c) => (
                          <Button
                            key={c.value}
                            variant="ghost"
                            size="sm"
                            disabled={busyKey === key}
                            onClick={() => respond(item, c)}
                          >
                            {c.label}
                          </Button>
                        ))}
                      </div>
                    </TD>
                  </TR>
                );
              })}
              {items.length === 0 && (
                <Empty colSpan={99}>
                  Inbox zero — no executions are waiting on human input.
                </Empty>
              )}
            </tbody>
          </Table>
        )}
      </Section>
    </div>
  );
}
