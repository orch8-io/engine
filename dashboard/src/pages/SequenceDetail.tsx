import { useEffect, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { getSequence, type SequenceDefinition } from "../api";
import { PageHeader } from "../components/ui/PageHeader";
import { Section } from "../components/ui/Section";
import { Glossary, type GlossaryItem } from "../components/ui/Glossary";
import { Badge } from "../components/ui/Badge";
import { Id } from "../components/ui/Mono";
import { IconChevronDown, IconChevronRight } from "../components/ui/Icons";

interface BlockLike {
  type?: string;
  id?: string;
  handler?: string;
  branches?: BlockLike[][];
  body?: BlockLike[];
  try_block?: BlockLike[];
  catch_block?: BlockLike[];
  finally_block?: BlockLike[];
  routes?: Array<{ condition: string; blocks: BlockLike[] }>;
  default?: BlockLike[];
  variants?: Array<{ name: string; weight: number; blocks: BlockLike[] }>;
  blocks?: BlockLike[];
  [k: string]: unknown;
}

const PAGE_GLOSSARY: GlossaryItem[] = [
  {
    term: "Sequence definition",
    definition:
      "The blueprint for a workflow. A directed graph of blocks with the retry, concurrency, and branching rules the engine uses when starting new executions.",
  },
  {
    term: "Version",
    definition:
      "A numbered snapshot of the definition. Once executions are running against a version it is immutable — new code deploys a new version.",
  },
  {
    term: "active / deprecated",
    definition:
      "Deprecated versions still run in-flight executions but new executions are not started against them. Used for safe rolling retirement.",
  },
  {
    term: "Block",
    definition:
      "One node in the graph. Either runs a handler, routes control flow, or coordinates child blocks (parallel, loops, try/catch, switch, A/B).",
  },
  {
    term: "Handler",
    definition:
      "The worker-side function name a block dispatches to. The engine routes tasks to whichever worker has registered this handler.",
  },
  {
    term: "Top-level blocks / Total nodes",
    definition:
      "Top-level blocks are the root of the DAG. Total nodes counts every block, including those nested inside branches, bodies, try/catch, switch routes and A/B variants.",
  },
  {
    term: "branch / body / try / catch / finally",
    definition: (
      <>
        Structural containers inside a block. <code className="font-mono">branches</code> run in parallel;{" "}
        <code className="font-mono">body</code> is a loop or group's children;{" "}
        <code className="font-mono">try / catch / finally</code> follow the usual error-handling semantics.
      </>
    ),
  },
  {
    term: "routes / default",
    definition:
      "Belong to a switch block. Each route's condition is evaluated in order; default runs if none match.",
  },
  {
    term: "variants",
    definition:
      "Belong to an A/B block. Each execution picks one variant weighted by the given number — useful for canaries and experiments.",
  },
];

function countNodes(blocks: BlockLike[]): number {
  let n = 0;
  for (const b of blocks) {
    n++;
    if (b.branches) for (const br of b.branches) n += countNodes(br);
    if (b.body) n += countNodes(b.body);
    if (b.try_block) n += countNodes(b.try_block);
    if (b.catch_block) n += countNodes(b.catch_block);
    if (b.finally_block) n += countNodes(b.finally_block);
    if (b.routes) for (const r of b.routes) n += countNodes(r.blocks);
    if (b.default) n += countNodes(b.default);
    if (b.variants) for (const v of b.variants) n += countNodes(v.blocks);
    if (b.blocks) n += countNodes(b.blocks);
  }
  return n;
}

function BlockList({ blocks, depth = 0 }: { blocks: BlockLike[]; depth?: number }) {
  return (
    <div className={depth > 0 ? "ml-4 border-l border-rule pl-3" : ""}>
      {blocks.map((b, i) => (
        <BlockView key={`${b.id ?? i}`} block={b} />
      ))}
    </div>
  );
}

function BlockView({ block }: { block: BlockLike }) {
  const [open, setOpen] = useState(true);
  const hasChildren = !!(
    block.branches ||
    block.body ||
    block.try_block ||
    block.catch_block ||
    block.finally_block ||
    block.routes ||
    block.default ||
    block.variants ||
    block.blocks
  );

  return (
    <div className="py-1">
      <div className="flex items-center gap-2">
        <button
          onClick={() => setOpen((o) => !o)}
          disabled={!hasChildren}
          className={`text-muted hover:text-ink w-4 shrink-0 ${
            !hasChildren ? "opacity-0 cursor-default" : ""
          }`}
          aria-label={open ? "collapse" : "expand"}
          title={open ? "Collapse children" : "Expand children"}
        >
          {open ? <IconChevronDown size={12} /> : <IconChevronRight size={12} />}
        </button>
        <span
          className="font-mono text-[13px] text-ink"
          title="Block id — stable identifier used in execution trees and logs"
        >
          {block.id ?? "(no id)"}
        </span>
        <span
          className="bg-sunken border border-rule text-muted text-[10px] font-mono uppercase tracking-wider px-1.5 py-0.5"
          title="Block type — determines how the engine evaluates this node"
        >
          {block.type}
        </span>
        {block.handler && (
          <span
            className="text-[11px] text-muted font-mono"
            title="Handler — the worker-side function this block dispatches to"
          >
            → {block.handler}
          </span>
        )}
      </div>
      {open && (
        <>
          {block.branches?.map((br, i) => (
            <div key={`br-${i}`} className="ml-4">
              <div
                className="text-[10px] text-faint font-mono uppercase tracking-wider py-1"
                title="Parallel branch — all branches run concurrently"
              >
                branch {i}
              </div>
              <BlockList blocks={br} depth={1} />
            </div>
          ))}
          {block.body && (
            <>
              <div className="ml-4 text-[10px] text-faint font-mono uppercase tracking-wider pt-1">
                body
              </div>
              <BlockList blocks={block.body} depth={1} />
            </>
          )}
          {block.try_block && (
            <>
              <div className="ml-4 text-[10px] text-faint font-mono uppercase tracking-wider pt-1">
                try
              </div>
              <BlockList blocks={block.try_block} depth={1} />
            </>
          )}
          {block.catch_block && (
            <>
              <div className="ml-4 text-[10px] text-warn font-mono uppercase tracking-wider pt-1">
                catch
              </div>
              <BlockList blocks={block.catch_block} depth={1} />
            </>
          )}
          {block.finally_block && (
            <>
              <div className="ml-4 text-[10px] text-faint font-mono uppercase tracking-wider pt-1">
                finally
              </div>
              <BlockList blocks={block.finally_block} depth={1} />
            </>
          )}
          {block.routes?.map((r, i) => (
            <div key={`r-${i}`} className="ml-4">
              <div
                className="text-[10px] text-faint font-mono uppercase tracking-wider py-1"
                title="Switch route — first matching condition wins"
              >
                when <span className="text-ink-dim normal-case tracking-normal">{r.condition}</span>
              </div>
              <BlockList blocks={r.blocks} depth={1} />
            </div>
          ))}
          {block.default && (
            <>
              <div
                className="ml-4 text-[10px] text-faint font-mono uppercase tracking-wider pt-1"
                title="Fallback route when no switch condition matches"
              >
                default
              </div>
              <BlockList blocks={block.default} depth={1} />
            </>
          )}
          {block.variants?.map((v, i) => (
            <div key={`v-${i}`} className="ml-4">
              <div
                className="text-[10px] text-faint font-mono uppercase tracking-wider py-1"
                title="A/B variant — probability is weight ÷ sum of all weights in this block"
              >
                variant{" "}
                <span className="text-ink-dim normal-case tracking-normal">{v.name}</span>{" "}
                (weight {v.weight})
              </div>
              <BlockList blocks={v.blocks} depth={1} />
            </div>
          ))}
          {block.blocks && <BlockList blocks={block.blocks} depth={1} />}
        </>
      )}
    </div>
  );
}

export default function SequenceDetail() {
  const { id } = useParams<{ id: string }>();
  const [seq, setSeq] = useState<SequenceDefinition | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!id) return;
    getSequence(id)
      .then(setSeq)
      .catch((e) => setError(e instanceof Error ? e.message : String(e)));
  }, [id]);

  if (!id)
    return <div className="notice notice-warn">Missing sequence id</div>;

  const totalNodes = seq ? countNodes(seq.blocks as BlockLike[]) : 0;

  return (
    <div className="space-y-12">
      <PageHeader
        eyebrow={
          <Link to="/sequences" className="hover:text-ink transition-colors">
            ← Sequences
          </Link>
        }
        title={
          seq ? (
            <span className="font-mono tracking-tight">
              {seq.name} <span className="text-muted">· v{seq.version}</span>
            </span>
          ) : (
            <span className="font-mono text-muted">{id.slice(0, 8)}…</span>
          )
        }
        description={
          seq
            ? "The full shape of this sequence version — metadata at the top, the block DAG below. Anything runnable by the engine is encoded in this tree."
            : "Loading sequence definition…"
        }
        actions={
          seq &&
          (seq.deprecated ? (
            <Badge tone="hold">deprecated</Badge>
          ) : (
            <Badge tone="ok">active</Badge>
          ))
        }
      />

      {error && <div className="notice notice-warn">{error}</div>}

      {seq && (
        <>
          <Glossary items={PAGE_GLOSSARY} />

          <Section
            eyebrow="Metadata"
            title="Identity & shape"
            description={
              <>
                Versioned identity of this definition. Each deploy mints a new{" "}
                <strong className="text-ink">version</strong>; in-flight executions
                keep running against the version they started on, so older versions
                stay readable here even after deprecation.
              </>
            }
            annotation={
              <>
                <strong className="text-ink">Tenant / namespace.</strong> Sequences
                are isolated by <code className="font-mono">tenant_id</code> and{" "}
                <code className="font-mono">namespace</code>. Two sequences with
                the same name in different namespaces are unrelated.
                <br />
                <br />
                <strong className="text-ink">Total nodes</strong> counts every
                block in the DAG, recursing into branches, bodies, try/catch,
                switch routes and A/B variants. The handler count inside your
                worker needs to cover every leaf.
              </>
            }
          >
            <div>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-x-8 gap-y-6">
                <Stat
                  label="Name"
                  hint="Stable across versions"
                  mono
                >
                  {seq.name}
                </Stat>
                <Stat
                  label="Version"
                  hint="Incremented each deploy"
                  mono
                >
                  v{seq.version}
                </Stat>
                <Stat
                  label="Namespace"
                  hint="Isolation scope"
                  mono
                >
                  {seq.namespace}
                </Stat>
                <Stat
                  label="Tenant"
                  hint="Copy to cross-reference"
                >
                  <Id value={seq.tenant_id} copy className="!text-[12px]" />
                </Stat>
                <Stat
                  label="Top-level blocks"
                  hint="Roots of the DAG"
                  mono
                >
                  {seq.blocks.length}
                </Stat>
                <Stat
                  label="Total nodes"
                  hint="Including nested blocks"
                  mono
                >
                  {totalNodes}
                </Stat>
                <Stat
                  label="Created"
                  hint="When this version was registered"
                >
                  <span className="text-[12px] font-mono tabular text-ink-dim">
                    {new Date(seq.created_at).toLocaleString()}
                  </span>
                </Stat>
                <Stat label="Status" hint="Accepts new executions?">
                  {seq.deprecated ? (
                    <Badge tone="hold">deprecated</Badge>
                  ) : (
                    <Badge tone="ok">active</Badge>
                  )}
                </Stat>
              </div>
            </div>
          </Section>

          <Section
            eyebrow="Definition"
            title="Block DAG"
            description={
              <>
                The full tree of blocks the engine walks when executing this
                sequence. Expand / collapse with the carets.{" "}
                <strong className="text-ink">Block id</strong> is the label you'll
                see in execution trees and logs; the{" "}
                <strong className="text-ink">uppercase tag</strong> is the block
                type; the <strong className="text-ink">→ handler</strong> is the
                worker function a dispatch block will call.
              </>
            }
            meta={
              <>
                <span>
                  <span className="text-faint">ROOTS</span>{" "}
                  <span className="text-ink-dim">{seq.blocks.length}</span>
                </span>
                <span>
                  <span className="text-faint">NODES</span>{" "}
                  <span className="text-ink-dim">{totalNodes}</span>
                </span>
              </>
            }
            annotation={
              <>
                <strong className="text-ink">Control-flow containers.</strong>{" "}
                <code className="font-mono">branches</code> run in parallel;{" "}
                <code className="font-mono">body</code> is a loop's children;{" "}
                <code className="font-mono">try/catch/finally</code> is error
                handling; <code className="font-mono">when</code> /{" "}
                <code className="font-mono">default</code> are switch routes;{" "}
                <code className="font-mono">variant</code> lines are A/B variants
                with their weights.
                <br />
                <br />
                <strong className="text-ink">Leaves are dispatch blocks.</strong>{" "}
                The <code className="font-mono">→ handler</code> suffix identifies
                which worker function each leaf calls. A sequence cannot run a
                handler that isn't registered by at least one live worker.
              </>
            }
          >
            <BlockList blocks={seq.blocks as BlockLike[]} />
          </Section>
        </>
      )}
    </div>
  );
}

function Stat({
  label,
  children,
  mono,
  hint,
}: {
  label: string;
  children: React.ReactNode;
  mono?: boolean;
  hint?: string;
}) {
  return (
    <div className="min-w-0">
      <div className="field-label mb-1">{label}</div>
      <div
        className={`truncate ${mono ? "font-mono text-[12px] text-ink" : "text-[13px] text-ink"}`}
      >
        {children}
      </div>
      {hint && <div className="annotation mt-1">{hint}</div>}
    </div>
  );
}
