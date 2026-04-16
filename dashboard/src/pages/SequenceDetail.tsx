import { useEffect, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { getSequence, type SequenceDefinition } from "../api";

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
    <div className={depth > 0 ? "ml-4 border-l border-border pl-3" : ""}>
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
          className={`text-muted text-xs w-4 ${!hasChildren ? "opacity-0" : ""}`}
        >
          {open ? "▼" : "▶"}
        </button>
        <span className="font-mono text-sm">{block.id ?? "(no id)"}</span>
        <span className="bg-secondary/60 text-muted text-xs font-mono px-1.5 py-0.5 rounded">
          {block.type}
        </span>
        {block.handler && (
          <span className="text-xs text-muted font-mono">→ {block.handler}</span>
        )}
      </div>
      {open && (
        <>
          {block.branches?.map((br, i) => (
            <div key={`br-${i}`} className="ml-4">
              <div className="text-xs text-muted py-1">branch {i}</div>
              <BlockList blocks={br} depth={1} />
            </div>
          ))}
          {block.body && (
            <>
              <div className="ml-4 text-xs text-muted pt-1">body</div>
              <BlockList blocks={block.body} depth={1} />
            </>
          )}
          {block.try_block && (
            <>
              <div className="ml-4 text-xs text-muted pt-1">try</div>
              <BlockList blocks={block.try_block} depth={1} />
            </>
          )}
          {block.catch_block && (
            <>
              <div className="ml-4 text-xs text-danger pt-1">catch</div>
              <BlockList blocks={block.catch_block} depth={1} />
            </>
          )}
          {block.finally_block && (
            <>
              <div className="ml-4 text-xs text-muted pt-1">finally</div>
              <BlockList blocks={block.finally_block} depth={1} />
            </>
          )}
          {block.routes?.map((r, i) => (
            <div key={`r-${i}`} className="ml-4">
              <div className="text-xs text-muted py-1">
                when <span className="font-mono">{r.condition}</span>
              </div>
              <BlockList blocks={r.blocks} depth={1} />
            </div>
          ))}
          {block.default && (
            <>
              <div className="ml-4 text-xs text-muted pt-1">default</div>
              <BlockList blocks={block.default} depth={1} />
            </>
          )}
          {block.variants?.map((v, i) => (
            <div key={`v-${i}`} className="ml-4">
              <div className="text-xs text-muted py-1">
                variant <span className="font-mono">{v.name}</span> (weight {v.weight})
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

  if (!id) return <div className="text-danger">Missing sequence id</div>;

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-3">
        <Link to="/sequences" className="text-muted hover:text-foreground text-sm">
          ← Sequences
        </Link>
        <span className="text-muted">/</span>
        <span className="font-mono text-sm">{id}</span>
      </div>

      {error && (
        <div className="rounded border border-danger/40 bg-danger/10 text-danger p-3 text-sm">
          {error}
        </div>
      )}

      {seq && (
        <>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4 bg-card border border-border rounded-lg p-4">
            <Stat label="Name" mono>
              {seq.name}
            </Stat>
            <Stat label="Version" mono>
              v{seq.version}
            </Stat>
            <Stat label="Namespace" mono>
              {seq.namespace}
            </Stat>
            <Stat label="Tenant" mono>
              {seq.tenant_id}
            </Stat>
            <Stat label="Status">
              {seq.deprecated ? (
                <span className="text-warning">deprecated</span>
              ) : (
                <span className="text-success">active</span>
              )}
            </Stat>
            <Stat label="Top-level blocks">{seq.blocks.length}</Stat>
            <Stat label="Total nodes">{countNodes(seq.blocks as BlockLike[])}</Stat>
            <Stat label="Created">{new Date(seq.created_at).toLocaleString()}</Stat>
          </div>

          <section>
            <h2 className="text-lg font-semibold mb-2">Block tree</h2>
            <div className="bg-card border border-border rounded-lg p-3">
              <BlockList blocks={seq.blocks as BlockLike[]} />
            </div>
          </section>
        </>
      )}
    </div>
  );
}

function Stat({
  label,
  children,
  mono,
}: {
  label: string;
  children: React.ReactNode;
  mono?: boolean;
}) {
  return (
    <div>
      <div className="text-muted text-xs mb-0.5">{label}</div>
      <div className={mono ? "font-mono text-sm" : "text-sm"}>{children}</div>
    </div>
  );
}
