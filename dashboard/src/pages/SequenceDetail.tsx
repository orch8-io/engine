import { useEffect, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { getSequence, type SequenceDefinition } from "../api";
import { PageHeader } from "../components/ui/PageHeader";
import { Panel, PanelBody, PanelHeader } from "../components/ui/Panel";
import { Badge } from "../components/ui/Badge";
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
    <div className={depth > 0 ? "ml-4 border-l border-hairline pl-3" : ""}>
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
          className={`text-muted hover:text-fg w-4 shrink-0 ${
            !hasChildren ? "opacity-0 cursor-default" : ""
          }`}
          aria-label={open ? "collapse" : "expand"}
        >
          {open ? <IconChevronDown size={12} /> : <IconChevronRight size={12} />}
        </button>
        <span className="font-mono text-[13px]">{block.id ?? "(no id)"}</span>
        <span className="bg-sunken border border-hairline text-muted text-[11px] font-mono px-1.5 py-0.5 rounded-sm">
          {block.type}
        </span>
        {block.handler && (
          <span className="text-[11px] text-muted font-mono">→ {block.handler}</span>
        )}
      </div>
      {open && (
        <>
          {block.branches?.map((br, i) => (
            <div key={`br-${i}`} className="ml-4">
              <div className="text-[11px] text-muted font-mono py-1">branch {i}</div>
              <BlockList blocks={br} depth={1} />
            </div>
          ))}
          {block.body && (
            <>
              <div className="ml-4 text-[11px] text-muted font-mono pt-1">body</div>
              <BlockList blocks={block.body} depth={1} />
            </>
          )}
          {block.try_block && (
            <>
              <div className="ml-4 text-[11px] text-muted font-mono pt-1">try</div>
              <BlockList blocks={block.try_block} depth={1} />
            </>
          )}
          {block.catch_block && (
            <>
              <div className="ml-4 text-[11px] text-warn font-mono pt-1">catch</div>
              <BlockList blocks={block.catch_block} depth={1} />
            </>
          )}
          {block.finally_block && (
            <>
              <div className="ml-4 text-[11px] text-muted font-mono pt-1">finally</div>
              <BlockList blocks={block.finally_block} depth={1} />
            </>
          )}
          {block.routes?.map((r, i) => (
            <div key={`r-${i}`} className="ml-4">
              <div className="text-[11px] text-muted font-mono py-1">
                when <span className="text-fg-dim">{r.condition}</span>
              </div>
              <BlockList blocks={r.blocks} depth={1} />
            </div>
          ))}
          {block.default && (
            <>
              <div className="ml-4 text-[11px] text-muted font-mono pt-1">default</div>
              <BlockList blocks={block.default} depth={1} />
            </>
          )}
          {block.variants?.map((v, i) => (
            <div key={`v-${i}`} className="ml-4">
              <div className="text-[11px] text-muted font-mono py-1">
                variant <span className="text-fg-dim">{v.name}</span> (weight {v.weight})
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
    return (
      <div className="rounded-md border border-warn/40 bg-warn/10 text-warn p-3 text-[13px]">
        Missing sequence id
      </div>
    );

  return (
    <div className="space-y-6">
      <PageHeader
        eyebrow={
          <Link to="/sequences" className="hover:text-fg transition-colors">
            ← Sequences
          </Link>
        }
        title={
          <span className="font-mono text-[20px] tracking-tight">
            {seq ? `${seq.name} · v${seq.version}` : id.slice(0, 8) + "…"}
          </span>
        }
        description={
          <span className="font-mono text-[11px] text-faint">{id}</span>
        }
        actions={
          seq && (seq.deprecated ? (
            <Badge tone="hold">deprecated</Badge>
          ) : (
            <Badge tone="ok">active</Badge>
          ))
        }
      />

      {error && (
        <div className="rounded-md border border-warn/40 bg-warn/10 text-warn p-3 text-[13px]">
          {error}
        </div>
      )}

      {seq && (
        <>
          <Panel>
            <PanelBody>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-5">
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
                <Stat label="Top-level blocks" mono>
                  {seq.blocks.length}
                </Stat>
                <Stat label="Total nodes" mono>
                  {countNodes(seq.blocks as BlockLike[])}
                </Stat>
                <Stat label="Created">
                  <span className="text-[13px] tabular">
                    {new Date(seq.created_at).toLocaleString()}
                  </span>
                </Stat>
              </div>
            </PanelBody>
          </Panel>

          <Panel>
            <PanelHeader>
              <span className="eyebrow">Block tree</span>
            </PanelHeader>
            <PanelBody>
              <BlockList blocks={seq.blocks as BlockLike[]} />
            </PanelBody>
          </Panel>
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
    <div className="min-w-0">
      <div className="eyebrow mb-1">{label}</div>
      <div className={`truncate ${mono ? "font-mono text-[12px]" : "text-[13px]"}`}>
        {children}
      </div>
    </div>
  );
}
