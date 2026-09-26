import { useEffect, useMemo, useState, type KeyboardEvent, type ReactNode } from "react";
import { getSequenceByName, preflightDraft, type PreflightReport, type PreflightStatus } from "../api";
import {
  BLOCK_TYPES,
  blockId,
  blockSlots,
  blockType,
  collectIds,
  duplicateBlock,
  getAt,
  insertBlock,
  isObject,
  listPosition,
  localIssues,
  moveBlock,
  newBlock,
  parseSequenceText,
  pathKey,
  preflightBody,
  removeAt,
  removeBlock,
  serializeSequence,
  setAt,
  slotAdditions,
  type BlockType,
  type Json,
  type JsonObject,
  type Path,
} from "../lib/sequenceModel";
import { BLOCK_SCHEMAS, schemaFields } from "../lib/schemaForm";
import { SchemaForm } from "./SchemaForm";
import { Badge } from "./ui/Badge";
import { Button } from "./ui/Button";
import { FieldLabel, Input, Select } from "./ui/Input";

const TEXTAREA =
  "w-full bg-sunken border border-hairline rounded-sm px-2.5 py-2 text-[12px] font-mono text-fg placeholder:text-faint focus:border-signal focus:outline-none";

const PREFLIGHT_TONE: Record<PreflightStatus, "ok" | "hold" | "dim" | "warn"> = {
  pass: "ok",
  warning: "hold",
  unknown: "dim",
  fail: "warn",
};

/** Sequence-level fields editable when no block is selected. */
const SEQUENCE_SCHEMA = {
  type: "object",
  required: ["name", "tenant_id", "namespace"],
  properties: {
    name: { type: "string" },
    tenant_id: { type: "string" },
    namespace: { type: "string" },
    input_schema: { type: "object", description: "JSON Schema for context.data; drives the Run form." },
    sla: { type: "object", description: "{ max_runtime, max_step_runtime } in ms (alert-only)." },
  },
};

/** Keys rendered structurally (graph) rather than as form fields. */
const STRUCTURAL = new Set([
  "type",
  "branches",
  "body",
  "try_block",
  "catch_block",
  "finally_block",
  "routes",
  "default",
  "variants",
  "blocks",
  "steps",
  "input",
]);

interface EditorCtx {
  doc: JsonObject;
  selected: string | null;
  issues: Map<string, string[]>;
  select: (path: Path) => void;
  update: (fn: (doc: JsonObject) => JsonObject) => void;
}

/**
 * Two-way visual ↔ JSON sequence editor. The JSON text is authoritative:
 * visual edits rewrite it through copy-on-write path updates (unknown fields
 * survive), and the visual tab is disabled while the text does not parse.
 */
export function SequenceEditor({
  text,
  onTextChange,
  preflight = true,
  rows = 22,
}: {
  text: string;
  onTextChange: (text: string) => void;
  preflight?: boolean;
  rows?: number;
}) {
  const parsed = useMemo(() => parseSequenceText(text), [text]);
  const [tab, setTab] = useState<"visual" | "json">(parsed.ok ? "visual" : "json");
  const [selectedPath, setSelectedPath] = useState<Path | null>(null);
  const [history, setHistory] = useState<string[]>([]);

  const doc = parsed.ok ? parsed.value : null;
  const issues = useMemo(() => (doc ? localIssues(doc) : []), [doc]);
  const issuesByPath = useMemo(() => {
    const m = new Map<string, string[]>();
    for (const i of issues) {
      const k = pathKey(i.path);
      m.set(k, [...(m.get(k) ?? []), i.message]);
    }
    return m;
  }, [issues]);

  // Drop a selection that no longer points at a block (after delete / JSON edit).
  const selected = doc && selectedPath && isObject(getAt(doc, selectedPath)) ? selectedPath : null;

  // Form drafts in the side panel must reset when the text changes from
  // outside the visual editor (JSON tab, template pick, undo) but not on the
  // editor's own writes, or typing would lose focus on every keystroke.
  const [emitted, setEmitted] = useState<string | null>(null);
  const [seenText, setSeenText] = useState(text);
  const [revision, setRevision] = useState(0);
  if (text !== seenText) {
    setSeenText(text);
    if (text !== emitted) setRevision((r) => r + 1);
  }

  const update = (fn: (d: JsonObject) => JsonObject) => {
    if (!doc) return;
    const next = serializeSequence(fn(doc));
    if (next === text) return;
    setHistory((h) => [...h.slice(-49), text]);
    setEmitted(next);
    onTextChange(next);
  };

  const undo = () => {
    const prev = history[history.length - 1];
    if (prev === undefined) return;
    setHistory((h) => h.slice(0, -1));
    onTextChange(prev);
  };

  const ctx: EditorCtx | null = doc
    ? { doc, selected: selected ? pathKey(selected) : null, issues: issuesByPath, select: setSelectedPath, update }
    : null;

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-2 flex-wrap">
        <div role="tablist" aria-label="Editor mode" className="flex border-b border-rule">
          {(["visual", "json"] as const).map((t) => (
            <button
              key={t}
              role="tab"
              type="button"
              aria-selected={tab === t}
              disabled={t === "visual" && !parsed.ok}
              title={t === "visual" && !parsed.ok ? "Fix the JSON first — the JSON tab is authoritative" : undefined}
              onClick={() => setTab(t)}
              className={`px-4 py-2 text-[12px] font-medium uppercase tracking-wider border-b-2 transition-colors disabled:opacity-40 ${
                tab === t ? "border-signal text-ink" : "border-transparent text-muted hover:text-ink"
              }`}
            >
              {t === "visual" ? "Visual" : "JSON"}
            </button>
          ))}
        </div>
        <Button size="sm" variant="ghost" disabled={history.length === 0} onClick={undo} title="Undo the last edit">
          Undo
        </Button>
        <span className="ml-auto text-[11px] text-muted">
          {parsed.ok ? (
            issues.length === 0 ? (
              <span className="text-ok">structure OK</span>
            ) : (
              <span className="text-warn">{issues.length} structural issue{issues.length === 1 ? "" : "s"}</span>
            )
          ) : (
            <span className="text-warn">invalid JSON</span>
          )}
        </span>
      </div>

      {tab === "json" || !ctx ? (
        <div>
          <textarea
            aria-label="Sequence definition JSON"
            value={text}
            onChange={(e) => {
              setHistory((h) => [...h.slice(-49), text]);
              onTextChange(e.target.value);
            }}
            rows={rows}
            spellCheck={false}
            className={TEXTAREA}
          />
          {!parsed.ok && (
            <p role="alert" className="text-warn text-[12px] mt-1">
              {parsed.error}
            </p>
          )}
        </div>
      ) : (
        <div className="grid grid-cols-1 xl:grid-cols-3 gap-6">
          <div className="xl:col-span-2 min-w-0 space-y-6" aria-label="Sequence graph">
            <p className="annotation text-[11px]">
              Click a block to edit it. Keyboard: Tab to a block, Enter to select,{" "}
              <kbd className="font-mono">Alt+↑/↓</kbd> to reorder.
            </p>
            <RootList ctx={ctx} listKey="blocks" label="blocks" />
            {Array.isArray(ctx.doc.on_failure) && <RootList ctx={ctx} listKey="on_failure" label="on_failure (cleanup)" />}
            {Array.isArray(ctx.doc.on_cancel) && <RootList ctx={ctx} listKey="on_cancel" label="on_cancel (cleanup)" />}
            <div className="flex gap-2 flex-wrap">
              {!Array.isArray(ctx.doc.on_failure) && (
                <Button size="sm" variant="ghost" onClick={() => update((d) => setAt(d, ["on_failure"], []))}>
                  + on_failure handlers
                </Button>
              )}
              {!Array.isArray(ctx.doc.on_cancel) && (
                <Button size="sm" variant="ghost" onClick={() => update((d) => setAt(d, ["on_cancel"], []))}>
                  + on_cancel handlers
                </Button>
              )}
            </div>
          </div>
          <aside className="min-w-0 border border-hairline rounded-sm p-4 space-y-4 self-start" aria-label="Properties">
            {selected ? (
              <BlockInspector
                key={`${revision}:${pathKey(selected)}`}
                ctx={ctx}
                path={selected}
                onDeselect={() => setSelectedPath(null)}
              />
            ) : (
              <SequenceInspector key={revision} ctx={ctx} />
            )}
          </aside>
        </div>
      )}

      {issues.length > 0 && parsed.ok && (
        <ul className="notice notice-warn text-[12px] space-y-0.5" aria-label="Structural issues">
          {issues.slice(0, 12).map((i, n) => (
            <li key={n}>
              <button type="button" className="underline-offset-2 hover:underline text-left" onClick={() => { setSelectedPath(i.path); setTab("visual"); }}>
                {i.message}
              </button>
            </li>
          ))}
        </ul>
      )}

      {preflight && <PreflightPanel doc={doc} />}
    </div>
  );
}

// ─── Graph ──────────────────────────────────────────────────────────────────

function RootList({ ctx, listKey, label }: { ctx: EditorCtx; listKey: string; label: string }) {
  return (
    <div>
      <div className="eyebrow mb-2 flex items-center gap-2">
        {label}
        {listKey !== "blocks" && (
          <button
            type="button"
            className="text-faint hover:text-warn normal-case tracking-normal"
            aria-label={`Remove ${listKey}`}
            onClick={() => ctx.update((d) => removeAt(d, [listKey]))}
          >
            ✕
          </button>
        )}
      </div>
      <BlockListView ctx={ctx} listPath={[listKey]} />
    </div>
  );
}

function BlockListView({ ctx, listPath }: { ctx: EditorCtx; listPath: Path }) {
  const items = getAt(ctx.doc, listPath);
  const list = Array.isArray(items) ? items : [];
  return (
    <ol className="space-y-0">
      {list.map((_, i) => (
        <li key={i} className="flex flex-col items-stretch">
          {i > 0 && <span aria-hidden className="self-center w-px h-3 bg-rule" />}
          <BlockNode ctx={ctx} path={[...listPath, i]} />
        </li>
      ))}
      <li className={list.length > 0 ? "pt-2" : ""}>
        <AddBlock
          label={`Add block to ${listPath.join(".")}`}
          onAdd={(type) => {
            ctx.update((d) => insertBlock(d, listPath, list.length, newBlock(type, collectIds(d))));
            ctx.select([...listPath, list.length]);
          }}
        />
      </li>
    </ol>
  );
}

function AddBlock({ label, onAdd }: { label: string; onAdd: (t: BlockType) => void }) {
  return (
    <Select
      aria-label={label}
      value=""
      onChange={(e) => {
        if (e.target.value) onAdd(e.target.value as BlockType);
      }}
      className="text-[12px] text-muted border-dashed"
    >
      <option value="">+ add block…</option>
      {BLOCK_TYPES.map((b) => (
        <option key={b.type} value={b.type}>
          {b.label} — {b.hint}
        </option>
      ))}
    </Select>
  );
}

function summary(block: JsonObject): string {
  switch (block.type) {
    case "step":
      return typeof block.handler === "string" ? `→ ${block.handler}` : "";
    case "sub_sequence":
      return `→ ${String(block.sequence_name ?? "?")}${typeof block.version === "number" ? ` v${block.version}` : ""}`;
    case "loop":
      return `while ${String(block.condition ?? "")}`;
    case "for_each":
      return `${String(block.item_var ?? "item")} in ${String(block.collection ?? "")}`;
    case "race":
      return String(block.semantics ?? "first_to_resolve");
    default:
      return "";
  }
}

function BlockNode({ ctx, path }: { ctx: EditorCtx; path: Path }) {
  const block = getAt(ctx.doc, path);
  if (!isObject(block)) {
    return <div className="text-warn text-[12px] font-mono">invalid block at {path.join(".")}</div>;
  }
  const key = pathKey(path);
  const isSelected = ctx.selected === key;
  const problems = ctx.issues.get(key);
  const slots = blockSlots(block);
  const additions = slotAdditions(block);
  const sideBySide = ["parallel", "race", "ab_split"].includes(String(block.type));
  const pos = listPosition(path);

  const onKey = (e: KeyboardEvent<HTMLButtonElement>) => {
    if (!pos || !e.altKey) return;
    const siblings = getAt(ctx.doc, pos.listPath);
    const len = Array.isArray(siblings) ? siblings.length : 0;
    const to = e.key === "ArrowUp" ? pos.index - 1 : e.key === "ArrowDown" ? pos.index + 1 : null;
    if (to === null || to < 0 || to >= len) return;
    e.preventDefault();
    ctx.update((d) => moveBlock(d, pos.listPath, pos.index, to));
    ctx.select([...pos.listPath, to]);
  };

  return (
    <div
      className={`border rounded-sm bg-surface ${
        isSelected ? "border-signal" : problems ? "border-warn" : "border-hairline"
      }`}
    >
      <button
        type="button"
        aria-pressed={isSelected}
        aria-label={`${blockType(block)} block ${blockId(block) || "(no id)"}${problems ? ` — ${problems.join("; ")}` : ""}`}
        onClick={() => ctx.select(path)}
        onKeyDown={onKey}
        className="w-full text-left px-2.5 py-1.5 flex items-center gap-2 flex-wrap focus:outline-none focus-visible:ring-2 focus-visible:ring-signal"
      >
        <span className="bg-sunken border border-rule text-muted text-[10px] font-mono uppercase tracking-wider px-1.5 py-0.5">
          {blockType(block)}
        </span>
        <span className="font-mono text-[13px] text-ink">{blockId(block) || "(no id)"}</span>
        <span className="font-mono text-[11px] text-muted truncate">{summary(block)}</span>
        {typeof block.when === "string" && block.when && (
          <span className="font-mono text-[10px] text-hold border border-hold/40 px-1" title="Guard: step is skipped when false">
            when {block.when}
          </span>
        )}
        {problems && <span className="text-warn text-[11px] ml-auto">⚠ {problems.length}</span>}
      </button>
      {(slots.length > 0 || additions.length > 0) && (
        <div className="px-2.5 pb-2.5">
          <div className={sideBySide ? "flex gap-2 overflow-x-auto" : "space-y-2"}>
            {slots.map((slot) => {
              const slotPath = [...path, ...slot.path];
              return (
                <div key={pathKey(slot.path)} className={`border-l border-rule pl-2 ${sideBySide ? "min-w-56 flex-1" : ""}`}>
                  <div className="flex items-center gap-2 py-1">
                    <span className="text-[10px] text-faint font-mono uppercase tracking-wider">{slot.label}</span>
                    {slot.removePath && (
                      <button
                        type="button"
                        className="text-faint hover:text-warn text-[11px]"
                        aria-label={`Remove ${slot.label} from ${blockId(block)}`}
                        onClick={() => ctx.update((d) => removeAt(d, [...path, ...slot.removePath!]))}
                      >
                        ✕
                      </button>
                    )}
                  </div>
                  {slot.kind === "list" ? (
                    <BlockListView ctx={ctx} listPath={slotPath} />
                  ) : (
                    <BlockNode ctx={ctx} path={slotPath} />
                  )}
                </div>
              );
            })}
          </div>
          {additions.length > 0 && (
            <div className="flex gap-2 pt-2 flex-wrap">
              {additions.map((a) => (
                <Button
                  key={a.label}
                  size="sm"
                  variant="ghost"
                  onClick={() =>
                    ctx.update((d) => {
                      const target = [...path, ...a.path];
                      const value = a.make(collectIds(d));
                      if (a.mode === "set") return setAt(d, target, value);
                      const cur = getAt(d, target);
                      return setAt(d, target, [...(Array.isArray(cur) ? cur : []), value]);
                    })
                  }
                >
                  + {a.label.replace(/^Add /, "")}
                </Button>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

// ─── Inspectors ─────────────────────────────────────────────────────────────

function SequenceInspector({ ctx }: { ctx: EditorCtx }) {
  const known = new Set(["blocks", "on_failure", "on_cancel", ...Object.keys(SEQUENCE_SCHEMA.properties)]);
  const preserved = Object.keys(ctx.doc).filter((k) => !known.has(k));
  return (
    <div className="space-y-4">
      <div>
        <div className="eyebrow">Sequence</div>
        <p className="annotation text-[11px] mt-1">Select a block in the graph to edit it.</p>
      </div>
      <SchemaForm
        schema={SEQUENCE_SCHEMA}
        value={ctx.doc}
        onChange={(k, v) => ctx.update((d) => (v === undefined ? removeAt(d, [k]) : setAt(d, [k], v)))}
      />
      <PreservedFields keys={preserved} />
    </div>
  );
}

function PreservedFields({ keys }: { keys: string[] }) {
  if (keys.length === 0) return null;
  return (
    <p className="annotation text-[11px]">
      Other fields kept as-is (edit in JSON):{" "}
      {keys.map((k, i) => (
        <span key={k}>
          {i > 0 && ", "}
          <code className="font-mono">{k}</code>
        </span>
      ))}
    </p>
  );
}

function BlockInspector({ ctx, path, onDeselect }: { ctx: EditorCtx; path: Path; onDeselect: () => void }) {
  const block = getAt(ctx.doc, path) as JsonObject;
  const type = blockType(block);
  const pos = listPosition(path);
  const siblings = pos ? getAt(ctx.doc, pos.listPath) : null;
  const len = Array.isArray(siblings) ? siblings.length : 0;
  const schema = BLOCK_SCHEMAS[type];
  const schemaKeys = new Set(schemaFields(schema).map((f) => f.key));
  const preserved = Object.keys(block).filter((k) => !schemaKeys.has(k) && !STRUCTURAL.has(k));
  const problems = ctx.issues.get(pathKey(path));

  const setField = (key: string, value: unknown) =>
    ctx.update((d) => (value === undefined ? removeAt(d, [...path, key]) : setAt(d, [...path, key], value)));

  const move = (to: number) => {
    if (!pos) return;
    ctx.update((d) => moveBlock(d, pos.listPath, pos.index, to));
    ctx.select([...pos.listPath, to]);
  };

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-2 flex-wrap">
        <span className="eyebrow">{type}</span>
        <span className="font-mono text-[13px] text-ink">{blockId(block)}</span>
        <Button size="sm" variant="ghost" className="ml-auto" onClick={onDeselect}>
          Sequence
        </Button>
      </div>
      {pos && (
        <div className="flex gap-1.5 flex-wrap">
          <Button size="sm" disabled={pos.index === 0} onClick={() => move(pos.index - 1)} aria-label="Move block up">
            ↑ Up
          </Button>
          <Button size="sm" disabled={pos.index >= len - 1} onClick={() => move(pos.index + 1)} aria-label="Move block down">
            ↓ Down
          </Button>
          <Button
            size="sm"
            onClick={() =>
              ctx.update((d) => {
                const copy = duplicateBlock(block, collectIds(d));
                return insertBlock(d, pos.listPath, pos.index + 1, copy);
              })
            }
          >
            Duplicate
          </Button>
          <Button
            size="sm"
            variant="danger"
            onClick={() => {
              ctx.update((d) => removeBlock(d, path));
              onDeselect();
            }}
          >
            Delete
          </Button>
        </div>
      )}
      {problems && (
        <ul className="text-warn text-[12px]" role="alert">
          {problems.map((p) => (
            <li key={p}>{p}</li>
          ))}
        </ul>
      )}
      {schema ? (
        <SchemaForm schema={schema} value={block} onChange={setField} />
      ) : (
        <p className="text-warn text-[12px]">Unknown block type — edit it in the JSON tab.</p>
      )}
      {type === "router" && <RouterRoutes ctx={ctx} path={path} block={block} />}
      {type === "ab_split" && <AbVariants ctx={ctx} path={path} block={block} />}
      {type === "saga" && <SagaSteps ctx={ctx} path={path} block={block} />}
      {type === "sub_sequence" && <SubSequenceInput ctx={ctx} path={path} block={block} />}
      <PreservedFields keys={preserved} />
    </div>
  );
}

function SubHeading({ children }: { children: ReactNode }) {
  return <div className="eyebrow pt-2 border-t border-rule">{children}</div>;
}

function RouterRoutes({ ctx, path, block }: { ctx: EditorCtx; path: Path; block: JsonObject }) {
  const routes = Array.isArray(block.routes) ? block.routes : [];
  return (
    <div className="space-y-2">
      <SubHeading>Routes (first match wins)</SubHeading>
      {routes.map((r, i) => (
        <div key={i}>
          <FieldLabel htmlFor={`route-${pathKey(path)}-${i}`}>route {i + 1} condition</FieldLabel>
          <Input
            id={`route-${pathKey(path)}-${i}`}
            value={isObject(r) && typeof r.condition === "string" ? r.condition : ""}
            onChange={(e) => ctx.update((d) => setAt(d, [...path, "routes", i, "condition"], e.target.value))}
            className="w-full font-mono"
          />
        </div>
      ))}
    </div>
  );
}

function AbVariants({ ctx, path, block }: { ctx: EditorCtx; path: Path; block: JsonObject }) {
  const variants = Array.isArray(block.variants) ? block.variants : [];
  const total = variants.reduce<number>((s, v) => s + (isObject(v) && typeof v.weight === "number" ? v.weight : 0), 0);
  return (
    <div className="space-y-2">
      <SubHeading>Variants</SubHeading>
      {variants.map((v, i) => {
        const w = isObject(v) && typeof v.weight === "number" ? v.weight : 0;
        return (
          <div key={i} className="grid grid-cols-3 gap-2 items-end">
            <div className="col-span-2">
              <FieldLabel htmlFor={`v-${pathKey(path)}-${i}`}>name</FieldLabel>
              <Input
                id={`v-${pathKey(path)}-${i}`}
                value={isObject(v) && typeof v.name === "string" ? v.name : ""}
                onChange={(e) => ctx.update((d) => setAt(d, [...path, "variants", i, "name"], e.target.value))}
                className="w-full font-mono"
              />
            </div>
            <div>
              <FieldLabel htmlFor={`w-${pathKey(path)}-${i}`}>weight</FieldLabel>
              <Input
                id={`w-${pathKey(path)}-${i}`}
                type="number"
                min={0}
                step={1}
                value={w}
                onChange={(e) => {
                  const n = Math.max(0, Math.trunc(Number(e.target.value) || 0));
                  ctx.update((d) => setAt(d, [...path, "variants", i, "weight"], n));
                }}
                className="w-full font-mono"
              />
            </div>
            <span className="col-span-3 annotation text-[11px]">{total > 0 ? `${((w / total) * 100).toFixed(0)}% of traffic` : ""}</span>
          </div>
        );
      })}
    </div>
  );
}

function SagaSteps({ ctx, path, block }: { ctx: EditorCtx; path: Path; block: JsonObject }) {
  const steps = Array.isArray(block.steps) ? block.steps : [];
  return (
    <div className="space-y-2">
      <SubHeading>Saga steps</SubHeading>
      {steps.map((s, i) => (
        <div key={i} className="flex items-end gap-2">
          <div className="flex-1">
            <FieldLabel htmlFor={`s-${pathKey(path)}-${i}`}>step {i + 1} id</FieldLabel>
            <Input
              id={`s-${pathKey(path)}-${i}`}
              value={isObject(s) && typeof s.id === "string" ? s.id : ""}
              onChange={(e) => ctx.update((d) => setAt(d, [...path, "steps", i, "id"], e.target.value))}
              className="w-full font-mono"
            />
          </div>
          {isObject(s) && isObject(s.compensation) ? (
            <Button size="sm" variant="ghost" onClick={() => ctx.update((d) => removeAt(d, [...path, "steps", i, "compensation"]))}>
              − compensation
            </Button>
          ) : (
            <Button
              size="sm"
              variant="ghost"
              onClick={() => ctx.update((d) => setAt(d, [...path, "steps", i, "compensation"], newBlock("step", collectIds(d))))}
            >
              + compensation
            </Button>
          )}
        </div>
      ))}
    </div>
  );
}

/** Sub-sequence `input`, as a form when the target declares an input_schema. */
function SubSequenceInput({ ctx, path, block }: { ctx: EditorCtx; path: Path; block: JsonObject }) {
  const name = typeof block.sequence_name === "string" ? block.sequence_name : "";
  const version = typeof block.version === "number" ? String(block.version) : undefined;
  const tenant = typeof ctx.doc.tenant_id === "string" ? ctx.doc.tenant_id : "";
  const namespace = typeof ctx.doc.namespace === "string" ? ctx.doc.namespace : "";
  const [schema, setSchema] = useState<Record<string, unknown> | null>(null);
  const [lookup, setLookup] = useState<"idle" | "loading" | "missing" | "found">("idle");
  const input = isObject(block.input) ? block.input : {};
  const [raw, setRaw] = useState(() => JSON.stringify(block.input ?? {}, null, 2));
  const [rawError, setRawError] = useState<string | null>(null);

  useEffect(() => {
    if (!name || !tenant || !namespace) return;
    const ac = new AbortController();
    const t = setTimeout(() => {
      setLookup("loading");
      getSequenceByName({ tenant_id: tenant, namespace, name, version }, ac.signal)
        .then((s) => {
          setSchema(isObject(s.input_schema) ? s.input_schema : null);
          setLookup("found");
        })
        .catch(() => {
          if (!ac.signal.aborted) {
            setSchema(null);
            setLookup("missing");
          }
        });
    }, 400);
    return () => {
      clearTimeout(t);
      ac.abort();
    };
  }, [name, version, tenant, namespace]);

  return (
    <div className="space-y-2">
      <SubHeading>Input for {name || "child"}</SubHeading>
      {lookup === "missing" && (
        <p className="text-hold text-[11px]">No stored sequence “{name}” in {tenant}/{namespace} — preflight will flag it.</p>
      )}
      {schema ? (
        <>
          <p className="annotation text-[11px]">Form generated from the child's input_schema.</p>
          <SchemaForm
            key={`${name}-${version ?? "latest"}`}
            schema={schema}
            value={input}
            onChange={(k, v) =>
              ctx.update((d) => (v === undefined ? removeAt(d, [...path, "input", k]) : setAt(d, [...path, "input", k], v)))
            }
          />
        </>
      ) : (
        <div>
          <FieldLabel htmlFor={`in-${pathKey(path)}`}>input (JSON)</FieldLabel>
          <textarea
            id={`in-${pathKey(path)}`}
            value={raw}
            rows={5}
            spellCheck={false}
            aria-invalid={!!rawError}
            onChange={(e) => setRaw(e.target.value)}
            onBlur={() => {
              try {
                const v = JSON.parse(raw) as Json;
                setRawError(null);
                ctx.update((d) => setAt(d, [...path, "input"], v));
              } catch {
                setRawError("input must be valid JSON");
              }
            }}
            className={TEXTAREA}
          />
          {rawError && <p role="alert" className="text-warn text-[11px]">{rawError}</p>}
        </div>
      )}
    </div>
  );
}

// ─── Live preflight ─────────────────────────────────────────────────────────

function PreflightPanel({ doc }: { doc: JsonObject | null }) {
  const [report, setReport] = useState<PreflightReport | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [placeholder] = useState(() => crypto.randomUUID());

  useEffect(() => {
    if (!doc) return;
    const ac = new AbortController();
    const t = setTimeout(() => {
      setBusy(true);
      preflightDraft(preflightBody(doc, placeholder, new Date()), ac.signal)
        .then((r) => {
          setReport(r);
          setError(null);
        })
        .catch((e) => {
          if (ac.signal.aborted) return;
          setReport(null);
          setError(e instanceof Error ? e.message : String(e));
        })
        .finally(() => {
          if (!ac.signal.aborted) setBusy(false);
        });
    }, 700);
    return () => {
      clearTimeout(t);
      ac.abort();
    };
  }, [doc, placeholder]);

  const notable = report?.checks.filter((c) => c.status !== "pass") ?? [];
  return (
    <div className="border border-hairline rounded-sm p-3 space-y-2" aria-live="polite">
      <div className="flex items-center gap-2">
        <span className="eyebrow">Preflight</span>
        {report && (
          <Badge tone={PREFLIGHT_TONE[report.overall]} dot>
            {report.overall}
          </Badge>
        )}
        {busy && <span className="text-[11px] text-muted">checking…</span>}
        <span className="ml-auto annotation text-[11px]">POST /sequences/preflight · live, nothing is saved</span>
      </div>
      {!doc && <p className="text-[12px] text-muted">Waiting for valid JSON.</p>}
      {error && <p className="text-warn text-[12px]">{error}</p>}
      {report && notable.length === 0 && (
        <p className="text-[12px] text-ok">All {report.checks.length} checks pass.</p>
      )}
      {notable.length > 0 && (
        <ul className="space-y-1.5">
          {notable.map((c) => (
            <li key={c.id} className="text-[12px]">
              <div className="flex items-baseline gap-2">
                <Badge tone={PREFLIGHT_TONE[c.status]}>{c.status}</Badge>
                <span className="font-mono text-muted">{c.id}</span>
                <span className="text-ink-dim">{c.summary}</span>
              </div>
              {(c.findings ?? []).slice(0, 5).map((f, i) => (
                <div key={i} className="ml-4 text-[11px] text-muted">
                  <span className="font-mono">{f.code}</span> — {f.summary}
                </div>
              ))}
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
