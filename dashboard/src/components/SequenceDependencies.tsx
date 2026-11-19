import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { getSequenceByName, type SequenceDefinition } from "../api";
import { sequenceDependencies } from "../lib/sequenceAuthoring";
import { Badge } from "./ui/Badge";

export function SequenceDependencies({ sequence }: { sequence: SequenceDefinition }) {
  const dependencies = useMemo(() => sequenceDependencies(sequence), [sequence]);
  const [results, setResults] = useState<Map<string, SequenceDefinition | null>>(new Map());
  const [expanded, setExpanded] = useState(false);

  useEffect(() => {
    if (!expanded) return;
    const controller = new AbortController();
    const targets = new Map(dependencies.map(({ name, version }) => [JSON.stringify([name, version]), { name, version }]));
    Promise.all([...targets].map(async ([key, { name, version }]) => {
      try {
        const target = await getSequenceByName({
          tenant_id: sequence.tenant_id,
          namespace: sequence.namespace,
          name,
          version: version === null ? undefined : String(version),
        }, controller.signal);
        const expected = target.name === name && target.tenant_id === sequence.tenant_id &&
          target.namespace === sequence.namespace && (version === null || target.version === version);
        return [key, expected ? target : null] as const;
      } catch {
        return [key, null] as const;
      }
    })).then((rows) => {
      if (!controller.signal.aborted) setResults(new Map(rows));
    });
    return () => controller.abort();
  }, [dependencies, expanded, sequence.namespace, sequence.tenant_id]);

  if (!dependencies.length) return null;
  return (
    <details className="border border-hairline rounded-sm p-4" onToggle={(event) => setExpanded(event.currentTarget.open)}>
      <summary className="cursor-pointer text-sm font-medium text-ink">Child sequence dependencies ({dependencies.length})</summary>
      <ul className="mt-3 space-y-2">
        {dependencies.map(({ blockId, name, version }, index) => {
          const resolved = results.get(JSON.stringify([name, version]));
          return (
            <li key={index} className="flex items-center gap-2 flex-wrap text-xs">
              <code>{blockId}</code>
              <span aria-hidden>→</span>
              {resolved ? <Link className="text-signal underline" to={`/sequences/${resolved.id}`}>{name} v{resolved.version}</Link> : <span>{name}{version !== null && ` v${version}`}</span>}
              <Badge tone={version === null ? "hold" : "dim"}>{version === null ? "Latest active at runtime" : "Pinned"}</Badge>
              {resolved?.deprecated && <Badge tone="hold">Deprecated</Badge>}
              {resolved === null && <span className="text-warn">Target unavailable</span>}
              {resolved === undefined && <span className="text-muted">Resolving…</span>}
            </li>
          );
        })}
      </ul>
    </details>
  );
}
