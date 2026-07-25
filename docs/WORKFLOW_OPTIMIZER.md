# Workflow compiler optimization

`orch8_engine::optimizer::optimize` compiles a validated
`SequenceDefinition` into an immutable optimization sidecar. The durable
definition remains the execution authority; the optimizer never rewrites it.

The first optimizer version performs conservative work that cannot alter
workflow behavior:

- canonical source hashing provides an executable exact-equivalence check;
- repeated JSON parameters, output schemas, and sub-sequence inputs are
  interned in a deterministic constant pool;
- the recursive block graph is flattened into ordered nodes and labelled
  edges while preserving branch, route, saga, and compensation order;
- literal `true` and `false` guards are classified ahead of execution, while
  every other expression remains dynamic;
- typed-dataflow findings are attached to the compiled artifact; and
- top-level composite/plugin dispatch decisions are precomputed for the
  scheduler hot path.

The sequence cache compiles and caches this sidecar with the same capacity and
TTL as its source definition. Before using a cached plan it verifies the
canonical source hash. A compiler rejection or hash mismatch is a safe
optimization miss: the scheduler uses its original structural scan.

Dynamic evaluator-injected blocks are deliberately outside the static plan.
When injected roots are present, dispatch falls back to scanning the merged
definition so runtime extension semantics remain unchanged.

## Equivalence contract

`OptimizationIr::verify_equivalent` recomputes the canonical SHA-256 of the
supplied workflow and requires an exact match. Tests prove determinism,
constant pooling, conservative guard classification, source immutability, and
rejection after a semantic mutation. New optimization passes must preserve
this fail-safe contract and add equivalence cases before being used by runtime
dispatch.
