# Agent-framework adapters

Orch8's stable integration boundary is the framework-neutral portable protocol,
not framework-specific state. LangGraph, CrewAI, AutoGen, and custom agents use
the same adapter shape:

1. serialize only allowlisted portable state into `context.data`;
2. expose one framework turn as a worker handler;
3. put tool calls behind idempotent Orch8 steps and effect guards;
4. checkpoint at turn boundaries;
5. return framework output as ordinary JSON or an artifact reference.

Use `orch8 portable wrapper generate` to create a wrapper descriptor and
`orch8 portable conformance` before deployment. The
`examples/portable-agent-product` example is the canonical adapter. Keeping the
wire boundary neutral avoids coupling durable state to rapidly changing
framework internals.
