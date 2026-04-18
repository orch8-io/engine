import { Orch8Client, testSequence, step } from "./client.ts";
import type { Block } from "./client.ts";

const client = new Orch8Client("http://localhost:18080");

function router(id: string, routes: unknown, defaultBlocks?: unknown): Record<string, unknown> {
  const block: Record<string, unknown> = { type: "router", id, routes };
  if (defaultBlocks) block.default = defaultBlocks;
  return block;
}

async function main() {
  const seq = testSequence("dbg-router", [
    router(
      "rt1",
      [
        { condition: "mode == fast", blocks: [step("fast_step", "log", { message: "fast path" })] },
      ],
      [step("default_step", "log", { message: "default path" })]
    ) as Block,
  ]);

  console.log("Creating sequence:", seq.id);
  await client.createSequence(seq);

  const { id } = await client.createInstance({
    sequence_id: seq.id,
    tenant_id: "test",
    namespace: "default",
    context: { data: { mode: "fast" } },
  });
  console.log("Created instance:", id);

  for (let i = 0; i < 30; i++) {
    const inst = await client.getInstance(id);
    console.log(`[${i}] state=${inst.state}`);
    if (["completed", "failed", "cancelled"].includes(inst.state)) {
      console.log("Terminal state reached:", inst.state);
      const outputs = await client.getOutputs(id);
      console.log("Outputs:", JSON.stringify(outputs.map(o => o.block_id)));
      return;
    }
    await new Promise(r => setTimeout(r, 500));
  }
  console.log("TIMEOUT: instance never reached terminal state");
}

main().catch(e => { console.error(e); process.exit(1); });
