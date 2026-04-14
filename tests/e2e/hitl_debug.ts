import { Orch8Client, testSequence, step } from "./client.ts";
import { startServer, stopServer } from "./harness.ts";

process.on("uncaughtException", (e) => {
  console.log("UNCAUGHT:", e.message);
  process.exit(1);
});
process.on("unhandledRejection", (e: any) => {
  console.log("UNHANDLED:", e?.message ?? e);
  process.exit(1);
});

async function main() {
  // Use default log level (warn) - debug level causes stderr pipe overflow
  const server = await startServer();
  const client = new Orch8Client(`http://localhost:${server.port}`);

  const seq = testSequence("hitl-debug", [
    step("review", "human_review", { prompt: "pick" }, {
      wait_for_input: { prompt: "pick" },
    }),
  ]);
  await client.createSequence(seq);
  const { id } = await client.createInstance({
    sequence_id: seq.id,
    tenant_id: seq.tenant_id,
    namespace: seq.namespace,
  });
  console.log("Instance created:", id);

  // Poll state
  for (let i = 0; i < 20; i++) {
    await new Promise(r => setTimeout(r, 500));
    try {
      const inst = await client.getInstance(id);
      console.log(`  [${i*500}ms] state=${inst.state}`);
    } catch (e: any) {
      console.log(`  [${i*500}ms] ERROR: ${e.message}`);
    }
  }

  // Send signal
  console.log("Sending signal...");
  try {
    await client.sendSignal(
      id,
      { custom: "human_input:review" } as unknown as string,
      { value: "yes" },
    );
    console.log("Signal sent");
  } catch (e: any) {
    console.log("Signal ERROR:", e.message);
  }

  // Poll again
  for (let i = 0; i < 20; i++) {
    await new Promise(r => setTimeout(r, 500));
    try {
      const inst = await client.getInstance(id);
      console.log(`  [+${(i+1)*500}ms] state=${inst.state}`);
      if (inst.state === "completed" || inst.state === "failed") {
        console.log("Final context:", JSON.stringify(inst.context));
        break;
      }
    } catch (e: any) {
      console.log(`  [+${(i+1)*500}ms] ERROR: ${e.message}`);
    }
  }

  await stopServer(server);
}
main().catch(e => { console.log("MAIN ERROR:", e.message); process.exit(1); });
