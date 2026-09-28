import { FlowProducer, Queue, Worker } from "bullmq";

const flowProducer = new FlowProducer({ connection });
const emails = new Queue("emails", { connection });

export async function startRenovation(houseId: string) {
  await flowProducer.add({
    name: "renovate-interior",
    queueName: "renovate",
    data: { houseId: "h-1" },
    opts: { attempts: 3, backoff: { type: "exponential", delay: 1000 } },
    children: [
      { name: "paint", data: { place: "ceiling" }, queueName: "steps" },
      { name: "paint", data: { place: "walls" }, queueName: "steps", opts: { delay: 5000 } },
      {
        name: "fix",
        data: { place: "floor", house: houseId },
        queueName: "steps",
        opts: { failParentOnFailure: true, jobId: "fix-floor" },
        children: [{ name: "buyMaterials", data: { item: "planks" }, queueName: "shopping" }],
      },
    ],
  });
  await emails.add("renovation-started", { houseId });
}

new Worker("steps", async (job) => job.data, { connection });
