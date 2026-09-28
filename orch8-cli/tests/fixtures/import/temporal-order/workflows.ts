import {
  proxyActivities,
  sleep,
  condition,
  defineSignal,
  setHandler,
  executeChild,
  CancellationScope,
} from "@temporalio/workflow";
import type * as activities from "./activities";

const { reserveInventory, chargeCard, refundCard, shipOrder } = proxyActivities<typeof activities>({
  startToCloseTimeout: "1 minute",
  retry: {
    maximumAttempts: 5,
    initialInterval: "2s",
    backoffCoefficient: 2,
    maximumInterval: "1m",
    nonRetryableErrorTypes: ["CardDeclined"],
  },
});

const notify = proxyActivities<typeof activities>({
  startToCloseTimeout: "30s",
  taskQueue: "notifications",
});

export const approveSignal = defineSignal<[boolean]>("approve");

export async function orderWorkflow(order: { id: string; total: number; email: string }): Promise<string> {
  let approved = false;
  setHandler(approveSignal, (value: boolean) => {
    approved = value;
  });

  await reserveInventory(order.id);

  if (order.total > 1000) {
    const ok = await condition(() => approved, "24 hours");
    if (!ok) {
      await notify.notifyCustomer(order.email, "order-expired");
      return "expired";
    }
  }

  const charge = await chargeCard(order.id, order.total);
  try {
    await shipOrder(order.id);
  } catch (err) {
    await refundCard(charge.chargeId);
    throw err;
  }

  await sleep("2 days");
  await CancellationScope.nonCancellable(async () => {
    await notify.notifyCustomer(order.email, "shipped");
  });
  await executeChild(loyaltyWorkflow, { args: [order.email], workflowId: `loyalty-${order.id}` });
  return charge.chargeId;
}

export async function loyaltyWorkflow(email: string): Promise<void> {
  await sleep(1000);
}
