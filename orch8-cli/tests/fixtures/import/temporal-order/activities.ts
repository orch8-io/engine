export async function reserveInventory(orderId: string): Promise<void> {}
export async function chargeCard(orderId: string, amount: number): Promise<{ chargeId: string }> {
  return { chargeId: "c1" };
}
export async function refundCard(chargeId: string): Promise<void> {}
export async function shipOrder(orderId: string): Promise<void> {}
export async function notifyCustomer(email: string, template: string): Promise<void> {}
