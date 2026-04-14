/**
 * @orch8/workflow-sdk — build durable workflows in TypeScript.
 *
 * ```ts
 * import { workflow, Orch8Client } from "@orch8/workflow-sdk";
 *
 * const wf = workflow("onboarding")
 *   .step("send_welcome", "send_email", { template: "welcome" })
 *   .delay({ duration: 24 * 60 * 60 * 1000 })
 *   .step("check_in", "send_email", { template: "day_1" });
 *
 * const client = new Orch8Client({
 *   baseUrl: "https://orch8.example.com",
 *   tenantId: "acme",
 *   apiKey: process.env.ORCH8_KEY,
 * });
 *
 * await client.createSequence(wf);
 * ```
 */
export * from "./schema.js";
export * from "./builder.js";
export * from "./client.js";
