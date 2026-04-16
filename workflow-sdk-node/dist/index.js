"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __exportStar = (this && this.__exportStar) || function(m, exports) {
    for (var p in m) if (p !== "default" && !Object.prototype.hasOwnProperty.call(exports, p)) __createBinding(exports, m, p);
};
Object.defineProperty(exports, "__esModule", { value: true });
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
__exportStar(require("./schema.js"), exports);
__exportStar(require("./builder.js"), exports);
__exportStar(require("./client.js"), exports);
