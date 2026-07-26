import { after, before, describe, it } from "node:test";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import { expectDeclaredOutput } from "../compiled_plan_cases.ts";

describe("Compiled plan output contracts", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("accepts declared string producer field customer_id", async () => {
    await expectDeclaredOutput("compiled-output-001", "customer_id", "string");
  });

  it("accepts declared string producer field order_id", async () => {
    await expectDeclaredOutput("compiled-output-002", "order_id", "string");
  });

  it("accepts declared string producer field account_name", async () => {
    await expectDeclaredOutput("compiled-output-003", "account_name", "string");
  });

  it("accepts declared string producer field region_code", async () => {
    await expectDeclaredOutput("compiled-output-004", "region_code", "string");
  });

  it("accepts declared string producer field request_token", async () => {
    await expectDeclaredOutput("compiled-output-005", "request_token", "string");
  });

  it("accepts declared integer producer field attempt_count", async () => {
    await expectDeclaredOutput("compiled-output-006", "attempt_count", "integer");
  });

  it("accepts declared integer producer field retry_count", async () => {
    await expectDeclaredOutput("compiled-output-007", "retry_count", "integer");
  });

  it("accepts declared integer producer field item_count", async () => {
    await expectDeclaredOutput("compiled-output-008", "item_count", "integer");
  });

  it("accepts declared integer producer field priority_level", async () => {
    await expectDeclaredOutput("compiled-output-009", "priority_level", "integer");
  });

  it("accepts declared integer producer field page_number", async () => {
    await expectDeclaredOutput("compiled-output-010", "page_number", "integer");
  });

  it("accepts declared number producer field total_amount", async () => {
    await expectDeclaredOutput("compiled-output-011", "total_amount", "number");
  });

  it("accepts declared number producer field tax_rate", async () => {
    await expectDeclaredOutput("compiled-output-012", "tax_rate", "number");
  });

  it("accepts declared number producer field confidence_score", async () => {
    await expectDeclaredOutput("compiled-output-013", "confidence_score", "number");
  });

  it("accepts declared number producer field latitude", async () => {
    await expectDeclaredOutput("compiled-output-014", "latitude", "number");
  });

  it("accepts declared number producer field longitude", async () => {
    await expectDeclaredOutput("compiled-output-015", "longitude", "number");
  });

  it("accepts declared boolean producer field is_active", async () => {
    await expectDeclaredOutput("compiled-output-016", "is_active", "boolean");
  });

  it("accepts declared boolean producer field is_verified", async () => {
    await expectDeclaredOutput("compiled-output-017", "is_verified", "boolean");
  });

  it("accepts declared boolean producer field requires_review", async () => {
    await expectDeclaredOutput("compiled-output-018", "requires_review", "boolean");
  });

  it("accepts declared boolean producer field has_consent", async () => {
    await expectDeclaredOutput("compiled-output-019", "has_consent", "boolean");
  });

  it("accepts declared boolean producer field is_priority", async () => {
    await expectDeclaredOutput("compiled-output-020", "is_priority", "boolean");
  });

  it("accepts declared object producer field customer", async () => {
    await expectDeclaredOutput("compiled-output-021", "customer", "object");
  });

  it("accepts declared object producer field metadata", async () => {
    await expectDeclaredOutput("compiled-output-022", "metadata", "object");
  });

  it("accepts declared object producer field shipping_address", async () => {
    await expectDeclaredOutput("compiled-output-023", "shipping_address", "object");
  });

  it("accepts declared object producer field policy", async () => {
    await expectDeclaredOutput("compiled-output-024", "policy", "object");
  });

  it("accepts declared object producer field attributes", async () => {
    await expectDeclaredOutput("compiled-output-025", "attributes", "object");
  });

  it("accepts declared array producer field items", async () => {
    await expectDeclaredOutput("compiled-output-026", "items", "array");
  });

  it("accepts declared array producer field tags", async () => {
    await expectDeclaredOutput("compiled-output-027", "tags", "array");
  });

  it("accepts declared array producer field regions", async () => {
    await expectDeclaredOutput("compiled-output-028", "regions", "array");
  });

  it("accepts declared array producer field attachments", async () => {
    await expectDeclaredOutput("compiled-output-029", "attachments", "array");
  });

  it("accepts declared array producer field approvers", async () => {
    await expectDeclaredOutput("compiled-output-030", "approvers", "array");
  });
});

