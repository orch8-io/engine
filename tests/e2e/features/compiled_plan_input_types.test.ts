import { after, before, describe, it } from "node:test";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import { expectDeclaredInput } from "../compiled_plan_cases.ts";

describe("Compiled plan input contracts", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("accepts declared string input field customer_id", async () => {
    await expectDeclaredInput("compiled-input-001", "customer_id", "string");
  });

  it("accepts declared string input field order_id", async () => {
    await expectDeclaredInput("compiled-input-002", "order_id", "string");
  });

  it("accepts declared string input field account_name", async () => {
    await expectDeclaredInput("compiled-input-003", "account_name", "string");
  });

  it("accepts declared string input field region_code", async () => {
    await expectDeclaredInput("compiled-input-004", "region_code", "string");
  });

  it("accepts declared string input field request_token", async () => {
    await expectDeclaredInput("compiled-input-005", "request_token", "string");
  });

  it("accepts declared integer input field attempt_count", async () => {
    await expectDeclaredInput("compiled-input-006", "attempt_count", "integer");
  });

  it("accepts declared integer input field retry_count", async () => {
    await expectDeclaredInput("compiled-input-007", "retry_count", "integer");
  });

  it("accepts declared integer input field item_count", async () => {
    await expectDeclaredInput("compiled-input-008", "item_count", "integer");
  });

  it("accepts declared integer input field priority_level", async () => {
    await expectDeclaredInput("compiled-input-009", "priority_level", "integer");
  });

  it("accepts declared integer input field page_number", async () => {
    await expectDeclaredInput("compiled-input-010", "page_number", "integer");
  });

  it("accepts declared number input field total_amount", async () => {
    await expectDeclaredInput("compiled-input-011", "total_amount", "number");
  });

  it("accepts declared number input field tax_rate", async () => {
    await expectDeclaredInput("compiled-input-012", "tax_rate", "number");
  });

  it("accepts declared number input field confidence_score", async () => {
    await expectDeclaredInput("compiled-input-013", "confidence_score", "number");
  });

  it("accepts declared number input field latitude", async () => {
    await expectDeclaredInput("compiled-input-014", "latitude", "number");
  });

  it("accepts declared number input field longitude", async () => {
    await expectDeclaredInput("compiled-input-015", "longitude", "number");
  });

  it("accepts declared boolean input field is_active", async () => {
    await expectDeclaredInput("compiled-input-016", "is_active", "boolean");
  });

  it("accepts declared boolean input field is_verified", async () => {
    await expectDeclaredInput("compiled-input-017", "is_verified", "boolean");
  });

  it("accepts declared boolean input field requires_review", async () => {
    await expectDeclaredInput("compiled-input-018", "requires_review", "boolean");
  });

  it("accepts declared boolean input field has_consent", async () => {
    await expectDeclaredInput("compiled-input-019", "has_consent", "boolean");
  });

  it("accepts declared boolean input field is_priority", async () => {
    await expectDeclaredInput("compiled-input-020", "is_priority", "boolean");
  });

  it("accepts declared object input field customer", async () => {
    await expectDeclaredInput("compiled-input-021", "customer", "object");
  });

  it("accepts declared object input field metadata", async () => {
    await expectDeclaredInput("compiled-input-022", "metadata", "object");
  });

  it("accepts declared object input field shipping_address", async () => {
    await expectDeclaredInput("compiled-input-023", "shipping_address", "object");
  });

  it("accepts declared object input field policy", async () => {
    await expectDeclaredInput("compiled-input-024", "policy", "object");
  });

  it("accepts declared object input field attributes", async () => {
    await expectDeclaredInput("compiled-input-025", "attributes", "object");
  });

  it("accepts declared array input field items", async () => {
    await expectDeclaredInput("compiled-input-026", "items", "array");
  });

  it("accepts declared array input field tags", async () => {
    await expectDeclaredInput("compiled-input-027", "tags", "array");
  });

  it("accepts declared array input field regions", async () => {
    await expectDeclaredInput("compiled-input-028", "regions", "array");
  });

  it("accepts declared array input field attachments", async () => {
    await expectDeclaredInput("compiled-input-029", "attachments", "array");
  });

  it("accepts declared array input field approvers", async () => {
    await expectDeclaredInput("compiled-input-030", "approvers", "array");
  });
});

