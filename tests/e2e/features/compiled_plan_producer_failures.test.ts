import { after, before, describe, it } from "node:test";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import { expectMissingProducer } from "../compiled_plan_cases.ts";

describe("Compiled plan producer diagnostics", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("reports missing producer ghost", async () => {
    await expectMissingProducer("compiled-producer-error-001", "ghost");
  });

  it("reports missing producer unknown", async () => {
    await expectMissingProducer("compiled-producer-error-002", "unknown");
  });

  it("reports missing producer absent", async () => {
    await expectMissingProducer("compiled-producer-error-003", "absent");
  });

  it("reports missing producer upstream", async () => {
    await expectMissingProducer("compiled-producer-error-004", "upstream");
  });

  it("reports missing producer source_a", async () => {
    await expectMissingProducer("compiled-producer-error-005", "source_a");
  });

  it("reports missing producer source_b", async () => {
    await expectMissingProducer("compiled-producer-error-006", "source_b");
  });

  it("reports missing producer producer_1", async () => {
    await expectMissingProducer("compiled-producer-error-007", "producer_1");
  });

  it("reports missing producer producer_2", async () => {
    await expectMissingProducer("compiled-producer-error-008", "producer_2");
  });

  it("reports missing producer fetch_customer", async () => {
    await expectMissingProducer("compiled-producer-error-009", "fetch_customer");
  });

  it("reports missing producer load_order", async () => {
    await expectMissingProducer("compiled-producer-error-010", "load_order");
  });

  it("reports missing producer resolve_account", async () => {
    await expectMissingProducer("compiled-producer-error-011", "resolve_account");
  });

  it("reports missing producer lookup_region", async () => {
    await expectMissingProducer("compiled-producer-error-012", "lookup_region");
  });

  it("reports missing producer read_token", async () => {
    await expectMissingProducer("compiled-producer-error-013", "read_token");
  });

  it("reports missing producer count_attempts", async () => {
    await expectMissingProducer("compiled-producer-error-014", "count_attempts");
  });

  it("reports missing producer count_items", async () => {
    await expectMissingProducer("compiled-producer-error-015", "count_items");
  });

  it("reports missing producer set_priority", async () => {
    await expectMissingProducer("compiled-producer-error-016", "set_priority");
  });

  it("reports missing producer compute_total", async () => {
    await expectMissingProducer("compiled-producer-error-017", "compute_total");
  });

  it("reports missing producer calculate_tax", async () => {
    await expectMissingProducer("compiled-producer-error-018", "calculate_tax");
  });

  it("reports missing producer score_risk", async () => {
    await expectMissingProducer("compiled-producer-error-019", "score_risk");
  });

  it("reports missing producer locate_device", async () => {
    await expectMissingProducer("compiled-producer-error-020", "locate_device");
  });

  it("reports missing producer check_active", async () => {
    await expectMissingProducer("compiled-producer-error-021", "check_active");
  });

  it("reports missing producer check_verified", async () => {
    await expectMissingProducer("compiled-producer-error-022", "check_verified");
  });

  it("reports missing producer request_review", async () => {
    await expectMissingProducer("compiled-producer-error-023", "request_review");
  });

  it("reports missing producer record_consent", async () => {
    await expectMissingProducer("compiled-producer-error-024", "record_consent");
  });

  it("reports missing producer load_profile", async () => {
    await expectMissingProducer("compiled-producer-error-025", "load_profile");
  });

  it("reports missing producer load_manifest", async () => {
    await expectMissingProducer("compiled-producer-error-026", "load_manifest");
  });

  it("reports missing producer load_address", async () => {
    await expectMissingProducer("compiled-producer-error-027", "load_address");
  });

  it("reports missing producer load_policy", async () => {
    await expectMissingProducer("compiled-producer-error-028", "load_policy");
  });

  it("reports missing producer list_files", async () => {
    await expectMissingProducer("compiled-producer-error-029", "list_files");
  });

  it("reports missing producer list_approvers", async () => {
    await expectMissingProducer("compiled-producer-error-030", "list_approvers");
  });
});
