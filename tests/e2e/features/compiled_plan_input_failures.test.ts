import { after, before, describe, it } from "node:test";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import { expectMissingInput } from "../compiled_plan_cases.ts";

describe("Compiled plan closed-schema diagnostics", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("reports closed-schema input path missing_customer", async () => {
    await expectMissingInput("compiled-input-error-001", "missing_customer");
  });

  it("reports closed-schema input path missing_order", async () => {
    await expectMissingInput("compiled-input-error-002", "missing_order");
  });

  it("reports closed-schema input path missing_account", async () => {
    await expectMissingInput("compiled-input-error-003", "missing_account");
  });

  it("reports closed-schema input path missing_region", async () => {
    await expectMissingInput("compiled-input-error-004", "missing_region");
  });

  it("reports closed-schema input path missing_token", async () => {
    await expectMissingInput("compiled-input-error-005", "missing_token");
  });

  it("reports closed-schema input path missing_attempt", async () => {
    await expectMissingInput("compiled-input-error-006", "missing_attempt");
  });

  it("reports closed-schema input path missing_retry", async () => {
    await expectMissingInput("compiled-input-error-007", "missing_retry");
  });

  it("reports closed-schema input path missing_items", async () => {
    await expectMissingInput("compiled-input-error-008", "missing_items");
  });

  it("reports closed-schema input path missing_priority", async () => {
    await expectMissingInput("compiled-input-error-009", "missing_priority");
  });

  it("reports closed-schema input path missing_page", async () => {
    await expectMissingInput("compiled-input-error-010", "missing_page");
  });

  it("reports closed-schema input path missing_total", async () => {
    await expectMissingInput("compiled-input-error-011", "missing_total");
  });

  it("reports closed-schema input path missing_tax", async () => {
    await expectMissingInput("compiled-input-error-012", "missing_tax");
  });

  it("reports closed-schema input path missing_score", async () => {
    await expectMissingInput("compiled-input-error-013", "missing_score");
  });

  it("reports closed-schema input path missing_latitude", async () => {
    await expectMissingInput("compiled-input-error-014", "missing_latitude");
  });

  it("reports closed-schema input path missing_longitude", async () => {
    await expectMissingInput("compiled-input-error-015", "missing_longitude");
  });

  it("reports closed-schema input path missing_active", async () => {
    await expectMissingInput("compiled-input-error-016", "missing_active");
  });

  it("reports closed-schema input path missing_verified", async () => {
    await expectMissingInput("compiled-input-error-017", "missing_verified");
  });

  it("reports closed-schema input path missing_review", async () => {
    await expectMissingInput("compiled-input-error-018", "missing_review");
  });

  it("reports closed-schema input path missing_consent", async () => {
    await expectMissingInput("compiled-input-error-019", "missing_consent");
  });

  it("reports closed-schema input path missing_flag", async () => {
    await expectMissingInput("compiled-input-error-020", "missing_flag");
  });

  it("reports closed-schema input path missing_profile", async () => {
    await expectMissingInput("compiled-input-error-021", "missing_profile");
  });

  it("reports closed-schema input path missing_metadata", async () => {
    await expectMissingInput("compiled-input-error-022", "missing_metadata");
  });

  it("reports closed-schema input path missing_address", async () => {
    await expectMissingInput("compiled-input-error-023", "missing_address");
  });

  it("reports closed-schema input path missing_policy", async () => {
    await expectMissingInput("compiled-input-error-024", "missing_policy");
  });

  it("reports closed-schema input path missing_attributes", async () => {
    await expectMissingInput("compiled-input-error-025", "missing_attributes");
  });

  it("reports closed-schema input path missing_lines", async () => {
    await expectMissingInput("compiled-input-error-026", "missing_lines");
  });

  it("reports closed-schema input path missing_tags", async () => {
    await expectMissingInput("compiled-input-error-027", "missing_tags");
  });

  it("reports closed-schema input path missing_regions", async () => {
    await expectMissingInput("compiled-input-error-028", "missing_regions");
  });

  it("reports closed-schema input path missing_files", async () => {
    await expectMissingInput("compiled-input-error-029", "missing_files");
  });

  it("reports closed-schema input path missing_approvers", async () => {
    await expectMissingInput("compiled-input-error-030", "missing_approvers");
  });
});

