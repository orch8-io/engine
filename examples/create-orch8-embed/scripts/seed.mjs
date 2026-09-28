// Publishes orch8/customer-onboarding.json once per demo customer (sub-tenant),
// so every customer owns an editable copy for <orch8-builder>.
// Usage: pnpm seed   (reads .env.local)
import { readFileSync } from "node:fs";

const url = (process.env.ORCH8_URL ?? "").replace(/\/+$/, "").replace(/\/api\/v1$/, "");
const apiKey = process.env.ORCH8_API_KEY;
const tenant = process.env.ORCH8_TENANT_ID || "demo";
if (!url || url === "mock") {
  console.log("ORCH8_URL is empty/mock: the built-in mock engine is already seeded. Nothing to do.");
  process.exit(0);
}
if (!apiKey) {
  console.error("Set ORCH8_API_KEY in .env.local first.");
  process.exit(1);
}

const template = JSON.parse(readFileSync(new URL("../orch8/customer-onboarding.json", import.meta.url), "utf8"));
// Keep in sync with lib/users.ts (subTenantFor).
const subTenants = ["org:northwind", "org:globex"];

for (const sub of subTenants) {
  const body = { ...template, id: crypto.randomUUID(), tenant_id: tenant, version: 1, created_at: new Date().toISOString() };
  const res = await fetch(`${url}/api/v1/sequences`, {
    method: "POST",
    headers: { "content-type": "application/json", "x-api-key": apiKey, "x-tenant-id": tenant, "x-orch8-sub-tenant": sub },
    body: JSON.stringify(body),
  });
  if (res.status === 409) console.log(`= ${sub}: ${template.name} already exists`);
  else if (res.ok) console.log(`+ ${sub}: published ${template.name}`);
  else {
    console.error(`! ${sub}: HTTP ${res.status} ${await res.text()}`);
    process.exitCode = 1;
  }
}
