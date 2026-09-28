import "server-only";

export interface Orch8Config {
  /** "mock" serves the embed API from this app (app/mock/...), no engine needed. */
  mode: "live" | "mock";
  /** Server-side engine origin. */
  url: string;
  /** Origin the browser uses for embed calls (the widgets' `base-url`). */
  publicUrl: string;
  apiKey: string;
  tenantId: string;
  sequences: string[] | null;
  demoSequence: string;
  vendor: string;
}

export function getConfig(env: Record<string, string | undefined> = process.env): Orch8Config {
  const raw = (env.ORCH8_URL ?? "").trim().replace(/\/+$/, "").replace(/\/api\/v1$/, "");
  const mode = raw === "" || raw === "mock" ? "mock" : "live";
  const sequences = (env.ORCH8_EMBED_SEQUENCES ?? "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
  return {
    mode,
    url: mode === "mock" ? "" : raw,
    publicUrl: mode === "mock" ? "/mock" : (env.ORCH8_PUBLIC_URL?.trim().replace(/\/+$/, "") || raw),
    apiKey: env.ORCH8_API_KEY ?? "",
    tenantId: env.ORCH8_TENANT_ID || "demo",
    sequences: sequences.length ? sequences : null,
    demoSequence: env.ORCH8_DEMO_SEQUENCE || "customer-onboarding",
    vendor: env.ORCH8_VENDOR || "acme-saas",
  };
}
