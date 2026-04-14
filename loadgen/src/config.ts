/**
 * Config resolution: defaults ← preset ← env vars ← CLI flags.
 */

export type PresetName = "light" | "steady" | "stress";

export interface LoadgenConfig {
  baseUrl: string;
  preset: PresetName;
  rate: number;            // instances per second
  concurrency: number;     // max alive instances
  workers: number;         // mock worker pool size
  signalRate: number;      // signals per second
  duration: number | null; // seconds; null = forever
  seed: number;
  tenants: string[];
  namespaces: string[];
  enableLlm: boolean;
  verbose: boolean;
}

const PRESETS: Record<PresetName, Omit<LoadgenConfig, "baseUrl" | "preset" | "duration" | "seed" | "tenants" | "namespaces" | "enableLlm" | "verbose">> = {
  light:  { rate: 1,  concurrency: 20,   workers: 2,  signalRate: 1 / 30 },
  steady: { rate: 5,  concurrency: 200,  workers: 8,  signalRate: 0.1 },
  stress: { rate: 50, concurrency: 2000, workers: 32, signalRate: 0.5 },
};

export function parseArgs(argv: string[]): LoadgenConfig {
  const flags = new Map<string, string>();
  for (const raw of argv) {
    if (!raw.startsWith("--")) continue;
    const eq = raw.indexOf("=");
    if (eq === -1) flags.set(raw.slice(2), "true");
    else flags.set(raw.slice(2, eq), raw.slice(eq + 1));
  }

  const presetName = (flags.get("preset") ?? process.env.LOADGEN_PRESET ?? "steady") as PresetName;
  if (!(presetName in PRESETS)) {
    throw new Error(`Unknown preset: ${presetName}. Use light | steady | stress.`);
  }
  const preset = PRESETS[presetName];

  const baseUrl =
    flags.get("server") ??
    process.env.LOADGEN_SERVER ??
    process.env.ORCH8_E2E_BASE_URL ??
    `http://localhost:${process.env.ORCH8_E2E_PORT ?? "18080"}`;

  const duration = parseOptionalInt(flags.get("duration") ?? process.env.LOADGEN_DURATION);
  const seed = parseOptionalInt(flags.get("seed") ?? process.env.LOADGEN_SEED) ?? Date.now();

  const tenants = (flags.get("tenants") ?? process.env.LOADGEN_TENANTS ?? "loadgen-a,loadgen-b,loadgen-c")
    .split(",").map((s) => s.trim()).filter(Boolean);
  const namespaces = (flags.get("namespaces") ?? process.env.LOADGEN_NAMESPACES ?? "prod,staging")
    .split(",").map((s) => s.trim()).filter(Boolean);

  return {
    baseUrl,
    preset: presetName,
    rate: parseOptionalFloat(flags.get("rate")) ?? preset.rate,
    concurrency: parseOptionalInt(flags.get("concurrency")) ?? preset.concurrency,
    workers: parseOptionalInt(flags.get("workers")) ?? preset.workers,
    signalRate: parseOptionalFloat(flags.get("signal-rate")) ?? preset.signalRate,
    duration,
    seed,
    tenants,
    namespaces,
    enableLlm:
      flag(flags.get("enable-llm")) ||
      process.env.LOADGEN_ENABLE_LLM === "1",
    verbose: flag(flags.get("verbose")) || process.env.LOADGEN_VERBOSE === "1",
  };
}

function parseOptionalInt(v: string | undefined): number | null {
  if (v == null) return null;
  const n = parseInt(v, 10);
  return Number.isFinite(n) ? n : null;
}

function parseOptionalFloat(v: string | undefined): number | null {
  if (v == null) return null;
  const n = parseFloat(v);
  return Number.isFinite(n) ? n : null;
}

function flag(v: string | undefined): boolean {
  return v === "true" || v === "1";
}

export function renderConfig(cfg: LoadgenConfig): string {
  return [
    `server=${cfg.baseUrl}`,
    `preset=${cfg.preset}`,
    `rate=${cfg.rate}/s`,
    `concurrency=${cfg.concurrency}`,
    `workers=${cfg.workers}`,
    `signal_rate=${cfg.signalRate}/s`,
    `duration=${cfg.duration ?? "forever"}`,
    `seed=${cfg.seed}`,
    `tenants=[${cfg.tenants.join(",")}]`,
    `namespaces=[${cfg.namespaces.join(",")}]`,
    `llm=${cfg.enableLlm ? "on" : "off"}`,
  ].join(" ");
}
