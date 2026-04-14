/**
 * Bootstrap: register all sequences per tenant/namespace and, if the
 * engine supports cron, create a couple of cron triggers so the
 * dashboard's scheduled-job surface has something to render.
 *
 * Idempotent: if `getSequenceByName` finds an existing sequence with the
 * same tenant/namespace/name, we reuse it instead of creating a duplicate.
 */

import type { LoadgenConfig } from "../config.ts";
import type { Template } from "../catalog/index.ts";
import type { SequenceDef } from "../types.ts";
import type { Orch8Client } from "../client.ts";

import { buildSequencesFor } from "../catalog/index.ts";
import { ApiError } from "../client.ts";
import type { Rng } from "../lib/rng.ts";
import { withRetry } from "../lib/retry.ts";

export interface BootstrapResult {
  /** sequences[tenantId][namespace][name] = SequenceDef */
  byName: Map<string, SequenceDef>;
  cronsCreated: number;
}

/** key = `${tenant}|${namespace}|${name}` */
export function seqKey(tenant: string, ns: string, name: string): string {
  return `${tenant}|${ns}|${name}`;
}

export async function bootstrap(
  client: Orch8Client,
  cfg: LoadgenConfig,
  rng: Rng,
  templates: readonly Template[],
): Promise<BootstrapResult> {
  const byName = new Map<string, SequenceDef>();

  for (const tenant of cfg.tenants) {
    for (const ns of cfg.namespaces) {
      const built = buildSequencesFor(tenant, ns, rng, templates);
      for (const seq of built) {
        const existing = await client.getSequenceByName(tenant, ns, seq.name);
        if (existing) {
          byName.set(seqKey(tenant, ns, seq.name), existing);
          continue;
        }
        try {
          await withRetry(`createSequence ${seq.name}`, () =>
            client.createSequence(seq),
          );
          byName.set(seqKey(tenant, ns, seq.name), seq);
        } catch (err) {
          if (err instanceof ApiError && err.status === 409) {
            // Race: another loadgen already created it.
            const again = await client.getSequenceByName(tenant, ns, seq.name);
            if (again) byName.set(seqKey(tenant, ns, seq.name), again);
          } else {
            throw err;
          }
        }
      }
    }
  }

  let cronsCreated = 0;
  // Cron is best-effort — if the endpoint isn't available, skip silently.
  try {
    await client.listCron();
    const firstTenant = cfg.tenants[0]!;
    const firstNs = cfg.namespaces[0]!;
    const order = byName.get(seqKey(firstTenant, firstNs, "order-fulfillment"));
    const pipeline = byName.get(seqKey(firstTenant, firstNs, "data-pipeline"));
    if (order) {
      await tryCreateCron(client, {
        id: rng.uuid(),
        tenant_id: firstTenant,
        namespace: firstNs,
        sequence_id: order.id,
        schedule: "*/1 * * * *",
        enabled: true,
      }) && cronsCreated++;
    }
    if (pipeline) {
      await tryCreateCron(client, {
        id: rng.uuid(),
        tenant_id: firstTenant,
        namespace: firstNs,
        sequence_id: pipeline.id,
        schedule: "*/5 * * * *",
        enabled: true,
      }) && cronsCreated++;
    }
  } catch {
    // listCron 404 → endpoint not exposed in this build, fine.
  }

  return { byName, cronsCreated };
}

async function tryCreateCron(
  client: Orch8Client,
  body: Record<string, unknown>,
): Promise<boolean> {
  try {
    await client.createCron(body);
    return true;
  } catch (err) {
    if (err instanceof ApiError && (err.status === 404 || err.status === 409)) {
      return false;
    }
    throw err;
  }
}
