import "server-only";
import type { Orch8Config } from "./config";
import { scopesFor, subTenantFor, type DemoUser } from "./users";

export const TOKEN_TTL_SECONDS = 900;

export interface MintedToken {
  token: string;
  expires_at: string;
}

export class EmbedTokenError extends Error {
  constructor(
    message: string,
    readonly status: number,
  ) {
    super(message);
    this.name = "EmbedTokenError";
  }
}

function b64url(s: string): string {
  return Buffer.from(s, "utf8").toString("base64url");
}

/** Mock mode only: an unsigned token the built-in mock engine accepts. */
export function mockToken(user: DemoUser, config: Orch8Config, now = Date.now()): MintedToken {
  const iat = Math.floor(now / 1000);
  const payload = {
    v: 1,
    tid: config.tenantId,
    sub: subTenantFor(user),
    scp: scopesFor(user),
    seq: config.sequences,
    iat,
    exp: iat + TOKEN_TTL_SECONDS,
    jti: crypto.randomUUID(),
  };
  return {
    token: `o8e1.${b64url(JSON.stringify(payload))}.${b64url("mock-signature")}`,
    expires_at: new Date((iat + TOKEN_TTL_SECONDS) * 1000).toISOString(),
  };
}

/**
 * Mints a short-lived embed token for `user` via `POST /api/v1/embed/tokens`,
 * authenticated with the tenant API key. Runs on the server only: the API key
 * never leaves this process; the browser only ever sees the scoped token.
 */
export async function mintEmbedToken(
  user: DemoUser,
  config: Orch8Config,
  fetchImpl: typeof fetch = fetch,
): Promise<MintedToken> {
  if (config.mode === "mock") return mockToken(user, config);
  if (!config.apiKey) throw new EmbedTokenError("ORCH8_API_KEY is not set", 500);

  let res: Response;
  try {
    res = await fetchImpl(`${config.url}/api/v1/embed/tokens`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-api-key": config.apiKey,
        "x-tenant-id": config.tenantId,
      },
      body: JSON.stringify({
        sub_tenant: subTenantFor(user),
        scopes: scopesFor(user),
        sequences: config.sequences,
        ttl_seconds: TOKEN_TTL_SECONDS,
      }),
      cache: "no-store",
    });
  } catch (err) {
    throw new EmbedTokenError(`Orch8 unreachable at ${config.url}: ${(err as Error).message}`, 502);
  }
  if (!res.ok) {
    const body = (await res.json().catch(() => null)) as { error?: { message?: string } } | null;
    const hint =
      res.status === 404
        ? " (is [embed] token_secret / ORCH8_EMBED_TOKEN_SECRET configured on the engine?)"
        : res.status === 401
          ? " (check ORCH8_API_KEY / ORCH8_TENANT_ID)"
          : "";
    throw new EmbedTokenError(`Orch8 refused to mint a token: HTTP ${res.status} ${body?.error?.message ?? ""}${hint}`.trim(), 502);
  }
  const json = (await res.json()) as Partial<MintedToken>;
  if (typeof json.token !== "string" || typeof json.expires_at !== "string") {
    throw new EmbedTokenError("Orch8 returned an unexpected token payload", 502);
  }
  return { token: json.token, expires_at: json.expires_at };
}
