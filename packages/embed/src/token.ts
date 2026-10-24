import type { EmbedTokenPayload } from "./types.js";

/** Returns a fresh token. `reason` tells the provider why it is being asked. */
export type TokenProvider = (context: {
  reason: "initial" | "expiring" | "unauthorized";
  /** Engine and application session requesting the credential. */
  baseUrl?: string;
  authContext?: string;
}) => string | Promise<string>;

const TOKEN_PREFIX = "o8e1.";

function base64UrlDecode(input: string): string {
  const b64 = input.replace(/-/g, "+").replace(/_/g, "/");
  const padded = b64 + "=".repeat((4 - (b64.length % 4)) % 4);
  const binary = atob(padded);
  const bytes = Uint8Array.from(binary, (c) => c.charCodeAt(0));
  return new TextDecoder().decode(bytes);
}

/**
 * Decodes the payload of an `o8e1` embed token WITHOUT verifying it. Used only for
 * UX decisions (refresh timing, hiding actions the token can't perform). Returns
 * null for anything that doesn't look like an embed token.
 */
export function decodeEmbedToken(token: string | null | undefined): EmbedTokenPayload | null {
  if (!token || !token.startsWith(TOKEN_PREFIX)) return null;
  const parts = token.split(".");
  if (parts.length !== 3 || !parts[1]) return null;
  try {
    const payload = JSON.parse(base64UrlDecode(parts[1])) as Partial<EmbedTokenPayload>;
    if (typeof payload !== "object" || payload === null) return null;
    if (
      payload.v !== 1 || typeof payload.tid !== "string" || !payload.tid ||
      typeof payload.sub !== "string" || !payload.sub ||
      typeof payload.jti !== "string" || !payload.jti ||
      !Number.isSafeInteger(payload.iat) || !Number.isSafeInteger(payload.exp) ||
      !Array.isArray(payload.scp) || !payload.scp.every((scope) => typeof scope === "string") ||
      !(payload.seq === null || (Array.isArray(payload.seq) && payload.seq.every((id) => typeof id === "string")))
    ) return null;
    return payload as EmbedTokenPayload;
  } catch {
    return null;
  }
}

/** True when the token is known to carry `scope`; true (optimistic) when undecodable. */
export function tokenAllows(token: string | null | undefined, scope: string): boolean {
  const payload = decodeEmbedToken(token);
  if (!payload) return true;
  return payload.scp.includes(scope);
}
