import type { TokenProvider } from "@orch8/embed/react";

/**
 * Browser-side token provider shared by every widget on the page. Calls this
 * app's own `/api/orch8/embed-token` route (session-cookie authenticated),
 * caches the token until shortly before it expires and dedupes concurrent calls.
 */
export function createTokenProvider(endpoint = "/api/orch8/embed-token"): TokenProvider {
  let cached: { token: string; exp: number } | null = null;
  let inflight: Promise<string> | null = null;

  return ({ reason }) => {
    const fresh = cached && cached.exp - 60_000 > Date.now();
    if (fresh && reason !== "unauthorized") return cached!.token;
    inflight ??= (async () => {
      try {
        const res = await fetch(endpoint, { method: "POST", credentials: "same-origin", cache: "no-store" });
        if (!res.ok) throw new Error(`embed token endpoint returned ${res.status}`);
        const body = (await res.json()) as { token: string; expires_at: string };
        cached = { token: body.token, exp: Date.parse(body.expires_at) };
        return body.token;
      } finally {
        inflight = null;
      }
    })();
    return inflight;
  };
}

export const tokenProvider = createTokenProvider();
