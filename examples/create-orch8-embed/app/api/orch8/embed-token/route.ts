import { NextResponse, type NextRequest } from "next/server";
import { getCurrentUser } from "@/lib/auth";
import { getConfig } from "@/lib/config";
import { EmbedTokenError, mintEmbedToken } from "@/lib/embed-token";

export const dynamic = "force-dynamic";

/**
 * POST /api/orch8/embed-token — exchanges the user's app session for a
 * short-lived, sub-tenant-scoped Orch8 embed token. The tenant API key stays
 * on the server.
 */
export async function POST(req: NextRequest) {
  // Same-origin only: the token is a bearer credential for this user's data.
  const origin = req.headers.get("origin");
  if (origin && origin !== req.nextUrl.origin) {
    return NextResponse.json({ error: "cross-origin request refused" }, { status: 403 });
  }
  const user = await getCurrentUser();
  // A real app returns 401 here when there is no session.
  try {
    const minted = await mintEmbedToken(user, getConfig());
    return NextResponse.json(minted, { headers: { "cache-control": "no-store" } });
  } catch (err) {
    const status = err instanceof EmbedTokenError ? err.status : 500;
    console.error("[orch8] embed token mint failed:", (err as Error).message);
    return NextResponse.json({ error: "could not mint embed token" }, { status, headers: { "cache-control": "no-store" } });
  }
}
