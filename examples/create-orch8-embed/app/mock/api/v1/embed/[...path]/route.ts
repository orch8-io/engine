import { NextResponse, type NextRequest } from "next/server";
import { getConfig } from "@/lib/config";
import { handleMock } from "@/lib/mock-engine";

export const dynamic = "force-dynamic";

type Ctx = { params: Promise<{ path: string[] }> };

async function handle(req: NextRequest, ctx: Ctx) {
  if (getConfig().mode !== "mock") return NextResponse.json({ error: { code: "not_found", message: "mock engine disabled" } }, { status: 404 });
  const { path } = await ctx.params;
  const body = req.method === "GET" ? undefined : await req.json().catch(() => undefined);
  const res = handleMock(req.method, path, req.headers.get("authorization"), body, req.nextUrl.searchParams);
  return NextResponse.json(res.body ?? null, { status: res.status, headers: { "cache-control": "no-store" } });
}

export const GET = handle;
export const POST = handle;
export const PUT = handle;
