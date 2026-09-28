/**
 * STUB AUTH. Replace with your real session (NextAuth, Clerk, Supabase, …).
 * The only thing Orch8 needs from it is a stable id for the customer account
 * the user belongs to: that becomes the Orch8 sub-tenant.
 */
export interface DemoUser {
  id: string;
  name: string;
  email: string;
  /** Your customer account / organisation id. */
  orgId: string;
  orgName: string;
  role: "admin" | "member";
}

export const DEMO_USERS: DemoUser[] = [
  { id: "u_jane", name: "Jane (admin)", email: "jane@northwind.test", orgId: "northwind", orgName: "Northwind", role: "admin" },
  { id: "u_raj", name: "Raj (member)", email: "raj@northwind.test", orgId: "northwind", orgName: "Northwind", role: "member" },
  { id: "u_li", name: "Li (admin)", email: "li@globex.test", orgId: "globex", orgName: "Globex", role: "admin" },
];

export const USER_COOKIE = "demo_user";

export function resolveUser(cookieValue: string | undefined): DemoUser {
  return DEMO_USERS.find((u) => u.id === cookieValue) ?? DEMO_USERS[0]!;
}

/** Orch8 sub-tenant for a user: one per customer account. `[A-Za-z0-9._:-]`, ≤128 chars. */
export function subTenantFor(user: DemoUser): string {
  return `org:${user.orgId}`.replace(/[^A-Za-z0-9._:-]/g, "_").slice(0, 128);
}

export type EmbedScope = "runs:read" | "runs:start" | "approvals:resolve" | "sequences:read" | "builder:edit";

/** Decide what the embedded widgets may do for this user. Only admins edit workflows. */
export function scopesFor(user: DemoUser): EmbedScope[] {
  const scopes: EmbedScope[] = ["runs:read", "runs:start", "approvals:resolve", "sequences:read"];
  if (user.role === "admin") scopes.push("builder:edit");
  return scopes;
}
