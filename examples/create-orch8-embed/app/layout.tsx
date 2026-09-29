import type { Metadata } from "next";
import Link from "next/link";
import type { ReactNode } from "react";
import { getCurrentUser } from "@/lib/auth";
import { getConfig } from "@/lib/config";
import { DEMO_USERS, subTenantFor } from "@/lib/users";
import { switchUser } from "./actions";
import "./globals.css";

export const metadata: Metadata = {
  title: "Acme SaaS · Orch8 embed starter",
  description: "Orch8 runs, approvals and workflow builder embedded in a SaaS app",
};

export default async function RootLayout({ children }: { children: ReactNode }) {
  const user = await getCurrentUser();
  const config = getConfig();
  return (
    <html lang="en">
      <body>
        <a className="skip" href="#main">Skip to content</a>
        <header className="topbar">
          <Link href="/" className="brand">Acme SaaS</Link>
          <nav aria-label="Main">
            <Link href="/runs">Runs</Link>
            <Link href="/approvals">Approvals</Link>
            <Link href="/builder">Builder</Link>
            <Link href="/badge">Badge</Link>
          </nav>
          <form action={switchUser} className="who">
            <label htmlFor="user">Signed in as</label>
            <select id="user" name="user" defaultValue={user.id}>
              {DEMO_USERS.map((u) => (
                <option key={u.id} value={u.id}>
                  {u.name} · {u.orgName}
                </option>
              ))}
            </select>
            <button type="submit">Switch</button>
          </form>
        </header>
        {config.mode === "mock" ? (
          <p className="mode" role="note">
            Mock engine: data is fake and lives in this dev server. Set <code>ORCH8_URL</code> in <code>.env.local</code> to use a real engine.
          </p>
        ) : null}
        <main id="main">
          <p className="context">
            Customer <strong>{user.orgName}</strong> → Orch8 sub-tenant <code>{subTenantFor(user)}</code>
          </p>
          {children}
        </main>
      </body>
    </html>
  );
}
