import Link from "next/link";
import { getConfig } from "@/lib/config";

export default function Home() {
  const config = getConfig();
  return (
    <>
      <h1>Your customers&apos; workflows, inside your product</h1>
      <p className="lede">
        Each page embeds one <code>@orch8/embed</code> widget. Tokens are minted per signed-in customer by{" "}
        <code>app/api/orch8/embed-token/route.ts</code>; the API key never reaches the browser.
      </p>
      <div className="cards">
        <Link className="card" href="/runs"><strong>Runs</strong>Recent runs, start <code>{config.demoSequence}</code>, open a timeline.</Link>
        <Link className="card" href="/approvals"><strong>Approvals</strong>Resolve human-in-the-loop steps with a comment.</Link>
        <Link className="card" href="/builder"><strong>Builder</strong>Add, reorder and configure steps (admins only).</Link>
        <Link className="card" href="/badge"><strong>Badge</strong>&ldquo;Powered by Orch8&rdquo;, hidden with a white-label license.</Link>
      </div>
    </>
  );
}
