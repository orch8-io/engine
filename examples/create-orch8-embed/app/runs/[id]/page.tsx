import Link from "next/link";
import { TimelineWidget } from "@/components/widgets";
import { getConfig } from "@/lib/config";

export default async function RunPage({ params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  const { publicUrl, vendor } = getConfig();
  return (
    <>
      <p><Link href="/runs">← All runs</Link></p>
      <h1>Run timeline</h1>
      <p className="lede">Updates live until the run finishes. Waiting steps are resolved on <Link href="/approvals">Approvals</Link>.</p>
      <TimelineWidget baseUrl={publicUrl} vendor={vendor} runId={id} />
    </>
  );
}
