import { ApprovalsWidget } from "@/components/widgets";
import { getConfig } from "@/lib/config";

export default function ApprovalsPage() {
  const { publicUrl, vendor } = getConfig();
  return (
    <>
      <h1>Approvals</h1>
      <p className="lede">Requests from your workflows that need a person to decide.</p>
      <ApprovalsWidget baseUrl={publicUrl} vendor={vendor} />
    </>
  );
}
