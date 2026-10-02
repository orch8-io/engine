import { BuilderWidget } from "@/components/widgets";
import { getCurrentUser } from "@/lib/auth";
import { getConfig } from "@/lib/config";

export default async function BuilderPage() {
  const { publicUrl, vendor, demoSequence } = getConfig();
  const user = await getCurrentUser();
  return (
    <>
      <h1>Workflow builder</h1>
      <p className="lede">
        {user.role === "admin"
          ? "Edit the steps of your onboarding workflow. Changes apply to new runs."
          : "Members can view steps; switch to an admin to edit (the token lacks builder:edit)."}
      </p>
      <BuilderWidget baseUrl={publicUrl} vendor={vendor} sequence={demoSequence} />
    </>
  );
}
