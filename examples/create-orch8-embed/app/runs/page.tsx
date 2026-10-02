import { RunsWidget } from "@/components/widgets";
import { getConfig } from "@/lib/config";

export default function RunsPage() {
  const { publicUrl, vendor, demoSequence } = getConfig();
  return (
    <>
      <h1>Automation runs</h1>
      <p className="lede">Start a run, then open it to watch each step.</p>
      <RunsWidget baseUrl={publicUrl} vendor={vendor} startSequence={demoSequence} />
    </>
  );
}
