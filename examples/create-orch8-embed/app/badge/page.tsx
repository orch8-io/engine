import { BadgeWidget } from "@/components/widgets";
import { getConfig } from "@/lib/config";

export default function BadgePage() {
  const { vendor } = getConfig();
  return (
    <>
      <h1>&ldquo;Powered by Orch8&rdquo;</h1>
      <p className="lede">
        Every widget shows this link in its footer. A license with the <code>white_label</code> feature lets you set{" "}
        <code>hide_badge</code> via <code>PUT /api/v1/embed/theme</code>. You can also place it yourself, e.g. in your footer:
      </p>
      <BadgeWidget vendor={vendor} />
    </>
  );
}
