import { h } from "./dom.js";

export const BADGE_HREF = "https://orch8.io/";

/** `https://orch8.io/?ref=embed&v=<vendor>` */
export function badgeHref(vendor: string | null | undefined): string {
  const url = new URL(BADGE_HREF);
  url.searchParams.set("ref", "embed");
  if (vendor) url.searchParams.set("v", vendor.slice(0, 64));
  return url.href;
}

/**
 * The "Powered by Orch8" link. Branded (never keyword-stuffed) anchor text and a
 * followed link: `rel="noopener"` protects the host page from `window.opener`
 * while keeping the backlink and the referrer for attribution.
 */
export function renderBadge(vendor: string | null | undefined, text: string, label: string): HTMLAnchorElement {
  return h(
    "a",
    {
      class: "badge-link",
      part: "badge",
      href: badgeHref(vendor),
      target: "_blank",
      rel: "noopener",
      "aria-label": label,
    },
    h("span", { class: "badge-mark", "aria-hidden": "true" }),
    text,
  );
}
