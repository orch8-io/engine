import { Orch8ApprovalsElement } from "./elements/approvals.js";
import { Orch8BadgeElement } from "./elements/badge.js";
import { Orch8BuilderElement } from "./elements/builder.js";
import { Orch8RunTimelineElement } from "./elements/run-timeline.js";
import { Orch8RunsElement } from "./elements/runs.js";

export const ELEMENTS = {
  "orch8-runs": Orch8RunsElement,
  "orch8-run-timeline": Orch8RunTimelineElement,
  "orch8-approvals": Orch8ApprovalsElement,
  "orch8-builder": Orch8BuilderElement,
  "orch8-badge": Orch8BadgeElement,
} as const;

/**
 * Registers all custom elements. Idempotent and a no-op outside the browser
 * (SSR), so it is safe to call from anywhere.
 */
export function defineOrch8Elements(registry?: CustomElementRegistry): void {
  const reg = registry ?? (typeof customElements !== "undefined" ? customElements : undefined);
  if (!reg) return;
  for (const [tag, ctor] of Object.entries(ELEMENTS)) {
    if (!reg.get(tag)) reg.define(tag, ctor as unknown as CustomElementConstructor);
  }
}

declare global {
  interface HTMLElementTagNameMap {
    "orch8-runs": Orch8RunsElement;
    "orch8-run-timeline": Orch8RunTimelineElement;
    "orch8-approvals": Orch8ApprovalsElement;
    "orch8-builder": Orch8BuilderElement;
    "orch8-badge": Orch8BadgeElement;
  }
}
