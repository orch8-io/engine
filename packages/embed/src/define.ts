import { VERSION } from "./version.js";
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

const warned = new Set<string>();

/**
 * Registers all custom elements. Idempotent and a no-op outside the browser
 * (SSR), so it is safe to call from anywhere. Warns once when a different
 * version of the package already registered a tag.
 */
export function defineOrch8Elements(registry?: CustomElementRegistry): void {
  const reg = registry ?? (typeof customElements !== "undefined" ? customElements : undefined);
  if (!reg) return;
  for (const [tag, ctor] of Object.entries(ELEMENTS)) {
    const existing = reg.get(tag);
    if (!existing) {
      reg.define(tag, ctor as unknown as CustomElementConstructor);
    } else if (existing !== (ctor as unknown) && !warned.has(`${tag}@${(existing as { orch8Version?: string }).orch8Version}`)) {
      // Another copy of the package registered first; its behaviour wins, which may surprise.
      const other = (existing as { orch8Version?: string }).orch8Version;
      if (other !== VERSION) {
        warned.add(`${tag}@${other}`);
        console.warn(
          `[@orch8/embed] <${tag}> is already registered by ${other ? `version ${other}` : "another copy"}; ` +
            `this page loaded version ${VERSION}. The first registration wins - load a single version.`,
        );
      }
    }
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
