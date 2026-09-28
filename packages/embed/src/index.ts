import { defineOrch8Elements } from "./define.js";

export { defineOrch8Elements, ELEMENTS } from "./define.js";
export { Orch8Element } from "./base.js";
export { Orch8RunsElement, type RunSelectDetail } from "./elements/runs.js";
export { Orch8RunTimelineElement } from "./elements/run-timeline.js";
export { Orch8ApprovalsElement, type ApprovalResolvedDetail } from "./elements/approvals.js";
export { Orch8BuilderElement, type SequenceSavedDetail } from "./elements/builder.js";
export { Orch8BadgeElement } from "./elements/badge.js";
export { EmbedClient, Orch8EmbedError, unwrapDefinition, type EmbedClientOptions, type Orch8ErrorKind } from "./client.js";
export { decodeEmbedToken, tokenAllows, type TokenProvider } from "./token.js";
export { defaultStrings, formatString, type Orch8StringKey, type Orch8Strings } from "./i18n.js";
export { clearThemeCache, sanitizeCssVars } from "./theme.js";
export { badgeHref } from "./badge.js";
export type * from "./types.js";

// Importing the package registers the elements (no-op during SSR).
defineOrch8Elements();
