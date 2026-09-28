/**
 * Every user-visible string. Override any subset via the `strings` property:
 *   el.strings = { runsTitle: "Automatisierungen" }
 * `{name}` placeholders are interpolated.
 */
export const defaultStrings = {
  loading: "Loading…",
  retry: "Try again",
  refreshing: "Updating…",
  reconnecting: "Connection lost. Retrying…",
  errorGeneric: "Something went wrong. Please try again.",
  errorNetwork: "Can't reach the server. Check your connection and try again.",
  errorUnauthorized: "Your session has expired. Reload the page to continue.",
  errorForbidden: "You don't have permission to do this.",
  errorNotFound: "We couldn't find this item.",
  errorRateLimited: "Too many requests. Please wait a moment and try again.",
  errorConfig: "This widget isn't configured: it needs a base URL and an access token.",
  errorInvalid: "The request was rejected: {message}",
  poweredBy: "Powered by Orch8",
  poweredByLabel: "Powered by Orch8 (opens in a new tab)",

  state_scheduled: "Scheduled",
  state_running: "Running",
  state_waiting: "Waiting",
  state_paused: "Paused",
  state_completed: "Completed",
  state_failed: "Failed",
  state_cancelled: "Cancelled",
  state_pending: "Pending",
  state_skipped: "Skipped",

  runsTitle: "Runs",
  runsEmpty: "No runs yet.",
  runsEmptyHint: "Runs appear here as soon as one starts.",
  runsLoadMore: "Load more",
  runsView: "View run {id} ({sequence})",
  runsColSequence: "Workflow",
  runsColState: "Status",
  runsColStep: "Current step",
  runsColUpdated: "Updated",
  runsStart: "Start {sequence}",
  runsStarted: "Run started.",

  timelineTitle: "Run of {sequence}",
  timelineNoRun: "Select a run to see its steps.",
  timelineEmpty: "No steps have started yet.",
  timelineRunState: "Status: {state}",
  timelineStepChanged: "{step}: {state}",
  timelineStarted: "Started {time}",
  timelineFinished: "Finished {time}",
  timelineDuration: "took {duration}",
  timelineOutput: "Output",

  approvalsTitle: "Approvals",
  approvalsEmpty: "Nothing is waiting for your decision.",
  approvalsDecision: "Decision",
  approvalsComment: "Comment (optional)",
  approvalsSubmit: "Submit decision",
  approvalsSubmitting: "Submitting…",
  approvalsResolved: "Decision recorded.",
  approvalsAlreadyResolved: "This request was already resolved.",
  approvalsChooseOne: "Choose an option first.",
  approvalsReadOnly: "You can view this request but not respond to it.",
  approvalsRequested: "Requested {time}",
  approvalsApprove: "Approve",
  approvalsReject: "Reject",

  builderTitle: "Steps of {sequence}",
  builderNoSequence: "No workflow selected.",
  builderEmpty: "This workflow has no steps yet. Add one below.",
  builderStep: "Step {n}",
  builderStepId: "Step ID",
  builderHandler: "Action",
  builderParams: "Parameters (JSON)",
  builderMoveUp: "Move {step} up",
  builderMoveDown: "Move {step} down",
  builderRemove: "Remove {step}",
  builderMoved: "Moved {step} to position {n}.",
  builderRemoved: "Removed {step}.",
  builderAdded: "Added {step}.",
  builderAdd: "Add step",
  builderAddHandler: "New step action",
  builderSave: "Save changes",
  builderSaving: "Saving…",
  builderSaved: "Changes saved.",
  builderUnsaved: "Unsaved changes",
  builderInvalidJson: "Parameters must be a valid JSON object.",
  builderIdRequired: "Step ID is required.",
  builderIdInvalid: "Use letters, numbers, dot, dash or underscore.",
  builderIdDuplicate: "Another step already uses this ID.",
  builderFixErrors: "Fix the highlighted fields before saving.",
  builderLocked: "{type} block “{id}” (edit it in Orch8)",
  builderReadOnly: "You can view these steps but not edit them.",
} as const;

export type Orch8StringKey = keyof typeof defaultStrings;
export type Orch8Strings = Record<Orch8StringKey, string>;

export function formatString(template: string, vars?: Record<string, string | number>): string {
  if (!vars) return template;
  return template.replace(/\{(\w+)\}/g, (m, k: string) => (k in vars ? String(vars[k]) : m));
}
