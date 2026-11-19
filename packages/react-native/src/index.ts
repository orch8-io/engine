import { NativeModules, NativeEventEmitter } from "react-native";
import { Orch8Client, type NativeOrch8 } from "./core";

export * from "./types";
export { previewDataVisibility, resolveOfflineConflict, workflowSyncState } from "./offlineWorkflow";
export type {
  BackgroundDiagnostics, DataDestination, DataVisibility, OfflineConflictDecision,
  OfflineConflictHandler, OfflineConflictResult, OfflineEdit, VisibilityEntry, WorkflowJson,
  WorkflowCompletion, WorkflowSyncEvidence, WorkflowSyncReceipt, WorkflowSyncState,
} from "./offlineWorkflow";
export { Orch8SyncStatus } from "./syncStatus";
export { DurableMediaUploader } from "./mediaUpload";
export type {
  MediaUploadOptions, MediaUploadProgress, MediaUploadState, MediaUploadStore, MediaUploadTransport,
} from "./mediaUpload";
export { Orch8ApprovalInbox, Orch8ExecutionStatus, Orch8ExecutionTimeline } from "./executionComponents";
export type {
  Orch8ApprovalInboxProps, Orch8ExecutionStatusProps, Orch8ExecutionTimelineProps,
} from "./executionComponents";
export type {
  Orch8Approval, Orch8ApprovalChoice, Orch8ApprovalDecision, Orch8ApprovalDecisionResult,
  Orch8ExecutionStep, Orch8ExecutionStepState,
} from "./executionPresentation";
export { Orch8Account, Orch8Catalog, Orch8CatalogRun, CatalogHistoryCursor } from "./scoped";
export type { AccountIdentity, CatalogConfiguration, CatalogRunRequest, CatalogCallbackContext,
  CatalogStepHandler, CatalogTokenSource, CatalogRunSnapshot, CatalogArchiveReceipt,
  CatalogNativeState, CatalogRunSummary, CatalogRunPage } from "./scoped";
export {
  Orch8Client,
  PermanentHandlerError,
  parseTaskContext,
  orch8ErrorKind,
  EVENTS,
  MAX_DELEGATION_TTL_SECS,
} from "./core";
export type { NativeOrch8, NativeDelegateRequest, EventSource, Subscription, TokenFetcher } from "./core";

const { Orch8Module } = NativeModules as { Orch8Module?: NativeOrch8 };

if (!Orch8Module) {
  throw new Error(
    "@orch8.io/react-native-orch8: NativeModule not found. " +
      "Rebuild the app after installing (pod install on iOS; on Android add " +
      "Orch8's Maven repository, see the package README)."
  );
}

const emitter = new NativeEventEmitter(Orch8Module as never);

export const orch8 = new Orch8Client(Orch8Module, emitter);
export default orch8;
