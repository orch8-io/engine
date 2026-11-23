import { NativeModules, NativeEventEmitter } from "react-native";
import { Orch8Client, type NativeOrch8 } from "./core";
import {
  createFileMediaUploadSource, describeMediaUploadFile, type MediaFilePathResolver, type NativeMediaFileReader,
} from "./mediaFileSource";

export type { MediaFilePathResolver } from "./mediaFileSource";

export * from "./types";
export { previewDataVisibility, resolveOfflineConflict, workflowSyncState } from "./offlineWorkflow";
export type {
  BackgroundDiagnostics, DataDestination, DataVisibility, OfflineConflictDecision,
  OfflineConflictHandler, OfflineConflictResult, OfflineEdit, VisibilityEntry, WorkflowJson,
  WorkflowCompletion, WorkflowSyncEvidence, WorkflowSyncReceipt, WorkflowSyncState,
} from "./offlineWorkflow";
export { Orch8SyncStatus } from "./syncStatus";
export { DurableMediaUploader } from "./mediaUpload";
export { createHttpMediaUploadTransport, type MediaUploadRequest } from "./mediaUploadHttp";
export {
  SqliteAccountOutcomeJournal, type AccountOutcomeReporter, type AccountOutcomeReceipt,
  type AccountOutcomeCredentials, type AccountOutcomeDelivery, type AccountOutcomeCredential,
  type OutcomePrincipal, type PendingOutcome,
} from "./outcomeReporting";
export { SqliteMediaUploadJournal } from "./mediaUploadJournal";
export { SqliteApprovalCommitAdapter } from "./approvalCommit";
export type { StoredApprovalDecision } from "./approvalCommit";
export type { Orch8SqliteDatabase } from "./sqlite";
export type { MediaUploadSqliteDatabase } from "./mediaUploadJournal";
export type {
  MediaUploadFile, MediaUploadOptions, MediaUploadProgress, MediaUploadState, MediaUploadStore, MediaUploadTransport,
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

const { Orch8Module } = NativeModules as { Orch8Module?: NativeOrch8 & NativeMediaFileReader };

if (!Orch8Module) {
  throw new Error(
    "@orch8.io/react-native-orch8: NativeModule not found. " +
      "Rebuild the app after installing (pod install on iOS; on Android add " +
      "Orch8's Maven repository, see the package README)."
  );
}

const nativeModule = Orch8Module;
const emitter = new NativeEventEmitter(nativeModule as never);

/** Resolve each upload ID to an absolute path to an app-owned local file. */
export function fileMediaUploadSource(resolvePath?: MediaFilePathResolver) {
  return createFileMediaUploadSource(nativeModule, resolvePath);
}

/** Capture an upload's original file identity and the SHA-256 of every chunk. */
export function prepareMediaUploadFile(path: string, chunkBytes?: number) {
  return describeMediaUploadFile(nativeModule, path, chunkBytes);
}

export const orch8 = new Orch8Client(Orch8Module, emitter);
export default orch8;
