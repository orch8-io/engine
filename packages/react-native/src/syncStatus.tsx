import React from "react";
import { Text } from "react-native";
import { workflowSyncState, type WorkflowSyncEvidence } from "./offlineWorkflow";

const labels = {
  in_progress: "In progress",
  local_complete: "Saved on this device",
  pending_sync: "Waiting to sync",
  acknowledged: "Received by the server",
  needs_review: "Needs review",
};

export function Orch8SyncStatus({ evidence }: { evidence: WorkflowSyncEvidence }) {
  return <Text accessibilityLiveRegion="polite">{labels[workflowSyncState(evidence)]}</Text>;
}
