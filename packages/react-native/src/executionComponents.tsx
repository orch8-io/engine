import React, { useEffect, useMemo, useReducer, useState } from "react";
import {
  AppState, FlatList, Pressable, StyleSheet, Text, View, useColorScheme,
  type StyleProp, type ViewStyle,
} from "react-native";
import {
  ApprovalDecisionLedger, approvalExpired, canDecide, EXECUTION_LABEL, visibleApprovals,
  type Orch8Approval, type Orch8ApprovalDecision, type Orch8ApprovalDecisionResult,
  type Orch8ExecutionStep, type Orch8ExecutionStepState,
} from "./executionPresentation";

interface PresentationProps { style?: StyleProp<ViewStyle> }

export interface Orch8ExecutionStatusProps extends PresentationProps {
  state: Orch8ExecutionStepState;
  label?: string;
}

export function Orch8ExecutionStatus({ state, label, style }: Orch8ExecutionStatusProps) {
  const colors = useColors();
  const text = label ?? EXECUTION_LABEL[state];
  const color = state === "failed" ? colors.danger : state === "completed" ? colors.ok : colors.ink;
  return (
    <View style={[styles.status, { backgroundColor: colors.surface }, style]}>
      <Text style={[styles.statusText, { color }]}>{text}</Text>
    </View>
  );
}

export interface Orch8ExecutionTimelineProps extends PresentationProps {
  /** Steps or events in display order. Supply a unique ID for each activation. */
  steps: readonly Orch8ExecutionStep[];
  emptyLabel?: string;
  onSelectStep?: (step: Orch8ExecutionStep) => void;
}

export function Orch8ExecutionTimeline({
  steps,
  style,
  emptyLabel = "No execution events yet.",
  onSelectStep,
}: Orch8ExecutionTimelineProps) {
  const colors = useColors();
  return (
    <FlatList
      style={style}
      data={steps}
      keyExtractor={(step) => step.id}
      ListEmptyComponent={<Text style={{ color: colors.muted }}>{emptyLabel}</Text>}
      ItemSeparatorComponent={ListSeparator}
      renderItem={({ item }) => {
        const timestamp = typeof item.timestampMs === "number" ? new Date(item.timestampMs) : null;
        const content = (
          <>
            <View style={styles.row}>
              <Text style={[styles.title, styles.fill, { color: colors.ink }]}>{item.label}</Text>
              <Orch8ExecutionStatus state={item.state} />
            </View>
            {item.detail && <Text style={[styles.body, { color: colors.muted }]}>{item.detail}</Text>}
            {(item.attempt !== undefined || (timestamp && Number.isFinite(timestamp.getTime()))) && (
              <Text style={[styles.body, { color: colors.muted }]}>
                {[item.attempt === undefined ? null : `Attempt ${item.attempt}`,
                  timestamp && Number.isFinite(timestamp.getTime()) ? timestamp.toLocaleString() : null].filter(Boolean).join(" · ")}
              </Text>
            )}
          </>
        );
        const rowStyle = [styles.card, { borderColor: colors.border }];
        return onSelectStep ? (
          <Pressable accessibilityRole="button" onPress={() => onSelectStep(item)} style={rowStyle}>
            {content}
          </Pressable>
        ) : <View style={rowStyle}>{content}</View>;
      }}
    />
  );
}

export interface Orch8ApprovalInboxProps extends PresentationProps {
  /** An opaque, non-secret identifier for the current account or catalog scope. */
  scopeKey: string;
  /** Authoritative snapshots from the host app, including terminal updates. */
  approvals: readonly Orch8Approval[];
  /** Commit with the supplied revision and idempotency key, then return the durable outcome. */
  onDecide: (decision: Orch8ApprovalDecision) => Promise<Orch8ApprovalDecisionResult>;
  refreshing?: boolean;
  onRefresh?: () => void;
  emptyLabel?: string;
}

export function Orch8ApprovalInbox(props: Orch8ApprovalInboxProps) {
  return <ScopedApprovalInbox key={props.scopeKey} {...props} />;
}

function ScopedApprovalInbox({
  approvals,
  scopeKey,
  onDecide,
  refreshing = false,
  onRefresh,
  emptyLabel = "No approvals waiting.",
  style,
}: Orch8ApprovalInboxProps) {
  const colors = useColors();
  const [ledger] = useState(() => new ApprovalDecisionLedger(scopeKey));
  const [now, setNow] = useState(Date.now);
  const [, redraw] = useReducer((value: number) => value + 1, 0);
  const items = useMemo(() => visibleApprovals(approvals), [approvals]);
  const nextExpiry = items.reduce((next, item) =>
    !approvalExpired(item, now) && item.expiresAtMs != null
      ? Math.min(next, item.expiresAtMs)
      : next, Infinity);
  useEffect(() => {
    if (!Number.isFinite(nextExpiry)) return;
    const delay = Math.min(2_147_483_647, Math.max(1, nextExpiry - Date.now()));
    const timer = setTimeout(() => setNow(Date.now()), delay);
    return () => clearTimeout(timer);
  }, [nextExpiry, now]);
  useEffect(() => {
    const listener = AppState.addEventListener("change", (state) => {
      if (state === "active") setNow(Date.now());
    });
    return () => listener.remove();
  }, []);

  const decide = async (approval: Orch8Approval, value: string) => {
    const current = Date.now();
    setNow(current);
    const decision = ledger.begin(approval, value, current);
    if (!decision) return;
    redraw();
    try {
      const result = await onDecide(decision);
      ledger.finish(decision, result === "accepted" || result === "stale" ? result : "retry");
    } catch {
      ledger.finish(decision, "retry");
    } finally {
      redraw();
    }
  };

  return (
    <FlatList
      style={style}
      data={items}
      extraData={now}
      keyExtractor={(item) => item.id}
      onRefresh={onRefresh}
      refreshing={onRefresh ? refreshing : undefined}
      ListEmptyComponent={<Text style={{ color: colors.muted }}>{emptyLabel}</Text>}
      ItemSeparatorComponent={ListSeparator}
      renderItem={({ item }) => {
        const action = ledger.get(item);
        const expired = approvalExpired(item, now);
        const valid = scopeKey.length > 0 && canDecide(item, now);
        const status = action?.status === "accepted" ? "Decision saved." :
          expired ? "This approval has expired." :
          action?.status === "stale" ? "This approval changed. Refresh to see the latest state." :
          action?.status === "submitting" ? "Saving decision…" :
          action?.status === "retry" ? "Decision could not be confirmed. Retry the same choice." :
          !valid ? "Approval options are unavailable." : null;
        return (
          <View style={[styles.card, { borderColor: colors.border }]}>
            <Text accessibilityRole="header" style={[styles.title, { color: colors.ink }]}>{item.title}</Text>
            <Text style={[styles.body, { color: colors.ink }]}>{item.prompt}</Text>
            {item.expiresAtMs != null && !expired && (
              <Text style={[styles.body, { color: colors.muted }]}>Expires {new Date(item.expiresAtMs).toLocaleString()}</Text>
            )}
            {status && <Text accessibilityLiveRegion="polite" style={[styles.body, { color: colors.muted }]}>{status}</Text>}
            {valid && (action === undefined || action.status === "retry") && (
              <View style={styles.choices}>
                {item.choices.filter((choice) => action?.status !== "retry" || choice.value === action.value).map((choice) => (
                  <Pressable
                    key={choice.value}
                    accessibilityRole="button"
                    accessibilityLabel={`${choice.label} for ${item.title}`}
                    onPress={() => { void decide(item, choice.value); }}
                    style={({ pressed }) => [styles.choice, { backgroundColor: colors.action, opacity: pressed ? 0.75 : 1 }]}
                  >
                    <Text style={[styles.statusText, { color: colors.onAction }]}>{choice.label}</Text>
                  </Pressable>
                ))}
              </View>
            )}
          </View>
        );
      }}
    />
  );
}

function ListSeparator() { return <View style={styles.separator} />; }

const light = {
  ink: "#172033", muted: "#475569", surface: "#e8edf5", border: "#a5b0c1",
  danger: "#9f1239", ok: "#166534", action: "#1d4ed8", onAction: "#ffffff",
};
const dark = {
  ink: "#e2e8f0", muted: "#b7c3d4", surface: "#293449", border: "#627089",
  danger: "#fecdd3", ok: "#bbf7d0", action: "#bfdbfe", onAction: "#172033",
};
function useColors() { return useColorScheme() === "dark" ? dark : light; }

const styles = StyleSheet.create({
  fill: { flex: 1 },
  card: { borderWidth: StyleSheet.hairlineWidth, borderRadius: 12, padding: 16, gap: 8 },
  row: { flexDirection: "row", alignItems: "center", gap: 8, flexWrap: "wrap" },
  title: { fontSize: 16, fontWeight: "600" },
  body: { fontSize: 14 },
  status: { alignSelf: "flex-start", paddingHorizontal: 10, paddingVertical: 4, borderRadius: 999 },
  statusText: { fontSize: 14, fontWeight: "600" },
  separator: { height: 8 },
  choices: { flexDirection: "row", flexWrap: "wrap", gap: 8 },
  choice: { minHeight: 48, justifyContent: "center", paddingVertical: 10, paddingHorizontal: 16, borderRadius: 8 },
});
