"use client";
/**
 * Thin, typed React wrappers. SSR-safe: on the server they render the bare
 * custom-element tag with its attributes; the elements are registered and
 * JS-only props (tokenProvider, strings, fetchImpl) are assigned after mount.
 */
import { createElement, forwardRef, useEffect, useImperativeHandle, useRef, type CSSProperties } from "react";
import { defineOrch8Elements } from "./define.js";
import type { Orch8ApprovalsElement, ApprovalResolvedDetail } from "./elements/approvals.js";
import type { Orch8BadgeElement } from "./elements/badge.js";
import type { Orch8BuilderElement, SequenceSavedDetail } from "./elements/builder.js";
import type { Orch8RunTimelineElement } from "./elements/run-timeline.js";
import type { Orch8RunsElement, RunSelectDetail } from "./elements/runs.js";
import type { Orch8Strings } from "./i18n.js";
import type { TokenProvider } from "./token.js";
import type { RunDetail } from "./types.js";

export interface Orch8CommonProps {
  /** Engine origin, e.g. `https://orch8.example.com`. */
  baseUrl?: string;
  /** Static embed token (`o8e1…`). Prefer `tokenProvider` for refresh. */
  token?: string;
  tokenProvider?: TokenProvider;
  strings?: Partial<Orch8Strings>;
  /** Extra `--orch8-*` variables; override the server theme. */
  theme?: Record<string, string>;
  colorScheme?: "light" | "dark" | "auto";
  locale?: string;
  /** Vendor slug used in the "Powered by" link. */
  vendor?: string;
  fetchImpl?: typeof fetch;
  id?: string;
  className?: string;
  style?: CSSProperties;
}

type Handler<D> = (detail: D, event: CustomEvent<D>) => void;

interface WrapperSpec {
  tag: string;
  attrs: Record<string, string>;
  events: Record<string, string>;
}

function toAttr(value: unknown): string | undefined {
  if (value === undefined || value === null || value === false) return undefined;
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

function createWrapper<E extends HTMLElement, P extends Orch8CommonProps>(spec: WrapperSpec, displayName: string) {
  const Component = forwardRef<E, P>(function Orch8Wrapper(props, ref) {
    const inner = useRef<E | null>(null);
    useImperativeHandle(ref, () => inner.current as E, []);
    const record = props as unknown as Record<string, unknown>;
    const common = props as unknown as Orch8CommonProps;

    useEffect(() => {
      defineOrch8Elements();
    }, []);

    const { tokenProvider, strings, fetchImpl } = common;
    useEffect(() => {
      const el = inner.current as unknown as Record<string, unknown> | null;
      if (!el) return;
      if (tokenProvider) el.tokenProvider = tokenProvider;
      if (strings) el.strings = strings;
      if (fetchImpl) el.fetchImpl = fetchImpl;
    }, [tokenProvider, strings, fetchImpl]);

    // Keep handlers in a ref so listeners are attached once.
    const handlers = useRef(record);
    handlers.current = record;
    useEffect(() => {
      const el = inner.current;
      if (!el) return;
      const entries = Object.entries(spec.events).map(([prop, eventName]) => {
        const listener = (ev: Event) => {
          const fn = handlers.current[prop];
          if (typeof fn === "function") fn((ev as CustomEvent).detail, ev);
        };
        el.addEventListener(eventName, listener);
        return () => el.removeEventListener(eventName, listener);
      });
      return () => entries.forEach((off) => off());
    }, []);

    const domProps: Record<string, unknown> = { ref: inner, id: common.id, className: common.className, style: common.style };
    const commonAttrs: Record<string, string> = {
      baseUrl: "base-url",
      token: "token",
      theme: "theme",
      colorScheme: "color-scheme",
      locale: "locale",
      vendor: "vendor",
    };
    for (const [prop, attr] of Object.entries({ ...commonAttrs, ...spec.attrs })) {
      const v = toAttr(record[prop]);
      if (v !== undefined) domProps[attr] = v;
    }
    return createElement(spec.tag, domProps);
  });
  Component.displayName = displayName;
  return Component;
}

export interface Orch8RunsProps extends Orch8CommonProps {
  pageSize?: number;
  pollInterval?: number;
  /** e.g. `/runs/{id}`: rows become links. */
  hrefTemplate?: string;
  startSequence?: string;
  onRunSelect?: Handler<RunSelectDetail>;
  onRunStarted?: Handler<{ id: string; sequence: string }>;
}

export const Orch8Runs = createWrapper<Orch8RunsElement, Orch8RunsProps>(
  {
    tag: "orch8-runs",
    attrs: { pageSize: "page-size", pollInterval: "poll-interval", hrefTemplate: "href-template", startSequence: "start-sequence" },
    events: { onRunSelect: "orch8-run-select", onRunStarted: "orch8-run-started" },
  },
  "Orch8Runs",
);

export interface Orch8RunTimelineProps extends Orch8CommonProps {
  runId: string;
  pollInterval?: number;
  onRunUpdate?: Handler<{ run: RunDetail }>;
}

export const Orch8RunTimeline = createWrapper<Orch8RunTimelineElement, Orch8RunTimelineProps>(
  { tag: "orch8-run-timeline", attrs: { runId: "run-id", pollInterval: "poll-interval" }, events: { onRunUpdate: "orch8-run-update" } },
  "Orch8RunTimeline",
);

export interface Orch8ApprovalsProps extends Orch8CommonProps {
  instanceId?: string;
  pollInterval?: number;
  onApprovalResolved?: Handler<ApprovalResolvedDetail>;
}

export const Orch8Approvals = createWrapper<Orch8ApprovalsElement, Orch8ApprovalsProps>(
  {
    tag: "orch8-approvals",
    attrs: { instanceId: "instance-id", pollInterval: "poll-interval" },
    events: { onApprovalResolved: "orch8-approval-resolved" },
  },
  "Orch8Approvals",
);

export interface Orch8BuilderProps extends Orch8CommonProps {
  sequence: string;
  onSequenceSaved?: Handler<SequenceSavedDetail>;
  onDirtyChange?: Handler<{ dirty: boolean }>;
}

export const Orch8Builder = createWrapper<Orch8BuilderElement, Orch8BuilderProps>(
  { tag: "orch8-builder", attrs: { sequence: "sequence" }, events: { onSequenceSaved: "orch8-sequence-saved", onDirtyChange: "orch8-dirty-change" } },
  "Orch8Builder",
);

export type Orch8BadgeProps = Pick<Orch8CommonProps, "vendor" | "colorScheme" | "strings" | "id" | "className" | "style">;

export const Orch8Badge = createWrapper<Orch8BadgeElement, Orch8BadgeProps>({ tag: "orch8-badge", attrs: {}, events: {} }, "Orch8Badge");

export type { TokenProvider } from "./token.js";
export type { Orch8Strings } from "./i18n.js";
