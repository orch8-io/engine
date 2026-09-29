import { BASE_ATTRIBUTES, Orch8Element, type LoadMode } from "../base.js";
import { formatDateTime, formatDuration, h, jsonPreview } from "../dom.js";
import type { RunDetail } from "../types.js";

const TERMINAL = new Set(["completed", "failed", "cancelled"]);

/**
 * `<orch8-run-timeline run-id="…">` — one run's steps as an ordered timeline.
 * Polls (`poll-interval`, default 3000 ms) until the run is terminal and
 * announces state changes through a polite live region.
 * Events: `orch8-run-update` {run}.
 */
export class Orch8RunTimelineElement extends Orch8Element {
  static observedAttributes = [...BASE_ATTRIBUTES, "run-id", "poll-interval"];

  private run: RunDetail | null = null;
  private prevStates = new Map<string, string>();
  private prevRunState: string | null = null;

  protected override extraCss(): string {
    return `
      ol.timeline { list-style: none; margin: 0; padding: 0; position: relative; }
      ol.timeline > li { position: relative; padding: 0 0 calc(var(--_space) * 2) 28px; }
      ol.timeline > li::before {
        content: ""; position: absolute; left: 7px; top: 18px; bottom: 0; width: 2px; background: var(--_border);
      }
      ol.timeline > li:last-child::before { display: none; }
      .dot { position: absolute; left: 0; top: 3px; width: 16px; height: 16px; border-radius: 50%;
        border: 2px solid currentColor; background: var(--_bg); display: grid; place-items: center; font-size: 10px; line-height: 1; }
      .step-name { font-weight: 600; }
      .step-meta { font-size: 0.875em; color: var(--_muted); }
      details { margin-top: 4px; }
      summary { cursor: pointer; color: var(--_accent); font-size: 0.875em; }
      pre { background: var(--_surface); border: 1px solid var(--_border); border-radius: calc(var(--_radius) - 2px);
        padding: var(--_space); overflow: auto; max-height: 240px; font-size: 0.8125em; margin: 4px 0 0; }
    `;
  }

  get runId(): string | null {
    return this.getAttribute("run-id");
  }
  set runId(value: string | null) {
    if (value) this.setAttribute("run-id", value);
    else this.removeAttribute("run-id");
  }

  protected override onAttributeChanged(name: string): void {
    if (name === "run-id") {
      this.run = null;
      this.prevStates.clear();
      this.prevRunState = null;
    }
  }

  protected override ready(): boolean {
    return Boolean(this.runId);
  }

  protected override pollInterval(): number {
    const raw = this.getAttribute("poll-interval");
    if (raw === null) return 3000;
    const n = Number(raw);
    return Number.isFinite(n) && n >= 0 ? (n === 0 ? 0 : Math.max(n, 500)) : 3000;
  }

  protected override wantsPolling(): boolean {
    return this.pollInterval() > 0 && Boolean(this.runId) && !(this.run && TERMINAL.has(String(this.run.state)));
  }

  protected async load(signal: AbortSignal, mode: LoadMode): Promise<void> {
    const id = this.runId;
    if (!id) return;
    const run = await this.client.getRun(id, signal);
    if (mode === "refresh") this.announceChanges(run);
    this.run = run;
    this.prevRunState = String(run.state);
    this.prevStates = new Map(run.steps.map((s) => [s.id, String(s.state)]));
    this.emit("orch8-run-update", { run });
  }

  private announceChanges(run: RunDetail): void {
    const changes: string[] = [];
    for (const step of run.steps) {
      const before = this.prevStates.get(step.id);
      if (before !== String(step.state)) {
        changes.push(this.t("timelineStepChanged", { step: step.name || step.id, state: this.stateLabel(String(step.state)) }));
      }
    }
    if (this.prevRunState !== null && this.prevRunState !== String(run.state)) {
      changes.push(this.t("timelineRunState", { state: this.stateLabel(String(run.state)) }));
    }
    if (changes.length) this.announce(changes.slice(-3).join(". "));
  }

  protected renderContent(): void {
    if (!this.runId || !this.run) {
      this.body.replaceChildren(h("div", { class: "status", part: "empty" }, this.t("timelineNoRun")));
      return;
    }
    const run = this.run;
    const header = this.renderHeader(this.t("timelineTitle", { sequence: run.sequence }), this.renderState(String(run.state)));
    const titleId = "timeline-title";
    header.querySelector("h2")?.setAttribute("id", titleId);

    if (run.steps.length === 0) {
      this.body.replaceChildren(header, h("div", { class: "status", part: "empty" }, this.t("timelineEmpty")));
      return;
    }

    const items = run.steps.map((step) => {
      const state = String(step.state);
      const started = step.started_at ? new Date(step.started_at).getTime() : NaN;
      const finished = step.finished_at ? new Date(step.finished_at).getTime() : NaN;
      const meta: string[] = [];
      if (step.started_at) meta.push(this.t("timelineStarted", { time: formatDateTime(step.started_at, this.displayLocale) }));
      if (step.finished_at) meta.push(this.t("timelineFinished", { time: formatDateTime(step.finished_at, this.displayLocale) }));
      if (Number.isFinite(started) && Number.isFinite(finished)) {
        meta.push(this.t("timelineDuration", { duration: formatDuration(finished - started) }));
      }
      const glyph = state === "completed" ? "✓" : state === "failed" ? "✕" : state === "cancelled" ? "–" : "";
      const hasOutput = step.output !== undefined && step.output !== null;
      return h(
        "li",
        { part: "step", "data-state": state },
        h("span", { class: `dot state-${state.replace(/[^a-z_-]/gi, "")}`, "aria-hidden": "true" }, glyph),
        h("div", null, h("span", { class: "step-name" }, step.name || step.id), " ", this.renderState(state)),
        meta.length ? h("div", { class: "step-meta" }, meta.join(" · ")) : null,
        hasOutput
          ? h(
              "details",
              { "data-out": step.id },
              h("summary", { "data-key": `out-${step.id}` }, this.t("timelineOutput")),
              h("pre", { tabindex: "0" }, jsonPreview(step.output)),
            )
          : null,
      );
    });

    // Keep <details> open across polls.
    const open = new Set(
      Array.from(this.body.querySelectorAll<HTMLDetailsElement>("details[open]")).map((d) => d.getAttribute("data-out")),
    );
    const list = h("ol", { class: "timeline", part: "timeline", "aria-labelledby": titleId }, items);
    for (const d of Array.from(list.querySelectorAll<HTMLDetailsElement>("details"))) {
      if (open.has(d.getAttribute("data-out"))) d.open = true;
    }
    this.withFocusPreserved(() => this.body.replaceChildren(header, list));
  }
}
