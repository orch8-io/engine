import { BASE_ATTRIBUTES, Orch8Element, type LoadMode } from "../base.js";
import { formatDateTime, formatRelative, h } from "../dom.js";
import type { RunSummary } from "../types.js";

export interface RunSelectDetail {
  id: string;
  run: RunSummary;
}

/**
 * `<orch8-runs>` — the sub-tenant's recent runs.
 *
 * Attributes: `page-size` (20), `poll-interval` ms (10000, 0 = off),
 * `href-template` (e.g. `/runs/{id}` renders links instead of buttons),
 * `start-sequence` (renders a "Start" button; needs `runs:start`).
 * Events: `orch8-run-select` {id, run}, `orch8-run-started` {id, sequence}.
 */
export class Orch8RunsElement extends Orch8Element {
  static observedAttributes = [...BASE_ATTRIBUTES, "page-size", "poll-interval", "href-template", "start-sequence"];

  private items: RunSummary[] = [];
  private nextCursor: string | null = null;
  private loadingMore = false;
  private starting = false;
  private startError: string | null = null;
  private firstPageLen = 0;

  protected override extraCss(): string {
    return `
      .runs-table td:first-child { font-weight: 500; }
      .more { margin-top: var(--_space); }
      @media (max-width: 480px) { .col-step, .col-updated { display: none; } }
    `;
  }

  private get pageSize(): number {
    const n = Number(this.getAttribute("page-size"));
    return Number.isInteger(n) && n > 0 && n <= 200 ? n : 20;
  }

  protected override pollInterval(): number {
    const raw = this.getAttribute("poll-interval");
    if (raw === null) return 10_000;
    const n = Number(raw);
    return Number.isFinite(n) && n >= 0 ? (n === 0 ? 0 : Math.max(n, 1000)) : 10_000;
  }

  protected async load(signal: AbortSignal, mode: LoadMode): Promise<void> {
    const page = await this.client.listRuns({ limit: this.pageSize }, signal);
    if (mode === "initial" || this.items.length === 0) {
      this.items = page.items;
      this.firstPageLen = page.items.length;
      this.nextCursor = page.next_cursor ?? null;
      return;
    }
    // Refresh: replace the first page, keep rows fetched via "Load more".
    const fresh = new Set(page.items.map((r) => r.id));
    const older = this.items.slice(this.firstPageLen).filter((r) => !fresh.has(r.id));
    this.items = [...page.items, ...older];
    this.firstPageLen = page.items.length;
  }

  private async loadMore(): Promise<void> {
    if (!this.nextCursor || this.loadingMore) return;
    this.loadingMore = true;
    this.render();
    try {
      const page = await this.client.listRuns({ limit: this.pageSize, cursor: this.nextCursor });
      const seen = new Set(this.items.map((r) => r.id));
      this.items = [...this.items, ...page.items.filter((r) => !seen.has(r.id))];
      this.nextCursor = page.next_cursor ?? null;
    } catch (err) {
      this.announce(this.errorMessage(err).message);
    } finally {
      this.loadingMore = false;
      this.render();
    }
  }

  /** Starts a run of `sequence` (defaults to the `start-sequence` attribute). */
  async startRun(sequence = this.getAttribute("start-sequence") ?? "", input: unknown = {}): Promise<string> {
    const res = await this.client.startRun({ sequence, input });
    this.emit("orch8-run-started", { id: res.id, sequence });
    this.announce(this.t("runsStarted"));
    this.refresh();
    return res.id;
  }

  private async onStartClick(): Promise<void> {
    if (this.starting) return;
    this.starting = true;
    this.render();
    try {
      await this.startRun();
    } catch (err) {
      this.announce(this.errorMessage(err).message);
      this.startError = this.errorMessage(err).message;
    } finally {
      this.starting = false;
      this.render();
    }
  }

  private select(run: RunSummary): void {
    this.emit<RunSelectDetail>("orch8-run-select", { id: run.id, run });
  }

  protected renderContent(): void {
    const startSeq = this.getAttribute("start-sequence");
    const canStart = startSeq && this.hasScope("runs:start");
    const startBtn = canStart
      ? h(
          "button",
          {
            type: "button",
            class: "primary",
            "data-key": "start",
            disabled: this.starting,
            "aria-busy": this.starting ? "true" : null,
            onclick: () => void this.onStartClick(),
          },
          this.t("runsStart", { sequence: startSeq }),
        )
      : null;
    const header = this.renderHeader(this.t("runsTitle"), startBtn);
    const startErr = this.startError ? h("div", { class: "field-error", role: "alert" }, this.startError) : null;
    this.startError = null;

    if (this.items.length === 0) {
      this.withFocusPreserved(() =>
        this.body.replaceChildren(
          header,
          startErr ?? "",
          h(
            "div",
            { class: "status", part: "empty" },
            h("p", { style: "margin:0;font-weight:500" }, this.t("runsEmpty")),
            h("p", { class: "small", style: "margin:4px 0 0" }, this.t("runsEmptyHint")),
          ),
        ),
      );
      return;
    }

    const template = this.getAttribute("href-template");
    const titleId = `runs-title`;
    header.querySelector("h2")?.setAttribute("id", titleId);
    const rows = this.items.map((run) => {
      const label = this.t("runsView", { id: run.id, sequence: run.sequence });
      const opener = template
        ? h(
            "a",
            {
              href: template.replace("{id}", encodeURIComponent(run.id)),
              "aria-label": label,
              "data-key": `run-${run.id}`,
              onclick: () => this.select(run),
            },
            run.sequence,
          )
        : h(
            "button",
            { type: "button", class: "link", "aria-label": label, "data-key": `run-${run.id}`, onclick: () => this.select(run) },
            run.sequence,
          );
      return h(
        "tr",
        { part: "row" },
        h("td", null, opener),
        h("td", null, this.renderState(String(run.state))),
        h("td", { class: "col-step" }, run.current_step ?? h("span", { class: "muted" }, "—")),
        h(
          "td",
          { class: "col-updated muted" },
          h("time", { datetime: run.updated_at, title: formatDateTime(run.updated_at, this.displayLocale) }, formatRelative(run.updated_at, this.displayLocale)),
        ),
      );
    });

    const table = h(
      "table",
      { class: "runs-table", part: "table", "aria-labelledby": titleId },
      h(
        "thead",
        null,
        h(
          "tr",
          null,
          h("th", { scope: "col" }, this.t("runsColSequence")),
          h("th", { scope: "col" }, this.t("runsColState")),
          h("th", { scope: "col", class: "col-step" }, this.t("runsColStep")),
          h("th", { scope: "col", class: "col-updated" }, this.t("runsColUpdated")),
        ),
      ),
      h("tbody", null, rows),
    );

    const more = this.nextCursor
      ? h(
          "div",
          { class: "more" },
          h(
            "button",
            {
              type: "button",
              "data-key": "more",
              disabled: this.loadingMore,
              "aria-busy": this.loadingMore ? "true" : null,
              onclick: () => void this.loadMore(),
            },
            this.loadingMore ? this.t("loading") : this.t("runsLoadMore"),
          ),
        )
      : null;

    this.withFocusPreserved(() => this.body.replaceChildren(header, startErr ?? "", table, more ?? ""));
  }
}
