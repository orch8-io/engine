import { test } from "node:test";
import * as assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import {
  BLANK_TEMPLATE,
  TEMPLATES,
  templateEditorContent,
  templatePickerOptions,
} from "../src/lib/templates.ts";

// ---------------------------------------------------------------------------
// Registry shape — mirrors orch8-cli/src/templates.rs
// ---------------------------------------------------------------------------

const EXPECTED_NAMES = [
  "default",
  "react-loop",
  "tool-calling-pipeline",
  "guardrail-validation",
  "multi-agent-delegation",
];

test("TEMPLATES contains exactly the 5 CLI templates, in display order", () => {
  assert.deepEqual(
    TEMPLATES.map((t) => t.name),
    EXPECTED_NAMES,
  );
});

test("every template has a non-empty description", () => {
  for (const t of TEMPLATES) {
    assert.ok(t.description.length > 0, `template ${t.name} has empty description`);
  }
});

test("every template sequence has a non-empty blocks array", () => {
  for (const t of TEMPLATES) {
    const blocks = t.sequence["blocks"];
    assert.ok(
      Array.isArray(blocks) && blocks.length > 0,
      `template ${t.name} is missing a non-empty blocks array`,
    );
  }
});

test("template names are unique kebab-case", () => {
  const seen = new Set<string>();
  for (const t of TEMPLATES) {
    assert.ok(!seen.has(t.name), `duplicate template name ${t.name}`);
    seen.add(t.name);
    assert.match(t.name, /^[a-z0-9-]+$/, `template name ${t.name} is not kebab-case`);
  }
});

// ---------------------------------------------------------------------------
// Picker options — the list the UI renders
// ---------------------------------------------------------------------------

test("templatePickerOptions lists blank first, then all 5 templates", () => {
  const opts = templatePickerOptions();
  assert.equal(opts.length, 6);
  assert.equal(opts[0]!.name, BLANK_TEMPLATE);
  assert.deepEqual(opts.slice(1).map((o) => o.name), EXPECTED_NAMES);
  for (const o of opts) {
    assert.ok(o.description.length > 0, `picker option ${o.name} has empty description`);
  }
});

// ---------------------------------------------------------------------------
// Editor content — what selecting a picker option fills the editor with
// ---------------------------------------------------------------------------

test("selecting blank fills the editor with an empty skeleton", () => {
  const content = templateEditorContent(BLANK_TEMPLATE, {
    tenantId: "acme",
    namespace: "staging",
  });
  const seq = JSON.parse(content) as Record<string, unknown>;
  assert.equal(seq["tenant_id"], "acme");
  assert.equal(seq["namespace"], "staging");
  assert.equal(seq["version"], 1);
  assert.deepEqual(seq["blocks"], []);
});

test("selecting a template fills the editor with its blocks and the form's tenant/namespace", () => {
  for (const t of TEMPLATES) {
    const content = templateEditorContent(t.name, {
      tenantId: "acme",
      namespace: "prod",
    });
    const seq = JSON.parse(content) as Record<string, unknown>;
    assert.equal(seq["tenant_id"], "acme", `${t.name}: tenant_id not adapted`);
    assert.equal(seq["namespace"], "prod", `${t.name}: namespace not adapted`);
    assert.equal(seq["version"], 1, `${t.name}: version not reset to 1`);
    assert.deepEqual(seq["blocks"], t.sequence["blocks"], `${t.name}: blocks differ`);
  }
});

test("the default template keeps its hello-world sequence name", () => {
  const seq = JSON.parse(
    templateEditorContent("default", { tenantId: "demo", namespace: "default" }),
  ) as Record<string, unknown>;
  assert.equal(seq["name"], "hello-world");
  assert.equal((seq["blocks"] as unknown[]).length, 3);
});

test("agent-pattern templates use their slug as the sequence name", () => {
  const seq = JSON.parse(
    templateEditorContent("react-loop", { tenantId: "t", namespace: "n" }),
  ) as Record<string, unknown>;
  assert.equal(seq["name"], "react-loop");
});

test("an unknown template name behaves like blank", () => {
  const ctx = { tenantId: "t", namespace: "n" };
  assert.equal(templateEditorContent("nope", ctx), templateEditorContent(BLANK_TEMPLATE, ctx));
});

// ---------------------------------------------------------------------------
// Drift guard — the embedded pattern JSONs must equal docs/agent-patterns/*.json
// (the source of truth that orch8-cli/src/templates.rs include_str!s).
// ---------------------------------------------------------------------------

const PATTERNS_DIR = join(import.meta.dirname, "..", "..", "docs", "agent-patterns");

for (const name of EXPECTED_NAMES.filter((n) => n !== "default")) {
  test(`embedded "${name}" template matches docs/agent-patterns/${name}.json`, () => {
    const file = JSON.parse(readFileSync(join(PATTERNS_DIR, `${name}.json`), "utf8"));
    const embedded = TEMPLATES.find((t) => t.name === name)!.sequence;
    assert.deepEqual(embedded, file);
  });
}
