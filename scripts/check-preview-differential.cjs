'use strict'

const { spawnSync } = require('node:child_process')
const { mkdirSync, mkdtempSync, rmSync, writeFileSync } = require('node:fs')
const { tmpdir } = require('node:os')
const { join, resolve } = require('node:path')
const { isDeepStrictEqual } = require('node:util')

function fixture(seed) {
  let state = seed >>> 0
  function draw() {
    state = (Math.imul(state, 1664525) + 1013904223) >>> 0
    return state >>> 16
  }
  const input = { eligible: draw() % 3 !== 0 }
  const mocks = {}
  const step = (id, output, error) => {
    mocks[id] = error === undefined ? { output } : { error }
    return { type: 'step', id, handler: 'preview_fixture' }
  }
  const start = step('lookup', { score: draw() % 100, labels: ['new', null] })
  const prefix = Array.from({ length: draw() % 4 }, (_, index) => (
    step(`prepare_${index}`, { trace: draw() % 13 })
  ))
  const accept = step('accept', { accepted: true })
  const review = step('review', { accepted: false }, draw() % 4 === 0 ? 'mocked rejection' : undefined)
  const route = {
    type: 'router', id: 'route',
    routes: [{
      condition: 'data.eligible && outputs.lookup.score >= 50',
      blocks: [accept, step('confirm', { confirmed: [1, false, null] })],
    }],
    default: [review],
  }
  const sequence = {
    name: 'preview-differential',
    blocks: [...prefix, start, route, step('audit', { status: 'recorded' })],
  }
  return { seed, input, sequence, mocks }
}

function leafIds(blocks) {
  return blocks.flatMap((block) => block.type === 'step'
    ? [block.id]
    : [...block.routes.flatMap((route) => leafIds(route.blocks)), ...leafIds(block.default)])
}

function* smallerGraphs(blocks) {
  for (let index = 0; index < blocks.length; index += 1) {
    if (blocks.length > 1) yield blocks.filter((_, position) => position !== index)
    const block = blocks[index]
    if (block.type !== 'router') continue
    for (let branch = 0; branch < block.routes.length; branch += 1) {
      for (const body of smallerGraphs(block.routes[branch].blocks)) {
        const smaller = { ...block, routes: block.routes.with(branch, { ...block.routes[branch], blocks: body }) }
        yield blocks.with(index, smaller)
      }
    }
    for (const body of smallerGraphs(block.default)) yield blocks.with(index, { ...block, default: body })
  }
}

function minimize(original, check, mismatchKind) {
  let current = original
  while (true) {
    let reduced
    for (const blocks of smallerGraphs(current.sequence.blocks)) {
      const candidate = { ...current, sequence: { ...current.sequence, blocks } }
      if (check(candidate).mismatch === mismatchKind) {
        reduced = candidate
        break
      }
    }
    if (!reduced) return current
    current = reduced
  }
}

function projectedOutputs(preview, leaves) {
  const outputs = Object.fromEntries(Object.entries(preview.outputs).filter(([id]) => leaves.has(id)))
  for (const entry of preview.timeline) {
    if (entry.kind === 'step' && entry.status === 'failed'
        && leaves.has(entry.step_id) && !Object.hasOwn(outputs, entry.step_id)) {
      // Native execution persists the failure that the preview reports in its timeline.
      outputs[entry.step_id] = { __error__: true, error: entry.error, error_code: 'States.TaskFailed' }
    }
  }
  return outputs
}

function compareResult(preview, report, leaves, outputs) {
  if (preview.status !== report.final_state) return 'terminal state'
  const attempts = preview.timeline
    .filter((step) => step.kind === 'step' && ['mocked', 'failed'].includes(step.status))
  const attempted = [...new Set(attempts.map((step) => step.step_id))].sort()
  if (!isDeepStrictEqual(attempted, [...(report.attempted_blocks ?? [])].sort())) return 'branch path'
  const nativeOutputs = report.executed_blocks.filter((id) => leaves.has(id))
  if (!report.passed || !isDeepStrictEqual(Object.keys(outputs).sort(), [...nativeOutputs].sort())) return 'outputs'
  if (!isDeepStrictEqual(attempts.map((step) => step.step_id), nativeOutputs)) return 'branch path'
}

function checkFixture(data, wasm, native, work) {
  const leaves = new Set(leafIds(data.sequence.blocks))
  const mocks = Object.fromEntries(Object.entries(data.mocks).filter(([id]) => leaves.has(id)))
  const preview = JSON.parse(wasm.run_dry_with_options(
    JSON.stringify(data.sequence),
    JSON.stringify({ input: data.input, seed: data.seed, mocks }),
  ))
  if (!preview.ok || !['completed', 'failed'].includes(preview.status)) {
    return { mismatch: 'preview status', preview }
  }
  const outputs = projectedOutputs(preview, leaves)
  const contract = {
    cases: [{
      name: `seed-${data.seed}`, input: data.input,
      mocks: Object.entries(mocks).map(([block, mock]) => mock.error
        ? { block, type: 'failure', message: mock.error }
        : { block, type: 'success', output: mock.output }),
      expect: {
        terminal_state: preview.status,
        assertions: Object.entries(outputs)
          .map(([block, value]) => ({ output: { block, path: '' }, op: 'equals', value })),
      },
    }],
  }
  writeFileSync(join(work, 'workflow.json'), JSON.stringify(data.sequence))
  writeFileSync(join(work, 'workflow.contracts.json'), JSON.stringify(contract))
  const result = spawnSync(native, ['test', 'run', 'workflow.contracts.json', '--report', 'json'], {
    cwd: work, encoding: 'utf8', timeout: 20_000,
    env: { ...process.env, ORCH8_CONTEXTS_FILE: join(work, 'contexts.json') },
  })
  if (result.error || !result.stdout) {
    throw new Error(`native fixture failed to run: ${result.error ?? result.stderr}`)
  }
  const report = JSON.parse(result.stdout).cases[0]
  const mismatch = compareResult(preview, report, leaves, outputs)
  return { mismatch, report, preview, contract }
}

function main() {
  const [nativePath, wasmPath, artifactDir] = process.argv.slice(2)
  if (!nativePath || !wasmPath || !artifactDir) {
    throw new Error('usage: node check-preview-differential.cjs ORCH8_BIN WASM_BINDGEN_JS ARTIFACT_DIR')
  }
  const native = resolve(nativePath)
  const wasm = require(resolve(wasmPath))
  const work = mkdtempSync(join(tmpdir(), 'orch8-preview-differential-'))
  try {
    const check = (data) => checkFixture(data, wasm, native, work)
    const seeds = [1, 2, 3, 4, 5, 17, 31, 41, 53, 61, 67, 73]
    let mismatches = 0
    for (const seed of seeds) {
      const original = fixture(seed)
      const result = check(original)
      if (!result.mismatch) continue
      const minimized = minimize(original, check, result.mismatch)
      const evidence = { original, minimized, result: check(minimized) }
      const artifact = join(artifactDir, `preview-seed-${seed}.json`)
      mkdirSync(artifactDir, { recursive: true })
      writeFileSync(artifact, JSON.stringify(evidence, null, 2) + '\n')
      console.error(`Seed ${seed} diverged on ${result.mismatch}. Fixture saved to ${artifact}`)
      mismatches += 1
    }
    console.log(`Compared ${seeds.length} seeded workflows in native and WebAssembly runners`)
    if (mismatches) process.exitCode = 1
  } finally {
    rmSync(work, { recursive: true, force: true })
  }
}

if (require.main === module) main()
module.exports = { compareResult, fixture, minimize, projectedOutputs }
