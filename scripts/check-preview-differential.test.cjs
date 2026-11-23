'use strict'

const assert = require('node:assert/strict')
const test = require('node:test')
const { compareResult, fixture, minimize, projectedOutputs } = require('./check-preview-differential.cjs')

test('seeded workflow definitions and mocks replay exactly', () => {
  assert.deepEqual(fixture(42), fixture(42))
  assert.notDeepEqual(fixture(42).mocks, fixture(43).mocks)
})

test('shrinking removes root steps and nested leaves without changing the mismatch', () => {
  const original = fixture(1)
  const check = (candidate) => {
    const router = candidate.sequence.blocks.find((block) => block.type === 'router')
    return { mismatch: router?.routes[0].blocks.some((block) => block.id === 'confirm')
      ? 'outputs'
      : 'branch path' }
  }
  const reduced = minimize(original, check, 'outputs')
  assert.deepEqual(reduced.sequence.blocks.map((block) => block.id), ['route'])
  assert.deepEqual(reduced.sequence.blocks[0].routes[0].blocks.map((block) => block.id), ['confirm'])
  assert.equal(check(reduced).mismatch, 'outputs')
})

test('native-only step outputs are reported even when both runners attempt the same path', () => {
  const leaves = new Set(['lookup', 'review', 'skipped'])
  const preview = {
    status: 'failed',
    timeline: [
      { kind: 'step', step_id: 'lookup', status: 'mocked' },
      { kind: 'step', step_id: 'review', status: 'failed', error: 'rejected' },
    ],
    outputs: { lookup: { score: 10 }, route: null },
  }
  const native = {
    final_state: 'failed',
    attempted_blocks: ['lookup', 'review'],
    executed_blocks: ['lookup', 'route', 'review'],
    passed: true,
  }
  const outputs = projectedOutputs(preview, leaves)
  assert.deepEqual(outputs.review, { __error__: true, error: 'rejected', error_code: 'States.TaskFailed' })
  assert.equal(compareResult(preview, native, leaves, outputs), undefined)
  assert.equal(compareResult(preview, { ...native, executed_blocks: ['lookup', 'review', 'skipped'] }, leaves, outputs), 'outputs')
  assert.equal(compareResult(preview, { ...native, passed: false }, leaves, outputs), 'outputs')
})
