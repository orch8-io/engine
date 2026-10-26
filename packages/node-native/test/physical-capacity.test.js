'use strict'

const test = require('node:test')
const assert = require('node:assert/strict')
const { mkdtempSync, rmSync } = require('node:fs')
const { tmpdir } = require('node:os')
const { join } = require('node:path')
const { Engine } = require('..')
const { setTimeout: delay } = require('node:timers/promises')
const { performance } = require('node:perf_hooks')

async function settledSnapshot(engine, id, expected) {
  let result = await engine.get(id)
  const deadline = performance.now() + 5000
  while (['running', 'scheduled'].includes(result.state) && performance.now() < deadline) {
    await delay(10)
    result = await engine.get(id)
  }
  assert.ok(expected.includes(result.state), `expected ${expected}, got ${result.state}`)
  return result
}

async function deferredSnapshot(engine, id) {
  let result = await engine.run(id, { timeoutMs: 150 })
  const deadline = performance.now() + 5000
  while (result.state === 'running' && performance.now() < deadline) {
    await delay(10)
    result = await engine.get(id)
  }
  assert.equal(result.state, 'scheduled', 'capacity must defer before host dispatch')
  return result
}

test('physical promise capacity survives step timeouts and engine replacement', { timeout: 60000 }, async () => {
  const directory = mkdtempSync(join(tmpdir(), 'orch8-physical-'))
  const path = join(directory, 'app.db')
  const release = []
  const heldIds = new Set()
  let active = 0, peak = 0, entered = 0, successor
  const host = async ({ params, instanceId }) => {
    active++; entered++; peak = Math.max(peak, active)
    try {
      if (params.hold) {
        heldIds.add(instanceId)
        await new Promise(resolve => release.push(resolve))
      }
      return true
    } finally { active-- }
  }
  const first = await Engine.open(path)
  first.handler('host', host)
  try {
    await first.deploy({ name: 'held', blocks: [{ type: 'step', id: 'work', handler: 'host', params: { hold: true }, timeout: 50 }] })
    const created = []
    const fillDeadline = performance.now() + 30000
    for (let attempts = 0; active < 128 && attempts < 256 && performance.now() < fillDeadline; attempts++) {
      const id = await first.start('held')
      created.push(id)
      const result = await first.run(id, { timeoutMs: 3000 })
      assert.ok(['failed', 'running', 'scheduled'].includes(result.state))
      // A timeout can precede host entry. Count observed physical calls,
      // allowing queued JS callbacks a turn before admitting another run.
      await delay(0)
    }
    assert.equal(active, 128)
    assert.equal(heldIds.size, 128, 'each physical callback must have a distinct instance')
    for (const id of heldIds) await settledSnapshot(first, id, ['failed'])
    for (const id of created) {
      if (heldIds.has(id) || (await first.get(id)).state === 'failed') continue
      // A spare run may have found full capacity while the final callback
      // was still queued. Retire it before testing the successor's free slot.
      await first.signal(id, 'cancel')
      await first.run(id, { timeoutMs: 3000 })
      await settledSnapshot(first, id, ['failed', 'cancelled'])
    }
    const deferredId = await first.start('held')
    const deferred = await deferredSnapshot(first, deferredId)
    assert.deepEqual(deferred.outputs, {})
    assert.equal(entered, 128)
    await first.signal(deferredId, 'cancel')
    assert.equal((await first.run(deferredId, { timeoutMs: 3000 })).state, 'cancelled')
    await first.close()
    successor = await Engine.open(path)
    successor.handler('host', host)
    await successor.deploy({ name: 'fast', blocks: [{ type: 'step', id: 'work', handler: 'host', params: { hold: false } }] })
    const fast = await successor.start('fast')
    const waiting = await deferredSnapshot(successor, fast)
    assert.equal(waiting.state, 'scheduled', 'new engine cannot reset unsettled admission')
    assert.equal(entered, 128)
    release.shift()()
    const completed = await successor.run(fast, { timeoutMs: 3000 })
    assert.equal(completed.state, 'completed', JSON.stringify(completed))
    assert.equal(active, 127)
    assert.equal(peak, 128)
    assert.equal(entered, 129)
  } finally {
    for (const settle of release) settle()
    await new Promise(resolve => setImmediate(resolve))
    await first.close()
    if (successor) await successor.close()
    rmSync(directory, { recursive: true, force: true })
  }
  assert.equal(active, 0)
})
