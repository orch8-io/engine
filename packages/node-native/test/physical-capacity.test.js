'use strict'

const test = require('node:test')
const assert = require('node:assert/strict')
const { mkdtempSync, rmSync } = require('node:fs')
const { tmpdir } = require('node:os')
const { join } = require('node:path')
const { Engine } = require('..')

test('physical promise capacity survives step timeouts and engine replacement', { timeout: 60000 }, async () => {
  const directory = mkdtempSync(join(tmpdir(), 'orch8-physical-'))
  const path = join(directory, 'app.db')
  const release = []
  let active = 0, peak = 0, entered = 0, successor
  const host = async ({ params }) => {
    active++; entered++; peak = Math.max(peak, active)
    try {
      if (params.hold) await new Promise(resolve => release.push(resolve))
      return true
    } finally { active-- }
  }
  const first = await Engine.open(path)
  first.handler('host', host)
  try {
    await first.deploy({ name: 'held', blocks: [{ type: 'step', id: 'work', handler: 'host', params: { hold: true }, timeout: 50 }] })
    for (let i = 0; i < 128; i++) {
      const result = await first.run(await first.start('held'), { timeoutMs: 3000 })
      assert.equal(result.state, 'failed')
    }
    assert.equal(active, 128)
    const deferredId = await first.start('held')
    const deferred = await first.run(deferredId, { timeoutMs: 150 })
    assert.equal(deferred.state, 'scheduled', 'capacity must defer before host dispatch')
    assert.deepEqual(deferred.outputs, {})
    assert.equal(entered, 128)
    await first.signal(deferredId, 'cancel')
    assert.equal((await first.run(deferredId, { timeoutMs: 3000 })).state, 'cancelled')
    await first.close()
    successor = await Engine.open(path)
    successor.handler('host', host)
    await successor.deploy({ name: 'fast', blocks: [{ type: 'step', id: 'work', handler: 'host', params: { hold: false } }] })
    const fast = await successor.start('fast')
    const waiting = await successor.run(fast, { timeoutMs: 150 })
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
