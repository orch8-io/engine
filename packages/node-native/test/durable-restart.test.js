'use strict'

const test = require('node:test')
const assert = require('node:assert/strict')
const { spawnSync } = require('node:child_process')
const { mkdtempSync, readFileSync, rmSync } = require('node:fs')
const { tmpdir } = require('node:os')
const { join } = require('node:path')

const { Engine, PermanentError } = require('..')

const worker = join(__dirname, 'flow-worker.js')

function withDir(fn) {
  return async () => {
    const dir = mkdtempSync(join(tmpdir(), 'orch8-durable-'))
    try {
      await fn(dir)
    } finally {
      rmSync(dir, { recursive: true, force: true })
    }
  }
}

function runWorker(dir, mode) {
  return spawnSync(process.execPath, [worker, join(dir, 'app.db'), join(dir, 'effects.log'), mode], {
    encoding: 'utf8',
    timeout: 60_000,
  })
}

const effects = (dir) => readFileSync(join(dir, 'effects.log'), 'utf8').trim().split('\n')

test(
  'a 3-step flow resumes in a new process after a hard kill between steps',
  withDir((dir) => {
    const crashed = runWorker(dir, 'crash-between')
    assert.equal(crashed.signal, 'SIGKILL', `process A should die hard: ${crashed.stderr}`)
    assert.deepEqual(effects(dir), ['reserve'])

    const resumed = runWorker(dir, 'resume')
    assert.equal(resumed.status, 0, resumed.stderr)
    const result = JSON.parse(resumed.stdout)
    assert.equal(result.state, 'completed')
    assert.deepEqual(result.data, { sku: 'A-1', amount: 42 })
    assert.deepEqual(result.outputs, {
      reserve: { sku: 'A-1', reserved: true },
      charge: { charged: 42, sku: 'A-1' },
      ship: { shipped: 'A-1' },
    })
    // Step 1 was not re-run by process B; every step ran exactly once.
    assert.deepEqual(effects(dir), ['reserve', 'charge', 'ship'])
  }),
)

test(
  'a hard kill inside a step is not silently re-run (at-most-once guard)',
  withDir((dir) => {
    const crashed = runWorker(dir, 'crash-inside')
    assert.equal(crashed.signal, 'SIGKILL', `process A should die hard: ${crashed.stderr}`)
    assert.deepEqual(effects(dir), ['reserve', 'charge'])

    const resumed = runWorker(dir, 'resume')
    assert.equal(resumed.status, 0, resumed.stderr)
    const result = JSON.parse(resumed.stdout)
    // The interrupted dispatch is ambiguous (did the charge happen?), so the
    // engine fails the instance instead of charging twice.
    assert.equal(result.state, 'failed')
    assert.deepEqual(result.outputs.reserve, { sku: 'A-1', reserved: true })
    assert.match(result.outputs.charge.message, /automatic redispatch is blocked/)
    assert.equal(result.outputs.ship, undefined)
    assert.deepEqual(effects(dir), ['reserve', 'charge'])
  }),
)

test(
  'retryable errors retry; PermanentError fails; late registration is rejected',
  withDir(async (dir) => {
    const engine = await Engine.open(join(dir, 'app.db'))
    let calls = 0
    engine
      .handler('flaky', ({ attempt }) => {
        calls += 1
        if (calls === 1) throw new Error('temporary outage')
        return { attempt }
      })
      .handler('boom', () => {
        throw new PermanentError('card declined')
      })
    await engine.deploy({
      name: 'flaky',
      blocks: [
        {
          type: 'step',
          id: 'x',
          handler: 'flaky',
          params: {},
          retry: { max_attempts: 3, initial_backoff: 10, max_backoff: 10 },
        },
      ],
    })
    await engine.deploy({ name: 'fail', blocks: [{ type: 'step', id: 'x', handler: 'boom', params: {} }] })

    const ok = await engine.run(await engine.start('flaky'), { timeoutMs: 10_000 })
    assert.equal(ok.state, 'completed')
    assert.equal(calls, 2)

    const failed = await engine.run(await engine.start('fail'), { timeoutMs: 10_000 })
    assert.equal(failed.state, 'failed')
    assert.match(failed.outputs.x.message, /card declined/)

    // Idempotency key: same key, same instance.
    const a = await engine.start('fail', {}, { idempotencyKey: 'k' })
    const b = await engine.start('fail', {}, { idempotencyKey: 'k' })
    assert.equal(a, b)

    assert.throws(() => engine.handler('late', () => 1), /before the first/)
    await engine.close()
    await assert.rejects(engine.get(a), /closed/)
  }),
)
