'use strict'

const test = require('node:test')
const assert = require('node:assert/strict')
const { spawnSync } = require('node:child_process')
const { mkdtempSync, readFileSync, rmSync } = require('node:fs')
const { tmpdir } = require('node:os')
const { join } = require('node:path')
const { DatabaseSync } = require('node:sqlite')

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

test('transactional batch keeps positions and idempotency after reopen', withDir(async (dir) => {
  const path = join(dir, 'batch.db')
  const sequence = { name: 'batch', blocks: [{ type: 'step', id: 'one', handler: 'noop' }] }
  const items = [
    { name: 'batch', input: { value: 1 }, idempotencyKey: 'one' },
    { name: 'batch', input: { value: 2 }, idempotencyKey: 'two' },
    { name: 'batch', input: { value: 99 }, idempotencyKey: 'one' },
  ]
  const engine = await Engine.open(path)
  let ids
  try {
    await engine.deploy(sequence)
    await assert.rejects(engine.startBatch([
      { name: 'batch', idempotencyKey: 'rolled-back' },
      { name: 'missing', idempotencyKey: 'invalid-sequence' },
    ]), /batch item 1/)
    assert.equal((await engine.iterInstances().next()).done, true)
    ids = await engine.startBatch(items)
    assert.equal(ids[0], ids[2])
    assert.notEqual(ids[0], ids[1])
    assert.deepEqual((await engine.get(ids[0])).data, { value: 1 })
  } finally {
    await engine.close()
  }

  const reopened = await Engine.open(path)
  try {
    await reopened.deploy(sequence)
    assert.deepEqual(await reopened.startBatch(items), ids)
    assert.equal(await reopened.start('batch', {}, { idempotencyKey: 'two' }), ids[1])
  } finally {
    await reopened.close()
  }
}))

test('observer deadline returns while a single durable attempt is still running', { timeout: 6000 }, withDir(async (dir) => {
  const engine = await Engine.open(join(dir, 'observer.db'))
  let calls = 0
  let entered
  let release
  const started = new Promise((resolve) => { entered = resolve })
  const hold = new Promise((resolve) => { release = resolve })
  try {
    engine.handler('host', async () => {
      calls += 1
      entered()
      await hold
      return { value: 42 }
    })
    await engine.deploy({ name: 'observed', blocks: [{ type: 'step', id: 'one', handler: 'host' }] })
    const id = await engine.start('observed')
    const observed = engine.run(id, { observerTimeoutMs: 500 })
    await Promise.all([
      started,
      assert.rejects(observed, /run observer deadline elapsed/),
    ])
    release()
    const finished = await engine.run(id, { timeoutMs: 5000 })
    assert.deepEqual(finished.outputs.one, { value: 42 })
    assert.equal(calls, 1)
  } finally {
    release()
    await engine.close()
  }
}))

test('iterates stable pages across a concurrent insert and restart', withDir(async (dir) => {
  const path = join(dir, 'app.db')
  const writer = await Engine.open(path)
  let sequenceId
  const expected = []
  try {
    const blocks = [{ type: 'step', id: 'one', handler: 'host' }]
    writer.handler('host', () => null)
    sequenceId = await writer.deploy({ name: 'paged', blocks })
    await writer.deploy({ name: 'other', blocks })
    await writer.start('other')
    for (let index = 0; index < 5; index++) expected.push(await writer.start('paged'))
    expected.sort().reverse()
    const iterator = writer.iterInstances({ state: 'scheduled', sequenceId, pageSize: 2 })
    const { value: first } = await iterator.next()
    assert.equal(first.id, expected[0])
    await new Promise((resolve) => setTimeout(resolve, 10))
    await writer.start('paged')
    const rest = []
    for await (const entry of iterator) rest.push(entry.id)
    assert.deepEqual(rest, expected.slice(1))
  } finally {
    await writer.close()
  }

  const reader = await Engine.open(path)
  try {
    const all = []
    for await (const entry of reader.iterInstances({ state: 'scheduled', sequenceId, pageSize: 1 })) {
      assert.equal(entry.sequence_id, sequenceId)
      assert.equal(entry.state, 'scheduled')
      assert.ok(Number.isFinite(Date.parse(entry.created_at)))
      all.push(entry.id)
    }
    assert.deepEqual(all.slice(1), expected)
    await assert.rejects(reader.iterInstances({ pageSize: 0 }).next(), /pageSize/)
  } finally {
    await reader.close()
  }
}))

test('reads one persisted output without decoding a different block', withDir(async (dir) => {
  const path = join(dir, 'app.db')
  const writer = await Engine.open(path)
  let id
  let attempt
  try {
    writer.handler('value', ({ stepId, attempt: current }) => {
      if (stepId === 'wanted') {
        attempt = current
        return { value: 42 }
      }
      return 'x'.repeat(256 * 1024)
    })
    await writer.deploy({ name: 'selected-output', blocks: [
      { type: 'step', id: 'wanted', handler: 'value' },
      { type: 'step', id: 'unrelated', handler: 'value' },
    ] })
    id = await writer.start('selected-output')
    assert.equal((await writer.run(id, { timeoutMs: 5000 })).state, 'completed')
  } finally {
    await writer.close()
  }
  const database = new DatabaseSync(path)
  try {
    const changed = database.prepare(
      "UPDATE block_outputs SET output = ? WHERE instance_id = ? AND block_id = 'unrelated'",
    ).run('{damaged JSON', id)
    assert.ok(changed.changes > 0)
  } finally {
    database.close()
  }

  const reader = await Engine.open(path)
  try {
    await assert.rejects(reader.get(id), /serialization/)
    const output = await reader.getOutput(id, 'wanted', attempt)
    assert.deepEqual(output.output, { value: 42 })
    assert.equal(output.attempt, attempt)
    assert.equal(output.instance_id, id)
    assert.equal(await reader.getOutput(id, 'wanted', attempt + 1), null)
    await assert.rejects(reader.getOutput(id, 'wanted', 1.5), /attempt must be an integer/)
  } finally {
    await reader.close()
  }
}))

test('current snapshots retain retry diagnostics while a later attempt is scheduled', withDir(async (dir) => {
  const engine = await Engine.open(join(dir, 'app.db'))
  let calls = 0
  engine.handler('retry_snapshot', () => {
    calls += 1
    throw new Error('node retry diagnostic')
  })
  try {
    await engine.deploy({ name: 'snapshot', blocks: [{
      type: 'step', id: 'work', handler: 'retry_snapshot',
      retry: { max_attempts: 3, initial_backoff: 3600000, max_backoff: 3600000 },
    }] })
    const instance = await engine.start('snapshot')
    const deadline = performance.now() + 5000
    let snapshot
    do {
      snapshot = await engine.run(instance, { timeoutMs: 100 })
      if (snapshot.state === 'scheduled' && JSON.stringify(snapshot.outputs.work ?? null).includes('node retry diagnostic')) break
    } while (performance.now() < deadline)
    assert.equal(snapshot.state, 'scheduled')
    assert.match(JSON.stringify(snapshot.outputs.work ?? null), /node retry diagnostic/)
    assert.deepEqual((await engine.get(instance)).outputs, snapshot.outputs)
    assert.equal(calls, 1, 'reading a diagnostic must not start the next attempt')
  } finally {
    await engine.close()
  }
}))

test('a second native handle cannot open a live database owner', withDir(async (dir) => {
  const path = join(dir, 'app.db')
  const first = await Engine.open(path)
  const second = await Engine.open(path)
  const workflow = { name: 'owner', blocks: [{ type: 'step', id: 'one', handler: 'noop' }] }
  try {
    await first.deploy(workflow)
    await assert.rejects(second.deploy(workflow), /owned/)
    await first.close()
    await second.deploy(workflow)
  } finally {
    await first.close()
    await second.close()
  }
}))

test('run deadline returns while its host callback remains owned', withDir(async (dir) => {
  const engine = await Engine.open(join(dir, 'app.db'))
  let enter, release, run, entryTimer, waitTimer
  const entered = new Promise((resolve) => { enter = resolve })
  const held = new Promise((resolve) => { release = resolve })
  let calls = 0
  engine.handler('held', async () => { calls += 1; enter(); await held; return true })
  try {
    await engine.deploy({ name: 'deadline', blocks: [{ type: 'step', id: 'work', handler: 'held', timeout: 10000 }] })
    const instance = await engine.start('deadline')
    run = engine.run(instance, { timeoutMs: 1000 })
    run.catch(() => {}) // Observe an early rejection while waiting for host entry.
    await Promise.race([entered, new Promise((_, reject) => {
      entryTimer = setTimeout(() => reject(new Error('host never entered')), 5000)
    })])
    const result = await Promise.race([run, new Promise((resolve) => {
      waitTimer = setTimeout(() => resolve(null), 3000)
    })])
    release()
    await run
    assert.notEqual(result, null, 'run deadline waited for physical callback completion')
    assert.notEqual(result.state, 'completed')
    assert.equal((await engine.run(instance, { timeoutMs: 2000 })).state, 'completed')
    assert.equal(calls, 1)
  } finally {
    clearTimeout(entryTimer); clearTimeout(waitTimer)
    release()
    if (run) await run.catch(() => {})
    await engine.close()
  }
}))

test('run preflight rejects invalid budgets and zero does not dispatch', withDir(async (dir) => {
  const engine = await Engine.open(join(dir, 'app.db'))
  const calls = []
  engine.handler('host', (ctx) => { calls.push(ctx.attempt); return true })
  try {
    await engine.deploy({ name: 'preflight', blocks: [{ type: 'step', id: 'work', handler: 'host' }] })
    const instance = await engine.start('preflight')
    for (const timeoutMs of [-1, NaN, Infinity, -Infinity, 0.5, 2 ** 32]) {
      await assert.rejects(engine.run(instance, { timeoutMs }), /timeout/)
    }
    assert.deepEqual(calls, [])
    assert.equal((await engine.run(instance, { timeoutMs: 0 })).state, 'scheduled')
    assert.deepEqual(calls, [])
    assert.equal((await engine.run(instance, { timeoutMs: 2000 })).state, 'completed')
    assert.deepEqual(calls, [0])
  } finally {
    await engine.close()
  }
}))

test(
  'a 3-step flow resumes in a new process after a hard kill between steps',
  withDir((dir) => {
    const crashed = runWorker(dir, 'crash-between')
    assert.equal(crashed.signal, 'SIGKILL', `process A should die hard: ${crashed.stderr}`)
    assert.deepEqual(effects(dir), ['reserve'])

    const resumed = runWorker(dir, 'resume-between')
    assert.equal(resumed.status, 0, resumed.stderr)
    const result = JSON.parse(resumed.stdout)
    assert.equal(result.state, 'completed')
    assert.deepEqual(result.data, { sku: 'A-1', amount: 42, restart_allowed: 'yes' })
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
