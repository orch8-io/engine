'use strict'

const test = require('node:test')
const assert = require('node:assert/strict')
const { mkdtempSync, rmSync } = require('node:fs')
const { tmpdir } = require('node:os')
const { join } = require('node:path')
const { Engine, PermanentError } = require('..')

async function failingHost(error, inspect) {
  const dir = mkdtempSync(join(tmpdir(), 'orch8-host-errors-'))
  const engine = await Engine.open(join(dir, 'app.db'))
  try {
    let calls = 0
    engine.handler('throw', () => { calls++; throw error })
    await engine.deploy({ name: 'failure', blocks: [{ type: 'step', id: 'work', handler: 'throw', retry: { max_attempts: 2, initial_backoff: 1, max_backoff: 1 } }] })
    const id = await engine.start('failure')
    const result = await engine.run(id, { timeoutMs: 3000 })
    assert.equal(result.state, 'failed')
    const output = result.outputs.work
    const prefix = `step failed: work on instance ${id}: `
    if (output.message !== undefined) assert.ok(output.message.startsWith(prefix))
    inspect({ ...output, message: output.message?.slice(prefix.length) ?? output.error }, calls)
  } finally {
    await engine.close()
    rmSync(dir, { recursive: true, force: true })
  }
}

test('error reporting never executes thrown-value accessors or coercion', async () => {
  let touched = 0
  const hostile = {
    get permanent() { touched++; throw new Error('permanent accessor ran') },
    get message() { touched++; throw new Error('message accessor ran') },
    toString() { touched++; throw new Error('coercion ran') },
  }
  await failingHost(hostile, (output) => {
    assert.equal(touched, 0)
    assert.equal(output.message, 'handler failed')
  })
})

test('thrown proxies and revoked proxies cannot trap error reporting', async () => {
  let touched = 0
  const trap = () => { touched++; throw new Error('proxy trap ran') }
  const hostile = new Proxy({}, { get: trap, getPrototypeOf: trap, getOwnPropertyDescriptor: trap })
  await failingHost(hostile, (output) => {
    assert.equal(touched, 0)
    assert.equal(output.message, 'handler failed')
  })
  const revoked = Proxy.revocable({}, {})
  revoked.revoke()
  await failingHost(revoked.proxy, (output) => assert.equal(output.message, 'handler failed'))
})

test('ordinary messages and explicit permanent markers survive bounded reporting', async () => {
  await failingHost(new Error('ordinary failure'), (output, calls) => {
    assert.equal(output.message, 'ordinary failure')
    assert.equal(calls, 3)
  })
  await failingHost(new PermanentError('terminal failure'), (output, calls) => {
    assert.equal(output.message, 'terminal failure')
    assert.equal(calls, 1)
  })
  await failingHost({ message: 'x'.repeat(100000), permanent: true }, (output, calls) => {
    assert.equal(calls, 1)
    assert.equal(output.message, 'x'.repeat(1024))
  })
  await failingHost('primitive failure', (output) => assert.equal(output.message, 'primitive failure'))
})

test('input serialization errors do not inspect the thrown value', async () => {
  const engine = new Engine({ start() { throw new Error('native start must not be reached') } })
  let touched = 0
  const error = new Proxy({}, { get() { touched++; throw new Error('proxy trap ran') } })
  assert.throws(() => engine.start('flow', { toJSON() { throw error } }), {
    name: 'TypeError', message: 'input must be JSON-serializable: handler failed',
  })
  assert.equal(touched, 0)
})


test('message truncation preserves UTF-8 and replaces lone surrogates', async () => {
  await failingHost(new PermanentError('a' + '😀'.repeat(512)), (output) => {
    assert.equal(output.message, 'a' + '😀'.repeat(511))
  })
  await failingHost(new PermanentError('before\ud800after'), (output) => {
    assert.equal(output.message, 'before�after')
  })
})

test('PermanentError retains its classification without instanceof or property getters', async () => {
  let touched = 0
  const error = new PermanentError('terminal')
  Object.defineProperty(error, 'permanent', { get() { touched++; throw new Error('marker getter ran') } })
  Object.defineProperty(PermanentError, Symbol.hasInstance, {
    configurable: true,
    value() { touched++; throw new Error('instanceof ran') },
  })
  try {
    await failingHost(error, (output, calls) => {
      assert.equal(touched, 0)
      assert.equal(calls, 1)
      assert.equal(output.message, 'terminal')
    })
  } finally {
    delete PermanentError[Symbol.hasInstance]
  }
})
