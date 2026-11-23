'use strict'

const assert = require('node:assert/strict')
const { execFileSync } = require('node:child_process')

const firstInput = { value: 21 }
const expectedResult = { value: 42 }
const double = ({ data }) => ({ value: data.value * 2 })

async function runPhase(phase, id) {
  assert.equal(process.platform, 'linux')
  assert.equal(process.arch, process.env.ORCH8_NATIVE_ARCH)
  assert.equal(process.report.getReport().header.glibcVersionRuntime, undefined)
  const { Engine } = require('@orch8/engine-native')
  const engine = await Engine.open('alpine.db')
  try {
    switch (phase) {
      case 'queue': {
        engine.handler('double', double)
        await engine.deploy({ name: 'alpine', blocks: [{ type: 'step', id: 'work', handler: 'double' }] })
        const runId = await engine.start('alpine', firstInput)
        assert.equal((await engine.get(runId)).state, 'scheduled')
        return runId
      }
      case 'execute': {
        engine.handler('double', double)
        assert.equal((await engine.get(id)).state, 'scheduled')
        const result = await engine.run(id, { timeoutMs: 5000 })
        assert.equal(result.state, 'completed')
        assert.deepEqual(result.outputs.work, expectedResult)
        break
      }
      case 'verify': {
        const snapshot = await engine.get(id)
        assert.equal(snapshot.state, 'completed')
        assert.deepEqual(snapshot.outputs.work, expectedResult)
        break
      }
      default:
        throw new Error(`unexpected smoke test phase: ${phase}`)
    }
  } finally {
    await engine.close()
  }
}

async function main() {
  const [phase, id] = process.argv.slice(2)
  if (phase) {
    const result = await runPhase(phase, id)
    if (result !== undefined) process.stdout.write(result)
    return
  }

  const launch = (...args) => execFileSync(process.execPath, [__filename, ...args], {
    encoding: 'utf8', stdio: ['ignore', 'pipe', 'inherit'], timeout: 30_000,
  })
  const runId = launch('queue')
  assert.match(runId, /^[0-9a-f-]{36}$/)
  launch('execute', runId)
  launch('verify', runId)
}

main().catch((error) => { console.error(error); process.exitCode = 1 })
