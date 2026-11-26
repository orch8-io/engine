'use strict'

// Child process used by durable-restart.test.js.
// argv: <dbPath> <effectsPath> <mode>
//   mode = "crash-between": run step 1, then SIGKILL at step 2's persisted gate
//   mode = "crash-inside":  SIGKILL from inside step 2's handler
//   mode = "resume-between": release step 2's gate and finish
//   mode = "resume":        run to completion, print the final snapshot
const { appendFileSync } = require('node:fs')
const { Engine } = require('..')

const [dbPath, effectsPath, mode] = process.argv.slice(2)
const betweenSteps = mode === 'crash-between' || mode === 'resume-between'

const sequence = {
  name: 'order',
  blocks: [
    { type: 'step', id: 'reserve', handler: 'reserve', params: {} },
    {
      type: 'step', id: 'charge', handler: 'charge', params: {},
      ...(betweenSteps && { wait_for_input: { prompt: 'Continue after restart?', store_as: 'restart_allowed' } }),
    },
    { type: 'step', id: 'ship', handler: 'ship', params: {} },
  ],
}

async function main() {
  const engine = await Engine.open(dbPath)
  engine
    .handler('reserve', async ({ data }) => {
      appendFileSync(effectsPath, 'reserve\n')
      return { sku: data.sku, reserved: true }
    })
    .handler('charge', async ({ data, outputs }) => {
      appendFileSync(effectsPath, 'charge\n')
      if (mode === 'crash-inside') process.kill(process.pid, 'SIGKILL')
      return { charged: data.amount, sku: outputs.reserve.sku }
    })
    .handler('ship', async ({ outputs }) => {
      appendFileSync(effectsPath, 'ship\n')
      return { shipped: outputs.charge.sku }
    })

  await engine.deploy(sequence)
  // Same idempotency key in every process: the restarted process gets the
  // original instance back instead of starting a second order.
  const id = await engine.start('order', { sku: 'A-1', amount: 42 }, { idempotencyKey: 'order-1' })
  if (mode === 'crash-between') {
    const snapshot = await engine.run(id, { timeoutMs: 20_000 })
    if (snapshot.state !== 'waiting' || !snapshot.outputs.reserve || snapshot.outputs.charge) {
      throw new Error(`expected to stop between steps, got ${JSON.stringify(snapshot)}`)
    }
    process.kill(process.pid, 'SIGKILL')
  }
  if (mode === 'resume-between') {
    await engine.signal(id, 'human_input:charge', { value: 'yes' })
  }
  const result = await engine.run(id, { timeoutMs: 20_000 })
  await engine.close()
  process.stdout.write(JSON.stringify(result))
}

main().catch((error) => {
  console.error(error)
  process.exit(2)
})
