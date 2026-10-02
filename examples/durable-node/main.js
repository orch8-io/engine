'use strict'

// A 3-step durable order flow, in-process, persisted to ./orders.db.
//
//   node main.js            # run order-1 to completion
//   CRASH=1 node main.js    # hard-kill after step 1 (while step 2 is delayed)
//   node main.js            # resumes order-1: step 1 is not repeated
//
// In your app: require('@orch8/engine-native')
const { Engine, PermanentError } = require('../../packages/node-native')

const orderFlow = {
  name: 'order',
  blocks: [
    { type: 'step', id: 'reserve', handler: 'reserve', params: {} },
    {
      type: 'step',
      id: 'charge',
      handler: 'charge',
      params: {},
      delay: { duration: 2000 }, // leaves a window to demonstrate a crash between steps
      retry: { max_attempts: 3, initial_backoff: 200, max_backoff: 1000 },
    },
    { type: 'step', id: 'ship', handler: 'ship', params: {} },
  ],
}

async function main() {
  const engine = await Engine.open(`${__dirname}/orders.db`)

  engine
    .handler('reserve', async ({ data }) => {
      console.log(`reserve: holding ${data.sku}`)
      return { sku: data.sku, reservation: `res-${data.orderId}` }
    })
    .handler('charge', async ({ data, outputs }) => {
      if (data.amount <= 0) throw new PermanentError('invalid amount') // no retry
      console.log(`charge: ${data.amount} for ${outputs.reserve.reservation}`)
      return { paymentId: `pay-${data.orderId}`, amount: data.amount }
    })
    .handler('ship', async ({ data, outputs }) => {
      console.log(`ship: ${data.sku} after ${outputs.charge.paymentId}`)
      return { tracking: `trk-${data.orderId}` }
    })

  await engine.deploy(orderFlow) // idempotent: do it on every startup
  const id = await engine.start(
    'order',
    { orderId: 1, sku: 'book-42', amount: 1999 },
    { idempotencyKey: 'order-1' }, // same key after a restart -> same instance
  )

  if (process.env.CRASH) {
    await engine.run(id, { timeoutMs: 500 })
    console.log('simulating a crash between step 1 and step 2')
    process.kill(process.pid, 'SIGKILL')
  }

  const result = await engine.run(id)
  console.log(result.state, JSON.stringify(result.outputs, null, 2))
  await engine.close()
}

main().catch((error) => {
  console.error(error)
  process.exit(1)
})
