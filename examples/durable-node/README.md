# Durable workflows in Node, no server

A 3-step order flow (`reserve` -> `charge` -> `ship`) running inside the Node
process on `@orch8/engine-native`, persisted to `orders.db` (SQLite).

## Run

Build the addon once (from the repo root):

```sh
cd packages/node-native && pnpm --package=@napi-rs/cli@3 dlx napi build --platform --no-js --dts native.d.ts
```

Then:

```sh
cd examples/durable-node
node main.js            # runs order-1 to completion
rm orders.db*           # start fresh
CRASH=1 node main.js    # step 1 runs, then the process is SIGKILLed
node main.js            # order-1 resumes at step 2; `reserve` is not printed again
```

`start()` uses the idempotency key `order-1`, so the second process gets the
original instance back instead of placing a second order. See
[`packages/node-native/README.md`](../../packages/node-native/README.md#semantics)
for the exact durability guarantees.
