# Durable workflows in Python, no server

A 3-step order flow (`reserve` -> `charge` -> `ship`) running inside the
Python process on `orch8-engine-native`, persisted to `orders.db` (SQLite).

## Run

Build the extension once (from the repo root). With maturin:

```sh
cd packages/python-native && maturin develop
```

or without maturin (macOS shown; use `.so` from `target/debug/lib_native.so` on Linux):

```sh
cd packages/python-native
cargo rustc --lib -- -C link-arg=-undefined -C link-arg=dynamic_lookup
cp "${CARGO_TARGET_DIR:-../../target}"/*/debug/lib_native.dylib python/orch8_engine/_native.abi3.so
```

Then:

```sh
cd examples/durable-python
python main.py            # runs order-1 to completion
rm orders.db*             # start fresh
CRASH=1 python main.py    # step 1 runs, then the process hard-exits
python main.py            # order-1 resumes at step 2; `reserve` is not printed again
```

See [`packages/python-native/README.md`](../../packages/python-native/README.md#semantics)
for the exact durability guarantees.
