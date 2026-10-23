# Durable workflows in Python, no server

A 3-step order flow (`reserve` -> `charge` -> `ship`) running inside the
Python process on `orch8-engine-native`, persisted to `orders.db` (SQLite).

## Run

Build the extension once with the virtual-environment instructions in
[the native Python package](../../packages/python-native/README.md#build-and-test).
Those instructions run inside `packages/python-native`; return to the repository
root before continuing, and keep that virtual environment active.

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
