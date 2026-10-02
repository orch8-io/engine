"""Child process used by test_durable_restart.py.

argv: <db_path> <effects_path> <mode>
  crash-between: run step 1, then os._exit while step 2 is delayed
  crash-inside:  os._exit from inside step 2's handler
  resume:        run to completion and print the final snapshot
"""

import json
import os
import sys

from orch8_engine import Engine

db_path, effects_path, mode = sys.argv[1:4]

SEQUENCE = {
    "name": "order",
    "blocks": [
        {"type": "step", "id": "reserve", "handler": "reserve", "params": {}},
        {"type": "step", "id": "charge", "handler": "charge", "params": {}, "delay": {"duration": 1000}},
        {"type": "step", "id": "ship", "handler": "ship", "params": {}},
    ],
}


def effect(name: str) -> None:
    with open(effects_path, "a") as f:
        f.write(name + "\n")


engine = Engine.open(db_path)


@engine.handler("reserve")
def reserve(ctx):
    effect("reserve")
    return {"sku": ctx["data"]["sku"], "reserved": True}


@engine.handler("charge")
def charge(ctx):
    effect("charge")
    if mode == "crash-inside":
        os._exit(137)
    return {"charged": ctx["data"]["amount"], "sku": ctx["outputs"]["reserve"]["sku"]}


@engine.handler("ship")
def ship(ctx):
    effect("ship")
    return {"shipped": ctx["outputs"]["charge"]["sku"]}


engine.deploy(SEQUENCE)
instance = engine.start("order", {"sku": "A-1", "amount": 42}, idempotency_key="order-1")
if mode == "crash-between":
    snapshot = engine.run(instance, timeout=0.3)
    assert "reserve" in snapshot["outputs"] and "charge" not in snapshot["outputs"], snapshot
    os._exit(137)
result = engine.run(instance, timeout=20)
engine.close()
print(json.dumps(result))
