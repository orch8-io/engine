"""A 3-step durable order flow, in-process, persisted to ./orders.db.

    python main.py            # run order-1 to completion
    CRASH=1 python main.py    # hard-exit after step 1 (while step 2 is delayed)
    python main.py            # resumes order-1: step 1 is not repeated
"""

import os
import sys
from pathlib import Path

# In your app, `pip install orch8-engine-native` instead.
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "packages/python-native/python"))

from orch8_engine import Engine, PermanentError  # noqa: E402

ORDER_FLOW = {
    "name": "order",
    "blocks": [
        {"type": "step", "id": "reserve", "handler": "reserve"},
        {
            "type": "step",
            "id": "charge",
            "handler": "charge",
            "delay": {"duration": 2000},  # window to demonstrate a crash between steps
            "retry": {"max_attempts": 3, "initial_backoff": 200, "max_backoff": 1000},
        },
        {"type": "step", "id": "ship", "handler": "ship"},
    ],
}

engine = Engine.open(str(Path(__file__).with_name("orders.db")))


@engine.handler("reserve")
def reserve(ctx):
    print(f"reserve: holding {ctx['data']['sku']}")
    return {"sku": ctx["data"]["sku"], "reservation": f"res-{ctx['data']['order_id']}"}


@engine.handler("charge")
def charge(ctx):
    if ctx["data"]["amount"] <= 0:
        raise PermanentError("invalid amount")  # no retry
    print(f"charge: {ctx['data']['amount']} for {ctx['outputs']['reserve']['reservation']}")
    return {"payment_id": f"pay-{ctx['data']['order_id']}", "amount": ctx["data"]["amount"]}


@engine.handler("ship")
def ship(ctx):
    print(f"ship: {ctx['data']['sku']} after {ctx['outputs']['charge']['payment_id']}")
    return {"tracking": f"trk-{ctx['data']['order_id']}"}


engine.deploy(ORDER_FLOW)  # idempotent: do it on every startup
order = engine.start(
    "order",
    {"order_id": 1, "sku": "book-42", "amount": 1999},
    idempotency_key="order-1",  # same key after a restart -> same instance
)

if os.environ.get("CRASH"):
    engine.run(order, timeout=0.5)
    print("simulating a crash between step 1 and step 2")
    sys.stdout.flush()
    os._exit(137)

result = engine.run(order)
print(result["state"], result["outputs"])
engine.close()
