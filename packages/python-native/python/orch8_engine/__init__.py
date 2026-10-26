"""Orch8 in-process: strict validation, dry runs, and a durable SQLite engine.

    from orch8_engine import Engine

    engine = Engine.open("app.db")

    @engine.handler("charge")
    def charge(ctx):
        return {"charged": ctx["data"]["amount"]}

    engine.deploy({"name": "pay", "blocks": [
        {"type": "step", "id": "charge", "handler": "charge"}]})
    print(engine.run(engine.start("pay", {"amount": 42}, idempotency_key="order-1")))
"""

from __future__ import annotations

import asyncio
import inspect
import json
from typing import Any, Callable, Mapping, Optional, Union

from ._native import DurableEngine as _DurableEngine
from ._native import run_sequence_json, sequence_schema_version, validate_sequence_json

__all__ = [
    "Engine",
    "PermanentError",
    "run_sequence_json",
    "sequence_schema_version",
    "validate_sequence_json",
]

Handler = Callable[[dict], Any]


class PermanentError(Exception):
    """Raise from a handler to fail the step without retrying.

    Any other exception is retryable (per the step's ``retry`` policy).
    """


# Read built-in exception storage directly: subclass accessors, __str__ and
# metaclass hooks are application code and must not run while reporting failure.
_exception_args = BaseException.__dict__["args"].__get__
_exception_dict = BaseException.__dict__["__dict__"].__get__
_class_mro = type.__dict__["__mro__"].__get__
_permanent_error_type = PermanentError


def _host_error(error: BaseException) -> dict:
    args = _exception_args(error)
    message = args[0] if len(args) == 1 and type(args[0]) is str else "handler failed"
    # Slice before encoding so a large diagnostic cannot allocate another large
    # copy. Lone surrogates must not reach Rust's strict JSON string decoder.
    message = (message[:1024] or "handler failed").encode("utf-8", "replace").decode("utf-8")
    permanent = any(cls is _permanent_error_type for cls in _class_mro(type(error)))
    if not permanent:
        # Iterating exact string keys avoids dict lookup invoking a hostile key's
        # __eq__. Bound inspection independently of the exception's attribute count.
        for index, (key, value) in enumerate(dict.items(_exception_dict(error))):
            if index == 64:
                break
            if type(key) is str and key == "permanent":
                permanent = value is True
                break
    return {"message": message, "permanent": permanent}


def _to_json(value: Any) -> Optional[str]:
    if value is None or isinstance(value, str):
        return value
    return json.dumps(value)


def _wrap(fn: Handler) -> Callable[[str], str]:
    def invoke(request_json: str) -> str:
        try:
            request = json.loads(request_json)
            output = fn(request)
            if inspect.isawaitable(output):
                output = asyncio.run(_await(output))
            return json.dumps({"ok": output})
        except BaseException as error:  # noqa: BLE001 - every failure becomes a step error
            return json.dumps({"error": _host_error(error)})

    return invoke


async def _await(awaitable: Any) -> Any:
    return await awaitable


class Engine:
    """A durable workflow engine persisted in one SQLite file, in-process.

    Calls block the calling thread but release the GIL while the engine works.
    """

    def __init__(self, path: str) -> None:
        self._inner = _DurableEngine(path)

    @classmethod
    def open(cls, path: str) -> "Engine":
        """Open (or create) the database at ``path``."""
        return cls(path)

    @property
    def path(self) -> str:
        return self._inner.path

    def handler(self, name: str, fn: Optional[Handler] = None):
        """Register a step handler; usable directly or as ``@engine.handler(name)``.

        The handler receives ``{"params", "data", "outputs", "instance_id",
        "step_id", "attempt"}`` and returns a JSON-serializable value. Register
        all handlers before the first other call.
        """
        if fn is None:
            def decorator(func: Handler) -> Handler:
                self._inner.register(name, _wrap(func))
                return func

            return decorator
        self._inner.register(name, _wrap(fn))
        return fn

    def deploy(self, sequence: Union[Mapping[str, Any], str]) -> str:
        """Store a sequence definition; idempotent per name + version."""
        return self._inner.deploy(_to_json(sequence))

    def start(
        self,
        name: str,
        input: Optional[Mapping[str, Any]] = None,
        *,
        idempotency_key: Optional[str] = None,
        version: Optional[int] = None,
    ) -> str:
        """Start an instance of a deployed sequence and return its id."""
        return self._inner.start(name, _to_json(input), idempotency_key, version)

    def run(self, id: str, *, timeout: Optional[float] = None) -> dict:
        """Drive the engine until the instance settles or ``timeout`` seconds pass."""
        return json.loads(self._inner.run(id, timeout))

    def get(self, id: str) -> dict:
        """Current snapshot ``{"id", "state", "data", "outputs"}``."""
        return json.loads(self._inner.get(id))

    def signal(self, id: str, signal: str, payload: Any = None) -> None:
        """Send ``pause``/``resume``/``cancel``/``update_context`` or a custom signal."""
        self._inner.signal(id, signal, _to_json(payload))

    def close(self) -> None:
        """Stop the engine and release the database."""
        self._inner.close()

    def __enter__(self) -> "Engine":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
