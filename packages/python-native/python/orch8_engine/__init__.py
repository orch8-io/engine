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
import math
from typing import Any, AsyncIterator, Callable, Iterator, Mapping, Optional, Sequence, TypedDict, Union, cast

from ._native import DurableEngine as _DurableEngine
from ._native import run_sequence_json, sequence_schema_version, validate_sequence_json

__all__ = [
    "AsyncEngine",
    "BatchStartItem",
    "BlockOutput",
    "Engine",
    "HandlerInput",
    "InstanceSnapshot",
    "InstanceSummary",
    "PermanentError",
    "run_sequence_json",
    "sequence_schema_version",
    "validate_sequence_json",
]

class HandlerInput(TypedDict):
    params: Any
    data: Any
    outputs: dict[str, Any]
    instance_id: str
    step_id: str
    attempt: int


class InstanceSnapshot(TypedDict):
    id: str
    state: str
    data: Any
    outputs: dict[str, Any]


class InstanceSummary(TypedDict):
    id: str
    sequence_id: str
    state: str
    created_at: str


class _BatchStartOptions(TypedDict, total=False):
    input: Mapping[str, Any]
    version: int


class BatchStartItem(_BatchStartOptions):
    name: str
    idempotency_key: str


class BlockOutput(TypedDict):
    id: str
    instance_id: str
    block_id: str
    output: Any
    output_ref: Optional[str]
    output_size: int
    attempt: int
    created_at: str


Handler = Union[Callable[[HandlerInput], Any], Callable[[dict[str, Any]], Any]]


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


def _wrap(
    fn: Handler, loop: Optional[asyncio.AbstractEventLoop] = None
) -> Callable[[str], str]:
    def invoke(request_json: str) -> str:
        try:
            request = json.loads(request_json)
            output = fn(request)
            if inspect.isawaitable(output):
                if loop is None:
                    output = asyncio.run(_await(output))
                else:
                    output = asyncio.run_coroutine_threadsafe(_await(output), loop).result()
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

    def start_batch(self, items: Sequence[BatchStartItem]) -> list[str]:
        """Start up to 1000 items transactionally and return IDs in input order."""
        return cast(list[str], json.loads(self._inner.start_batch(json.dumps(list(items)))))

    def run(
        self, id: str, *, timeout: Optional[float] = None, observer_timeout: Optional[float] = None
    ) -> InstanceSnapshot:
        """Drive the engine until settlement or the scheduling budget elapses.

        Storage reads can continue past the budget given by ``timeout`` seconds.
        ``observer_timeout`` also covers those reads and raises ``TimeoutError``.
        """
        return cast(InstanceSnapshot, json.loads(self._inner.run(id, timeout, observer_timeout)))

    def get(self, id: str) -> InstanceSnapshot:
        """Current snapshot ``{"id", "state", "data", "outputs"}``."""
        return cast(InstanceSnapshot, json.loads(self._inner.get(id)))

    def get_output(self, id: str, block: str, attempt: int) -> Optional[BlockOutput]:
        """Read one output by block and persisted attempt number."""
        return cast(Optional[BlockOutput], json.loads(self._inner.get_output(id, block, attempt)))

    def _list_instance_page(
        self, state: Optional[str], sequence_id: Optional[str], before: Optional[str], page_size: int
    ) -> list[InstanceSummary]:
        return cast(list[InstanceSummary], json.loads(
            self._inner.list_instance_page(state, sequence_id, before, page_size)
        ))

    def iter_instances(
        self, *, state: Optional[str] = None, sequence_id: Optional[str] = None, page_size: int = 100
    ) -> Iterator[InstanceSummary]:
        """Iterate newest-first by instance ID. ``sequence_id`` is returned by ``deploy``."""
        before = None
        while True:
            page = self._list_instance_page(state, sequence_id, before, page_size)
            before = page[-1]["id"] if page else None
            yield from page
            if len(page) < page_size:
                return

    def signal(self, id: str, signal: str, payload: Any = None) -> None:
        """Send ``pause``/``resume``/``cancel``/``update_context`` or a custom signal."""
        self._inner.signal(id, signal, _to_json(payload))

    def close(self) -> None:
        """Stop the engine and release the database."""
        self._inner.close()

    def _close_draining(self) -> None:
        self._inner.close_draining()

    def __enter__(self) -> "Engine":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


class AsyncEngine:
    """An embedded engine whose operations leave its owning event loop free.

    Async handlers execute on that loop. Synchronous handlers execute on the
    native callback thread. Cancelling an await stops observation of the
    operation. Use a durable workflow signal to change the run itself.
    """

    def __init__(
        self, engine: Engine, loop: asyncio.AbstractEventLoop, shutdown_timeout: float
    ) -> None:
        self._engine = engine
        self._loop = loop
        self._shutdown_timeout = shutdown_timeout
        self._closing: Optional[asyncio.Task[None]] = None

    @classmethod
    async def open(cls, path: str, *, shutdown_timeout: float = 5.0) -> AsyncEngine:
        if not math.isfinite(shutdown_timeout) or shutdown_timeout <= 0:
            raise ValueError("shutdown_timeout must be positive and finite")
        loop = asyncio.get_running_loop()
        engine = await asyncio.to_thread(Engine.open, path)
        return cls(engine, loop, shutdown_timeout)

    @property
    def path(self) -> str:
        return self._engine.path

    def _require_owning_loop(self) -> None:
        if asyncio.get_running_loop() is not self._loop:
            raise RuntimeError("AsyncEngine must be used on its owning event loop")

    async def _call(self, operation, /, *args, **kwargs):
        self._require_owning_loop()
        return await asyncio.to_thread(operation, *args, **kwargs)

    def handler(self, name: str, fn: Optional[Handler] = None):
        def register(func: Handler) -> Handler:
            self._require_owning_loop()
            self._engine._inner.register(name, _wrap(func, self._loop))
            return func

        return register if fn is None else register(fn)

    async def deploy(self, sequence: Union[Mapping[str, Any], str]) -> str:
        return await self._call(self._engine.deploy, sequence)

    async def start(
        self,
        name: str,
        input: Optional[Mapping[str, Any]] = None,
        *,
        idempotency_key: Optional[str] = None,
        version: Optional[int] = None,
    ) -> str:
        return await self._call(
            self._engine.start, name, input, idempotency_key=idempotency_key, version=version
        )

    async def start_batch(self, items: Sequence[BatchStartItem]) -> list[str]:
        return await self._call(self._engine.start_batch, items)

    async def run(
        self, id: str, *, timeout: Optional[float] = None, observer_timeout: Optional[float] = None
    ) -> InstanceSnapshot:
        return await self._call(
            self._engine.run, id, timeout=timeout, observer_timeout=observer_timeout
        )

    async def get(self, id: str) -> InstanceSnapshot:
        return await self._call(self._engine.get, id)

    async def get_output(self, id: str, block: str, attempt: int) -> Optional[BlockOutput]:
        return await self._call(self._engine.get_output, id, block, attempt)

    async def iter_instances(
        self, *, state: Optional[str] = None, sequence_id: Optional[str] = None, page_size: int = 100
    ) -> AsyncIterator[InstanceSummary]:
        """Iterate newest-first without blocking the application's event loop."""
        before = None
        while True:
            page = await self._call(self._engine._list_instance_page, state, sequence_id, before, page_size)
            before = page[-1]["id"] if page else None
            for instance in page:
                yield instance
            if len(page) < page_size:
                return

    async def signal(self, id: str, signal: str, payload: Any = None) -> None:
        await self._call(self._engine.signal, id, signal, payload)

    async def close(self) -> None:
        """Stop scheduling and await outstanding host handlers before releasing SQLite."""
        self._require_owning_loop()
        if self._closing is None:
            self._closing = self._loop.create_task(asyncio.to_thread(self._engine._close_draining))
        await asyncio.shield(self._closing)

    async def __aenter__(self) -> AsyncEngine:
        self._require_owning_loop()
        return self

    async def __aexit__(self, *exc: object) -> None:
        # A timed-out observer leaves the owned close in progress. Explicit
        # close can subsequently join the same native retirement.
        await asyncio.wait_for(self.close(), self._shutdown_timeout)
