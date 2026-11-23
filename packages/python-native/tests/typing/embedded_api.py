"""Check with MYPYPATH=python mypy --strict tests/typing/embedded_api.py."""

from typing import Any

from orch8_engine import AsyncEngine, Engine, HandlerInput, InstanceSnapshot


def typed_handler(context: HandlerInput) -> dict[str, int]:
    attempt: int = context["attempt"]
    return {"attempt": attempt}


def legacy_handler(context: dict[str, Any]) -> str:
    return str(context["step_id"])


def configure(engine: Engine, run_id: str) -> InstanceSnapshot:
    engine.handler("typed", typed_handler)
    engine.handler("legacy", legacy_handler)
    snapshot = engine.get(run_id)
    state: str = snapshot["state"]
    assert state
    return snapshot


async def configure_async(engine: AsyncEngine, run_id: str) -> InstanceSnapshot:
    @engine.handler("async")
    async def handler(context: HandlerInput) -> int:
        return context["attempt"]

    return await engine.get(run_id)


def misspelled_input(context: HandlerInput) -> object:
    return context["instnace_id"]  # type: ignore[typeddict-item]
