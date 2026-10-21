"""Управляемый HTTP-получатель webhook для изолированной сети Compose."""

import asyncio
from dataclasses import dataclass, field

from fastapi import FastAPI, Request, Response
from pydantic import BaseModel, Field

app = FastAPI()


class Behavior(BaseModel):
    statuses: list[int] = Field(default_factory=lambda: [204], min_length=1)
    hold: bool = False


@dataclass
class Hook:
    behavior: Behavior
    requests: list[dict] = field(default_factory=list)
    release: asyncio.Event = field(default_factory=asyncio.Event)


hooks: dict[str, Hook] = {}


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}


@app.put("/control/{token}")
async def configure(token: str, behavior: Behavior) -> dict:
    hooks[token] = Hook(behavior)
    return {"status": "ok"}


@app.post("/control/{token}/release")
async def release(token: str) -> dict:
    hooks[token].release.set()
    return {"status": "ok"}


@app.get("/control/{token}")
async def received(token: str) -> list[dict]:
    return hooks[token].requests


@app.post("/hooks/{token}")
async def webhook(token: str, request: Request) -> Response:
    hook = hooks.setdefault(token, Hook(Behavior()))
    index = len(hook.requests)
    hook.requests.append(
        {
            "payload": await request.json(),
            "event_id": request.headers.get("X-Webhook-Id"),
            "idempotency_key": request.headers.get("Idempotency-Key"),
            "time": asyncio.get_running_loop().time(),
        }
    )
    if hook.behavior.hold:
        await hook.release.wait()
    return Response(status_code=hook.behavior.statuses[min(index, len(hook.behavior.statuses) - 1)])
