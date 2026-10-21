import asyncio
from dataclasses import dataclass, field

from fastapi import FastAPI, Request, Response
from pydantic import BaseModel, Field

app = FastAPI()


class WebhookSettings(BaseModel):
    statuses: list[int] = Field(default_factory=lambda: [204], min_length=1)
    hold: bool = False


@dataclass
class Webhook:
    settings: WebhookSettings
    requests: list[dict] = field(default_factory=list)
    release_event: asyncio.Event = field(default_factory=asyncio.Event)


webhooks: dict[str, Webhook] = {}


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}


@app.put("/control/{webhook_id}")
async def configure_webhook(webhook_id: str, settings: WebhookSettings) -> dict:
    webhooks[webhook_id] = Webhook(settings)
    return {"status": "ok"}


@app.post("/control/{webhook_id}/release")
async def release_webhook(webhook_id: str) -> dict:
    webhooks[webhook_id].release_event.set()
    return {"status": "ok"}


@app.get("/control/{webhook_id}")
async def get_requests(webhook_id: str) -> list[dict]:
    return webhooks[webhook_id].requests


@app.post("/hooks/{webhook_id}")
async def receive_webhook(webhook_id: str, request: Request) -> Response:
    webhook = webhooks.get(webhook_id)
    if webhook is None:
        webhook = Webhook(WebhookSettings())
        webhooks[webhook_id] = webhook

    attempt = len(webhook.requests)
    webhook.requests.append(
        {
            "payload": await request.json(),
            "event_id": request.headers.get("X-Webhook-Id"),
            "idempotency_key": request.headers.get("Idempotency-Key"),
            "time": asyncio.get_running_loop().time(),
        }
    )
    if webhook.settings.hold:
        await webhook.release_event.wait()

    statuses = webhook.settings.statuses
    status_code = statuses[min(attempt, len(statuses) - 1)]
    return Response(status_code=status_code)
