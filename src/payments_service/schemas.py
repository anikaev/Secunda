from datetime import datetime
from decimal import Decimal
from typing import Literal
from uuid import UUID, uuid5

from pydantic import AnyHttpUrl, BaseModel, ConfigDict, Field, JsonValue, field_validator

from payments_service.db.models import Currency, PaymentStatus


class PaymentCreate(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)

    amount: Decimal = Field(gt=0, max_digits=18, decimal_places=2, allow_inf_nan=False)
    currency: Currency
    description: str = Field(max_length=2000)
    metadata: dict[str, JsonValue] = Field(default_factory=dict)
    webhook_url: AnyHttpUrl = Field(max_length=2048)

    @field_validator("webhook_url")
    @classmethod
    def validate_webhook_url(cls, value: AnyHttpUrl) -> AnyHttpUrl:
        if value.username is not None or value.password is not None or value.fragment is not None:
            raise ValueError("Webhook URL must not contain credentials or a fragment")
        return value


class PaymentAccepted(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    payment_id: UUID = Field(validation_alias="id")
    status: PaymentStatus
    created_at: datetime


class PaymentDetail(PaymentAccepted):
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, JsonValue] = Field(validation_alias="metadata_")
    idempotency_key: str
    webhook_url: str
    processed_at: datetime | None
    webhook_delivered_at: datetime | None


class PaymentEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: UUID
    payment_id: UUID
    attempt: int = Field(default=1, ge=1, le=3, strict=True)
    error_type: str | None = Field(default=None, max_length=100)

    @property
    def retry_id(self) -> UUID:
        # При повторной доставке этой попытки ID записи в outbox должен остаться тем же.
        return uuid5(self.event_id, f"attempt:{self.attempt}")


class WebhookPayload(BaseModel):
    event_id: UUID
    event_type: Literal["payment.processed"] = "payment.processed"
    payment_id: UUID
    amount: Decimal
    currency: Currency
    status: PaymentStatus
    created_at: datetime
    processed_at: datetime
