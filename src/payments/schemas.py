from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, JsonValue, field_validator

Currency = Literal["RUB", "USD", "EUR"]
Status = Literal["pending", "succeeded", "failed"]


def validate_storage_text(value: JsonValue) -> None:
    if isinstance(value, str):
        if "\x00" in value:
            raise ValueError("NUL characters cannot be stored in PostgreSQL")
        value.encode("utf-8")
    elif isinstance(value, list):
        for item in value:
            validate_storage_text(item)
    elif isinstance(value, dict):
        for key, item in value.items():
            validate_storage_text(key)
            validate_storage_text(item)


class PaymentCreate(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)
    amount: Annotated[Decimal, Field(gt=0, max_digits=20, decimal_places=2, allow_inf_nan=False)]
    currency: Currency
    description: str = Field(max_length=1000)
    metadata: dict[str, JsonValue] = Field(default_factory=dict)
    webhook_url: HttpUrl = Field(max_length=2048)

    @field_validator("amount", mode="before")
    @classmethod
    def reject_float(cls, value: Any) -> Any:
        if isinstance(value, (float, bool)):
            raise ValueError("Send amount as a decimal string or integer")
        return value

    @field_validator("description", "metadata")
    @classmethod
    def check_storage_text(cls, value: str | dict[str, JsonValue]) -> str | dict[str, JsonValue]:
        validate_storage_text(value)
        return value

    def canonical_payload(self) -> dict[str, Any]:
        payload = self.model_dump(mode="json")
        payload["amount"] = format(self.amount.quantize(Decimal("0.01")), "f")
        return payload


class PaymentAccepted(BaseModel):
    payment_id: UUID
    status: Status
    created_at: datetime


class PaymentDetail(PaymentAccepted):
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any]
    webhook_url: HttpUrl
    processed_at: datetime | None
