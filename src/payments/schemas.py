from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator

Currency = Literal["RUB", "USD", "EUR"]
Status = Literal["pending", "succeeded", "failed"]


class PaymentCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    amount: Annotated[Decimal, Field(gt=0, max_digits=20, decimal_places=2, allow_inf_nan=False)]
    currency: Currency
    description: str = Field(max_length=1000)
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: HttpUrl = Field(max_length=2048)

    @field_validator("amount", mode="before")
    @classmethod
    def reject_float(cls, value: Any) -> Any:
        if isinstance(value, (float, bool)):
            raise ValueError("Send amount as a decimal string or integer")
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
