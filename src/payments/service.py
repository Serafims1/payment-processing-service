from uuid import UUID

from payments.models import Payment
from payments.repositories import PaymentRepository
from payments.schemas import PaymentCreate


class PaymentService:
    def __init__(self, repository: PaymentRepository) -> None:
        self.repository = repository

    async def create(self, key: str, request: PaymentCreate) -> Payment:
        return await self.repository.create(key, request)

    async def get(self, payment_id: UUID) -> Payment:
        return await self.repository.get(payment_id)
