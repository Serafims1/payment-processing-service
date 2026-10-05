import os
from collections.abc import AsyncIterator

import pytest
from pydantic import SecretStr
from sqlalchemy import text

from payments.config import Settings
from payments.database import Database
from payments.repositories import PaymentRepository
from payments.schemas import PaymentCreate


@pytest.fixture
def settings() -> Settings:
    return Settings(
        api_key=SecretStr("test-api-key"),
        database_url=SecretStr(os.getenv("TEST_DATABASE_URL", "postgresql+asyncpg://unused")),
        rabbitmq_url=SecretStr(os.getenv("TEST_RABBITMQ_URL", "amqp://unused")),
        retry_base=0.001,
    )


@pytest.fixture
def payment_request() -> PaymentCreate:
    return PaymentCreate(
        amount="12.30",
        currency="RUB",
        description="Order",
        metadata={"order": 1},
        webhook_url="http://localhost:9999/hook",
    )


@pytest.fixture
async def db(settings: Settings) -> AsyncIterator[Database]:
    if not os.getenv("TEST_DATABASE_URL"):
        pytest.skip("Set TEST_DATABASE_URL to a dedicated migrated test database")
    database = Database(settings)
    async with database.sessions.begin() as session:
        await session.execute(text("TRUNCATE outbox, payments"))
    try:
        yield database
    finally:
        await database.close()


@pytest.fixture
def repository(db: Database) -> PaymentRepository:
    return PaymentRepository(db.sessions)
