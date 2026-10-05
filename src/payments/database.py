from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from payments.config import Settings


class Database:
    def __init__(self, settings: Settings) -> None:
        self.engine = create_async_engine(
            settings.database_url.get_secret_value(), pool_pre_ping=True, hide_parameters=True
        )
        self.sessions = async_sessionmaker(self.engine, expire_on_commit=False, class_=AsyncSession)

    async def close(self) -> None:
        await self.engine.dispose()
