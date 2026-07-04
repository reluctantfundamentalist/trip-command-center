"""
Database connection management using SQLAlchemy async engine.
"""

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from config.settings import settings

engine = create_async_engine(
    settings.db.url,
    pool_size=settings.db.pool_max_size,
    pool_pre_ping=True,
    echo=settings.db.echo,
)

async_session_factory = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


async def get_session() -> AsyncSession:  # type: ignore[misc]
    """Dependency injection for FastAPI endpoints."""
    async with async_session_factory() as session:
        yield session
