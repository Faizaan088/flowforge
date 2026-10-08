import os
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import declarative_base

DB_URL = os.getenv("DATABASE_URL")

if not DB_URL:
    DB_URL = "postgresql+asyncpg://flowforge_user:flowforge_password@127.0.0.1:5432/flowforge"

DB_ECHO = os.getenv("DB_ECHO", "false").lower() in ("true", "1", "yes")

engine = create_async_engine(DB_URL, echo=DB_ECHO)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)
Base = declarative_base()

async def get_db():
    async with AsyncSessionLocal() as session:
        yield session