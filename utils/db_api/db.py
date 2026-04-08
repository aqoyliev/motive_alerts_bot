import asyncpg
from data import config

pool: asyncpg.Pool | None = None


async def init_pool():
    global pool
    pool = await asyncpg.create_pool(config.DATABASE_URL)


async def close_pool():
    global pool
    if pool:
        await pool.close()
        pool = None


async def fetch(query: str, *args):
    async with pool.acquire() as conn:
        return await conn.fetch(query, *args)


async def fetchrow(query: str, *args):
    async with pool.acquire() as conn:
        return await conn.fetchrow(query, *args)


async def fetchval(query: str, *args):
    async with pool.acquire() as conn:
        return await conn.fetchval(query, *args)


async def execute(query: str, *args):
    async with pool.acquire() as conn:
        return await conn.execute(query, *args)
