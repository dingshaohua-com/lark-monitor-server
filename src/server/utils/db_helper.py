import os

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

_client: AsyncIOMotorClient | None = None
_db: AsyncIOMotorDatabase | None = None


async def init_db():
    global _client, _db
    uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    _client = AsyncIOMotorClient(uri)
    _db = _client.get_database("lark_monitor")


async def close_db():
    global _client, _db
    if _client:
        _client.close()
        _client = None
        _db = None


def get_collection(name: str):
    if _db is None:
        raise RuntimeError("Database not initialized")
    return _db[name]
