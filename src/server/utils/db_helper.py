from motor.motor_asyncio import AsyncIOMotorClient
from beanie import init_beanie
from server.model.work_order import WorkOrder  # 导入模型

RAW_COLLECTION = "raw_msg"
OPT_COLLECTION = "optimize_msg"

client: AsyncIOMotorClient | None = None


async def init_db(uri: str = "mongodb+srv://root:dshvv@one.0brbjhh.mongodb.net"):
    """应用启动时调用（异步）"""
    global client

    client = AsyncIOMotorClient(
        uri,
        maxPoolSize=50,
        minPoolSize=10,
        serverSelectionTimeoutMS=5000
    )

    # 测试连接
    try:
        await client.admin.command('ping')
        print("✅ MongoDB 连接成功")
    except Exception as e:
        print(f"❌ MongoDB 连接失败: {e}")
        raise

    # 初始化 Beanie（关键！注册所有 Document 模型）
    await init_beanie(
        database=client["lark_monitor"],
        document_models=[WorkOrder]
    )
    print("✅ Beanie 初始化完成")


async def close_db():
    """关闭连接"""
    global client
    if client:
        client.close()
        print("MongoDB 连接已关闭")


def get_db():
    """获取数据库实例（需在 init_db 之后调用）"""
    if client is None:
        raise RuntimeError("Database not initialized. Call init_db() first.")
    return client["lark_monitor"]

def get_collections():
    db = get_db()
    return db[RAW_COLLECTION], db[OPT_COLLECTION]