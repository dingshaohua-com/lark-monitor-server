from pymongo import UpdateOne
from server.utils.db_helper import get_collection
from server.utils.msg_parser import parse_raw


async def sync_msg() -> dict:
    """从 raw_msg 读取原始消息，解析 body 后写入 opt_msg。"""
    raw_col = get_collection("raw_msg")
    opt_col = get_collection("opt_msg")

    cursor = raw_col.find({})
    total = 0
    ops = []

    async for raw in cursor:
        doc = parse_raw(raw)
        ops.append(UpdateOne({"_id": doc["_id"]}, {"$set": doc}, upsert=True))
        total += 1

        if len(ops) >= 200:
            await opt_col.bulk_write(ops)
            ops = []

    if ops:
        await opt_col.bulk_write(ops)

    return {"success": True, "total_processed": total}
