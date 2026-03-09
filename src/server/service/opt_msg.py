from pymongo import UpdateOne
from server.utils.db_helper import get_collection
from server.utils.msg_parser import parse_raw


async def sync_msg() -> dict:
    """从 raw_msg 全量重建 opt_msg（先清空再写入）。"""
    raw_col = get_collection("raw_msg")
    opt_col = get_collection("opt_msg")

    await opt_col.delete_many({})

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


async def get_work_order(
    page: int = 1,
    page_size: int = 20,
    keyword: str | None = None,
    priority: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict:
    """查询工单列表：根消息 + 关联的全部回复内容。"""
    col = get_collection("opt_msg")
    skip = (page - 1) * page_size

    match_filter: dict = {"root_id": {"$in": [None, ""]}}

    if keyword:
        match_filter["$or"] = [
            {"content.fields.user_content": {"$regex": keyword, "$options": "i"}},
            {"content.fields.cs_remark": {"$regex": keyword, "$options": "i"}},
            {"content.text": {"$regex": keyword, "$options": "i"}},
        ]
    if priority:
        match_filter["content.fields.priority"] = priority
    if start_date:
        match_filter.setdefault("content.fields.feedback_time", {})["$gte"] = start_date
    if end_date:
        match_filter.setdefault("content.fields.feedback_time", {})["$lte"] = end_date + " 23:59:59"

    pipeline = [
        {"$match": match_filter},
        {"$sort": {"create_time": -1}},
        {"$skip": skip},
        {"$limit": page_size},
        {
            "$lookup": {
                "from": "opt_msg",
                "localField": "message_id",
                "foreignField": "root_id",
                "as": "replies",
            }
        },
        {
            "$addFields": {
                "reply_count": {"$size": "$replies"},
                "replies": {
                    "$sortArray": {"input": "$replies", "sortBy": {"create_time": 1}}
                },
            }
        },
    ]

    total = await col.count_documents(match_filter)
    items = await col.aggregate(pipeline).to_list(length=page_size)

    for item in items:
        item.pop("_id", None)
        for reply in item.get("replies", []):
            reply.pop("_id", None)

    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "items": items,
    }
