from datetime import datetime, timedelta

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
    has_bot_reply: str | None = None,
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

    lookup_stage = {
        "$lookup": {
            "from": "opt_msg",
            "localField": "message_id",
            "foreignField": "root_id",
            "as": "replies",
        }
    }
    add_fields_stage = {
        "$addFields": {
            "reply_count": {"$size": "$replies"},
            "has_bot_reply": {
                "$gt": [
                    {"$size": {"$filter": {
                        "input": "$replies",
                        "cond": {"$eq": ["$$this.sender.sender_type", "app"]},
                    }}},
                    0,
                ]
            },
            "replies": {
                "$sortArray": {"input": "$replies", "sortBy": {"create_time": 1}}
            },
        }
    }

    if has_bot_reply:
        bot_filter = {"has_bot_reply": True} if has_bot_reply == "yes" else {"has_bot_reply": False}
        pipeline = [
            {"$match": match_filter},
            lookup_stage,
            add_fields_stage,
            {"$match": bot_filter},
            {"$sort": {"content.fields.feedback_time": -1}},
            {"$skip": skip},
            {"$limit": page_size},
        ]
        count_pipeline = [
            {"$match": match_filter},
            lookup_stage,
            add_fields_stage,
            {"$match": bot_filter},
            {"$count": "total"},
        ]
        count_result = await col.aggregate(count_pipeline).to_list(length=1)
        total = count_result[0]["total"] if count_result else 0
    else:
        pipeline = [
            {"$match": match_filter},
            {"$sort": {"content.fields.feedback_time": -1}},
            {"$skip": skip},
            {"$limit": page_size},
            lookup_stage,
            add_fields_stage,
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



async def _count_bot_stats(col, start: str, end: str) -> dict:
    """统计指定日期范围内工单总数和机器人参与数。"""
    match_filter: dict = {
        "root_id": {"$in": [None, ""]},
        "content.fields.feedback_time": {"$gte": start, "$lte": end + " 23:59:59"},
    }
    pipeline = [
        {"$match": match_filter},
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
                "has_bot_reply": {
                    "$gt": [
                        {"$size": {"$filter": {
                            "input": "$replies",
                            "cond": {"$eq": ["$$this.sender.sender_type", "app"]},
                        }}},
                        0,
                    ]
                }
            }
        },
        {
            "$group": {
                "_id": None,
                "total": {"$sum": 1},
                "bot_count": {"$sum": {"$cond": ["$has_bot_reply", 1, 0]}},
            }
        },
    ]
    result = await col.aggregate(pipeline).to_list(length=1)
    if not result:
        return {"total": 0, "bot_count": 0, "bot_ratio": 0}
    r = result[0]
    total = r["total"]
    bot_count = r["bot_count"]
    return {
        "total": total,
        "bot_count": bot_count,
        "bot_ratio": round(bot_count / total * 100, 1) if total else 0,
    }


async def _count_bot_stats_dedup(col, start: str, end: str) -> dict:
    """带去重的统计：获取文档后基于内容相似度去重。"""
    from server.utils.dedup import deduplicate_docs

    match_filter: dict = {
        "root_id": {"$in": [None, ""]},
        "content.fields.feedback_time": {"$gte": start, "$lte": end + " 23:59:59"},
    }
    pipeline = [
        {"$match": match_filter},
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
                "has_bot_reply": {
                    "$gt": [
                        {"$size": {"$filter": {
                            "input": "$replies",
                            "cond": {"$eq": ["$$this.sender.sender_type", "app"]},
                        }}},
                        0,
                    ]
                }
            }
        },
        {"$project": {"content": 1, "has_bot_reply": 1}},
    ]

    docs = await col.aggregate(pipeline).to_list(length=None)

    if not docs:
        return {"total": 0, "bot_count": 0, "bot_ratio": 0, "original_total": 0}

    original_total = len(docs)
    groups = deduplicate_docs(docs)
    total = len(groups)
    bot_count = sum(
        1 for group in groups
        if any(docs[i].get("has_bot_reply", False) for i in group)
    )

    return {
        "total": total,
        "bot_count": bot_count,
        "bot_ratio": round(bot_count / total * 100, 1) if total else 0,
        "original_total": original_total,
    }


async def analyze(start_date: str, end_date: str, deduplicate: bool = False) -> dict:
    """分析工单数据：机器人参与占比 + 与两周前对比。"""
    col = get_collection("opt_msg")
    count_fn = _count_bot_stats_dedup if deduplicate else _count_bot_stats

    current = await count_fn(col, start_date, end_date)

    fmt = "%Y-%m-%d"
    prev_start = (datetime.strptime(start_date, fmt) - timedelta(weeks=2)).strftime(fmt)
    prev_end = (datetime.strptime(end_date, fmt) - timedelta(weeks=2)).strftime(fmt)
    previous = await count_fn(col, prev_start, prev_end)

    ratio_change = round(current["bot_ratio"] - previous["bot_ratio"], 1)

    return {
        "current": {**current, "start_date": start_date, "end_date": end_date},
        "previous": {**previous, "start_date": prev_start, "end_date": prev_end},
        "ratio_change": ratio_change,
    }