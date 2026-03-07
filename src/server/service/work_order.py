import re
from typing import Dict, Any, List
from beanie import operators as op  # 提供 $gt, $or 等操作符
from server.model.work_order import WorkOrder  # 你的 Beanie 模型
from server.utils.sync_helper import sync_msg
from server.utils.db_helper import get_collections


async def get_work_order( params ) -> Dict[str, Any]:
    """查询主消息列表"""

    # 1. 基础查询：非回复消息
    query = WorkOrder.find(WorkOrder.is_reply == False)

    # 2. 关键词搜索（正则匹配，忽略大小写）
    if params.keyword:
        # 使用 regex 操作符，类似原来的 $regex
        query = query.find(WorkOrder.content.regex(re.escape(params.keyword), options="i"))

    # 3. 日期范围查询（create_date 是字符串格式 "2026-03-05"）
    if params.start_date and params.end_date:
        query = query.find(
            (WorkOrder.create_date >= params.start_date) &
            (WorkOrder.create_date <= params.end_date)
        )
    elif params.start_date:
        query = query.find(WorkOrder.create_date >= params.start_date)
    elif params.end_date:
        query = query.find(WorkOrder.create_date <= params.end_date)

    # 4. 消息类型过滤
    if params.msg_type:
        query = query.find(WorkOrder.msg_type == params.msg_type)

    # 5. 发送者类型过滤
    if params.sender_type:
        query = query.find(WorkOrder.sender_type == params.sender_type)

    # 6. 是否有回复的复杂逻辑（$or 条件）
    if params.has_reply == "yes":
        query = query.find(WorkOrder.reply_count > 0)  # $gt: 0
    elif params.has_reply == "no":
        # $or: reply_count == 0 OR reply_count 不存在
        # Beanie 原生支持 $or 操作符
        query = query.find(
            op.Or(
                WorkOrder.reply_count == 0,
                WorkOrder.reply_count.exists(False)  # $exists: False
            )
        )

    # 7. 获取总数（在分页前计算）
    total = await query.count()

    # 8. 分页查询（链式调用）
    skip = (params.page - 1) * params.page_size
    messages = await query.sort(-WorkOrder.create_time).skip(skip).limit(params.page_size).to_list()  # 自动返回 List[WorkOrder] 对象


    # 9. 转换为字典（兼容原有返回格式）
    # WorkOrder 对象有 .dict() 方法（继承自 Pydantic）
    items: List[Dict] = [msg.dict(by_alias=True, exclude={"_id"}) for msg in messages]
    # 注意：by_alias=True 保留原始字段名，exclude={"_id"} 类似原来的 {"_id": 0}

    return {
        "items": items,
        "total": total,
        "page": params.page,
        "page_size": params.page_size,
        "total_pages": (total + params.page_size - 1) // params.page_size,
    }


async def get_replies(msg_id: str) -> List[Dict]:
    """获取某条主消息的所有回复，按时间正序"""
    collection, optimize_collection = get_collections()
    cursor = (
        optimize_collection
        .find({"parent_id": msg_id}, {"_id": 0})
        .sort("create_time", 1)
    )
    return await _serialize(cursor)


async def _serialize(cursor) -> List[Dict]:
    items: List[Dict] = []
    async for doc in cursor:
        if "sync_at" in doc:
            doc["sync_at"] = doc["sync_at"].isoformat()
        items.append(doc)
    return items



async def sync_work_order(params):
    return await sync_msg(start=params.start, end=params.end, optimize=params.optimize)