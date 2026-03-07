
import os
import time
import asyncio
import logging
from datetime import datetime, date
from typing import Dict, Any, Optional, List, Set
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError

from monitor.service.msg import get_chat_messages
from monitor.utils.msg_parser import parse_raw_msg
from server.utils.db_helper import get_collections

logger = logging.getLogger(__name__)


async def sync_msg(
    start: Optional[date] = None,
    end: Optional[date] = None,
    optimize: bool = False,
) -> Dict[str, Any]:
    """
    从飞书拉取消息并写入数据库
    - start/end 都传：按日期范围同步
    - 都不传：全量同步（不限时间）
    - optimize=True：同时生成优化数据写入 optimize_msg
    - 自动拉取有 thread_id 的消息的所有回复
    """
    collection, optimize_collection = get_collections()

    chat_id = os.getenv('MONITOR_CHAT_ID')
    if not chat_id:
        raise ValueError("MONITOR_CHAT_ID 未配置")

    start_ts = str(int(datetime.combine(start, datetime.min.time()).timestamp())) if start else None
    end_ts = str(int(datetime.combine(end, datetime.max.time()).timestamp())) if end else None

    mode = f"{start} -> {end}" if start and end else "全量"
    logger.info(f"[{chat_id}] 同步模式: {mode}, optimize={optimize}")

    clock = time.time()
    inserted, updated = 0, 0
    opt_inserted, opt_updated = 0, 0
    raw_buffer: List[dict] = []
    opt_buffer: List[dict] = []
    processed_threads: Set[str] = set()

    async def flush_buffers():
        nonlocal inserted, updated, opt_inserted, opt_updated
        if raw_buffer:
            i, u = await _bulk_upsert(collection, raw_buffer)
            inserted += i
            updated += u
            raw_buffer.clear()
        if optimize and opt_buffer:
            oi, ou = await _bulk_upsert(optimize_collection, opt_buffer)
            opt_inserted += oi
            opt_updated += ou
            opt_buffer.clear()

    async def enqueue(msg: dict):
        raw_doc = {
            **msg,
            "chat_id": chat_id,
            "sync_at": datetime.now(),
            "_id": f"{chat_id}_{msg['message_id']}",
        }
        raw_buffer.append(raw_doc)
        if optimize:
            opt_buffer.append(parse_raw_msg(msg, chat_id))
        if len(raw_buffer) >= 100:
            await flush_buffers()

    # ---- 第一步：拉取主消息 ----
    page_token = None
    main_messages: List[dict] = []

    while True:
        params: Dict[str, Any] = {
            "container_id_type": "chat",
            "container_id": chat_id,
            "page_size": 50,
        }
        if start_ts:
            params["start_time"] = start_ts
        if end_ts:
            params["end_time"] = end_ts
        if page_token:
            params["page_token"] = page_token

        data = await asyncio.to_thread(_fetch_with_retry, params)

        messages = data.get("items", [])
        if not messages:
            break

        for msg in messages:
            await enqueue(msg)
            main_messages.append(msg)

        if not data.get("has_more"):
            break
        page_token = data.get("page_token")
        await asyncio.sleep(0.05)

    # ---- 第二步：拉取话题回复 ----
    reply_count = 0
    for msg in main_messages:
        thread_id = msg.get("thread_id")
        if not thread_id or thread_id in processed_threads:
            continue
        processed_threads.add(thread_id)
        root_msg_id = msg["message_id"]

        logger.debug(f"拉取话题 {thread_id} 的回复...")
        tp = None
        while True:
            tp_params: Dict[str, Any] = {
                "container_id_type": "thread",
                "container_id": thread_id,
                "page_size": 50,
            }
            if tp:
                tp_params["page_token"] = tp

            try:
                td = await asyncio.to_thread(_fetch_with_retry, tp_params)
            except Exception as e:
                logger.warning(f"拉取话题 {thread_id} 失败: {e}")
                break

            for reply in td.get("items", []):
                if reply.get("message_id") == root_msg_id:
                    continue
                reply["chat_id"] = chat_id
                reply["thread_id"] = thread_id
                if not reply.get("parent_id"):
                    reply["parent_id"] = root_msg_id
                await enqueue(reply)
                reply_count += 1

            if not td.get("has_more"):
                break
            tp = td.get("page_token")
            await asyncio.sleep(0.05)

    await flush_buffers()

    if optimize:
        await _update_reply_counts(optimize_collection)

    duration = round(time.time() - clock, 2)
    sync_at = datetime.now().isoformat()
    logger.info(
        f"[{chat_id}] 同步完成: raw +{inserted}, 回复 {reply_count} 条, "
        f"optimize +{opt_inserted}, 耗时{duration}s"
    )

    result: Dict[str, Any] = {
        "inserted": inserted,
        "updated": updated,
        "replies": reply_count,
        "duration": duration,
        "sync_at": sync_at,
    }
    if optimize:
        result["optimize_inserted"] = opt_inserted
        result["optimize_updated"] = opt_updated

    return result


async def rebuild_optimize() -> Dict[str, Any]:
    """从 raw_msg 全量重建 optimize_msg（不请求飞书）"""
    collection, optimize_collection = get_collections()

    clock = time.time()
    inserted, updated = 0, 0
    buffer = []

    async for raw in collection.find():
        chat_id = raw.get("chat_id", "")
        if not chat_id or "message_id" not in raw:
            continue
        buffer.append(parse_raw_msg(raw, chat_id))

        if len(buffer) >= 200:
            i, u = await _bulk_upsert(optimize_collection, buffer)
            inserted += i
            updated += u
            buffer.clear()

    if buffer:
        i, u = await _bulk_upsert(optimize_collection, buffer)
        inserted += i
        updated += u

    await _update_reply_counts(optimize_collection)

    duration = round(time.time() - clock, 2)
    logger.info(f"optimize 重建完成: +{inserted}, 更新{updated}, 耗时{duration}s")
    return {"optimize_inserted": inserted, "optimize_updated": updated, "duration": duration}


async def get_sync_status() -> Dict[str, Any]:
    """获取同步状态：最后同步时间、各表消息数"""
    collection, optimize_collection = get_collections()
    raw_total = await collection.count_documents({})
    opt_total = await optimize_collection.count_documents({})
    opt_main = await optimize_collection.count_documents({"is_reply": False})
    opt_replies = opt_total - opt_main
    last_doc = await collection.find_one(sort=[("sync_at", -1)])
    last_sync_at = last_doc["sync_at"].isoformat() if last_doc and last_doc.get("sync_at") else None
    return {
        "raw_total": raw_total,
        "optimize_total": opt_total,
        "optimize_main": opt_main,
        "optimize_replies": opt_replies,
        "last_sync_at": last_sync_at,
    }


def _fetch_with_retry(params: dict, max_retries: int = 3):
    for attempt in range(max_retries):
        try:
            return get_chat_messages(params)
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            logger.warning(f"请求失败，第{attempt + 1}次重试: {e}")
            time.sleep(2 ** attempt)
    return {"items": []}


async def _bulk_upsert(col, docs: list) -> tuple:
    if not docs:
        return 0, 0

    operations = [
        UpdateOne({"_id": doc["_id"]}, {"$setOnInsert": doc}, upsert=True)
        for doc in docs
    ]

    try:
        result = await col.bulk_write(operations, ordered=False)
        return result.upserted_count, result.modified_count
    except BulkWriteError as bwe:
        logger.warning(f"BulkWrite 部分失败: {bwe.details}")
        written = bwe.details.get("nInserted", 0) + bwe.details.get("nUpserted", 0)
        return written, 0


async def _update_reply_counts(col):
    """聚合回复数并写入主消息的 reply_count 字段"""
    pipeline = [
        {"$match": {"is_reply": True, "parent_id": {"$ne": None}}},
        {"$group": {"_id": "$parent_id", "count": {"$sum": 1}}},
    ]
    counts = {doc["_id"]: doc["count"] async for doc in col.aggregate(pipeline)}
    if not counts:
        return

    await col.update_many({"is_reply": False}, {"$set": {"reply_count": 0}})

    ops = [
        UpdateOne({"message_id": mid}, {"$set": {"reply_count": cnt}})
        for mid, cnt in counts.items()
    ]
    await col.bulk_write(ops, ordered=False)
    logger.info(f"reply_count 已更新: {len(counts)} 条主消息")


