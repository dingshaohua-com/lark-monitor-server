import json
import os
from datetime import date, datetime, timedelta, timezone
import lark_oapi as lark
from lark_oapi.api.im.v1 import ListMessageRequest
from pymongo import UpdateOne
from server.utils.db_helper import get_collection
from server.utils.lark_client import get_lark_client


async def status() -> dict:
    """返回 raw_msg 集合的统计信息。"""
    col = get_collection("raw_msg")
    meta_col = get_collection("sync_meta")
    total = await col.count_documents({})
    main_count = await col.count_documents({"root_id": {"$in": [None, ""]}})
    reply_count = total - main_count

    meta = await meta_col.find_one({"_id": "raw_msg_sync"})
    last_sync_at = meta.get("last_sync_at") if meta else None

    root_filter = {"root_id": {"$in": [None, ""]}}
    earliest = await col.find_one(root_filter, sort=[("create_time", 1)])
    latest = await col.find_one(root_filter, sort=[("create_time", -1)])

    return {
        "total": total,
        "main_count": main_count,
        "reply_count": reply_count,
        "last_sync_at": last_sync_at,
        "earliest_time": earliest.get("create_time") if earliest else None,
        "latest_time": latest.get("create_time") if latest else None,
    }


async def clear_all() -> dict:
    """删除所有原始数据。"""
    raw_col = get_collection("raw_msg")
    opt_col = get_collection("opt_msg")
    raw_result = await raw_col.delete_many({})
    opt_result = await opt_col.delete_many({})
    return {
        "raw_deleted": raw_result.deleted_count,
        "opt_deleted": opt_result.deleted_count,
    }


async def sync(
    mode: str = "continue",
    start: date | None = None,
    end: date | None = None,
) -> dict:
    """从飞书群拉取原始消息（含话题回复）并写入 MongoDB raw_msg 集合。

    mode: continue(从上次续更) / range(指定日期) / full(全量)
    """
    client = get_lark_client()
    collection = get_collection("raw_msg")
    chat_id = os.environ["MONITOR_CHAT_ID"]
    now = datetime.now(timezone.utc)

    start_ts: str | None = None
    end_ts: str | None = None

    if mode == "continue":
        last_doc = await collection.find_one(sort=[("update_time", -1)])
        if last_doc and last_doc.get("update_time"):
            raw_ts = str(last_doc["update_time"])
            ts_val = int(raw_ts) if raw_ts.isdigit() else int(datetime.fromisoformat(raw_ts).timestamp())
            if ts_val > 1e12:
                ts_val = ts_val // 1000
            start_ts = str(ts_val)
            start_dt = datetime.fromtimestamp(ts_val, tz=timezone.utc)
        else:
            start_dt = now - timedelta(days=30)
            start_ts = str(int(start_dt.timestamp()))
        end_dt = now
        end_ts = str(int(now.timestamp()))
    elif mode == "range":
        start_dt = datetime.combine(start, datetime.min.time(), tzinfo=timezone.utc) if start else now - timedelta(days=7)
        end_dt = datetime.combine(end, datetime.max.time(), tzinfo=timezone.utc) if end else now
        start_ts = str(int(start_dt.timestamp()))
        end_ts = str(int(end_dt.timestamp()))
    else:
        start_dt = now
        end_dt = now

    total_fetched = 0
    total_upserted = 0

    # 第一步：拉取群聊主消息
    chat_items, upserted = await _fetch_messages(
        client, collection, "chat", chat_id,
        start_ts=start_ts, end_ts=end_ts,
    )
    total_fetched += len(chat_items)
    total_upserted += upserted

    # 第二步：对有 thread_id 的消息，拉取话题内的回复
    processed_threads: set[str] = set()
    reply_count = 0

    for msg in chat_items:
        thread_id = msg.get("thread_id")
        if not thread_id or thread_id in processed_threads:
            continue
        processed_threads.add(thread_id)
        root_msg_id = msg["message_id"]

        thread_items, upserted = await _fetch_messages(
            client, collection, "thread", thread_id,
            skip_message_id=root_msg_id,
        )
        total_fetched += len(thread_items)
        total_upserted += upserted
        reply_count += len(thread_items)

    meta_col = get_collection("sync_meta")
    sync_at = datetime.now(timezone.utc).isoformat()
    await meta_col.update_one(
        {"_id": "raw_msg_sync"},
        {"$set": {"last_sync_at": sync_at}},
        upsert=True,
    )

    return {
        "success": True,
        "total_fetched": total_fetched,
        "total_upserted": total_upserted,
        "threads_synced": len(processed_threads),
        "reply_count": reply_count,
        "time_range": {
            "start": start_dt.isoformat(),
            "end": end_dt.isoformat(),
        },
    }


async def _fetch_messages(
    client, collection,
    container_type: str, container_id: str,
    start_ts: str | None = None, end_ts: str | None = None,
    skip_message_id: str | None = None,
) -> tuple[list[dict], int]:
    """拉取一个容器（chat 或 thread）内的所有消息并写入数据库。"""
    all_items: list[dict] = []
    total_upserted = 0
    page_token: str | None = None

    while True:
        builder = (
            ListMessageRequest.builder()
            .container_id_type(container_type)
            .container_id(container_id)
            .page_size(50)
        )
        if start_ts:
            builder = builder.start_time(start_ts)
        if end_ts:
            builder = builder.end_time(end_ts)
        if page_token:
            builder = builder.page_token(page_token)

        response = await client.im.v1.message.alist(builder.build())

        if not response.success():
            break

        items = response.data.items or []

        if items:
            ops = []
            for item in items:
                doc = json.loads(lark.JSON.marshal(item))
                if skip_message_id and doc.get("message_id") == skip_message_id:
                    continue
                doc["_id"] = doc.get("message_id")
                ops.append(UpdateOne({"_id": doc["_id"]}, {"$set": doc}, upsert=True))
                all_items.append(doc)
            if ops:
                result = await collection.bulk_write(ops)
                total_upserted += result.upserted_count + result.modified_count

        if not response.data.has_more:
            break
        page_token = response.data.page_token

    return all_items, total_upserted
