import json
import os
from datetime import date, datetime, timedelta, timezone
import lark_oapi as lark
from lark_oapi.api.im.v1 import ListMessageRequest
from pymongo import UpdateOne
from server.utils.db_helper import get_collection
from server.utils.lark_client import get_lark_client


async def sync(start: date | None = None, end: date | None = None) -> dict:
    """从飞书群拉取原始消息并写入 MongoDB raw_msg 集合。"""
    client = get_lark_client()
    collection = get_collection("raw_msg")
    chat_id = os.environ["MONITOR_CHAT_ID"]

    now = datetime.now(timezone.utc)
    start_dt = datetime.combine(start, datetime.min.time(), tzinfo=timezone.utc) if start else now - timedelta(days=30)
    end_dt = datetime.combine(end, datetime.max.time(), tzinfo=timezone.utc) if end else now

    start_ts = str(int(start_dt.timestamp()))
    end_ts = str(int(end_dt.timestamp()))

    total_fetched = 0
    total_upserted = 0
    page_token: str | None = None

    while True:
        builder = (
            ListMessageRequest.builder()
            .container_id_type("chat")
            .container_id(chat_id)
            .start_time(start_ts)
            .end_time(end_ts)
            .sort_type("ByCreateTimeAsc")
            .page_size(50)
        )
        if page_token:
            builder = builder.page_token(page_token)

        response = await client.im.v1.message.alist(builder.build())

        if not response.success():
            return {
                "success": False,
                "error": f"[{response.code}] {response.msg}",
            }

        items = response.data.items or []
        total_fetched += len(items)

        if items:
            ops = []
            for item in items:
                # doc = _message_to_doc(item)
                doc = json.loads(lark.JSON.marshal(item))
                doc["_id"] = doc.get("message_id")
                ops.append(UpdateOne({"_id": doc["_id"]}, {"$set": doc}, upsert=True))
            result = await collection.bulk_write(ops)
            total_upserted += result.upserted_count + result.modified_count

        if not response.data.has_more:
            break
        page_token = response.data.page_token

    return {
        "success": True,
        "total_fetched": total_fetched,
        "total_upserted": total_upserted,
        "time_range": {
            "start": start_dt.isoformat(),
            "end": end_dt.isoformat(),
        },
    }

