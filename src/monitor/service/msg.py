
import os
import time
from datetime import datetime, date
from typing import Dict, Any, Optional, List, Set

def sync_msg(
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
    from web_server.utils.db_helper import collection, optimize_collection
    import msg_monitor.service.index as msg_monitor_service

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

    def flush_buffers():
        nonlocal inserted, updated, opt_inserted, opt_updated
        if raw_buffer:
            i, u = _bulk_upsert(collection, raw_buffer)
            inserted += i
            updated += u
            raw_buffer.clear()
        if optimize and opt_buffer:
            oi, ou = _bulk_upsert(optimize_collection, opt_buffer)
            opt_inserted += oi
            opt_updated += ou
            opt_buffer.clear()

    def enqueue(msg: dict):
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
            flush_buffers()

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

        data = _fetch_with_retry(msg_monitor_service, params)

        messages = data.get("items", [])
        if not messages:
            break

        for msg in messages:
            enqueue(msg)
            main_messages.append(msg)

        if not data.get("has_more"):
            break
        page_token = data.get("page_token")
        time.sleep(0.05)

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
                td = _fetch_with_retry(msg_monitor_service, tp_params)
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
                enqueue(reply)
                reply_count += 1

            if not td.get("has_more"):
                break
            tp = td.get("page_token")
            time.sleep(0.05)

    flush_buffers()

    if optimize:
        _update_reply_counts(optimize_collection)

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