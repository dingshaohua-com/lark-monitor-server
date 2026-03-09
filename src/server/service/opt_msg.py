import json
import re
from datetime import datetime, timezone

from pymongo import UpdateOne

from server.utils.db_helper import get_collection

FIELD_NAME_MAP = {
    "用户原文": "user_content",
    "功能异常": "func_exception",
    "优先级": "priority",
    "客户端": "client_type",
    "模块": "module",
    "内容标签": "content_tag",
    "反馈ID": "feedback_id",
    "分类": "category",
    "一级标签": "tag_l1",
    "二级标签": "tag_l2",
    "三级标签": "tag_l3",
    "学段ID": "education_stage_id",
    "学科ID": "subject_id",
    "课程ID": "course_id",
    "课程版本": "course_version",
    "题集ID": "question_set_id",
    "题集版本": "question_set_version",
    "题目ID": "question_id",
    "题目版本": "question_version",
    "知识点ID": "knowledge_id",
    "词书ID": "wordbook_id",
    "单词组ID": "word_group_id",
    "单词ID": "word_id",
    "组件ID": "component_id",
    "组件版本": "component_version",
    "组件索引": "component_index",
    "版本号": "app_version",
    "设备型号": "device_model",
    "来源学校ID": "school_id",
    "来源学校名称": "school_name",
    "年级名称": "grade_name",
    "班级名称": "class_name",
    "学科名称": "subject_name",
    "姓名": "student_name",
    "uid": "uid",
    "线上反馈时间": "feedback_time",
    "所属客服": "customer_service",
    "客服备注": "cs_remark",
    "文档ID": "doc_id",
    "播放时间线": "play_timeline",
    "扩展信息": "extra_info",
    "载荷链接": "payload_url",
}


async def sync_msg() -> dict:
    """从 raw_msg 读取原始消息，解析 body 后写入 opt_msg。"""
    raw_col = get_collection("raw_msg")
    opt_col = get_collection("opt_msg")

    cursor = raw_col.find({})
    total = 0
    ops = []

    async for raw in cursor:
        doc = _parse_raw(raw)
        ops.append(UpdateOne({"_id": doc["_id"]}, {"$set": doc}, upsert=True))
        total += 1

        if len(ops) >= 200:
            await opt_col.bulk_write(ops)
            ops = []

    if ops:
        await opt_col.bulk_write(ops)

    await _update_reply_counts(opt_col)

    return {"success": True, "total_processed": total}


def _parse_raw(raw: dict) -> dict:
    """保留原始字段，仅将 body 解析为结构化 content。"""
    doc = {k: v for k, v in raw.items() if k != "body"}
    doc["content"] = _parse_body(raw.get("msg_type", ""), raw.get("body"))
    doc["sync_at"] = datetime.now(timezone.utc).isoformat()
    return doc


def _parse_body(msg_type: str, body: dict | None) -> dict:
    """根据消息类型解析 body，返回结构化 content。"""
    if not body or not body.get("content"):
        return {"type": msg_type, "text": "", "raw": ""}

    raw_str = body["content"]

    try:
        data = json.loads(raw_str)
    except (json.JSONDecodeError, TypeError):
        return {"type": msg_type, "text": raw_str, "raw": raw_str}

    if msg_type == "interactive":
        return _parse_interactive(data, raw_str)

    if msg_type == "text":
        return {"type": "text", "text": data.get("text", ""), "raw": raw_str}

    if msg_type == "post":
        return _parse_post(data, raw_str)

    return {"type": msg_type, "text": "", "raw": raw_str}


def _parse_interactive(data: dict, raw_str: str) -> dict:
    """解析卡片消息：提取 title、全文、以及【key】：value 键值对。"""
    title = data.get("title", "")
    elements = data.get("elements", [])

    text_parts: list[str] = []
    for row in elements:
        if not isinstance(row, list):
            row = [row]
        for node in row:
            if node.get("tag") == "text":
                text_parts.append(node.get("text", ""))
            elif node.get("tag") == "a":
                text_parts.append(node.get("text", ""))

    full_text = "\n".join(text_parts)

    fields = {}
    for m in re.finditer(r"【(.+?)】：(.*?)(?=\n【|$)", full_text, re.DOTALL):
        cn_key = m.group(1)
        en_key = FIELD_NAME_MAP.get(cn_key, cn_key)
        fields[en_key] = m.group(2).strip()

    return {
        "type": "interactive",
        "title": title,
        "text": full_text,
        "fields": fields,
        "raw": raw_str,
    }


def _parse_post(data: dict, raw_str: str) -> dict:
    """解析富文本消息：提取 title 和所有文本节点。"""
    parts: list[str] = []
    title = ""

    for _lang, block in data.items():
        if not isinstance(block, dict):
            continue
        if not title:
            title = block.get("title", "")
        for paragraph in block.get("content", []):
            for node in paragraph:
                tag = node.get("tag")
                if tag in ("text", "a"):
                    parts.append(node.get("text", ""))

    return {
        "type": "post",
        "title": title,
        "text": " ".join(parts).strip(),
        "raw": raw_str,
    }


async def _update_reply_counts(opt_col):
    """聚合回复数并更新到根消息。"""
    pipeline = [
        {"$match": {"root_id": {"$ne": None}}},
        {"$group": {"_id": "$root_id", "count": {"$sum": 1}}},
    ]
    ops = []
    async for doc in opt_col.aggregate(pipeline):
        ops.append(
            UpdateOne(
                {"message_id": doc["_id"]},
                {"$set": {"reply_count": doc["count"]}},
            )
        )
    if ops:
        await opt_col.bulk_write(ops)
