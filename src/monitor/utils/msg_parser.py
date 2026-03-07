"""
将飞书原始消息转换为结构化的优化文档，适合查询和展示
"""
import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


def parse_raw_msg(msg: dict, chat_id: str) -> Dict[str, Any]:
    """
    将 raw_msg 中的一条飞书原始消息转换为 optimize_msg 文档

    optimize_msg schema:
      _id, chat_id, message_id,
      sender_type, sender_id,
      msg_type, content,
      thread_id, parent_id, is_reply,
      create_time (ms), create_date (YYYY-MM-DD), create_time_str (HH:MM:SS),
      sync_at
    """
    ts_ms = int(msg.get("create_time", 0))
    dt = datetime.fromtimestamp(ts_ms / 1000) if ts_ms else None

    sender = msg.get("sender", {})
    msg_type = msg.get("msg_type", "unknown")
    body = msg.get("body", {})

    return {
        "_id": f"{chat_id}_{msg['message_id']}",
        "chat_id": chat_id,
        "message_id": msg["message_id"],
        "sender_type": sender.get("sender_type", "unknown"),
        "sender_id": sender.get("id", ""),
        "msg_type": msg_type,
        "content": _extract_content(msg_type, body),
        "thread_id": msg.get("thread_id") or None,
        "parent_id": msg.get("parent_id") or None,
        "is_reply": bool(msg.get("parent_id")),
        "create_time": ts_ms,
        "create_date": dt.strftime("%Y-%m-%d") if dt else None,
        "create_time_str": dt.strftime("%H:%M:%S") if dt else None,
        "sync_at": datetime.now(),
    }


# --------------- 内容提取 ---------------

def _extract_content(msg_type: str, body: dict) -> str:
    """根据消息类型提取可读文本"""
    try:
        extractor = _EXTRACTORS.get(msg_type, _extract_fallback)
        return extractor(body, msg_type)
    except Exception as e:
        logger.debug(f"内容解析失败 [{msg_type}]: {e}")
        return f"[{msg_type}消息 - 解析失败]"


def _extract_text(body: dict, _: str) -> str:
    raw = body.get("content", "")
    if not raw:
        return ""
    try:
        parsed = json.loads(raw) if isinstance(raw, str) else raw
        return parsed.get("text", raw)
    except (json.JSONDecodeError, AttributeError):
        return raw


def _extract_post(body: dict, _: str) -> str:
    content = body.get("content", {})
    try:
        if isinstance(content, str):
            content = json.loads(content)
    except json.JSONDecodeError:
        return content if isinstance(content, str) else "[富文本解析失败]"

    title = content.get("title", "")
    paragraphs = content.get("content", [])
    lines = []

    for paragraph in paragraphs:
        parts = []
        for el in paragraph:
            tag = el.get("tag")
            if tag == "text":
                parts.append(el.get("text", ""))
            elif tag == "a":
                parts.append(el.get("text", el.get("href", "")))
            elif tag == "at":
                parts.append(f"@{el.get('user_name', '用户')}")
            elif tag == "img":
                parts.append("[图片]")
            elif tag == "media":
                parts.append("[视频/媒体]")
        if parts:
            lines.append("".join(parts))

    result = ""
    if title:
        result += f"{title}\n"
    if lines:
        result += "\n".join(lines)
    return result.strip() or "[空富文本消息]"


def _extract_interactive(body: dict, _: str) -> str:
    raw = body.get("content", "")
    if not raw:
        return "[空交互式消息]"
    try:
        content = json.loads(raw) if isinstance(raw, str) else raw
    except json.JSONDecodeError:
        return raw if isinstance(raw, str) else "[卡片解析失败]"

    title = content.get("title", "")
    elements = content.get("elements", [])
    parts = []

    for group in elements:
        if not isinstance(group, list):
            group = [group]
        for el in group:
            tag = el.get("tag")
            if tag == "text":
                parts.append(el.get("text", ""))
            elif tag == "a":
                parts.append(el.get("text", el.get("href", "")))
            elif tag == "button":
                parts.append(f"[按钮: {el.get('text', '')}]")
            elif tag == "div":
                text_el = el.get("text", {})
                if isinstance(text_el, dict):
                    parts.append(text_el.get("content", ""))
                elif isinstance(text_el, str):
                    parts.append(text_el)
            elif tag == "markdown":
                parts.append(el.get("content", ""))

    result = ""
    if title:
        result += f"{title}\n"
    if parts:
        result += "\n".join(parts)
    return result.strip() or "[空交互式消息]"


def _extract_image(body: dict, _: str) -> str:
    return "[图片]"


def _extract_file(body: dict, _: str) -> str:
    name = body.get("file_name", "")
    return f"[文件: {name}]" if name else "[文件]"


def _extract_audio(body: dict, _: str) -> str:
    return "[语音]"


def _extract_media(body: dict, _: str) -> str:
    return "[视频]"


def _extract_sticker(body: dict, _: str) -> str:
    return "[表情]"


def _extract_share_chat(body: dict, _: str) -> str:
    return "[分享群聊]"


def _extract_share_user(body: dict, _: str) -> str:
    return "[分享用户]"


def _extract_merge_forward(body: dict, _: str) -> str:
    return "[合并转发]"


def _extract_fallback(body: dict, msg_type: str) -> str:
    return f"[{msg_type}消息]"


_EXTRACTORS = {
    "text": _extract_text,
    "post": _extract_post,
    "interactive": _extract_interactive,
    "image": _extract_image,
    "file": _extract_file,
    "audio": _extract_audio,
    "media": _extract_media,
    "sticker": _extract_sticker,
    "share_chat": _extract_share_chat,
    "share_user": _extract_share_user,
    "merge_forward": _extract_merge_forward,
}
