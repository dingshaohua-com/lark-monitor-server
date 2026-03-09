import json
import re

from server.utils import dict_registry

FIELD_NAME_MAP = dict_registry.get("field_name_map")


def parse_raw(raw: dict) -> dict:
    """保留原始字段，仅将 body 解析为结构化 content。"""
    doc = {k: v for k, v in raw.items() if k != "body"}
    doc["content"] = parse_body(raw.get("msg_type", ""), raw.get("body"))
    return doc


def parse_body(msg_type: str, body: dict | None) -> dict:
    """根据消息类型解析 body，返回结构化 content。"""
    if not body or not body.get("content"):
        return {"text": "", "raw": ""}

    raw_str = body["content"]

    try:
        data = json.loads(raw_str)
    except (json.JSONDecodeError, TypeError):
        return {"text": raw_str, "raw": raw_str}

    if msg_type == "interactive":
        return _parse_interactive(data, raw_str)

    if msg_type == "text":
        return {"text": data.get("text", ""), "raw": raw_str}

    if msg_type == "post":
        return _parse_post(data, raw_str)

    return {"text": "", "raw": raw_str}


def _parse_interactive(data: dict, raw_str: str) -> dict:
    """解析卡片消息：提取 title、全文、以及【key】：value 键值对。"""
    title = data.get("title", "")
    elements = data.get("elements", [])

    text_parts: list[str] = []
    for row in elements:
        if not isinstance(row, list):
            row = [row]
        row_text = ""
        row_href = ""
        has_content_text = False
        for node in row:
            tag = node.get("tag")
            if tag in ("text", "a"):
                node_text = node.get("text", "")
                row_text += node_text
                if tag == "a" and node.get("href"):
                    row_href = node["href"]
                if tag == "text" and "【" in node_text:
                    has_content_text = True
        if not row_text:
            continue
        if row_href and not has_content_text:
            text_parts.append(f"【{row_text}】：{row_href}")
        else:
            text_parts.append(row_text)

    full_text = "\n".join(text_parts)

    fields = {}
    for m in re.finditer(r"【(.+?)】：(.*?)(?=\n【|$)", full_text, re.DOTALL):
        cn_key = m.group(1)
        en_key = FIELD_NAME_MAP.get(cn_key, cn_key)
        fields[en_key] = m.group(2).strip()

    return {
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
        "title": title,
        "text": " ".join(parts).strip(),
        "raw": raw_str,
    }
