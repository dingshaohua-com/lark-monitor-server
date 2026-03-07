import os
import time
import logging
from datetime import datetime, date
from typing import Dict, Any, Optional, List, Set
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError
from monitor.utils.msg_parser import parse_raw_msg

logger = logging.getLogger(__name__)



def get_chat_messages(params) -> Dict:
    """
    获取群消息列表，返回 data 层级对象（含 items / has_more / page_token）

    Args:
     container_id_type: "chat"
     container_id: 群 ID
     start_time: 起始时间戳(毫秒,字符串)
     end_time: 结束时间戳(毫秒,字符串)
     page_size: 每页消息数量
     page_token: 分页标记
    """
    from monitor.utils.client import lark_client
    res = lark_client.get("/im/v1/messages", params=params)
    body = res.json()
    if body.get("code") != 0:
        raise RuntimeError(f"飞书 API 错误: code={body.get('code')}, msg={body.get('msg')}")
    return body.get("data", {})



