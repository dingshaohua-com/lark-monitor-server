from beanie import Document, Indexed
from pydantic import Field
from datetime import datetime
from typing import Optional


class WorkOrder(Document):
    """
    飞书工单消息模型
    注意：从旧数据迁移，字段可能不完整，大部分设为 Optional
    """
    # 核心字段（通常是有的）
    id: str = Field(alias="_id")
    chat_id: Optional[str] = Indexed(str, default=None)  # 改为可选
    is_reply: bool = False  # 有默认值，不会报错

    # 以下字段可能缺失，必须设为 Optional
    content: Optional[str] = None  # ← 原来是 str，改为 Optional[str]
    create_date: Optional[str] = None  # ← 原来是 str，改为 Optional[str]
    create_time: Optional[int] = None  # ← 原来是 int，改为 Optional[int]
    create_time_str: Optional[str] = None  # ← 原来是 str，改为 Optional[str]

    # 其他可能缺失的字段
    message_id: Optional[str] = None
    msg_type: Optional[str] = None
    parent_id: Optional[str] = None
    sender_id: Optional[str] = None
    sender_type: Optional[str] = "app"
    thread_id: Optional[str] = None
    reply_count: int = 0  # 有默认值，安全

    # MongoDB Date 类型
    sync_at: Optional[datetime] = Field(default_factory=datetime.utcnow)

    class Settings:
        name = "optimize_msg" # 集合名（对应 MongoDB 的 collection name）

    class Config:
        populate_by_name = True # 允许通过字段名（id）或别名（_id）填充数据
    #
    # # 业务方法：解析 content 中的工单信息
    # def extract_priority(self) -> Optional[str]:
    #     """从 content 中提取优先级如 P1/P2"""
    #     import re
    #     match = re.search(r'【优先级】：([P\d]+)', self.content)
    #     return match.group(1) if match else None
    #
    # def extract_feedback_id(self) -> Optional[str]:
    #     """提取反馈ID"""
    #     import re
    #     match = re.search(r'【反馈ID】：(\d+)', self.content)
    #     return match.group(1) if match else None
    #
    # @property
    # def is_high_priority(self) -> bool:
    #     """是否高优先级工单"""
    #     priority = self.extract_priority()
    #     return priority in ["P0", "P1"] if priority else False