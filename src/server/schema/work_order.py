# src/server/schemas/message.py
from pydantic import BaseModel, Field
from typing import Optional


class WorkOrderQuery(BaseModel):
    """工单查询参数"""
    keyword: Optional[str] = Field(None, description="关键词搜索")
    start_date: Optional[str] = Field(None, description="开始日期，格式 YYYY-MM-DD")
    end_date: Optional[str] = Field(None, description="结束日期")
    msg_type: Optional[str] = Field(None, description="消息类型")
    sender_type: Optional[str] = Field(None, description="发送者类型")
    has_reply: Optional[str] = Field(None, description="是否有回复")
    page: int = Field(1, ge=1, description="页码")
    page_size: int = Field(20, ge=1, le=100, description="每页条数")

    class Config:
        json_schema_extra = {
            "example": {
                "keyword": "会议",
                "page": 1,
                "page_size": 20
            }
        }