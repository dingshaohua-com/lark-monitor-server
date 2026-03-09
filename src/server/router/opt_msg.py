from fastapi import APIRouter, Query

import server.service.opt_msg as opt_msg_service

router = APIRouter(prefix="/opt-msg", tags=["opt-msg"])


@router.get("/work-order")
async def get_work_order(
    page: int = Query(1, ge=1, description="页码"),
    page_size: int = Query(20, ge=1, le=100, description="每页数量"),
    keyword: str | None = Query(None, description="关键字搜索（用户原文/客服备注）"),
    priority: str | None = Query(None, description="优先级，如 P0、P1"),
    start_date: str | None = Query(None, description="起始日期，如 2026-01-01"),
    end_date: str | None = Query(None, description="结束日期，如 2026-01-31"),
):
    result = await opt_msg_service.get_work_order(
        page=page, page_size=page_size,
        keyword=keyword, priority=priority,
        start_date=start_date, end_date=end_date,
    )
    return {"data": result}


@router.get("/sync")
async def sync_msg():
    result = await opt_msg_service.sync_msg()
    return {"data": result}
