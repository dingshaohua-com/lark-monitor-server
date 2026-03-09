from datetime import date

from fastapi import APIRouter, Query

import server.service.raw_msg as raw_msg_service

router = APIRouter(prefix="/raw-msg", tags=["raw-msg"])


@router.get("/status")
async def get_status():
    result = await raw_msg_service.status()
    return {"data": result}


@router.delete("/clear")
async def clear_all():
    result = await raw_msg_service.clear_all()
    return {"data": result}


@router.get("/sync")
async def sync(
    start: date | None = Query(None, description="起始日期，如 2026-01-01"),
    end: date | None = Query(None, description="结束日期，如 2026-01-31"),
):
    result = await raw_msg_service.sync(start=start, end=end)
    return {"data": result}
