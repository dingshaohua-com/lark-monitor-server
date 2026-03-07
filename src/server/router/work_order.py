from datetime import date
from typing import Optional

from fastapi import Depends

from fastapi import APIRouter

from server.schema.work_order import WorkOrderQuery, WorkOrderSync
import server.service.work_order as work_order_service
from server.utils.sync_helper import get_sync_status

router = APIRouter(prefix="/work-order", tags=["work-order"])

@router.get("/")
async def get_work_order(params: WorkOrderQuery=Depends()):
    result=await work_order_service.get_work_order(params)
    return {"data": result}

@router.get("/sync")
async def sync_report( params:WorkOrderSync=Depends()):
    result=await work_order_service.sync_work_order(params)
    return {"data": result}

@router.get("/sync-status")
async def sync_status():
    status = await get_sync_status()
    return {"data": status}

@router.get("/replies")
async def get_replies(msg_id: str):
    items = await work_order_service.get_replies(msg_id)
    return {"data": items}
