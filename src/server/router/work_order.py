from datetime import date
from typing import Optional

from fastapi import Depends

from fastapi import APIRouter

from server.schema.work_order import WorkOrderQuery, WorkOrderSync
import server.service.work_order as work_order_service

router = APIRouter(prefix="/work-order", tags=["work-order"])

@router.get("/")
async def get_work_order(params: WorkOrderQuery=Depends()):
    result=await work_order_service.get_work_order(params)
    return {"data": result}


@router.get("/sync")
async def sync_report( params:WorkOrderSync=Depends()):
    pass
    # try:
    #     stats = await asyncio.to_thread(run_sync, start, end, optimize)
    #     return {"msg": "同步完成", "data": stats}
    # except ValueError as e:
    #     raise HTTPException(status_code=400, detail=str(e))
    # except Exception as e:
    #     raise HTTPException(status_code=500, detail=f"同步失败: {e}")