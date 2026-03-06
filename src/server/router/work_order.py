
from fastapi import Depends

from fastapi import APIRouter

from server.schema.work_order import WorkOrderQuery
import server.service.work_order as work_order_service

router = APIRouter(prefix="/work-order", tags=["work-order"])

@router.get("/")
async def get_work_order(params: WorkOrderQuery=Depends()):
    result=await work_order_service.get_work_order(params)
    return {"data": result}