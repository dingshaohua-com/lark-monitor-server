from datetime import date

from fastapi import APIRouter, Query

import server.service.opt_msg as opt_msg_service

router = APIRouter(prefix="/opt-msg", tags=["opt-msg"])



@router.get("/")
async def get_msg():
    pass


@router.get("/sync")
async def sync_msg():
    result = await opt_msg_service.sync_msg()
    return {"data": result}