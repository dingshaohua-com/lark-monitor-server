from fastapi import APIRouter, Query, HTTPException

from server.utils import dict_registry

router = APIRouter(prefix="/dict", tags=["dict"])


@router.get("/")
async def list_dicts():
    return {"data": dict_registry.list_names()}


@router.get("/detail")
async def get_dict(
    name: str = Query(..., description="字典名称，如 field_name_map"),
):
    data = dict_registry.get(name)
    if data is None:
        raise HTTPException(status_code=404, detail=f"字典 '{name}' 不存在")
    return {"data": data}
