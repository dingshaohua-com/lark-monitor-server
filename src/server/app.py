from fastapi import FastAPI
from server.router.index import router
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from typing import AsyncGenerator
from server.utils.db_helper import init_db, close_db
from monitor.service.account import login



@asynccontextmanager
async def on_start(app: FastAPI)-> AsyncGenerator[None, None]:
    login()
    await init_db()
    print("🚀 启动成功...")
    yield
    await close_db()
server = FastAPI(title="Lark Msg Monitor API", version="0.0.1", lifespan=on_start)

# 解决跨域问题
server.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],           # 生产环境改成具体域名，如 ["http://localhost:5173"]
    allow_credentials=True,        # 允许携带 cookie/token
    allow_methods=["*"],           # 允许所有 HTTP 方法（GET/POST/PUT/DELETE 等）
    allow_headers=["*"],           # 允许所有请求头
    max_age=600,                   # 预检请求缓存 10 分钟（减少 OPTIONS 请求次数）
)

# 只需注册一次聚合路由
server.include_router(router)


