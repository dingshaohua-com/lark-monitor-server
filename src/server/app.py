from contextlib import asynccontextmanager
from fastapi import FastAPI
from server.router.index import router
from server.utils.db_helper import init_db, close_db
from server.utils.lark_client import init_lark_client
from fastapi.middleware.cors import CORSMiddleware


@asynccontextmanager
async def lifespan(_app: FastAPI):
    init_lark_client()
    await init_db()
    yield
    await close_db()


server = FastAPI(lifespan=lifespan)
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

