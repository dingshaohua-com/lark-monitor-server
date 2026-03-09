所有文件已实现，没有 lint 错误。下面是完整方案的总结：

---

## 实现概览

### 新建文件

**`src/server/app.py`** — FastAPI 应用入口，在 lifespan 中初始化飞书客户端和 MongoDB 连接：

```1:19:src/server/app.py
from contextlib import asynccontextmanager

from fastapi import FastAPI

from server.router.index import router
from server.utils.db_helper import init_db, close_db
from server.utils.lark_client import init_lark_client


@asynccontextmanager
async def lifespan(_app: FastAPI):
    init_lark_client()
    await init_db()
    yield
    await close_db()


server = FastAPI(lifespan=lifespan)
server.include_router(router)
```

**`src/server/utils/lark_client.py`** — 封装 `lark-oapi` 官方 SDK，自动管理 token 生命周期：

```1:21:src/server/utils/lark_client.py
import os

import lark_oapi as lark

_client: lark.Client | None = None


def init_lark_client():
    global _client
    _client = (
        lark.Client.builder()
        .app_id(os.environ["LARK_APP_ID"])
        .app_secret(os.environ["LARK_APP_SECRET"])
        .log_level(lark.LogLevel.INFO)
        .build()
    )


def get_lark_client() -> lark.Client:
    if _client is None:
        raise RuntimeError("Lark client not initialized")
    return _client
```

**`src/server/utils/db_helper.py`** — MongoDB 连接管理，URI 从环境变量读取。

### 核心实现：`sync()` 方法

```11:60:src/server/service/raw_msg.py
async def sync(start: date | None = None, end: date | None = None) -> dict:
    """从飞书群拉取原始消息并写入 MongoDB raw_msg 集合。"""
    client = get_lark_client()
    collection = get_collection("raw_msg")
    chat_id = os.environ["MONITOR_CHAT_ID"]

    now = datetime.now(timezone.utc)
    start_dt = datetime.combine(start, datetime.min.time(), tzinfo=timezone.utc) if start else now - timedelta(days=30)
    end_dt = datetime.combine(end, datetime.max.time(), tzinfo=timezone.utc) if end else now

    # ... 分页拉取 ...

    while True:
        # 构建 SDK 请求
        builder = (
            ListMessageRequest.builder()
            .container_id_type("chat")
            .container_id(chat_id)
            .start_time(start_ts)
            .end_time(end_ts)
            .sort_type("ByCreateTimeAsc")
            .page_size(50)
        )
        # 原生 async 调用，无需 asyncio.to_thread
        response = await client.im.v1.message.alist(builder.build())

        # 批量 upsert 到 MongoDB
        if items:
            ops = [UpdateOne({"_id": doc["_id"]}, {"$set": doc}, upsert=True) for ...]
            await collection.bulk_write(ops)

        if not response.data.has_more:
            break
```

### 关键设计点

| 方面 | 实现 |
|------|------|
| **认证** | `lark-oapi` SDK 自动管理 `tenant_access_token`，无需手写 |
| **异步** | 使用 SDK 原生 `alist()` 异步方法，全链路 async |
| **分页** | 自动循环拉取，通过 `page_token` + `has_more` 翻页 |
| **去重** | 以 `message_id` 为 MongoDB `_id`，`bulk_write` + `upsert` 幂等写入 |
| **时间范围** | 支持 `start`/`end` 日期参数，默认拉取最近 30 天 |

### 使用方式

```bash
# 默认同步最近 30 天
GET /api/raw-msg/sync

# 指定日期范围
GET /api/raw-msg/sync?start=2025-01-01&end=2025-03-01
```

记得把 `.env` 里的 `MONGO_URI` 改成你实际的 MongoDB 连接地址（如 Atlas 的连接串）。