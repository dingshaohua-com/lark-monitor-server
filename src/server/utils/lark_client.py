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
