import os

def login():
    """获取 tenant_access_token"""
    url = "/auth/v3/tenant_access_token/internal"
    payload = {
        "app_id": os.getenv("LARK_APP_ID"),
        "app_secret": os.getenv("LARK_APP_SECRET"),
    }
    from monitor.utils.client import lark_client
    res = lark_client.post(url, json=payload)
    token = res.json().get("tenant_access_token")
    from monitor.utils import store
    store.tenant_access_token=token