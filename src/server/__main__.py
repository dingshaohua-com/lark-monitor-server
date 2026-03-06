import uvicorn

if __name__ == "__main__":
    # 仅开发使用，硬编码参数
    uvicorn.run("server.app:server", host="0.0.0.0", port=8000, reload=True)