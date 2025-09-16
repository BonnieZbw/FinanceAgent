from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from api.impl_stock import router as stock_router

# 配置日志
from config.logging_config import setup_default_logging
setup_default_logging()

app = FastAPI(
    title="A股AI决策支持平台 V1 (LLM Refactored)",
    description="一个实现多智能体并行分析与结构化辩论的AI决策支持平台后端。",
    version="1.1.0"
)

# 添加CORS中间件
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 允许所有来源
    allow_credentials=True,
    allow_methods=["*"],  # 允许所有方法
    allow_headers=["*"],  # 允许所有头部
)

app.include_router(stock_router, prefix="/api/v1", tags=["Stock Analysis"])

@app.get("/", tags=["Root"])
async def read_root():
    return {"message": "Welcome to the Stock Agent Platform API V1. Go to /docs for API documentation."}

if __name__ == "__main__":
    import uvicorn
    print("🚀 启动股票分析流式输出服务器...")
    print("📡 服务器地址: http://localhost:8000")
    print("📖 API文档: http://localhost:8000/docs")
    print("🔄 流式输出端点: http://localhost:8000/api/v1/stream_analysis")
    uvicorn.run(app, host="0.0.0.0", port=8000)
