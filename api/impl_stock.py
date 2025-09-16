from fastapi import APIRouter, BackgroundTasks, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import uuid
import json
import asyncio
from typing import Dict, Any, Optional
from datetime import datetime

from graph.main_graph import build_graph
from config.logging_config import setup_default_logging
from core.cache_manager import cache_manager
from .streaming_protocol import format_event 

# --- 初始化 ---
setup_default_logging()
router = APIRouter()
tasks: Dict[str, Dict[str, Any]] = {} # 用于传统的后台任务

try:
    cache_manager.initialize()
    print("✅ 缓存管理器初始化成功")
except Exception as e:
    print(f"⚠️ 缓存管理器初始化失败: {e}")

# --- API 请求模型 ---
class StockAnalysisRequest(BaseModel):
    stock_code: str
    end_date: Optional[str] = None 

# --- 核心: 事件驱动的流式分析生成器 ---
async def stream_analysis_generator(stock_code: str, end_date: Optional[str] = None):
    """
    一个异步生成器函数，用于运行LangGraph并流式传输符合协议的事件。
    """
    app = build_graph()
    thread_id = str(uuid.uuid4())
    
    # 如果未提供 end_date，则使用当前日期
    if end_date is None:
        end_date = datetime.now().strftime("%Y%m%d")
        
    # astream_events 需要一个字典作为输入，不是列表
    inputs = {"stock_code": stock_code, "end_date": end_date}
    config = {"configurable": {"thread_id": thread_id}}

    try:
        # 使用 astream_events 来获取所有内部事件
        # 这会实时地、逐一地产生LangGraph内部的每一个动作
        async for event in app.astream_events(inputs, config=config, version="v1"):
            
            # 使用我们的协议转换器，将原始事件格式化为前端需要的JSON
            formatted_event_str = format_event(event, thread_id)
            
            if formatted_event_str:
                # 如果事件有效，则通过yield发送给客户端
                yield formatted_event_str
                await asyncio.sleep(0.01) # 短暂休眠，防止事件过于密集，给前端渲染留出时间

    except Exception as e:
        # 如果过程中出现任何错误，也以标准事件的格式发送错误信息
        error_event = {
            "event_type": "message_chunk",
            "thread_id": thread_id,
            "agent": "system_error",
            "id": "error-run",
            "content": f"分析过程中出现严重错误: {str(e)}",
            "finish_reason": "stop"
        }
        yield f"data: {json.dumps(error_event, ensure_ascii=False)}\n\n"
    
    finally:
        # 发送一个最终的结束信号（可选，但推荐）
        final_event = {
            "event_type": "message_chunk",
            "thread_id": thread_id,
            "agent": "system",
            "id": "final-run",
            "content": "分析流程已结束。",
            "finish_reason": "stop"
        }
        yield f"data: {json.dumps(final_event, ensure_ascii=False)}\n\n"


# --- API 端点定义 ---

@router.post("/stream_analysis")
async def stream_analysis(request: StockAnalysisRequest):
    """
    接收股票分析请求，并以 Server-Sent Events (SSE) 的形式
    实时流式返回分析过程中的每一个细节事件。
    """
    return StreamingResponse(
        stream_analysis_generator(request.stock_code, request.end_date),
        media_type="text/event-stream"
    )

@router.get("/stream_analysis")
async def stream_analysis_get(stock_code: str, end_date: Optional[str] = None):
    """
    GET方法的流式分析端点，用于EventSource连接
    """
    return StreamingResponse(
        stream_analysis_generator(stock_code, end_date),
        media_type="text/event-stream"
    )


# --- 保留一个传统的非流式后台任务端点，用于不需要实时反馈的场景 ---

def run_analysis_background(task_id: str, stock_code: str, end_date: Optional[str] = None):
    """一个简单的、非流式的后台任务，只关心最终结果。"""
    app = build_graph()
    tasks[task_id]['status'] = 'running'
    
    if end_date is None:
        end_date = datetime.now().strftime("%Y%m%d")
        
    try:
        final_state = app.invoke({"stock_code": stock_code, "end_date": end_date})
        tasks[task_id]['status'] = 'completed'
        tasks[task_id]['result'] = final_state.get('final_report', "分析完成，但未找到最终报告。")
    except Exception as e:
        tasks[task_id]['status'] = 'failed'
        tasks[task_id]['result'] = {"error": str(e)}

@router.post("/analyze_stock", status_code=202)
async def analyze_stock(request: StockAnalysisRequest, background_tasks: BackgroundTasks):
    """启动一个后台分析任务，并立即返回任务ID。"""
    task_id = str(uuid.uuid4())
    tasks[task_id] = {"status": "pending", "result": None}
    background_tasks.add_task(run_analysis_background, task_id, request.stock_code, request.end_date)
    return {"message": "后台分析任务已启动。", "task_id": task_id}

@router.get("/get_task_status/{task_id}")
async def get_task_status(task_id: str):
    """查询后台任务的状态和最终结果。"""
    task = tasks.get(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="任务ID未找到。")
    return task
