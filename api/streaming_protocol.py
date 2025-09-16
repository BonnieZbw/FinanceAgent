import json
import uuid
from typing import List, Dict, Any, Literal, Optional
from pydantic import BaseModel, Field

# --- Pydantic Models for Protocol Definition ---

class ToolCall(BaseModel):
    name: str
    args: Dict[str, Any]
    id: str
    type: str = "tool_call"

class ToolCallChunk(BaseModel):
    name: Optional[str] = None
    args: Optional[str] = None
    id: Optional[str] = None
    index: int
    type: str = "tool_call_chunk"

class StreamEvent(BaseModel):
    event_type: Literal["message_chunk", "tool_calls", "tool_call_chunks", "progress", "node_complete", "analysis_result"]
    thread_id: str
    agent: str
    id: str # Run ID
    role: str = "assistant"
    content: Optional[str] = ""
    finish_reason: Optional[str] = None
    progress_symbol: Optional[bool] = None
    parsed: Optional[Any] = None
    refusal: Optional[Any] = None
    tool_calls: Optional[List[ToolCall]] = None
    tool_call_chunks: Optional[List[ToolCallChunk]] = None
    node_status: Optional[str] = None
    result_data: Optional[Dict[str, Any]] = None

# --- Event Formatting Logic ---

def format_event(data: dict, thread_id: str) -> Optional[str]:
    """
    将 LangGraph 的原始事件转换为符合协议的JSON字符串。
    优化后的版本确保每个节点完成后都能输出相应的报告和结果。
    """
    event_name = data.get("event")
    run_id = data.get("run_id")
    node_name = data.get("name") # Corresponds to 'agent'
    
    # 过滤掉不需要的事件
    if event_name in ["on_graph_end"]:
        return None

    # 当agent节点完成时，输出其报告内容
    if event_name == "on_chain_end" and node_name in [
        "fundamental_analysis", "technical_analysis", "sentiment_analysis", 
        "news_analysis", "fund_analysis", "bull_debate", "bear_debate", 
        "debate_analyst", "supervisor", "final_result_save"
    ]:
        # 获取agent的输出结果
        output = data.get("data", {}).get("output", {})
        
        # 发送节点完成事件
        complete_event = StreamEvent(
            event_type="node_complete",
            thread_id=thread_id,
            agent=node_name,
            id=run_id,
            content=f"节点 '{node_name}' 执行完成",
            node_status="completed",
            finish_reason="stop"
        )
        
        # 构建完整的输出字符串
        result_strings = []
        result_strings.append(f"data: {complete_event.model_dump_json(exclude_none=True)}\n\n")
        
        if output:
            # 根据节点名称确定报告类型
            report_type_map = {
                "fundamental_analysis": "基本面分析报告",
                "technical_analysis": "技术分析报告", 
                "sentiment_analysis": "情绪分析报告",
                "news_analysis": "新闻分析报告",
                "fund_analysis": "资金分析报告",
                "bull_debate": "多头辩论报告",
                "bear_debate": "空头辩论报告",
                "debate_analyst": "辩论分析报告",
                "supervisor": "总决策报告",
                "final_result_save": "最终结果保存"
            }
            
            report_title = report_type_map.get(node_name, f"{node_name}报告")
            
            # 构建报告内容
            report_content = f"=== {report_title} ===\n"
            
            # 处理不同类型的输出
            if node_name == "final_result_save":
                # 最终结果保存节点的特殊处理
                if "saved_files" in output:
                    report_content += f"已保存文件数量: {len(output['saved_files'])}\n"
                if "summary_filepath" in output:
                    report_content += f"摘要文件路径: {output['summary_filepath']}\n"
                if "final_report" in output:
                    report_content += f"{output['final_report']}\n"
            else:
                # 其他分析节点的处理
                for key, value in output.items():
                    if key.endswith("_report") or key.endswith("_result"):
                        if isinstance(value, str):
                            report_content += f"{value}\n\n"
                        else:
                            report_content += f"{key}: {str(value)}\n\n"
            
            # 发送分析结果事件
            result_event = StreamEvent(
                event_type="analysis_result",
                thread_id=thread_id,
                agent=node_name,
                id=run_id,
                content=report_content,
                result_data=output,
                finish_reason="stop"
            )
            result_strings.append(f"data: {result_event.model_dump_json(exclude_none=True)}\n\n")
        
        # 返回组合后的字符串
        return "".join(result_strings)

    # 节点开始执行事件
    if event_name == "on_chain_start":
        start_event = StreamEvent(
            event_type="progress",
            thread_id=thread_id,
            agent=node_name,
            id=run_id,
            content=f"节点 '{node_name}' 开始执行...",
            node_status="started",
            progress_symbol=True
        )
        return f"data: {start_event.model_dump_json(exclude_none=True)}\n\n"

    # 工具开始执行事件，用于前端显示进度
    if event_name == "on_tool_start":
        tool_name = data.get("data", {}).get("input", {}).get("tool", "未知工具")
        event = StreamEvent(
            event_type="progress",
            thread_id=thread_id,
            agent=node_name,
            id=run_id,
            content=f"工具 '{tool_name}' 正在执行...",
            progress_symbol=True
        )
        return f"data: {event.model_dump_json(exclude_none=True)}\n\n"

    # 工具执行完成事件
    if event_name == "on_tool_end":
        tool_name = data.get("data", {}).get("input", {}).get("tool", "未知工具")
        tool_output = data.get("data", {}).get("output", "")
        
        # 如果工具输出内容较多，可以截断显示
        if isinstance(tool_output, str) and len(tool_output) > 200:
            tool_output = tool_output[:200] + "..."
        
        event = StreamEvent(
            event_type="progress",
            thread_id=thread_id,
            agent=node_name,
            id=run_id,
            content=f"工具 '{tool_name}' 执行完成: {tool_output}",
            progress_symbol=False
        )
        return f"data: {event.model_dump_json(exclude_none=True)}\n\n"

    # LLM流式输出事件
    if event_name == "on_chat_model_stream":
        chunk = data.get("data", {}).get("chunk")
        if not chunk: return None

        # 1. 处理 tool_call_chunks
        if chunk.tool_call_chunks:
            event = StreamEvent(
                event_type="tool_call_chunks",
                thread_id=thread_id,
                agent=node_name,
                id=run_id,
                tool_call_chunks=[c.dict() for c in chunk.tool_call_chunks]
            )
            return f"data: {event.model_dump_json(exclude_none=True)}\n\n"

        # 2. 处理 message_chunk
        if chunk.content:
            event = StreamEvent(
                event_type="message_chunk",
                thread_id=thread_id,
                agent=node_name,
                id=run_id,
                content=chunk.content,
                finish_reason=chunk.response_metadata.get("finish_reason")
            )
            return f"data: {event.model_dump_json(exclude_none=True)}\n\n"

        # 3. 处理 tool_calls (通常在流的末尾)
        if chunk.tool_calls:
            event = StreamEvent(
                event_type="tool_calls",
                thread_id=thread_id,
                agent=node_name,
                id=run_id,
                tool_calls=[tc.dict() for tc in chunk.tool_calls],
                finish_reason="tool_calls"
            )
            return f"data: {event.model_dump_json(exclude_none=True)}\n\n"

    return None