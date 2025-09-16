from langgraph.graph import StateGraph, END
from graph.type import StockAgentState
from graph.nodes.analysis_nodes import *
from graph.nodes.common_nodes import *
from core.result_manager import result_manager
from logging import getLogger

# 配置日志
from config.logging_config import setup_default_logging
setup_default_logging()

logger = getLogger(__name__)

def final_result_save(state: StockAgentState) -> dict:
    """最终结果保存节点：保存所有报告并生成摘要"""
    stock_code = state['stock_code']
    
    # 保存所有分析报告
    saved_files = result_manager.save_all_reports(stock_code, state)
    
    # 生成结果摘要
    summary = result_manager.get_result_summary(stock_code)
    
    # 保存摘要到文件
    summary_filepath = result_manager.save_report(stock_code, "summary", summary, "analysis_summary")
    
    logger.info(f"所有结果保存完成，共保存 {len(saved_files)} 个报告文件")
    logger.info(f"结果摘要保存到: {summary_filepath}")
    
    return {
        "saved_files": saved_files,
        "summary_filepath": summary_filepath,
        "final_report": f"分析完成！所有结果已保存到 result/{stock_code}/ 目录"
    }

def build_graph():
    workflow = StateGraph(StockAgentState)
    workflow.add_node("start", start_node)

    # 第一层：基本面与新闻（并行）
    workflow.add_node("fundamental_analysis", run_fundamental_analysis)
    workflow.add_node("news_analysis", run_news_analysis)

    # 第二层：情绪、技术、资金（情绪依赖基本面与新闻；技术/资金可直接运行）
    workflow.add_node("sentiment_analysis", run_sentiment_analysis)
    workflow.add_node("technical_analysis", run_technical_analysis)
    workflow.add_node("fund_analysis", run_fund_analysis)

    # 第三层：监督总结
    workflow.add_node("supervisor", run_supervisor)

    # 终态：保存
    workflow.add_node("final_result_save", final_result_save)

    workflow.set_entry_point("start")

    # 并行启动基本面与新闻
    workflow.add_edge("start", "fundamental_analysis")
    workflow.add_edge("start", "news_analysis")

    # 两者完成后进入情绪
    workflow.add_edge("fundamental_analysis", "sentiment_analysis")
    workflow.add_edge("news_analysis", "sentiment_analysis")

    # 技术、资金节点从 start 启动（不依赖前置），也可改为依赖情绪
    workflow.add_edge("start", "technical_analysis")
    workflow.add_edge("start", "fund_analysis")

    # 监督节点等待三大分析完成
    workflow.add_edge("sentiment_analysis", "supervisor")
    workflow.add_edge("technical_analysis", "supervisor")
    workflow.add_edge("fund_analysis", "supervisor")
    workflow.add_edge("fundamental_analysis", "supervisor")  # 也让基本面作为依赖，确保其报告入库

    # 保存
    workflow.add_edge("supervisor", "final_result_save")
    workflow.add_edge("final_result_save", END)

    app = workflow.compile()
    return app