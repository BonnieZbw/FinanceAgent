import json
from datetime import datetime, timedelta
from langchain_core.runnables import Runnable
from config.agent_roles import AGENT_ROLES
from tools.stock_tools import *
from prompts.stock.analyst_prompts import *
from prompts.stock.debate_prompts import *
from prompts.stock.supervisor_prompts import *
from config.llm_config import get_llm
from core.result_manager import result_manager

from graph.type import StockAgentState
from tools.parsers import parse_analyst_report, parse_debater_report, parse_debate_report, parse_supervisor_report
from logging import getLogger
logger = getLogger(__name__)

llm = get_llm()

def create_analyst_chain(prompt: Runnable) -> Runnable:
    """创建一个分析师的执行链，使用模块级的llm实例"""
    return prompt | llm

def _parse_date(date_str: str) -> datetime:
    """解析多种格式的日期字符串"""
    if not date_str:
        raise ValueError("日期字符串为空")
    
    # 尝试多种日期格式
    date_formats = [
        "%Y-%m-%d",      # 2025-08-19
        "%Y%m%d",        # 20250819
        "%Y/%m/%d",      # 2025/08/19
        "%Y.%m.%d",      # 2025.08.19
        "%Y年%m月%d日",   # 2025年08月19日
    ]
    
    for fmt in date_formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    
    raise ValueError(f"无法解析日期格式: {date_str}")

def _get_analysis_period(end_date: str) -> str:
    """计算分析时间段"""
    if not end_date:
        return "近两年数据"
    
    try:
        end_dt = _parse_date(end_date)
        # 使用replace方法精确计算两年前的日期，与ResultManager保持一致
        start_dt = end_dt.replace(year=end_dt.year - 2)
        start_date = start_dt.strftime("%Y-%m-%d")
        return f"{start_date} 至 {end_dt.strftime('%Y-%m-%d')}"
    except Exception as e:
        logger.warning(f"解析日期失败: {e}")
        return "近两年数据"

def run_fundamental_analysis(state: StockAgentState) -> dict:
    stock_code = state['stock_code']
    end_date = state.get('end_date')
    analysis_period = _get_analysis_period(end_date)
    
    fundamental_data = get_fundamental_data.invoke({"stock_code": stock_code, "end_date": end_date})
    chain = create_analyst_chain(FUNDAMENTAL_PROMPT)
    result = chain.invoke({
        "stock_code": stock_code, 
        "fundamental_data": fundamental_data, 
        "role_description": AGENT_ROLES['fundamental_analyst'],
        "analysis_period": analysis_period
    })
    analyst_report = parse_analyst_report(result.content)
    logger.info(f"基本面分析师报告: {analyst_report}")
    
    # 保存分析报告
    result_manager.save_report(stock_code, "fundamental", analyst_report, "fundamental_report", end_date)
    
    return {"fundamental_report": analyst_report}

def run_technical_analysis(state: StockAgentState) -> dict:
    stock_code = state['stock_code']
    end_date = state.get('end_date')
    analysis_period = _get_analysis_period(end_date)
    
    tech_data = get_tech_data.invoke({"stock_code": stock_code, "end_date": end_date})
    chain = create_analyst_chain(TECHNICAL_PROMPT)
    result = chain.invoke({
        "stock_code": stock_code, 
        "tech_data": tech_data, 
        "role_description": AGENT_ROLES['technical_analyst'],
        "analysis_period": analysis_period
    })
    analyst_report = parse_analyst_report(result.content)
    logger.info(f"技术分析师报告: {analyst_report}")
    
    # 保存分析报告
    result_manager.save_report(stock_code, "technical", analyst_report, "technical_report", end_date)
    
    return {"technical_report": analyst_report}

def run_sentiment_analysis(state: StockAgentState) -> dict:
    stock_code = state['stock_code']
    end_date = state.get('end_date')
    analysis_period = _get_analysis_period(end_date)

    # 1) 读取新闻数据的combined_summary字段
    news_combined_summary = ""
    try:
        if hasattr(result_manager, 'load_tool_result'):
            news_data = result_manager.load_tool_result(stock_code, "news_data", end_date)
        else:
            news_data = result_manager.load_news_data(stock_code, end_date)
        
        if isinstance(news_data, dict):
            # 检查data字段中的内容
            data_section = news_data.get('data', {})
            if isinstance(data_section, dict):
                # 优先取combined_summary字段
                news_combined_summary = data_section.get('combined_summary', "")
            else:
                # 如果data不是dict，直接从顶层获取
                news_combined_summary = news_data.get('combined_summary', "")
        elif isinstance(news_data, str):
            news_combined_summary = news_data
            
        logger.info(f"成功提取新闻combined_summary，长度: {len(news_combined_summary)} 字符")
    except Exception as e:
        logger.warning(f"读取新闻combined_summary失败: {e}")
        news_combined_summary = ""

    # 2) 读取基本面数据的result字段
    fundamental_result = ""
    try:
        if hasattr(result_manager, 'load_tool_result'):
            fundamental_data = result_manager.load_tool_result(stock_code, "fundamental_data", end_date)
        else:
            fundamental_data = {}
        
        if isinstance(fundamental_data, dict):
            # 检查data字段中的内容
            data_section = fundamental_data.get('data', {})
            if isinstance(data_section, dict):
                # 从interfaces中提取各个接口的result
                interfaces = data_section.get('interfaces', {})
                if interfaces:
                    # 收集所有接口的result
                    results = []
                    for interface_name, interface_data in interfaces.items():
                        if isinstance(interface_data, dict) and 'result' in interface_data:
                            result = interface_data['result']
                            if result and result.strip():
                                results.append(f"【{interface_data.get('objective', interface_name)}】\n{result}")
                    
                    if results:
                        fundamental_result = "\n\n".join(results)
                else:
                    # 如果没有interfaces，直接取result
                    fundamental_result = data_section.get('result', "")
            else:
                # 如果data不是dict，直接从顶层获取
                fundamental_result = fundamental_data.get('result', "")
                
        logger.info(f"成功提取基本面result，长度: {len(fundamental_result)} 字符")
    except Exception as e:
        logger.warning(f"读取基本面result失败: {e}")
        fundamental_result = ""

    # 若未能取到result，则用上游报告兜底
    if not fundamental_result:
        fundamental_report_backup = state.get('fundamental_report', {})
        if isinstance(fundamental_report_backup, dict) and fundamental_report_backup:
            # 从报告中提取关键信息
            if 'reason' in fundamental_report_backup:
                fundamental_result = fundamental_report_backup['reason']
            elif 'detailed_analysis' in fundamental_report_backup:
                fundamental_result = fundamental_report_backup['detailed_analysis']

    # 3) 组装情绪面输入（包含新闻combined_summary和基本面result）
    sentiment_input = {
        "stock_code": stock_code,
        "end_date": end_date,
        "news_combined_summary": news_combined_summary,
        "fundamental_result": fundamental_result
    }

    # 4) 保存情绪输入快照，便于回溯
    try:
        result_manager.save_tool_result(stock_code, "sentiment_input", sentiment_input, end_date=end_date)
    except Exception as _e:
        logger.warning(f"保存情绪输入快照失败: {_e}")

    # 5) 调用情绪分析 Prompt
    chain = create_analyst_chain(SENTIMENT_PROMPT)
    result = chain.invoke({
        "stock_code": stock_code,
        "sentiment_data": json.dumps(sentiment_input, ensure_ascii=False),
        "role_description": AGENT_ROLES['sentiment_analyst'],
        "analysis_period": analysis_period
    })
    analyst_report = parse_analyst_report(result.content)
    logger.info(f"情绪分析师报告: {analyst_report}")

    # 保存分析报告
    result_manager.save_report(stock_code, "sentiment", analyst_report, "sentiment_report", end_date)

    return {"sentiment_report": analyst_report}

def run_news_analysis(state: StockAgentState) -> dict:
    stock_code = state['stock_code']
    end_date = state.get('end_date')
    analysis_period = _get_analysis_period(end_date)
    
    # 优先从result文件读取已存在的新闻数据
    news_data = None
    try:
        news_data_dict = result_manager.load_news_data(stock_code, end_date)
        if news_data_dict:
            # 如果是从结构化数据加载，提取data部分
            if news_data_dict.get("data"):
                news_data = json.dumps(news_data_dict["data"], ensure_ascii=False)
                logger.info(f"成功从result文件加载新闻数据: {stock_code}")
            else:
                # 如果是基础数据，直接使用
                news_data = news_data_dict.get("result", str(news_data_dict))
                logger.info(f"成功从result文件加载基础新闻数据: {stock_code}")
    except Exception as e:
        logger.warning(f"从result文件加载新闻数据失败: {e}")
    
    # 如果从result文件加载失败，则调用接口获取
    if not news_data:
        logger.info(f"result文件中无新闻数据，调用接口获取: {stock_code}")
        news_data = get_news.invoke({"stock_code": stock_code, "end_date": end_date})
    
    # 注意：news_data 现在优先返回结构化 JSON（字符串），不再是纯文本摘要
    chain = create_analyst_chain(NEWS_PROMPT)
    result = chain.invoke({
        "stock_code": stock_code, 
        "news_data": news_data, 
        "role_description": AGENT_ROLES['news_analyst'],
        "analysis_period": analysis_period
    })
    analyst_report = parse_analyst_report(result.content)
    logger.info(f"新闻分析师报告: {analyst_report}")
    
    # 保存分析报告
    result_manager.save_report(stock_code, "news", analyst_report, "news_report", end_date)
    
    return {"news_report": analyst_report}

def run_fund_analysis(state: StockAgentState) -> dict:
    stock_code = state['stock_code']
    end_date = state.get('end_date')
    analysis_period = _get_analysis_period(end_date)
    
    try:
        logger.info(f"开始调用资金面数据工具: {stock_code}, {end_date}")
        fund_data = get_fund_data.invoke({"stock_code": stock_code, "end_date": end_date})
        logger.info(f"资金面数据工具调用成功，数据长度: {len(fund_data) if fund_data else 0}")
        
        # 工具结果已在get_fund_data函数中保存，无需重复保存
        
        chain = create_analyst_chain(FUND_PROMPT)
        result = chain.invoke({
            "stock_code": stock_code, 
            "fund_data": fund_data, 
            "role_description": AGENT_ROLES['fund_analyst'],
            "analysis_period": analysis_period
        })
        analyst_report = parse_analyst_report(result.content)
        logger.info(f"资金分析师报告: {analyst_report}")
        
        # 保存分析报告
        result_manager.save_report(stock_code, "fund", analyst_report, "fund_report", end_date)
        
        return {"fund_report": analyst_report}
    except Exception as e:
        logger.error(f"资金面分析失败: {e}")
        # 返回一个默认的报告
        default_report = {
            "analyst_name": "资金流向分析师",
            "viewpoint": "中性",
            "reason": f"资金面数据获取失败: {e}",
            "scores": {"main_capital": 0, "institution_capital": 0, "retail_capital": 0},
            "detailed_analysis": f"资金面数据工具调用失败，错误信息: {e}"
        }
        result_manager.save_report(stock_code, "fund", default_report, "fund_report", end_date)
        return {"fund_report": default_report}

def run_bull_debate(state: StockAgentState) -> dict:
    stock_code = state['stock_code']
    end_date = state.get('end_date')
    analysis_period = _get_analysis_period(end_date)
    
    chain = create_analyst_chain(BULL_DEBATER_PROMPT)
    result = chain.invoke({
        "stock_code": stock_code, 
        "fundamental_report": state['fundamental_report'],
        "technical_report": state['technical_report'],
        "sentiment_report": state['sentiment_report'],
        "fund_report": state['fund_report'],
        "news_report": state['news_report'],
        "role_description": AGENT_ROLES['bull_debater'],
        "analysis_period": analysis_period
    })
    debater_report = parse_debater_report(result.content, "多头辩论者")
    logger.info(f"多头辩论者报告: {debater_report}")
    
    # 保存分析报告
    result_manager.save_report(stock_code, "bull", debater_report, "bull_report", end_date)
    
    return {"bull_report": debater_report}

def run_bear_debate(state: StockAgentState) -> dict:
    stock_code = state['stock_code']
    end_date = state.get('end_date')
    analysis_period = _get_analysis_period(end_date)
    
    chain = create_analyst_chain(BEAR_DEBATER_PROMPT)
    result = chain.invoke({
        "stock_code": stock_code, 
        "fundamental_report": state['fundamental_report'],
        "technical_report": state['technical_report'],
        "sentiment_report": state['sentiment_report'],
        "fund_report": state['fund_report'],
        "news_report": state['news_report'],
        "role_description": AGENT_ROLES['bear_debater'],
        "analysis_period": analysis_period
    })
    debater_report = parse_debater_report(result.content, "空头辩论者")
    logger.info(f"空头辩论者报告: {debater_report}")
    
    # 保存分析报告
    result_manager.save_report(stock_code, "bear", debater_report, "bear_report", end_date)
    
    return {"bear_report": debater_report}

def run_debate_analyst(state: StockAgentState) -> dict:
    stock_code = state['stock_code']
    end_date = state.get('end_date')
    analysis_period = _get_analysis_period(end_date)
    
    chain = create_analyst_chain(DEBATE_ANALYST_PROMPT)
    result = chain.invoke({
        "stock_code": stock_code, 
        "fundamental_report": state['fundamental_report'],
        "technical_report": state['technical_report'],
        "sentiment_report": state['sentiment_report'],
        "fund_report": state['fund_report'],
        "news_report": state['news_report'],
        "bull_report": state['bull_report'],
        "bear_report": state['bear_report'],
        "role_description": AGENT_ROLES['debate_analyst'],
        "analysis_period": analysis_period
    })

    debate_report = parse_debate_report(result.content)
    logger.info(f"辩论分析师报告: {debate_report}")
    
    # 保存分析报告
    result_manager.save_report(stock_code, "debate", debate_report, "debate_report", end_date)
    
    return {"debate_report": debate_report}

def run_supervisor(state: StockAgentState) -> dict:
    stock_code = state['stock_code']
    end_date = state.get('end_date')
    analysis_period = _get_analysis_period(end_date)
    
    # 读取新闻合并摘要（优先），若不可用则回退为空
    news_summary = ""
    try:
        news_data = result_manager.load_news_data(stock_code, end_date)
        if isinstance(news_data, dict):
            # 可能直接是 final_result 结构
            if news_data.get("combined_summary"):
                news_summary = news_data["combined_summary"]
            elif news_data.get("result"):
                news_summary = news_data.get("result", "")
        elif isinstance(news_data, str):
            news_summary = news_data
    except Exception:
        pass

    chain = create_analyst_chain(SUPERVISOR_PROMPT)
    result = chain.invoke({
        "stock_code": stock_code,
        "fundamental_report": state.get('fundamental_report', {}),
        "technical_report": state.get('technical_report', {}),
        "sentiment_report": state.get('sentiment_report', {}),
        "fund_report": state.get('fund_report', {}),
        "news_summary": news_summary,
        "role_description": AGENT_ROLES['supervisor'],
        "analysis_period": analysis_period
    })
    supervisor_report = parse_supervisor_report(result.content)
    logger.info(f"总决策分析师报告: {supervisor_report}")
    
    # 保存分析报告
    result_manager.save_report(stock_code, "supervisor", supervisor_report, "supervisor_report", end_date)
    
    return {"supervisor_report": supervisor_report}