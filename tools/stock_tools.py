from langchain_core.tools import tool
from logging import getLogger
from core.cache_manager import cache_manager
from core.data_processor import DataProcessor
from core.result_manager import result_manager
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import json as _json
from typing import Optional

logger = getLogger(__name__)

# 延迟导入cache_manager以避免循环导入问题
def get_cache_manager():
    try:
        return cache_manager
    except ImportError:
        logger.error("无法导入cache_manager")
        return None

def get_data_processor():
    return DataProcessor()


"""--------------------------------- 基本面分析工具 ---------------------------------"""

def _process_data_type(provider, processor, dtype, objective, stock_code, end_date=None):
    """并行处理单个数据类型"""
    try:
        # 数据获取方法映射表
        data_fetch_methods = {
            'fina_indicator': 'fetch_fina_indicator_data',
            'daily_basic': 'fetch_daily_basic_data',
            'dividend': 'fetch_dividend_data',
            'income': 'fetch_income_data',
            'balance': 'fetch_balance_data',
            'cashflow': 'fetch_cashflow_data',
            'forecast': 'fetch_forecast_data',
            'express': 'fetch_express_data',
            'mainbz': 'fetch_mainbz_data',
            'pro_bar': 'fetch_pro_bar_data',
            'pro_bar_W': 'fetch_pro_bar_data',
            'pro_bar_M': 'fetch_pro_bar_data',
            'stk_factor': 'fetch_stk_factor_data',
            'limit_list': 'fetch_limit_list_data',
            'top10_holders': 'fetch_top10_holders_data',
            'top10_floatholders': 'fetch_top10_floatholders_data',
            'stk_holdernumber': 'fetch_stk_holdernumber_data',
            'moneyflow_ths': 'fetch_moneyflow_ths_data',
            'moneyflow_cnt_ths': 'fetch_moneyflow_cnt_ths_data',
            'moneyflow_ind_ths': 'fetch_moneyflow_ind_ths_data',
            'moneyflow_mkt_dc': 'fetch_moneyflow_mkt_dc_data',
            'moneyflow_ind_dc': 'fetch_moneyflow_ind_dc_data',
            'top_list': 'fetch_top_list_data',
            'top_inst': 'fetch_top_inst_data',
            'moneyflow_hsgt': 'fetch_moneyflow_hsgt_data',
            'cyq_perf': 'fetch_cyq_perf_data',
        }
        
        # 使用字典映射获取对应的获取方法
        if dtype in data_fetch_methods:
            method_name = data_fetch_methods[dtype]
            method = getattr(provider, method_name)
            
            # 特殊处理不需要stock_code的接口
            if dtype == 'moneyflow_hsgt':
                df = method(end_date)
            else:
                df = method(stock_code, end_date)
        else:
            logger.error(f"错误: 不支持的数据类型 '{dtype}'")
            return { "summary": f"【{objective}】: 不支持的数据类型 '{dtype}'", "raw": [] }

        if df is not None and not df.empty:
            summary = processor.process_and_summarize(df, objective)
            raw_json = df.to_dict(orient='records')
            return { "summary": summary, "raw": raw_json }
        else:
            # 数据为空时，返回具体的空数据信息（这不是错误）
            from datetime import datetime, timedelta

            # 计算日期范围
            if end_date:
                try:
                    end_date_dt = datetime.strptime(end_date, '%Y%m%d')
                    start_date_dt = end_date_dt - timedelta(days=365 * 2)  # 默认2年
                    start_date = start_date_dt.strftime('%Y%m%d')
                    date_range = f"{start_date}到{end_date}"
                except ValueError:
                    date_range = f"指定日期范围内"
            else:
                date_range = "近两年"

            logger.info(f"{dtype}数据为空（正常情况）")
            
            # 为特定数据类型提供更详细的说明
            if dtype == 'moneyflow_cnt_ths':
                return { "summary": f"【{objective}】: 在{date_range}之间{objective}数据为空。板块主力动向数据通常有1-2天延迟，建议查询前一个交易日的数据。", "raw": [] }
            elif dtype == 'moneyflow_hsgt':
                return { "summary": f"【{objective}】: 在{date_range}之间{objective}数据为空。北向资金数据通常有1天延迟，建议查询前一个交易日的数据。", "raw": [] }
            else:
                return { "summary": f"【{objective}】: 在{date_range}之间{objective}数据为空", "raw": [] }
    except Exception as e:
        # 这是真正的错误情况
        logger.error(f"处理数据类型 {dtype} 时出错: {e}")
        return { "summary": f"【{objective}】: 数据获取失败 - {e}", "raw": [] }

def _process_company_info(processor, company_basic_info):
    """并行处理公司基本信息"""
    try:
        if company_basic_info.get('stock_basic') or company_basic_info.get('company_detail'):
            return processor.process_company_info(company_basic_info)
        return None
    except Exception as e:
        logger.error(f"处理公司基本信息时出错: {e}")
        return None


@tool
def get_fundamental_data(stock_code: str, end_date: Optional[str] = None) -> str:
    """评估公司的内在价值，关注盈利能力、财务健康状况和长期增长潜力。"""
    try:
        logger.info(f"get_fundamental_data: {stock_code}")
        
        # 获取cache_manager实例
        cache_manager = get_cache_manager()
        
        # 确保cache_manager已初始化
        if not cache_manager or not getattr(cache_manager, "provider", None):
            logger.info("cache_manager未初始化，开始初始化...")
            try:
                cache_manager.initialize()
                logger.info("cache_manager初始化成功")
            except Exception as e:
                logger.error(f"cache_manager初始化失败: {e}")
                error_result = f"cache_manager初始化失败: {e}"
                result_manager.save_tool_result(stock_code, "fundamental_data", error_result, end_date=end_date)
                return error_result

        # 获取数据提供者和处理器
        provider = cache_manager.get_provider()
        if not provider:
            error_result = "数据提供者未初始化"
            result_manager.save_tool_result(stock_code, "fundamental_data", error_result, end_date=end_date)
            return error_result
            
        processor = get_data_processor()

        # 1. 获取公司基本信息（这个需要先获取，因为其他处理可能需要用到）
        company_basic_info = cache_manager.get_company_basic_info(stock_code)
        
        # 2. 定义需要分析的数据和对应的目标
        data_objectives = {
            'fina_indicator': '盈利能力与财务指标',
            'daily_basic': '每日估值水平',
            'dividend': '股东分红回报',
            'income': '营业收入与利润构成',
            'balance': '资产与负债结构',
            'cashflow': '现金流量质量',
            'forecast': '未来业绩预期',
            'mainbz': '主营业务构成'
        }
        
        # 3. 并行获取、处理和摘要所有数据
        interface_results = {}
        company_summaries = []
        
        # 使用线程池并行处理
        with ThreadPoolExecutor(max_workers=min(len(data_objectives) + 1, 10)) as executor:
            # 提交所有数据处理任务
            future_to_dtype = {}
            for dtype, objective in data_objectives.items():
                future = executor.submit(_process_data_type, provider, processor, dtype, objective, stock_code, end_date)
                future_to_dtype[future] = dtype
            
            # 提交公司基本信息处理任务
            company_future = executor.submit(_process_company_info, processor, company_basic_info)
            
            # 收集所有结果
            for future in as_completed(future_to_dtype):
                dtype = future_to_dtype[future]
                try:
                    payload = future.result()
                    if payload:
                        interface_results[dtype] = {
                            "objective": data_objectives[dtype],
                            "result": payload.get("summary", ""),
                            "raw": payload.get("raw", []),
                            "status": "success" if not any(err in payload.get("summary", "") for err in ["生成报告时出错", "生成摘要时出错", "数据获取失败", "Error code:"]) else "error"
                        }
                        logger.info(f"完成数据类型 {dtype} 的处理")
                except Exception as e:
                    logger.error(f"获取数据类型 {dtype} 的结果时出错: {e}")
                    interface_results[dtype] = {
                        "objective": data_objectives[dtype],
                        "result": f"处理失败: {e}",
                        "raw": [],
                        "status": "error"
                    }
            
            # 获取公司基本信息处理结果
            try:
                company_summary = company_future.result()
                if company_summary:
                    company_summaries.append(company_summary)
                    logger.info("完成公司基本信息的处理")
            except Exception as e:
                logger.error(f"获取公司基本信息处理结果时出错: {e}")
        
        # 4. 组合最终结果，为每个接口创建独立字段
        final_result = {
            "analysis_type": "基本面数据分析",
            "company_overview": company_summaries if company_summaries else [],
            "interfaces": interface_results,
            "summary": {
                "total_interfaces": len(interface_results),
                "successful_interfaces": len([r for r in interface_results.values() if r["status"] == "success"]),
                "error_interfaces": len([r for r in interface_results.values() if r["status"] == "error"])
            }
        }
        
        logger.info(f"[FINAL RESULT for {stock_code}]\n{final_result}")
        
        # 6. 保存工具执行结果
        result_manager.save_tool_result(stock_code, "fundamental_data", final_result, end_date=end_date)
        
        # 为了向后兼容，也返回文本格式
        text_result = _format_result_as_text(final_result)
        return text_result

    except Exception as e:
        logger.info(f"[ERROR] 获取基本面数据时出错: {e}")
        error_result = f"获取行情数据时出错: {e}"
        # 保存错误结果
        result_manager.save_tool_result(stock_code, "fundamental_data", error_result, end_date=end_date)
        return error_result


"""--------------------------------- 资金流向分析工具 ---------------------------------"""

def _get_mock_fund_data(stock_code: str) -> dict:
    """返回结构化的占位数据，避免保存为纯文本。"""
    return {
        "analysis_type": "资金流向数据分析",
        "interfaces": {},
        "summary": {
            "total_interfaces": 0,
            "successful_interfaces": 0,
            "error_interfaces": 0
        },
        "note": {
            "type": "mock",
            "message": [
                f"【资金流向】{stock_code} 主力资金持续流入，机构资金大幅增持，散户小幅流入",
                f"【资金结构】{stock_code} 主力资金占比70%，机构25%，散户5%",
                f"【资金趋势】{stock_code} 资金流入趋势持续上升，市场热度提升"
            ]
        }
    }

def _process_fund_data_with_llm(processor, tushare_provider, stock_code, end_date, data_type, objective):
    """获取单个资金流向数据并交给LLM总结"""
    try:
        logger.info(f"开始处理数据类型: {data_type}, 目标: {objective}")

        if data_type == 'top10_holders':
            df = tushare_provider.fetch_top10_holders_data(stock_code, end_date)
            logger.info(f"获取{data_type}数据成功，数据形状: {df.shape if df is not None else 'None'}")
        elif data_type == 'top10_floatholders':
            df = tushare_provider.fetch_top10_floatholders_data(stock_code, end_date)
            logger.info(f"获取{data_type}数据成功，数据形状: {df.shape if df is not None else 'None'}")
        elif data_type == 'stk_holdernumber':
            df = tushare_provider.fetch_stk_holdernumber_data(stock_code, end_date)
            logger.info(f"获取{data_type}数据成功，数据形状: {df.shape if df is not None else 'None'}")
        elif data_type == 'moneyflow_ths':
            df = tushare_provider.fetch_moneyflow_ths_data(stock_code, end_date)
            logger.info(f"获取{data_type}数据成功，数据形状: {df.shape if df is not None else 'None'}")
        elif data_type == 'moneyflow_cnt_ths':
            df = tushare_provider.fetch_moneyflow_cnt_ths_data(stock_code, end_date)
            logger.info(f"获取{data_type}数据成功，数据形状: {df.shape if df is not None else 'None'}")
        elif data_type == 'moneyflow_ind_ths':
            df = tushare_provider.fetch_moneyflow_ind_ths_data(stock_code, end_date)
            logger.info(f"获取{data_type}数据成功，数据形状: {df.shape if df is not None else 'None'}")
        elif data_type == 'moneyflow_mkt_dc':
            df = tushare_provider.fetch_moneyflow_mkt_dc_data(stock_code, end_date)
            logger.info(f"获取{data_type}数据成功，数据形状: {df.shape if df is not None else 'None'}")
        elif data_type == 'moneyflow_ind_dc':
            df = tushare_provider.fetch_moneyflow_ind_dc_data(stock_code, end_date)
            logger.info(f"获取{data_type}数据成功，数据形状: {df.shape if df is not None else 'None'}")
        elif data_type == 'top_list':
            df = tushare_provider.fetch_top_list_data(stock_code, end_date)
            logger.info(f"获取{data_type}数据成功，数据形状: {df.shape if df is not None else 'None'}")
        elif data_type == 'top_inst':
            df = tushare_provider.fetch_top_inst_data(stock_code, end_date)
            logger.info(f"获取{data_type}数据成功，数据形状: {df.shape if df is not None else 'None'}")
        elif data_type == 'moneyflow_hsgt':
            df = tushare_provider.fetch_moneyflow_hsgt_data(end_date)
            logger.info(f"获取{data_type}数据成功，数据形状: {df.shape if df is not None else 'None'}")
        elif data_type == 'cyq_perf':
            df = tushare_provider.fetch_cyq_perf_data(stock_code, end_date)
            logger.info(f"获取{data_type}数据成功，数据形状: {df.shape if df is not None else 'None'}")
        else:
            logger.warning(f"未定义的数据类型: {data_type}")
            return { "summary": f"未定义的数据类型: {data_type}", "raw": [] }

        if df is not None and not df.empty:
            logger.info(f"处理{data_type}数据成功，行数: {len(df)}")
            raw_json = df.to_dict(orient='records')
            return {
                "summary": f"【{objective}】\n{processor._analyze_fund_table(df, objective)}",
                "raw": raw_json
            }
        else:
            # 数据为空时，返回具体的空数据信息（这不是错误）
            from datetime import datetime, timedelta

            # 计算日期范围
            if end_date:
                try:
                    end_date_dt = datetime.strptime(end_date, '%Y%m%d')
                    start_date_dt = end_date_dt - timedelta(days=365 * 2)  # 默认2年
                    start_date = start_date_dt.strftime('%Y%m%d')
                    prefix = f"{start_date}到{end_date}之间"
                except ValueError:
                    prefix = "指定日期范围内"
            else:
                prefix = "近两年内"

            logger.info(f"{data_type}数据为空（正常情况）")
            return {
                "summary": f"【{objective}】: {prefix}{objective}数据为空",
                "raw": []
            }
    except Exception as e:
        # 这是真正的错误情况
        logger.error(f"处理 {data_type} 出错: {e}")
        return {
            "summary": f"【{objective}】: 数据获取失败 - {e}",
            "raw": []
        }

@tool
def get_fund_data(stock_code: str, end_date: Optional[str] = None) -> str:
    """追踪市场中各类资金的流向，判断主力资金意图。"""
    try:
        # 获取cache_manager实例
        cache_manager = get_cache_manager()
        
        # 确保cache_manager已初始化
        if not cache_manager or not getattr(cache_manager, "provider", None):
            logger.info("cache_manager未初始化，开始初始化...")
            try:
                cache_manager.initialize()
                logger.info("cache_manager初始化成功")
            except Exception as e:
                logger.error(f"cache_manager初始化失败: {e}")
                error_result = {"analysis_type": "资金流向数据分析", "error": f"cache_manager初始化失败: {e}"}
                result_manager.save_tool_result(stock_code, "fund_data", error_result, end_date=end_date)
                return _json.dumps(error_result, ensure_ascii=False)
        
        # 获取数据提供者
        provider = cache_manager.get_provider()
        if not provider:
            logger.warning("数据提供者未初始化，使用模拟数据")
            mock = _get_mock_fund_data(stock_code)
            result_manager.save_tool_result(stock_code, "fund_data", mock, end_date=end_date)
            return _json.dumps(mock, ensure_ascii=False)

        tushare_provider = provider

        processor = get_data_processor()

        '''主力动向、龙虎榜(包含换手率)、北向资金、每日筹码'''
        fund_data_objectives = {
            'top10_holders': '前十大股东持股情况',
            'top10_floatholders': '前十大流通股东持股情况',
            'stk_holdernumber': '股东人数',

            'moneyflow_ths': '个股主力动向',
            'moneyflow_cnt_ths': '板块主力动向',
            'moneyflow_ind_ths': '行业主力动向',
            'moneyflow_mkt_dc': '大盘资金流向',
            'moneyflow_ind_dc': '板块资金流向',

            'top_list': '龙虎榜每日统计',
            'top_inst': '龙虎榜机构明细',

            'moneyflow_hsgt': '北向资金',

            'cyq_perf': '每日筹码分布',
        }

        interface_results = {}
        with ThreadPoolExecutor(max_workers=min(len(fund_data_objectives) + 1, 10)) as executor:
            futures = {
                executor.submit(
                    _process_fund_data_with_llm,
                    processor,
                    tushare_provider,
                    stock_code,
                    end_date,
                    dtype,
                    obj
                ): dtype
                for dtype, obj in fund_data_objectives.items()
            }

            for future in as_completed(futures):
                dtype = futures[future]
                payload = future.result()
                if payload:
                    interface_results[dtype] = {
                        "objective": fund_data_objectives[dtype],
                        "result": payload.get("summary", ""),
                        "raw": payload.get("raw", []),
                        "status": "success" if not any(err in payload.get("summary", "") for err in ["生成报告时出错", "生成摘要时出错", "数据获取失败", "Error code:"]) else "error"
                    }

        # 组合最终结果，为每个接口创建独立字段
        final_result = {
            "analysis_type": "资金流向数据分析",
            "interfaces": interface_results,
            "summary": {
                "total_interfaces": len(interface_results),
                "successful_interfaces": len([r for r in interface_results.values() if r["status"] == "success"]),
                "error_interfaces": len([r for r in interface_results.values() if r["status"] == "error"])
            }
        }
        
        logger.info(f"[FUND FINAL RESULT for {stock_code}]\n{final_result}")

        result_manager.save_tool_result(stock_code, "fund_data", final_result, end_date=end_date)
        
        # 为了向后兼容，也返回文本格式
        text_result = _format_result_as_text(final_result)
        return text_result

    except Exception as e:
        error_result = {"analysis_type": "资金流向数据分析", "error": f"获取资金流向时出错: {e}"}
        result_manager.save_tool_result(stock_code, "fund_data", error_result, end_date=end_date)
        return _json.dumps(error_result, ensure_ascii=False)


"""--------------------------------- 技术面分析工具 ---------------------------------"""

def _get_mock_tech_data(stock_code: str) -> str:
    return "\n".join([
        f"【技术指标】{stock_code} MACD金叉，RSI超卖，KDJ低位金叉",
        f"【K线形态】{stock_code} 形成头肩底形态，突破颈线位，上涨趋势明显",
        f"【成交量】{stock_code} 成交量放大，市场关注度提升"
    ])


def _process_tech_data_with_llm(processor, tushare_provider, stock_code, end_date, data_type, objective):
    """获取单个技术数据并交给LLM总结"""
    try:
        logger.info(f"开始处理数据类型: {data_type}, 目标: {objective}")

        if data_type == 'pro_bar_D':
            df = tushare_provider.fetch_pro_bar_data(stock_code, end_date, freq="D")
            logger.info(f"获取{data_type}数据成功，数据形状: {df.shape if df is not None else 'None'}")
        elif data_type == 'pro_bar_W':
            df = tushare_provider.fetch_pro_bar_data(stock_code, end_date, freq="W")
            logger.info(f"获取{data_type}数据成功，数据形状: {df.shape if df is not None else 'None'}")
        elif data_type == 'pro_bar_M':
            df = tushare_provider.fetch_pro_bar_data(stock_code, end_date, freq="M")
            logger.info(f"获取{data_type}数据成功，数据形状: {df.shape if df is not None else 'None'}")
        elif data_type == 'stk_factor':
            df = tushare_provider.fetch_stk_factor_data(stock_code, end_date)
            logger.info(f"获取{data_type}数据成功，数据形状: {df.shape if df is not None else 'None'}")
        elif data_type == 'daily_basic':
            df = tushare_provider.fetch_daily_basic_enhanced(stock_code, end_date)
            logger.info(f"获取{data_type}数据成功，数据形状: {df.shape if df is not None else 'None'}")
        elif data_type == 'limit_list':
            df = tushare_provider.fetch_limit_list_data(stock_code)
            logger.info(f"获取{data_type}数据成功，数据形状: {df.shape if df is not None else 'None'}")
        else:
            logger.warning(f"未定义的数据类型: {data_type}")
            return { "summary": f"未定义的数据类型: {data_type}", "raw": [] }

        if df is not None and not df.empty:
            logger.info(f"处理{data_type}数据成功，行数: {len(df)}")
            raw_json = df.to_dict(orient='records')
            return {
                "summary": f"【{objective}】\n{processor._analyze_tech_table(df, objective)}",
                "raw": raw_json
            }
        else:
            # 数据为空时，返回具体的空数据信息（这不是错误）
            from datetime import datetime, timedelta

            # 计算日期范围
            if end_date:
                try:
                    end_date_dt = datetime.strptime(end_date, '%Y%m%d')
                    start_date_dt = end_date_dt - timedelta(days=365 * 2)  # 默认2年
                    start_date = start_date_dt.strftime('%Y%m%d')
                    prefix = f"{start_date}到{end_date}之间"
                except ValueError:
                    prefix = "指定日期范围内"
            else:
                prefix = "近两年内"

            logger.info(f"{data_type}数据为空（正常情况）")
            return {
                "summary": f"【{objective}】: {prefix}{objective}数据为空",
                "raw": []
            }
    except Exception as e:
        # 这是真正的错误情况
        logger.error(f"处理 {data_type} 出错: {e}")
        return {
            "summary": f"【{objective}】: 数据获取失败 - {e}",
            "raw": []
        }


@tool
def get_tech_data(stock_code: str, end_date: Optional[str] = None) -> str:
    """通过LLM分析历史价格、成交量和技术指标，预测未来走势。"""
    try:
        cache_manager = get_cache_manager()
        
        # 确保cache_manager已初始化
        if not cache_manager or not getattr(cache_manager, "provider", None):
            logger.info("cache_manager未初始化，开始初始化...")
            try:
                cache_manager.initialize()
                logger.info("cache_manager初始化成功")
            except Exception as e:
                logger.error(f"cache_manager初始化失败: {e}")
                mock = _get_mock_tech_data(stock_code)
                result_manager.save_tool_result(stock_code, "tech_data", mock, end_date=end_date)
                return mock
        
        # 获取数据提供者
        provider = cache_manager.get_provider()
        if not provider:
            logger.warning("数据提供者未初始化，使用模拟数据")
            mock = _get_mock_tech_data(stock_code)
            result_manager.save_tool_result(stock_code, "tech_data", mock, end_date=end_date)
            return mock

        # 检查provider类型，确保有必要的技术分析方法
        if not hasattr(provider, 'fetch_pro_bar_data'):
            logger.warning(f"provider类型不正确: {type(provider)}，缺少必要的方法，使用模拟数据")
            mock = _get_mock_tech_data(stock_code)
            result_manager.save_tool_result(stock_code, "tech_data", mock, end_date=end_date)
            return mock

        tushare_provider = provider
        processor = get_data_processor()

        tech_data_objectives = {
            'pro_bar_D': '短期（日线K线与均线走势）',
            'pro_bar_W': '中期（周线K线与均线走势）',
            'pro_bar_M': '长期（月线K线与均线走势）',
            'stk_factor': '技术指标（MACD/RSI/KDJ等）',
            'daily_basic': '估值与成交特征',
            'limit_list': '涨跌停与市场情绪'
        }

        interface_results = {}
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {
                executor.submit(
                    _process_tech_data_with_llm,
                    processor,
                    tushare_provider,
                    stock_code,
                    end_date,
                    dtype,
                    obj
                ): dtype
                for dtype, obj in tech_data_objectives.items()
            }

            for future in as_completed(futures):
                dtype = futures[future]
                payload = future.result()
                if payload:
                    interface_results[dtype] = {
                        "objective": tech_data_objectives[dtype],
                        "result": payload.get("summary", ""),
                        "raw": payload.get("raw", []),
                        "status": "success" if not any(err in payload.get("summary", "") for err in ["生成报告时出错", "生成摘要时出错", "数据获取失败", "Error code:"]) else "error"
                    }

        # 组合最终结果，为每个接口创建独立字段
        final_result = {
            "analysis_type": "技术数据分析",
            "interfaces": interface_results,
            "summary": {
                "total_interfaces": len(interface_results),
                "successful_interfaces": len([r for r in interface_results.values() if r["status"] == "success"]),
                "error_interfaces": len([r for r in interface_results.values() if r["status"] == "error"])
            }
        }
        
        logger.info(f"[TECH FINAL RESULT for {stock_code}]\n{final_result}")

        result_manager.save_tool_result(stock_code, "tech_data", final_result, end_date=end_date)
        
        # 为了向后兼容，也返回文本格式
        text_result = _format_result_as_text(final_result)
        return text_result

    except Exception as e:
        error_result = f"获取技术数据时出错: {e}"
        result_manager.save_tool_result(stock_code, "tech_data", error_result, end_date=end_date)
        return error_result



"""--------------------------------- 新闻分析工具 ---------------------------------"""



def _get_mock_news_data(stock_code: str) -> str:
    return "\n".join([
        f"【新闻摘要】{stock_code} 相关新闻显示公司业绩稳健增长，市场关注度提升",
        f"【情绪分析】{stock_code} 新闻情绪偏向正面，投资者信心增强",
        f"【重要事件】{stock_code} 近期发布重要公告，对股价产生积极影响"
    ])

def _process_news_data_with_llm(processor, news_provider, stock_code, end_date, data_type, objective):
    """获取单个新闻数据并交给LLM总结"""
    try:
        logger.info(f"开始处理新闻数据类型: {data_type}, 目标: {objective}")
        if data_type == 'news':
            df = news_provider.fetch_news(src='cls', end_date=end_date, days=3)
            logger.info(f"获取{data_type}数据成功，数据形状: {df.shape if df is not None else 'None'}")
        elif data_type == 'major_news':
            df = news_provider.fetch_major_news(end_date=end_date, days=3)
            logger.info(f"获取{data_type}数据成功，数据形状: {df.shape if df is not None else 'None'}")
        elif data_type == 'cctv_news':
            df = news_provider.fetch_cctv_news_recent(days=3)
            logger.info(f"获取{data_type}数据成功，数据形状: {df.shape if df is not None else 'None'}")
        else:
            logger.warning(f"未定义的新闻数据类型: {data_type}")
            return { "summary": f"未定义的新闻数据类型: {data_type}", "raw": [] }

        if df is not None and not df.empty:
            raw_json = df.to_dict(orient='records')
            if data_type == 'news':
                built_summary = f"【{objective}】\n" + processor.analyze_news_batched(
                    df, objective, max_chars=None, min_pack_chars=600, model_max_tokens=65000, input_ratio=0.55
                )
            elif data_type in ('major_news', 'cctv_news'):
                built_summary = f"【{objective}】\n" + processor.analyze_news_batched(
                    df, objective, max_chars=None, min_pack_chars=600, model_max_tokens=65000, input_ratio=0.65
                )
            else:
                built_summary = f"【{objective}】\n" + processor.analyze_news_batched(
                    df, objective, max_chars=None, min_pack_chars=600, model_max_tokens=65000, input_ratio=0.60
                )
            return { "summary": built_summary, "raw": raw_json }
        else:
            # 数据为空时，返回具体的空数据信息（这不是错误）
            from datetime import datetime, timedelta
            if end_date:
                try:
                    end_date_dt = datetime.strptime(end_date, '%Y%m%d')
                    start_date_dt = end_date_dt - timedelta(days=3)
                    start_date = start_date_dt.strftime('%Y%m%d')
                    prefix = f"{start_date}到{end_date}之间"
                except ValueError:
                    prefix = "指定日期范围内"
            else:
                prefix = "近3天内"
            logger.info(f"{data_type}数据为空（正常情况）")
            return { "summary": f"【{objective}】: {prefix}{objective}数据为空", "raw": [] }
    except Exception as e:
        logger.error(f"处理 {data_type} 出错: {e}")
        return { "summary": f"【{objective}】: 数据获取失败 - {e}", "raw": [] }

@tool
def get_news(stock_code: str, end_date: Optional[str] = None, lookback_days: int = 3) -> str:
    """获取和分析与股票相关的新闻信息，包括快讯、重要新闻和央视新闻。"""
    try:
        logger.info(f"get_news: {stock_code}")
        
        # 获取cache_manager实例
        cache_manager = get_cache_manager()
        
        # 确保cache_manager已初始化
        if not cache_manager or not getattr(cache_manager, "provider", None):
            logger.info("cache_manager未初始化，开始初始化...")
            try:
                cache_manager.initialize()
                logger.info("cache_manager初始化成功")
            except Exception as e:
                logger.error(f"cache_manager初始化失败: {e}")
                error_result = f"cache_manager初始化失败: {e}"
                result_manager.save_tool_result(stock_code, "news_data", error_result, end_date=end_date)
                return error_result

        # 获取新闻提供者
        news_provider = cache_manager.get_news_provider()
        if not news_provider:
            logger.warning("新闻提供者未初始化，使用模拟数据")
            mock = _get_mock_news_data(stock_code)
            result_manager.save_tool_result(stock_code, "news_data", mock, end_date=end_date)
            return mock

        processor = get_data_processor()

        # 定义需要分析的新闻数据类型和对应的目标
        news_data_objectives = {
            'news': '快讯新闻分析',
            'major_news': '重要新闻分析', 
            'cctv_news': '央视新闻分析'
        }

        # 并行获取、处理和摘要所有新闻数据
        interface_results = {}
        
        # 使用线程池并行处理
        with ThreadPoolExecutor(max_workers=min(len(news_data_objectives), 4)) as executor:
            # 提交所有新闻数据处理任务
            future_to_dtype = {}
            for dtype, objective in news_data_objectives.items():
                future = executor.submit(_process_news_data_with_llm, processor, news_provider, stock_code, end_date, dtype, objective)
                future_to_dtype[future] = dtype
            
            # 收集所有结果
            for future in as_completed(future_to_dtype):
                dtype = future_to_dtype[future]
                try:
                    payload = future.result()
                    if payload:
                        interface_results[dtype] = {
                            "objective": news_data_objectives[dtype],
                            "result": payload.get("summary", ""),
                            "raw": payload.get("raw", []),
                            "status": "success" if not any(err in payload.get("summary", "") for err in ["生成报告时出错", "生成摘要时出错", "数据获取失败", "Error code:"]) else "error"
                        }
                        logger.info(f"完成新闻数据类型 {dtype} 的处理")
                except Exception as e:
                    logger.error(f"获取新闻数据类型 {dtype} 的结果时出错: {e}")
                    interface_results[dtype] = {
                        "objective": news_data_objectives[dtype],
                        "result": f"处理失败: {e}",
                        "raw": [],
                        "status": "error"
                    }
        
        # 构建拼接后的整体摘要（短讯合并总结 + 重大新闻总结 + 央视新闻总结）
        combined_summaries = []
        if interface_results.get('news', {}).get('result'):
            combined_summaries.append(interface_results['news']['result'])
        if interface_results.get('major_news', {}).get('result'):
            combined_summaries.append(interface_results['major_news']['result'])
        if interface_results.get('cctv_news', {}).get('result'):
            combined_summaries.append(interface_results['cctv_news']['result'])
        overall_summary_text = "\n\n====\n\n".join(combined_summaries) if combined_summaries else "暂无新闻摘要"

        # 组合最终结果，为每个接口创建独立字段
        final_result = {
            "analysis_type": "新闻数据分析",
            "combined_summary": overall_summary_text,  # 供下游直接消费的一体化摘要
            "interfaces": interface_results,
            "summary": {
                "total_interfaces": len(interface_results),
                "successful_interfaces": len([r for r in interface_results.values() if r["status"] == "success"]),
                "error_interfaces": len([r for r in interface_results.values() if r["status"] == "error"])
            }
        }
        logger.info(f"[NEWS FINAL RESULT for {stock_code}]\n{final_result}")

        # 保存工具执行结果
        result_manager.save_tool_result(stock_code, "news_data", final_result, end_date=end_date)

        # 返回拼接好的整体摘要，满足上游对单一文本摘要的需求
        return overall_summary_text

    except Exception as e:
        logger.info(f"[ERROR] 获取新闻数据时出错: {e}")
        error_result = f"获取新闻数据时出错: {e}"
        # 保存错误结果
        result_manager.save_tool_result(stock_code, "news_data", error_result, end_date=end_date)
        return error_result


"""--------------------------------- 情绪分析工具 ---------------------------------"""



def _format_result_as_text(result_data: dict) -> str:
    """将结构化数据格式化为文本，用于向后兼容"""
    if not isinstance(result_data, dict):
        return str(result_data)
    
    text_parts = []
    
    # 添加公司概况
    if result_data.get("company_overview"):
        text_parts.append("公司概况：\n" + "\n---\n".join(result_data["company_overview"]))
    
    # 添加接口分析结果
    if result_data.get("interfaces"):
        analysis_type = result_data.get("analysis_type", "数据分析")
        interface_texts = []
        
        for interface_name, interface_data in result_data["interfaces"].items():
            objective = interface_data.get("objective", "")
            result = interface_data.get("result", "")
            status = interface_data.get("status", "unknown")
            
            # 添加状态标识
            status_prefix = "✅" if status == "success" else "❌" if status == "error" else "⚠️"
            interface_texts.append(f"{status_prefix}【{objective}】\n{result}")
        
        text_parts.append(f"{analysis_type}：\n" + "\n---\n".join(interface_texts))
    
    return "\n---\n".join(text_parts) if text_parts else str(result_data)