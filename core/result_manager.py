import os
import json
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from logging import getLogger

logger = getLogger(__name__)

class ResultManager:
    """结果管理器：负责保存所有分析报告和工具结果到文件系统"""
    
    def __init__(self, base_dir: str = "result"):
        self.base_dir = base_dir
        self._ensure_base_dir()
    
    def _ensure_base_dir(self):
        """确保基础目录存在"""
        if not os.path.exists(self.base_dir):
            os.makedirs(self.base_dir)
            logger.info(f"创建基础目录: {self.base_dir}")
    
    def _get_stock_dir(self, stock_code: str) -> str:
        """获取股票代码对应的目录路径"""
        stock_dir = os.path.join(self.base_dir, stock_code)
        if not os.path.exists(stock_dir):
            os.makedirs(stock_dir)
            logger.info(f"创建股票目录: {stock_dir}")
        return stock_dir
    
    def _get_date_dir(self, stock_dir: str, end_date: Optional[str] = None) -> str:
        """获取日期对应的目录路径"""
        logger.info(f"_get_date_dir 被调用 - stock_dir: {stock_dir}, end_date: {end_date}")
        
        if end_date:
            try:
                # 解析传入的日期
                parsed_date = self._parse_date(end_date)
                date_str = parsed_date.strftime("%Y%m%d")
                logger.info(f"解析传入日期成功: {end_date} -> {date_str}")
            except Exception as e:
                # 如果解析失败，使用当前日期
                logger.warning(f"解析传入日期失败: {e}, 使用当前日期")
                date_str = datetime.now().strftime("%Y%m%d")
        else:
            # 如果没有传入日期，使用当前日期
            logger.info("没有传入end_date，使用当前日期")
            date_str = datetime.now().strftime("%Y%m%d")
        
        date_dir = os.path.join(stock_dir, date_str)
        logger.info(f"最终日期目录路径: {date_dir}")
        
        if not os.path.exists(date_dir):
            os.makedirs(date_dir)
            logger.info(f"创建日期目录: {date_dir}")
        return date_dir
    
    def _parse_date(self, date_str: str) -> datetime:
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

    def _get_analysis_period(self, end_date: str) -> str:
        """获取分析时间段描述"""
        try:
            if end_date:
                end_dt = self._parse_date(end_date)
                # 精确计算两年前的日期
                start_dt = end_dt.replace(year=end_dt.year - 2)
                start_date = start_dt.strftime("%Y-%m-%d")
                return f"{start_date} 至 {end_dt.strftime('%Y-%m-%d')}"
            else:
                # 如果没有提供end_date，返回默认描述
                return "近两年数据"
        except Exception as e:
            logger.warning(f"解析日期失败: {e}")
            return "近两年数据"
    
    def save_report(self, stock_code: str, report_type: str, report_data: Any, 
                   report_name: Optional[str] = None, end_date: Optional[str] = None) -> str:
        """
        保存分析报告（以JSON格式）
        
        Args:
            stock_code: 股票代码
            report_type: 报告类型 (fundamental, technical, sentiment, news, fund, bull, bear, debate, supervisor)
            report_data: 报告数据
            report_name: 报告名称，如果为None则使用默认名称
            end_date: 分析结束日期，用于计算分析时间段
        
        Returns:
            保存的文件路径
        """
        try:
            stock_dir = self._get_stock_dir(stock_code)
            logger.info(f"保存报告 - stock_code: {stock_code}, end_date: {end_date}")
            date_dir = self._get_date_dir(stock_dir, end_date)
            logger.info(f"生成的日期目录: {date_dir}")
            
            # 生成文件名
            if report_name is None:
                report_name = f"{report_type}_report"
            
            filename = f"{report_name}.json"
            filepath = os.path.join(date_dir, filename)
            
            # 生成JSON内容
            from datetime import datetime as _dt
            payload = {
                "report_type": report_type,
                "timestamp": _dt.now().strftime("%Y-%m-%d %H:%M:%S"),
                "analysis_period": self._get_analysis_period(end_date),
                "data": report_data
            }
            # 保存JSON文件
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
            
            logger.info(f"报告保存成功: {filepath}")
            return filepath
            
        except Exception as e:
            logger.error(f"保存报告失败: {e}")
            return ""
    
    def save_tool_result(self, stock_code: str, tool_name: str, tool_result: str, end_date: Optional[str] = None) -> str:
        """
        保存工具执行结果
        
        Args:
            stock_code: 股票代码
            tool_name: 工具名称
            tool_result: 工具执行结果
        
        Returns:
            保存的文件路径
        """
        try:
            stock_dir = self._get_stock_dir(stock_code)
            date_dir = self._get_date_dir(stock_dir, end_date)
            
            # 生成文件名
            filename = f"{tool_name}_tool_result.json"
            filepath = os.path.join(date_dir, filename)

            # 构造JSON内容
            from datetime import datetime as _dt
            payload = {
                "tool": tool_name,
                "timestamp": _dt.now().strftime("%Y-%m-%d %H:%M:%S"),
                "analysis_period": self._get_analysis_period(end_date)
            }
            
            # 处理工具结果
            if isinstance(tool_result, (dict, list)):
                # 如果已经是结构化数据，直接使用
                payload["data"] = tool_result
            elif isinstance(tool_result, str):
                try:
                    # 尝试解析JSON字符串
                    parsed = json.loads(tool_result)
                    payload["data"] = parsed
                except json.JSONDecodeError:
                    # 如果不是JSON，作为文本存储
                    payload["text"] = tool_result
            else:
                payload["text"] = str(tool_result)
            # 保存JSON文件
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)

            logger.info(f"工具结果保存成功: {filepath}")
            return filepath

        except Exception as e:
            logger.error(f"保存工具结果失败: {e}")
            return ""
    
    
    def save_all_reports(self, stock_code: str, state: Dict[str, Any]) -> Dict[str, str]:
        """
        保存所有分析报告
        
        Args:
            stock_code: 股票代码
            state: 包含所有报告的状态字典
        
        Returns:
            保存的文件路径字典
        """
        saved_files = {}
        end_date = state.get('end_date')
        
        # 保存各种分析报告
        report_mapping = {
            'fundamental_report': 'fundamental',
            'technical_report': 'technical', 
            'sentiment_report': 'sentiment',
            'news_report': 'news',
            'fund_report': 'fund',
            'bull_report': 'bull',
            'bear_report': 'bear',
            'debate_report': 'debate',
            'supervisor_report': 'supervisor'
        }
        
        for report_key, report_type in report_mapping.items():
            if report_key in state and state[report_key]:
                filepath = self.save_report(stock_code, report_type, state[report_key], end_date=end_date)
                if filepath:
                    saved_files[report_key] = filepath
        
        logger.info(f"所有报告保存完成，共保存 {len(saved_files)} 个文件")
        return saved_files
    
    def load_tool_result(self, stock_code: str, tool_name: str, end_date: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        加载已保存的工具结果
        
        Args:
            stock_code: 股票代码
            tool_name: 工具名称
            end_date: 结束日期
        
        Returns:
            工具结果数据，如果不存在则返回None
        """
        try:
            stock_dir = self._get_stock_dir(stock_code)
            date_dir = self._get_date_dir(stock_dir, end_date)
            
            # 生成文件名
            filename = f"{tool_name}_tool_result.json"
            filepath = os.path.join(date_dir, filename)
            
            if not os.path.exists(filepath):
                logger.info(f"工具结果文件不存在: {filepath}")
                return None
            
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
                logger.info(f"成功加载工具结果: {filepath}")
                return data
                
        except Exception as e:
            logger.error(f"加载工具结果失败: {e}")
            return None
    
    def load_news_data(self, stock_code: str, end_date: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        加载新闻数据，优先从结构化数据读取
        
        Args:
            stock_code: 股票代码
            end_date: 结束日期
        
        Returns:
            新闻数据，如果不存在则返回None
        """
        try:
            # 优先尝试加载结构化数据
            structured_data = self.load_tool_result(stock_code, "news_sentiment_structured", end_date)
            if structured_data and structured_data.get("data"):
                logger.info(f"成功从结构化数据加载新闻数据: {stock_code}")
                return structured_data
            
            # 如果结构化数据不存在，尝试加载基础新闻数据
            basic_data = self.load_tool_result(stock_code, "news_sentiment", end_date)
            if basic_data:
                logger.info(f"成功从基础数据加载新闻数据: {stock_code}")
                return basic_data
            
            logger.info(f"未找到新闻数据: {stock_code}")
            return None
            
        except Exception as e:
            logger.error(f"加载新闻数据失败: {e}")
            return None

    def get_result_summary(self, stock_code: str) -> str:
        """
        获取结果目录的摘要信息
        
        Args:
            stock_code: 股票代码
        
        Returns:
            摘要信息
        """
        try:
            stock_dir = self._get_stock_dir(stock_code)
            if not os.path.exists(stock_dir):
                return f"股票 {stock_code} 暂无结果文件"
            
            summary = f"# 股票 {stock_code} 分析结果摘要\n\n"
            
            # 遍历日期目录
            for date_dir in sorted(os.listdir(stock_dir), reverse=True):
                date_path = os.path.join(stock_dir, date_dir)
                if os.path.isdir(date_path):
                    summary += f"## {date_dir} 分析结果\n\n"
                    
                    # 统计文件数量
                    files = [f for f in os.listdir(date_path) if f.endswith('.md') or f.endswith('.json')]
                    summary += f"**文件总数**: {len(files)}\n\n"

                    # 列出所有文件
                    for file in sorted(files):
                        summary += f"- {file}\n"

                    summary += "\n"
            
            return summary
            
        except Exception as e:
            logger.error(f"获取结果摘要失败: {e}")
            return f"获取结果摘要失败: {e}"

# 创建全局实例
result_manager = ResultManager() 