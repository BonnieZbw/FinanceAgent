# 文件: graph/parsers.py
# 描述: 包含所有用于解析不同类型报告的解析函数
# -----------------------------------------------------------------
import json
import logging

logger = logging.getLogger(__name__)

def parse_analyst_report(content: str) -> dict:
    """解析分析师报告内容，提取JSON并转换为AnalystReport对象"""
    try:
        # 清理内容，移除可能的markdown标记
        cleaned_content = content.strip()
        if '```json' in cleaned_content:
            # 提取JSON部分
            start_idx = cleaned_content.find('```json') + 7
            end_idx = cleaned_content.rfind('```')
            if end_idx > start_idx:
                json_str = cleaned_content[start_idx:end_idx].strip()
            else:
                json_str = cleaned_content[start_idx:].strip()
        else:
            json_str = cleaned_content
        
        # 解析JSON
        report_data = json.loads(json_str)
        
        # 验证必要字段
        required_fields = ['analyst_name', 'viewpoint', 'reason', 'scores', 'detailed_analysis']
        for field in required_fields:
            if field not in report_data:
                logger.warning(f"缺少必要字段: {field}")
                report_data[field] = "" if field != 'scores' else {}
        
        return report_data
    except json.JSONDecodeError as e:
        logger.error(f"JSON解析失败: {e}")
        logger.error(f"原始内容: {content}")
        # 返回默认结构
        return {
            "analyst_name": "分析失败",
            "viewpoint": "中性",
            "reason": "数据解析失败",
            "scores": {},
            "detailed_analysis": f"解析失败: {content[:200]}..."
        }
    except Exception as e:
        logger.error(f"解析分析师报告时发生错误: {e}")
        return {
            "analyst_name": "分析失败",
            "viewpoint": "中性",
            "reason": "系统错误",
            "scores": {},
            "detailed_analysis": f"系统错误: {str(e)}"
        }

def parse_debater_report(content: str, default_name: str) -> dict:
    """解析辩论者报告内容，提取JSON并转换为DebaterReport对象"""
    try:
        # 清理内容，移除可能的markdown标记
        cleaned_content = content.strip()
        if '```json' in cleaned_content:
            # 提取JSON部分
            start_idx = cleaned_content.find('```json') + 7
            end_idx = cleaned_content.rfind('```')
            if end_idx > start_idx:
                json_str = cleaned_content[start_idx:end_idx].strip()
            else:
                json_str = cleaned_content[start_idx:].strip()
        else:
            json_str = cleaned_content
        
        # 解析JSON
        report_data = json.loads(json_str)
        
        # 验证必要字段
        required_fields = ['analyst_name', 'viewpoint', 'core_arguments', 'rebuttals', 'final_statement']
        for field in required_fields:
            if field not in report_data:
                logger.warning(f"缺少必要字段: {field}")
                if field == 'core_arguments' or field == 'rebuttals':
                    report_data[field] = []
                else:
                    report_data[field] = ""
        
        return report_data
    except json.JSONDecodeError as e:
        logger.error(f"JSON解析失败: {e}")
        logger.error(f"原始内容: {content}")
        # 返回默认结构
        return {
            "analyst_name": default_name,
            "viewpoint": "中性",
            "core_arguments": ["解析失败"],
            "rebuttals": ["解析失败"],
            "final_statement": f"解析失败: {content[:200]}..."
        }
    except Exception as e:
        logger.error(f"解析辩论者报告时发生错误: {e}")
        return {
            "analyst_name": default_name,
            "viewpoint": "中性",
            "core_arguments": ["系统错误"],
            "rebuttals": ["系统错误"],
            "final_statement": f"系统错误: {str(e)}"
        }

def parse_debate_report(content: str) -> dict:
    """解析辩论分析报告内容，提取JSON并转换为DebateReport对象"""
    try:
        # 清理内容，移除可能的markdown标记
        cleaned_content = content.strip()
        if '```json' in cleaned_content:
            # 提取JSON部分
            start_idx = cleaned_content.find('```json') + 7
            end_idx = cleaned_content.rfind('```')
            if end_idx > start_idx:
                json_str = cleaned_content[start_idx:end_idx].strip()
            else:
                json_str = cleaned_content[start_idx:].strip()
        else:
            json_str = cleaned_content
        
        # 解析JSON
        report_data = json.loads(json_str)
        
        # 验证必要字段
        required_fields = ['analyst_name', 'bull_summary', 'bear_summary', 'score_comparison', 'final_viewpoint', 'final_reason']
        for field in required_fields:
            if field not in report_data:
                logger.warning(f"缺少必要字段: {field}")
                if field == 'bull_summary' or field == 'bear_summary':
                    report_data[field] = []
                elif field == 'score_comparison':
                    report_data[field] = {}
                else:
                    report_data[field] = ""
        
        return report_data
    except json.JSONDecodeError as e:
        logger.error(f"JSON解析失败: {e}")
        logger.error(f"原始内容: {content}")
        # 返回默认结构
        return {
            "analyst_name": "辩论分析师",
            "bull_summary": ["解析失败"],
            "bear_summary": ["解析失败"],
            "score_comparison": {},
            "final_viewpoint": "中性",
            "final_reason": f"解析失败: {content[:200]}..."
        }
    except Exception as e:
        logger.error(f"解析辩论分析报告时发生错误: {e}")
        return {
            "analyst_name": "辩论分析师",
            "bull_summary": ["系统错误"],
            "bear_summary": ["系统错误"],
            "score_comparison": {},
            "final_viewpoint": "中性",
            "final_reason": f"系统错误: {str(e)}"
        }

def parse_supervisor_report(content: str) -> dict:
    """解析监督者报告内容，提取JSON并转换为结构化对象"""
    try:
        # 清理内容，移除可能的markdown标记
        cleaned_content = content.strip()
        if '```json' in cleaned_content:
            # 提取JSON部分
            start_idx = cleaned_content.find('```json') + 7
            end_idx = cleaned_content.rfind('```')
            if end_idx > start_idx:
                json_str = cleaned_content[start_idx:end_idx].strip()
            else:
                json_str = cleaned_content[start_idx:].strip()
        else:
            json_str = cleaned_content
        
        # 解析JSON
        report_data = json.loads(json_str)
        return report_data
    except json.JSONDecodeError as e:
        logger.error(f"JSON解析失败: {e}")
        logger.error(f"原始内容: {content}")
        # 返回默认结构
        return {
            "error": "Failed to parse supervisor report JSON", 
            "raw_content": content[:200]
        }
    except Exception as e:
        logger.error(f"解析监督者报告时发生错误: {e}")
        return {
            "error": f"System error: {str(e)}", 
            "raw_content": content[:200]
        } 