#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试优化后的情绪面分析节点功能
验证情绪节点只使用固定的result或summary作为输入
"""

import sys
import os
import logging
from datetime import datetime

# 添加项目根目录到 Python 路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from graph.nodes.analysis_nodes import run_sentiment_analysis
from graph.type import StockAgentState
from core.cache_manager import cache_manager
from core.result_manager import result_manager

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_optimized_sentiment_node():
    """测试优化后的情绪面分析节点功能"""
    print("🧠 开始测试优化后的情绪面分析节点")
    print("=" * 60)
    
    # 测试股票代码
    test_stock = "000001.SZ"  # 平安银行
    test_date = "20250914"
    
    # 初始化状态
    state = StockAgentState(
        stock_code=test_stock,
        end_date=test_date
    )
    
    # 检查前置数据是否存在
    print(f"\n1️⃣ 检查前置数据")
    
    # 检查新闻数据
    try:
        news_data = result_manager.load_tool_result(test_stock, "news_data", test_date)
        if news_data:
            print(f"   ✅ 新闻数据存在")
            if isinstance(news_data, dict):
                summary = news_data.get('summary') or news_data.get('combined_summary') or news_data.get('result')
                print(f"   📰 新闻摘要长度: {len(summary) if summary else 0} 字符")
            else:
                print(f"   📰 新闻数据长度: {len(str(news_data))} 字符")
        else:
            print(f"   ⚠️ 新闻数据不存在")
    except Exception as e:
        print(f"   ❌ 读取新闻数据失败: {e}")
    
    # 检查基本面数据
    try:
        fundamental_data = result_manager.load_tool_result(test_stock, "fundamental_data", test_date)
        if fundamental_data:
            print(f"   ✅ 基本面数据存在")
            if isinstance(fundamental_data, dict):
                result = fundamental_data.get('result')
                print(f"   📊 基本面结果长度: {len(result) if result else 0} 字符")
            else:
                print(f"   📊 基本面数据长度: {len(str(fundamental_data))} 字符")
        else:
            print(f"   ⚠️ 基本面数据不存在")
    except Exception as e:
        print(f"   ❌ 读取基本面数据失败: {e}")
    
    # 检查基本面报告（兜底数据）
    try:
        fundamental_report = result_manager.load_report(test_stock, "fundamental_report", test_date)
        if fundamental_report:
            print(f"   ✅ 基本面报告存在")
            print(f"   📋 基本面报告: {fundamental_report.get('analyst_name', 'N/A')} - {fundamental_report.get('viewpoint', 'N/A')}")
        else:
            print(f"   ⚠️ 基本面报告不存在")
    except Exception as e:
        print(f"   ❌ 读取基本面报告失败: {e}")
    
    # 执行情绪面分析
    print(f"\n2️⃣ 执行优化后的情绪面分析")
    start_time = datetime.now()
    
    try:
        result = run_sentiment_analysis(state)
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        
        print(f"   ✅ 情绪面分析执行成功")
        print(f"   ⏱️ 执行耗时: {execution_time:.2f} 秒")
        
        sentiment_report = result.get('sentiment_report', {})
        print(f"   🎯 分析师: {sentiment_report.get('analyst_name', 'N/A')}")
        print(f"   🎯 观点: {sentiment_report.get('viewpoint', 'N/A')}")
        print(f"   🎯 综合评分: {sentiment_report.get('scores', {})}")
        
    except Exception as e:
        print(f"   ❌ 情绪面分析执行失败: {e}")
        logger.error(f"情绪面分析失败: {e}", exc_info=True)
        return
    
    # 检查生成的输入数据
    print(f"\n3️⃣ 检查生成的输入数据")
    try:
        sentiment_input = result_manager.load_tool_result(test_stock, "sentiment_input", test_date)
        if sentiment_input:
            print(f"   ✅ 情绪输入数据已保存")
            print(f"   📝 输入数据结构:")
            for key, value in sentiment_input.items():
                if isinstance(value, str):
                    print(f"      - {key}: {len(value)} 字符")
                else:
                    print(f"      - {key}: {type(value).__name__}")
        else:
            print(f"   ⚠️ 情绪输入数据未保存")
    except Exception as e:
        print(f"   ❌ 读取情绪输入数据失败: {e}")
    
    # 检查最终报告
    print(f"\n4️⃣ 检查最终报告")
    try:
        final_report = result_manager.load_report(test_stock, "sentiment_report", test_date)
        if final_report:
            print(f"   ✅ 情绪面报告已保存")
            print(f"   📄 报告详情:")
            print(f"      分析师: {final_report.get('analyst_name', 'N/A')}")
            print(f"      观点: {final_report.get('viewpoint', 'N/A')}")
            print(f"      理由: {final_report.get('reason', 'N/A')[:100]}...")
            scores = final_report.get('scores', {})
            if scores:
                print(f"      评分: {scores}")
        else:
            print(f"   ⚠️ 情绪面报告未保存")
    except Exception as e:
        print(f"   ❌ 读取情绪面报告失败: {e}")
    
    print(f"\n🎉 优化后的情绪面节点测试完成！")

if __name__ == "__main__":
    test_optimized_sentiment_node()
