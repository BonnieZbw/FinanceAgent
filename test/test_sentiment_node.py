#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试情绪面分析节点功能
验证情绪面分析节点是否可以正常工作
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

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_sentiment_node():
    """测试情绪面分析节点功能"""
    print("🧠 开始测试情绪面分析节点")
    print("=" * 60)
    
    # 测试股票代码
    test_stock = "000001.SZ"  # 平安银行
    test_date = "20250914"
    
    # 初始化状态
    state = StockAgentState(
        stock_code=test_stock,
        end_date=test_date
    )
    
    print(f"测试股票: {test_stock}")
    print(f"测试日期: {test_date}")
    
    # 确保cache_manager已初始化
    try:
        if not cache_manager.provider:
            print("\n初始化 cache_manager...")
            cache_manager.initialize()
            print("✅ cache_manager 初始化成功")
        else:
            print("✅ cache_manager 已初始化")
    except Exception as e:
        print(f"❌ cache_manager 初始化失败: {e}")
        return
    
    # 测试情绪面分析节点
    print("\n" + "="*50)
    print("🧠 测试情绪面分析节点")
    print("="*50)
    
    try:
        print("开始执行情绪面分析...")
        start_time = datetime.now()
        
        sentiment_result = run_sentiment_analysis(state)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print("✅ 情绪面分析节点执行成功")
        print(f"⏱️ 执行耗时: {duration:.2f} 秒")
        print(f"结果键: {list(sentiment_result.keys())}")
        
        if 'sentiment_report' in sentiment_result:
            report = sentiment_result['sentiment_report']
            print(f"\n📊 分析报告详情:")
            print(f"   分析师名称: {report.get('analyst_name', 'N/A')}")
            print(f"   观点: {report.get('viewpoint', 'N/A')}")
            print(f"   评分: {report.get('scores', {})}")
            print(f"   理由: {report.get('reason', 'N/A')[:150]}...")
            print(f"   详细分析: {report.get('detailed_analysis', 'N/A')[:200]}...")
            
            # 检查评分结构
            scores = report.get('scores', {})
            if scores:
                print(f"\n📈 评分详情:")
                for key, value in scores.items():
                    print(f"   {key}: {value}/5")
        
        print(f"\n✅ 情绪面分析节点测试完成")
        
    except Exception as e:
        print(f"❌ 情绪面分析节点执行失败: {e}")
        logger.error(f"情绪面分析失败: {e}", exc_info=True)

def test_sentiment_node_with_different_stocks():
    """测试不同股票的情绪面分析"""
    print("\n" + "="*60)
    print("🧠 测试多股票情绪面分析")
    print("="*60)
    
    test_stocks = ["000001.SZ", "000002.SZ", "600000.SH"]
    
    for stock in test_stocks:
        print(f"\n--- 测试股票: {stock} ---")
        state = StockAgentState(stock_code=stock, end_date="20250914")
        
        try:
            result = run_sentiment_analysis(state)
            if 'sentiment_report' in result:
                report = result['sentiment_report']
                print(f"✅ {stock}: {report.get('viewpoint', 'N/A')} - {report.get('reason', 'N/A')[:50]}...")
            else:
                print(f"❌ {stock}: 无分析报告")
        except Exception as e:
            print(f"❌ {stock}: 分析失败 - {e}")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="测试情绪面分析节点")
    parser.add_argument("--stock", default="000001.SZ", help="股票代码")
    parser.add_argument("--date", default="20250914", help="结束日期")
    parser.add_argument("--multi", action="store_true", help="测试多只股票")
    
    args = parser.parse_args()
    
    if args.multi:
        test_sentiment_node_with_different_stocks()
    else:
        # 更新测试股票
        test_sentiment_node()
