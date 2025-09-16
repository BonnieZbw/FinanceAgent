#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试分析节点功能
分别测试基本面、技术面、资金面节点的执行
"""

import sys
import os
import logging
from datetime import datetime

# 添加项目根目录到 Python 路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from graph.nodes.analysis_nodes import (
    run_fundamental_analysis, 
    run_technical_analysis, 
    run_fund_analysis
)
from graph.type import StockAgentState
from core.cache_manager import cache_manager

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_analysis_nodes():
    """测试分析节点功能"""
    print("🔍 开始测试分析节点")
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
    
    # 测试基本面分析节点
    print("\n" + "="*50)
    print("1️⃣ 测试基本面分析节点")
    print("="*50)
    
    try:
        print("开始执行基本面分析...")
        fundamental_result = run_fundamental_analysis(state)
        print("✅ 基本面分析节点执行成功")
        print(f"结果键: {list(fundamental_result.keys())}")
        
        if 'fundamental_report' in fundamental_result:
            report = fundamental_result['fundamental_report']
            print(f"分析师名称: {report.get('analyst_name', 'N/A')}")
            print(f"观点: {report.get('viewpoint', 'N/A')}")
            print(f"评分: {report.get('scores', {})}")
            print(f"理由: {report.get('reason', 'N/A')[:100]}...")
        
    except Exception as e:
        print(f"❌ 基本面分析节点执行失败: {e}")
        logger.error(f"基本面分析失败: {e}", exc_info=True)
    
    # 测试技术面分析节点
    print("\n" + "="*50)
    print("2️⃣ 测试技术面分析节点")
    print("="*50)
    
    try:
        print("开始执行技术面分析...")
        technical_result = run_technical_analysis(state)
        print("✅ 技术面分析节点执行成功")
        print(f"结果键: {list(technical_result.keys())}")
        
        if 'technical_report' in technical_result:
            report = technical_result['technical_report']
            print(f"分析师名称: {report.get('analyst_name', 'N/A')}")
            print(f"观点: {report.get('viewpoint', 'N/A')}")
            print(f"评分: {report.get('scores', {})}")
            print(f"理由: {report.get('reason', 'N/A')[:100]}...")
        
    except Exception as e:
        print(f"❌ 技术面分析节点执行失败: {e}")
        logger.error(f"技术面分析失败: {e}", exc_info=True)
    
    # 测试资金面分析节点
    print("\n" + "="*50)
    print("3️⃣ 测试资金面分析节点")
    print("="*50)
    
    try:
        print("开始执行资金面分析...")
        fund_result = run_fund_analysis(state)
        print("✅ 资金面分析节点执行成功")
        print(f"结果键: {list(fund_result.keys())}")
        
        if 'fund_report' in fund_result:
            report = fund_result['fund_report']
            print(f"分析师名称: {report.get('analyst_name', 'N/A')}")
            print(f"观点: {report.get('viewpoint', 'N/A')}")
            print(f"评分: {report.get('scores', {})}")
            print(f"理由: {report.get('reason', 'N/A')[:100]}...")
        
    except Exception as e:
        print(f"❌ 资金面分析节点执行失败: {e}")
        logger.error(f"资金面分析失败: {e}", exc_info=True)
    
    print("\n" + "="*60)
    print("🎉 分析节点测试完成")
    print("="*60)

def test_single_node(node_name: str, stock_code: str = "000001.SZ", end_date: str = "20250914"):
    """测试单个节点"""
    print(f"🔍 测试单个节点: {node_name}")
    print("=" * 50)
    
    state = StockAgentState(
        stock_code=stock_code,
        end_date=end_date
    )
    
    try:
        if not cache_manager.provider:
            print("初始化 cache_manager...")
            cache_manager.initialize()
            print("✅ cache_manager 初始化成功")
    except Exception as e:
        print(f"❌ cache_manager 初始化失败: {e}")
        return
    
    try:
        if node_name == "fundamental":
            result = run_fundamental_analysis(state)
            report_key = "fundamental_report"
        elif node_name == "technical":
            result = run_technical_analysis(state)
            report_key = "technical_report"
        elif node_name == "fund":
            result = run_fund_analysis(state)
            report_key = "fund_report"
        else:
            print(f"❌ 不支持的节点名称: {node_name}")
            return
        
        print(f"✅ {node_name} 节点执行成功")
        
        if report_key in result:
            report = result[report_key]
            print(f"分析师: {report.get('analyst_name', 'N/A')}")
            print(f"观点: {report.get('viewpoint', 'N/A')}")
            print(f"详细分析: {report.get('detailed_analysis', 'N/A')[:200]}...")
        
    except Exception as e:
        print(f"❌ {node_name} 节点执行失败: {e}")
        logger.error(f"{node_name} 节点失败: {e}", exc_info=True)

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="测试分析节点")
    parser.add_argument("--node", choices=["fundamental", "technical", "fund"], 
                       help="测试单个节点")
    parser.add_argument("--stock", default="000001.SZ", help="股票代码")
    parser.add_argument("--date", default="20250914", help="结束日期")
    
    args = parser.parse_args()
    
    if args.node:
        test_single_node(args.node, args.stock, args.date)
    else:
        test_analysis_nodes()
