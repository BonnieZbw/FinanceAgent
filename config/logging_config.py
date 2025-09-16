#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
日志配置模块
配置项目的日志输出格式和级别
"""

import logging
import sys
from pathlib import Path

def setup_logging(level=logging.INFO, log_file=None):
    """
    设置日志配置
    
    Args:
        level: 日志级别，默认INFO
        log_file: 日志文件路径，如果为None则使用默认日志文件
    """
    # 创建根日志记录器
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    
    # 清除现有的处理器
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # 创建控制台处理器
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    
    # 设置日志格式
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(formatter)
    
    # 添加控制台处理器到根日志记录器
    root_logger.addHandler(console_handler)
    
    # 设置默认日志文件路径
    if log_file is None:
        # 创建logs目录
        logs_dir = Path("logs")
        logs_dir.mkdir(exist_ok=True)
        
        # 使用当前日期作为日志文件名
        from datetime import datetime
        current_date = datetime.now().strftime("%Y-%m-%d")
        log_file = logs_dir / f"stock_analysis_{current_date}.log"
    
    # 确保日志目录存在
    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    
    # 创建文件处理器
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)
    
    # 设置特定模块的日志级别
    logging.getLogger('langchain').setLevel(logging.WARNING)
    logging.getLogger('openai').setLevel(logging.WARNING)
    logging.getLogger('httpx').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    
    print(f"🔧 日志配置完成 - 级别: {logging.getLevelName(level)}")
    print(f"📝 日志文件: {log_file}")
    
    return str(log_file)

def get_logger(name):
    """
    获取指定名称的日志记录器
    
    Args:
        name: 日志记录器名称
        
    Returns:
        logging.Logger: 配置好的日志记录器
    """
    return logging.getLogger(name)

# 默认配置
def setup_default_logging():
    """设置默认的日志配置"""
    return setup_logging(level=logging.INFO)

def get_current_log_file():
    """获取当前日志文件路径"""
    logs_dir = Path("logs")
    if not logs_dir.exists():
        return None
    
    # 查找最新的日志文件
    log_files = list(logs_dir.glob("stock_analysis_*.log"))
    if not log_files:
        return None
    
    # 按修改时间排序，返回最新的
    latest_log = max(log_files, key=lambda x: x.stat().st_mtime)
    return str(latest_log)

def list_log_files():
    """列出所有可用的日志文件"""
    logs_dir = Path("logs")
    if not logs_dir.exists():
        return []
    
    log_files = list(logs_dir.glob("stock_analysis_*.log"))
    log_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
    
    return [str(f) for f in log_files]

if __name__ == "__main__":
    # 测试日志配置
    setup_default_logging()
    
    logger = get_logger("test")
    logger.info("这是一条信息日志")
    logger.warning("这是一条警告日志")
    logger.error("这是一条错误日志")
    logger.debug("这是一条调试日志（不会显示，因为级别是INFO）") 