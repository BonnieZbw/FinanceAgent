# 文件: scripts/update_static_data.py
# 描述: 独立脚本，用于一次性拉取静态基础数据并存入数据库。
# -----------------------------------------------------------------
import sys
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import tushare as ts
import os
from sqlalchemy import create_engine
import pandas as pd
from config.settings import settings


import logging
logger = logging.getLogger(__name__)

DB_PATH = Path(settings.DB_PATH)
DB_URL = settings.DATABASE_URL

def update_static_data():
    """获取最新的静态基础数据并存入SQLite数据库。"""
    logger.info("--- 开始更新静态基础数据 ---")
    try:
        ts.set_token(settings.TUSHARE_TOKEN)
        pro = ts.pro_api()
    except Exception as e:
        logger.error(f"初始化Tushare失败: {e}")
        return
    
    engine = create_engine(DB_URL)
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    # --- 定义需要更新的静态数据表 ---
    static_tables = {
        "stock_basic": lambda: pro.stock_basic(list_status='L', fields='ts_code,symbol,name,area,industry,market,list_date'),
        "trade_cal": lambda: pro.trade_cal(exchange='', start_date='20180101', end_date='20291231'),
    }

    for name, fetch_func in static_tables.items():
        try:
            logger.info(f"正在拉取 '{name}' 数据...")
            data = fetch_func()
            logger.info(f"成功获取 {len(data)} 条 '{name}' 数据，正在写入数据库...")
            data.to_sql(name, engine, if_exists='replace', index=False)
            logger.info(f"'{name}' 数据表更新成功！")
        except Exception as e:
            logger.error(f"更新 '{name}' 数据失败: {e}")
    
    # --- 单独处理 stock_company，按交易所分批拉取 ---
    try:
        logger.info("正在分批拉取 'stock_company' 数据...")
        exchanges = ['SSE', 'SZSE', 'BSE'] # 上海、深圳、北京交易所
        all_companies = []
        for ex in exchanges:
            logger.info(f"  -> 正在拉取 {ex} 交易所的公司信息...")
            companies_df = pro.stock_company(exchange=ex)
            all_companies.append(companies_df)
            logger.info(f"  -> 成功获取 {len(companies_df)} 条。")
        
        # 合并所有交易所的数据
        full_company_data = pd.concat(all_companies, ignore_index=True)
        logger.info(f"成功获取全部 {len(full_company_data)} 条 'stock_company' 数据，正在写入数据库...")
        full_company_data.to_sql('stock_company', engine, if_exists='replace', index=False)
        logger.info("'stock_company' 数据表更新成功！")
    except Exception as e:
        logger.error(f"更新 'stock_company' 数据失败: {e}")


    logger.info("--- 静态基础数据更新完成 ---")

if __name__ == "__main__":
    update_static_data()