# 文件: core/db_manager.py
# 描述: 封装所有与SQLite数据库的交互。
# -----------------------------------------------------------------
import pandas as pd
from sqlalchemy import create_engine, text
from pathlib import Path
from config.settings import settings
import logging

logger = logging.getLogger(__name__)


class DBManager:
    def __init__(self):
        self.engine = create_engine(settings.DATABASE_URL)

    def load_static_table(self, table_name: str) -> pd.DataFrame:
        """从数据库加载一个完整的静态表。"""
        try:
            return pd.read_sql_table(table_name, self.engine)
        except Exception as e:
            print(f"从数据库加载表 '{table_name}' 失败: {e}")
            return pd.DataFrame()

    def load_company_detail(self, stock_code: str) -> pd.DataFrame:
        """根据股票代码加载公司详细信息。"""
        try:
            # 从stock_company表加载公司详细信息
            query = text("SELECT * FROM stock_company WHERE ts_code = :stock_code")
            return pd.read_sql(query, self.engine, params={'stock_code': stock_code})
        except Exception as e:
            logger.warning(f"从数据库加载公司详细信息失败: {e}")
            return pd.DataFrame()