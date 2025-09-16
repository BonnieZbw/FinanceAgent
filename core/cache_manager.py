# 文件: core/cache_manager.py (已重构)
# 描述: 核心模块，负责初始化数据提供者，不再负责缓存和数据获取。
# -----------------------------------------------------------------
import tushare as ts
import pandas as pd
from .db_manager import DBManager
from config.settings import settings
from tools.tushare_provider import TushareProvider
from tools.tinyshare_provider import TinyshareProvider, NewsProvider
from tools.akshare_provider import AkshareProvider

import logging

logger = logging.getLogger(__name__)

class CacheManager:
    def __init__(self):
        self.db = DBManager()
        self.provider = None # 数据提供者实例
        self.news_provider = None  # 新闻提供者实例
        self.static_cache = {}
        
        # 数据提供者类型
        self.provider_type = "tushare"  # 默认使用 tushare

    def initialize(self):
        """初始化数据提供者和静态缓存。
        优先级：tushare（主要）→ tinyshare（第一备用）→ akshare（第二备用）
        """
        try:
            # 1. 尝试初始化 Tushare（主要数据源）
            tushare_success = self._try_initialize_tushare()
            
            # 2. 如果 Tushare 失败，尝试 Tinyshare（第一备用）
            if not tushare_success:
                tinyshare_success = self._try_initialize_tinyshare()
                
                # 3. 如果 Tinyshare 也失败，尝试 Akshare（第二备用）
                if not tinyshare_success:
                    akshare_success = self._try_initialize_akshare()
                    
                    # 4. 如果所有数据源都失败，抛出异常
                    if not akshare_success:
                        raise Exception("所有数据提供者初始化失败：tushare、tinyshare、akshare 均不可用")
                        
        except Exception as e:
            logger.error(f"数据提供者初始化失败: {e}")
            raise Exception("无法初始化任何数据提供者")

        # 5. 初始化新闻提供者（独立于行情数据提供者）
        self._try_initialize_news_provider()

        logger.info("--- CacheManager: 开始加载静态数据到内存... ---")
        try:
            self.static_cache['stock_basic'] = self.db.load_static_table('stock_basic').set_index('ts_code')
            self.static_cache['trade_cal'] = self.db.load_static_table('trade_cal')
            # 尝试加载公司详细信息表
            try:
                self.static_cache['stock_company'] = self.db.load_static_table('stock_company').set_index('ts_code')
                logger.info("--- CacheManager: 公司详细信息表加载成功 ---")
            except Exception as e:
                logger.warning(f"公司详细信息表加载失败: {e}")
                self.static_cache['stock_company'] = pd.DataFrame()
            logger.info("--- CacheManager: 静态数据加载完成 ---")
        except Exception as e:
            logger.warning(f"静态数据加载失败: {e}")
            logger.info("--- CacheManager: 静态数据加载失败，将使用模拟数据 ---")

    def _try_initialize_tushare(self):
        """尝试初始化 Tushare 数据提供者"""
        try:
            logger.info("--- CacheManager: 尝试初始化 Tushare（主要数据源） ---")
            
            # 初始化 tushare 客户端
            pro_client = None
            if settings.TUSHARE_ENABLED and settings.TUSHARE_TOKEN != "default_token":
                try:
                    ts.set_token(settings.TUSHARE_TOKEN)
                    pro_client = ts.pro_api(settings.TUSHARE_TOKEN)
                    logger.info("--- CacheManager: Tushare客户端初始化成功 ---")
                except Exception as e:
                    logger.warning(f"Tushare客户端初始化失败: {e}")
                    pro_client = None
            
            # 创建 TushareProvider
            self.provider = TushareProvider(pro_client=pro_client)
            if self.provider.is_available:
                self.provider_type = "tushare"
                logger.info("--- CacheManager: TushareProvider 初始化并测试成功（主要数据源） ---")
                return True
            else:
                logger.warning("--- CacheManager: TushareProvider 接口测试失败 ---")
                return False
                
        except Exception as e:
            logger.warning(f"TushareProvider 初始化失败: {e}")
            return False

    def _try_initialize_tinyshare(self):
        """尝试初始化 Tinyshare 数据提供者（第一备用）"""
        try:
            logger.info("--- CacheManager: 尝试初始化 Tinyshare（第一备用数据源） ---")
            
            # 初始化 tinyshare 客户端
            tnspro_client = None
            if hasattr(settings, 'TINYSHARE_TOKEN') and settings.TINYSHARE_TOKEN != "default_token":
                try:
                    import tinyshare as tns
                    tnspro_client = tns.pro_api(settings.TINYSHARE_TOKEN)
                    logger.info("--- CacheManager: Tinyshare客户端初始化成功 ---")
                except Exception as e:
                    logger.warning(f"Tinyshare客户端初始化失败: {e}")
                    tnspro_client = None
            else:
                logger.warning("--- CacheManager: Tinyshare Token 未配置 ---")
                return False
            
            # 创建 TinyshareProvider
            self.provider = TinyshareProvider(tnspro_client=tnspro_client)
            if self.provider.is_available:
                self.provider_type = "tinyshare"
                logger.info("--- CacheManager: TinyshareProvider 初始化并测试成功（第一备用数据源） ---")
                return True
            else:
                logger.warning("--- CacheManager: TinyshareProvider 接口测试失败 ---")
                return False
                
        except Exception as e:
            logger.warning(f"TinyshareProvider 初始化失败: {e}")
            return False

    def _try_initialize_akshare(self):
        """尝试初始化 Akshare 数据提供者（第二备用）"""
        try:
            logger.info("--- CacheManager: 尝试初始化 Akshare（第二备用数据源） ---")
            
            # 创建 AkshareProvider
            self.provider = AkshareProvider()
            if self.provider.is_available:
                self.provider_type = "akshare"
                logger.info("--- CacheManager: AkshareProvider 初始化并测试成功（第二备用数据源） ---")
                return True
            else:
                logger.warning("--- CacheManager: AkshareProvider 接口测试失败 ---")
                return False
                
        except Exception as e:
            logger.warning(f"AkshareProvider 初始化失败: {e}")
            return False

    def get_stock_name(self, stock_code: str) -> str:
        """从静态缓存中快速获取股票名称。"""
        try:
            # 检查静态缓存是否已加载
            if 'stock_basic' not in self.static_cache:
                logger.error("静态缓存尚未加载，请先调用 initialize() 方法")
                return "未知股票"
            
            return self.static_cache['stock_basic'].loc[stock_code, 'name']
        except (KeyError, AttributeError):
            return "未知股票"

    def get_company_basic_info(self, stock_code: str) -> dict:
        """获取公司的基本信息，包括股票基本信息和公司详细信息。"""
        try:
            company_info = {}
            
            # 获取股票基本信息
            if 'stock_basic' in self.static_cache:
                try:
                    stock_basic = self.static_cache['stock_basic'].loc[stock_code]
                    company_info['stock_basic'] = {
                        'name': stock_basic.get('name', '未知'),
                        'area': stock_basic.get('area', '未知'),
                        'industry': stock_basic.get('industry', '未知'),
                        'market': stock_basic.get('market', '未知'),
                        'list_date': stock_basic.get('list_date', '未知')
                    }
                except (KeyError, AttributeError):
                    logger.warning(f"未找到股票 {stock_code} 的基本信息")
                    company_info['stock_basic'] = {}
            
            # 获取公司详细信息 - 优先使用静态缓存
            if 'stock_company' in self.static_cache and not self.static_cache['stock_company'].empty:
                try:
                    company_detail = self.static_cache['stock_company'].loc[stock_code]
                    company_info['company_detail'] = company_detail.to_dict()
                except (KeyError, AttributeError):
                    logger.warning(f"静态缓存中未找到股票 {stock_code} 的公司详细信息")
                    company_info['company_detail'] = {}
            else:
                # 如果静态缓存中没有，则从数据库加载
                try:
                    company_detail = self.db.load_company_detail(stock_code)
                    if not company_detail.empty:
                        company_info['company_detail'] = company_detail.to_dict('records')[0]
                    else:
                        company_info['company_detail'] = {}
                except Exception as e:
                    logger.warning(f"获取公司详细信息失败: {e}")
                    company_info['company_detail'] = {}
            
            return company_info
            
        except Exception as e:
            logger.error(f"获取公司基本信息失败: {e}")
            return {'stock_basic': {}, 'company_detail': {}}

    def get_provider(self):
        """获取数据提供者实例"""
        if self.provider is None:
            logger.error("数据提供者尚未初始化，请先调用 initialize() 方法")
            return None
        return self.provider

    def _try_initialize_news_provider(self):
        """尝试初始化新闻提供者"""
        try:
            if settings.NEWS_ENABLED and settings.NEWS_TOKEN and settings.NEWS_TOKEN != "default_token":
                logger.info("--- CacheManager: 尝试初始化 NewsProvider ---")
                self.news_provider = NewsProvider(settings.NEWS_TOKEN)
                if self.news_provider.is_available:
                    logger.info("--- CacheManager: NewsProvider 初始化成功 ---")
                else:
                    logger.warning("--- CacheManager: NewsProvider 初始化失败 ---")
                    self.news_provider = None
            else:
                logger.info("--- CacheManager: 新闻功能未启用或未配置token ---")
        except Exception as e:
            logger.error(f"NewsProvider 初始化失败: {e}")
            self.news_provider = None

    def get_news_provider(self):
        """获取新闻提供者实例"""
        if self.news_provider is None:
            logger.warning("新闻提供者尚未初始化或不可用")
            return None
        return self.news_provider

    def get_stock_basic(self):
        """获取股票基础信息"""
        try:
            if self.provider_type == "akshare":
                # 使用 akshare 获取股票基础信息
                import akshare as ak
                stock_basic = ak.stock_info_a_code_name()
                return stock_basic
            elif self.provider_type == "tinyshare":
                # 使用 tinyshare 获取股票基础信息
                if hasattr(self.provider, 'tnspro') and self.provider.tnspro:
                    stock_basic = self.provider.tnspro.stock_basic()
                    return stock_basic
                else:
                    logger.warning("Tinyshare 客户端未初始化，返回空数据")
                    return pd.DataFrame()
            else:
                # 使用 tushare 获取股票基础信息
                if hasattr(self.provider, 'pro') and self.provider.pro:
                    stock_basic = self.provider.pro.stock_basic()
                    return stock_basic
                else:
                    logger.warning("Tushare 客户端未初始化，返回空数据")
                    return pd.DataFrame()
        except Exception as e:
            logger.error(f"获取股票基础信息失败: {e}")
            return pd.DataFrame()

# 创建一个全局实例供整个应用使用
cache_manager = CacheManager()
