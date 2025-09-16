# 文件: tools/tinyshare_provider.py
# 描述: 封装所有 Tinyshare API 调用，作为备用数据源提供者。
# -----------------------------------------------------------------
import pandas as pd
from datetime import datetime, timedelta
import logging
import inspect
import tinyshare as tns

logger = logging.getLogger(__name__)


class TinyshareProvider:
    def __init__(self, tnspro_client=None):
        """
        初始化TinyshareProvider，使用tinyshare作为数据源
        
        Args:
            tnspro_client: Tinyshare pro客户端
        """
        self.tnspro = tnspro_client
        self.is_available = False
        
        if self.tnspro:
            logger.info("--- TinyshareProvider: 使用tinyshare作为数据源 ---")
            # 进行接口测试验证可用性
            self.is_available = self._test_availability()
            if self.is_available:
                logger.info("--- TinyshareProvider: 接口测试成功，数据源可用 ---")
            else:
                logger.warning("--- TinyshareProvider: 接口测试失败，数据源不可用 ---")
        else:
            logger.warning("--- TinyshareProvider: 未提供tinyshare客户端 ---")
    
    def _test_availability(self):
        """测试TinyshareProvider是否真正可用"""
        try:
            # 使用一个简单的接口调用测试
            test_stock = "000001.SZ"  # 平安银行作为测试股票
            
            # 尝试获取日线数据
            test_data = self.fetch_daily_basic_data(test_stock)
            if test_data is not None and not test_data.empty:
                logger.info(f"TinyshareProvider 接口测试成功，获取到 {len(test_data)} 条数据")
                return True
            else:
                logger.warning("TinyshareProvider 接口测试返回空数据")
                return False
        except Exception as e:
            logger.warning(f"TinyshareProvider 接口测试失败: {e}")
            return False
    
    def _call_tinyshare_api(self, method_name, *args, **kwargs):
        """
        调用tinyshare API的方法
        返回非空 DataFrame 即视为成功。
        """
        def _normalize_result(res):
            # 将返回统一为 DataFrame（若本身就是 DF 则原样返回）
            if isinstance(res, pd.DataFrame):
                return res
            try:
                return pd.DataFrame(res)
            except Exception:
                return pd.DataFrame()

        # 1) tinyshare 优先
        if self.tnspro:
            try:
                logger.info(f"尝试使用tinyshare调用 {method_name}")
                method = getattr(self.tnspro, method_name)
                result = _normalize_result(method(*args, **kwargs))
                if result is not None and not result.empty:
                    logger.info(f"✅ tinyshare调用 {method_name} 成功，返回 {result.shape}")
                    return result
                else:
                    logger.info(f"tinyshare调用 {method_name} 返回空数据（正常情况）")
                    return pd.DataFrame()
            except Exception as e:
                logger.warning(f"❌ tinyshare调用 {method_name} 失败: {e}")
                # 异常情况下，继续执行到最后的错误处理
        else:
            # 如果没有 tnspro 客户端，直接记录错误
            logger.error(f"❌ {method_name} 调用失败，tinyshare无法获取数据")
            return pd.DataFrame()

        # 只有在异常情况下才执行到这里
        logger.error(f"❌ {method_name} 调用失败，tinyshare无法获取数据")
        return pd.DataFrame()

    def _get_date_range(self, end_date: str = None, years: int = 2):
        """
        获取日期范围的辅助函数
        
        Args:
            end_date: 结束日期，支持多种格式：'YYYYMMDD'、'YYYY-MM-DD'，如果为None则使用当前日期
            years: 往前推的年数，默认2年
            
        Returns:
            tuple: (start_date, end_date) 两个都是字符串格式 'YYYYMMDD'
        """
        
        if end_date is None:
            # 如果没有指定结束日期，使用当前日期
            end_date_dt = datetime.now()
        else:
            # 如果指定了结束日期，转换为datetime对象
            # 支持多种日期格式
            date_formats = ['%Y%m%d', '%Y-%m-%d', '%Y/%m/%d']
            end_date_dt = None
            
            for fmt in date_formats:
                try:
                    end_date_dt = datetime.strptime(end_date, fmt)
                    logger.info(f"成功解析日期格式 {fmt}: {end_date}")
                    break
                except ValueError:
                    continue
            
            if end_date_dt is None:
                logger.error(f"无效的日期格式: {end_date}，支持格式：YYYYMMDD、YYYY-MM-DD、YYYY/MM/DD，使用当前日期")
                end_date_dt = datetime.now()
        
        # 计算开始日期（往前推指定年数）
        start_date_dt = end_date_dt - timedelta(days=365 * years)
        
        # 转换为字符串格式
        start_date = start_date_dt.strftime('%Y%m%d')
        end_date_str = end_date_dt.strftime('%Y%m%d')
        logger.info(f"get_date_range: {start_date}----- {end_date_str}")
        
        return start_date, end_date_str

    """--------------------------------- 基本面数据 ---------------------------------"""

    def fetch_fina_indicator_data(self, stock_code: str, end_date: str = None) -> pd.DataFrame:
        """调用Tinyshare API获取财务指标数据。"""
        try:
            start_date, end_date = self._get_date_range(end_date)
            result = self._call_tinyshare_api(
                'fina_indicator',
                ts_code=stock_code,
                start_date=start_date,
                end_date=end_date
            )
            logger.info(f"fetch_fina_indicator_data: {result}")
            return result 
        except Exception as e:
            logger.error(f"fetch_fina_indicator_data error: {e}")
            return pd.DataFrame()
        

    def fetch_daily_basic_data(self, stock_code: str, end_date: str = None) -> pd.DataFrame:
        """调用Tinyshare API获取日线行情数据（基本面分析使用）。"""
        try:
            start_date, end_date = self._get_date_range(end_date)
            result = self._call_tinyshare_api(
                'daily_basic',
                ts_code=stock_code,
                start_date=start_date,
                end_date=end_date
            )
            logger.info(f"fetch_daily_basic_data: {result}")
            return result
        except Exception as e:
            logger.error(f"fetch_daily_basic_data error: {e}")
            return pd.DataFrame()

    def fetch_dividend_data(self, stock_code: str, end_date: str = None) -> pd.DataFrame:
        """调用Tinyshare API获取分红数据。"""
        try:
            result = self._call_tinyshare_api('dividend', ts_code=stock_code)
            logger.info(f"fetch_dividend_data: {result}")
            return result
        except Exception as e:
            logger.error(f"fetch_dividend_data error: {e}")
            return pd.DataFrame()

    def fetch_income_data(self, stock_code: str, end_date: str = None) -> pd.DataFrame:
        """调用Tinyshare API获取营业收入数据。"""
        try:
            start_date, end_date = self._get_date_range(end_date)
            result = self._call_tinyshare_api(
                'income',
                ts_code=stock_code,
                start_date=start_date,
                end_date=end_date
            )
            logger.info(f"fetch_income_data: {result}")
            return result
        except Exception as e:
            logger.error(f"fetch_income_data error: {e}")
            return pd.DataFrame()


    def fetch_balance_data(self, stock_code: str, end_date: str = None) -> pd.DataFrame:
        """调用Tinyshare API获取资产负债表数据。"""
        try:
            start_date, end_date = self._get_date_range(end_date)
            result = self._call_tinyshare_api(
                'balancesheet',
                ts_code=stock_code,
                start_date=start_date,
                end_date=end_date
            )
            logger.info(f"fetch_balance_data: {result}")
            return result
        except Exception as e:
            logger.error(f"fetch_balance_data error: {e}")
            return pd.DataFrame()

    
    def fetch_cashflow_data(self, stock_code: str, end_date: str = None) -> pd.DataFrame:
        """调用Tinyshare API获取现金流量表数据。"""
        try:
            start_date, end_date = self._get_date_range(end_date)
            result = self._call_tinyshare_api(
                'cashflow',
                ts_code=stock_code,
                start_date=start_date,
                end_date=end_date
            )
            logger.info(f"fetch_cashflow_data: {result}")
            return result
        except Exception as e:
            logger.error(f"fetch_cashflow_data error: {e}")
            return pd.DataFrame()

    
    def fetch_forecast_data(self, stock_code: str, end_date: str = None) -> pd.DataFrame:
        """调用Tinyshare API获取业绩预告数据。"""
        try:
            start_date, end_date = self._get_date_range(end_date)
            result = self._call_tinyshare_api(
                'forecast',
                ts_code=stock_code,
                # start_date=start_date,
                # end_date=end_date
            )
            logger.info(f"fetch_forecast_data: {result}")
            return result
        except Exception as e:
            logger.error(f"fetch_forecast_data error: {e}")
            return pd.DataFrame()

    
    def fetch_express_data(self, stock_code: str, end_date: str = None) -> pd.DataFrame:
        """调用Tinyshare API获取业绩快报数据。"""
        try:
            start_date, end_date = self._get_date_range(end_date)
            result = self._call_tinyshare_api(
                'express',
                ts_code=stock_code,
                start_date=start_date,
                end_date=end_date
            )
            logger.info(f"fetch_express_data: {result}")
            return result
        except Exception as e:
            logger.error(f"fetch_express_data error: {e}")
            return pd.DataFrame()

    
    def fetch_mainbz_data(self, stock_code: str, end_date: str = None) -> pd.DataFrame:
        """调用Tinyshare API获取主营业务数据。"""
        try:
            start_date, end_date = self._get_date_range(end_date)
            result = self._call_tinyshare_api(
                'fina_mainbz',
                ts_code=stock_code,
                start_date=start_date,
                end_date=end_date
            )
            logger.info(f"fetch_mainbz_data: {result}")
            return result
        except Exception as e:
            logger.error(f"fetch_mainbz_data error: {e}")
            return pd.DataFrame()


    """--------------------------------- 技术面数据 ---------------------------------"""

    def fetch_pro_bar_data(self, stock_code: str, end_date: str = None,
                   freq: str = "D", adj: str = None, ma: list = [5, 10, 20, 60]) -> pd.DataFrame:
        """K线+均线数据，支持日/周/月。
        使用 tinyshare 模块函数 tns.pro_bar。
        """
        try:
            start_date, end_date = self._get_date_range(end_date, years=5)

            # 使用 tinyshare: 模块函数 tns.pro_bar
            try:
                logger.info("尝试使用tinyshare调用 tns.pro_bar")
                result = tns.pro_bar(
                    ts_code=stock_code,
                    start_date=start_date,
                    end_date=end_date,
                    adj=adj,
                    ma=ma,
                    asset="E",
                    freq=freq,
                )
                if result is not None and not result.empty:
                    logger.info("✅ tinyshare调用 tns.pro_bar 成功")
                    return result
                else:
                    logger.info("tinyshare调用 tns.pro_bar 返回空数据（正常情况）")
                    return pd.DataFrame()
            except Exception as e:
                logger.warning(f"❌ tinyshare调用 tns.pro_bar 失败: {e}")

            logger.error("❌ pro_bar 调用失败，tinyshare无法获取数据")
            return pd.DataFrame()

        except Exception as e:
            logger.error(f"fetch_pro_bar_data error: {e}")
            return pd.DataFrame()

    def fetch_stk_factor_data(self, stock_code: str, end_date: str = None) -> pd.DataFrame:
        """技术指标数据（MACD/KDJ/RSI等）"""
        try:
            start_date, end_date = self._get_date_range(end_date)
            result = self._call_tinyshare_api(
                'stk_factor',
                ts_code=stock_code,
                start_date=start_date,
                end_date=end_date
            )
            logger.info(f"fetch_stk_factor_data: {result}")
            return result
        except Exception as e:
            logger.error(f"fetch_stk_factor_data error: {e}")
            return pd.DataFrame()

    def fetch_daily_basic_enhanced(self, stock_code: str, end_date: str = None) -> pd.DataFrame:
        """增强的 daily_basic（估值+成交量指标）"""
        try:
            start_date, end_date = self._get_date_range(end_date)
            result = self._call_tinyshare_api(
                'daily_basic',
                ts_code=stock_code,
                start_date=start_date,
                end_date=end_date
            )
            logger.info(f"fetch_daily_basic_enhanced: {result}")
            return result
        except Exception as e:
            logger.error(f"fetch_daily_basic_enhanced error: {e}")
            return pd.DataFrame()

    def fetch_limit_list_data(self, stock_code: str) -> pd.DataFrame:
        """获取股票全部涨跌停、炸板数据（无时限）"""
        try:
            result = self._call_tinyshare_api(
                'limit_list',
                ts_code=stock_code
            )
            logger.info(f"fetch_limit_list_data: {result}")
            return result
        except Exception as e:
            logger.error(f"fetch_limit_list_data error: {e}")
            return pd.DataFrame()

    """--------------------------------- 资金面数据 ---------------------------------"""

    def fetch_top10_holders_data(self, stock_code: str, end_date: str = None) -> pd.DataFrame:
        """获取前十大股东持股情况"""
        try:
            start_date, end_date = self._get_date_range(end_date)
            result = self._call_tinyshare_api(
                'top10_holders',
                ts_code=stock_code,
                start_date=start_date,
                end_date=end_date
            )
            logger.info(f"fetch_top10_holders_data: {result}")
            return result
        except Exception as e:
            logger.error(f"fetch_top10_holders_data error: {e}")
            return pd.DataFrame()

    def fetch_top10_floatholders_data(self, stock_code: str, end_date: str = None) -> pd.DataFrame:
        """获取前十大流通股东持股情况"""
        try:
            start_date, end_date = self._get_date_range(end_date)
            result = self._call_tinyshare_api(
                'top10_floatholders',
                ts_code=stock_code,
                start_date=start_date,
                end_date=end_date
            )
            logger.info(f"fetch_top10_floatholders_data: {result}")
            return result
        except Exception as e:
            logger.error(f"fetch_top10_floatholders_data error: {e}")
            return pd.DataFrame()

    def fetch_stk_holdernumber_data(self, stock_code: str, end_date: str = None) -> pd.DataFrame:
        """获取股东人数"""
        try:
            start_date, end_date = self._get_date_range(end_date)
            result = self._call_tinyshare_api(
                'stk_holdernumber',
                ts_code=stock_code,
                start_date=start_date,
                end_date=end_date
            )
            logger.info(f"fetch_stk_holdernumber_data: {result}")
            return result
        except Exception as e:
            logger.error(f"fetch_stk_holdernumber_data error: {e}")
            return pd.DataFrame()
        
    def fetch_moneyflow_ths_data(self, stock_code: str, end_date: str = None) -> pd.DataFrame:
        """获取个股主力动向"""
        try:
            start_date, end_date = self._get_date_range(end_date, years=1)
            result = self._call_tinyshare_api(
                'moneyflow_ths',
                ts_code=stock_code,
                start_date=start_date,
                end_date=end_date
            )
            logger.info(f"fetch_moneyflow_ths_data: {result}")
            return result
        except Exception as e:
            logger.error(f"fetch_moneyflow_ths_data error: {e}")
            return pd.DataFrame()
    
    def fetch_moneyflow_cnt_ths_data(self, stock_code: str, end_date: str = None) -> pd.DataFrame:
        """获取板块主力动向"""
        try:
            # 默认12个月回溯
            start_date, end_date_calc = self._get_date_range(end_date, years=1)
            trade_date = end_date if end_date else end_date_calc
            result = self._call_tinyshare_api(
                'moneyflow_cnt_ths',
                trade_date=trade_date
            )
            logger.info(f"fetch_moneyflow_cnt_ths_data: {result}")
            return result
        except Exception as e:
            logger.error(f"fetch_moneyflow_cnt_ths_data error: {e}")
            return pd.DataFrame()   
    
    def fetch_moneyflow_ind_ths_data(self, stock_code: str, end_date: str = None) -> pd.DataFrame:
        """获取行业主力动向"""
        try:
            # 默认12个月回溯
            start_date, end_date_calc = self._get_date_range(end_date, years=1)
            trade_date = end_date if end_date else end_date_calc
            result = self._call_tinyshare_api(
                'moneyflow_ind_ths',
                trade_date=trade_date
            )
            logger.info(f"fetch_moneyflow_ind_ths_data: {result}")
            return result
        except Exception as e:
            logger.error(f"fetch_moneyflow_ind_ths_data error: {e}")
            return pd.DataFrame()

    def fetch_moneyflow_mkt_dc_data(self, stock_code: str, end_date: str = None) -> pd.DataFrame:
        """获取大盘资金流向"""
        try:
            # 默认12个月回溯
            start_date, end_date_calc = self._get_date_range(end_date, years=1)
            trade_date = end_date if end_date else end_date_calc
            result = self._call_tinyshare_api(
                'moneyflow_mkt_dc',
                trade_date=trade_date
            )
            logger.info(f"fetch_moneyflow_mkt_dc_data: {result}")
            return result
        except Exception as e:
            logger.error(f"fetch_moneyflow_mkt_dc_data error: {e}")
            return pd.DataFrame()


    def fetch_top_list_data(self, stock_code: str, end_date: str = None) -> pd.DataFrame:
        """获取龙虎榜每日统计(日期+代码)"""
        try:
            start_date, end_date_calc = self._get_date_range(end_date, years=1)
            trade_date = end_date if end_date else end_date_calc
            result = self._call_tinyshare_api(
                'top_list',
                ts_code=stock_code, 
                trade_date=trade_date
            )
            logger.info(f"fetch_top_list_data: {result}")
            return result
        except Exception as e:
            logger.error(f"fetch_top_list_data error: {e}")
            return pd.DataFrame()


    def fetch_top_inst_data(self, stock_code: str, end_date: str = None) -> pd.DataFrame:
        """获取龙虎榜机构明细(日期+代码)"""
        try:
            start_date, end_date_calc = self._get_date_range(end_date, years=1)
            trade_date = end_date if end_date else end_date_calc
            result = self._call_tinyshare_api(
                'top_inst',
                ts_code=stock_code, 
                trade_date=trade_date
            )
            logger.info(f"fetch_top_inst_data: {result}")
            return result
        except Exception as e:
            logger.error(f"fetch_top_inst_data error: {e}")
            return pd.DataFrame()
        
    def fetch_moneyflow_hsgt_data(self, end_date: str = None) -> pd.DataFrame:
        """获取北向资金"""
        try:
            start_date, end_date = self._get_date_range(end_date, years=1)
            result = self._call_tinyshare_api(
                'moneyflow_hsgt',
                start_date=start_date, 
                end_date=end_date
            )
            logger.info(f"fetch_moneyflow_hsgt_data: {result}")
            return result
        except Exception as e:
            logger.error(f"fetch_moneyflow_hsgt_data error: {e}")
            return pd.DataFrame()

    def fetch_cyq_perf_data(self, stock_code: str, end_date: str = None) -> pd.DataFrame:
        """每日筹码及胜率"""
        try:
            start_date, end_date = self._get_date_range(end_date, years=1)
            result = self._call_tinyshare_api(
                'cyq_perf',
                ts_code=stock_code,
                start_date=start_date,
                end_date=end_date
            )
            logger.info(f"fetch_cyq_perf_data: {result}")
            return result
        except Exception as e:
            logger.error(f"fetch_cyq_perf_data error: {e}")
            return pd.DataFrame()
    
    def fetch_cyq_chips_data(self, stock_code: str, end_date: str = None) -> pd.DataFrame:
        """获取每日筹码分布"""
        try:
            start_date, end_date = self._get_date_range(end_date, years=1)
            result = self._call_tinyshare_api(
                'cyq_chips',
                ts_code=stock_code,
                start_date=start_date,
                end_date=end_date
            )
            logger.info(f"fetch_cyq_chips_data: {result}")
            return result
        except Exception as e:
            logger.error(f"fetch_cyq_chips_data error: {e}")
            return pd.DataFrame()

# --------------------------------- 新闻数据 Provider（独立使用 news_token） ---------------------------------
import pandas as _pd
from datetime import datetime as _dt, timedelta as _td

class NewsProvider:
    """
    使用 tinyshare 的 pro_api(news_token) 拉取新闻/公告/互动问答等信息，
    与行情/基本面数据分离，便于使用不同的授权码。

    默认提供“最近3天”的便捷查询窗口，也可显式传入 start_date/end_date。
    时间字符串统一为 'YYYY-MM-DD HH:MM:SS'。
    """

    #: 快讯可用来源标识
    NEWS_SOURCES = [
        'sina',         # 新浪财经
        'wallstreetcn', # 华尔街见闻
        '10jqka',       # 同花顺
        'eastmoney',    # 东方财富
        'yuncaijing',   # 云财经
        'fenghuang',    # 凤凰新闻
        'jinrongjie',   # 金融界
        'cls',          # 财联社
        'yicai',        # 第一财经
    ]

    def __init__(self, news_token: str | None):
        self._token = news_token
        self._client = None
        self.is_available = False
        if news_token:
            try:
                self._client = tns.pro_api(news_token)
                # 轻量自检：拉取近1条快讯（容错 src）
                _end = _dt.now().strftime('%Y-%m-%d %H:%M:%S')
                _start = (_dt.now() - _td(days=1)).strftime('%Y-%m-%d %H:%M:%S')
                try:
                    # 部分版本为 news_ts，部分为 news，做兼容
                    if hasattr(self._client, 'news_ts'):
                        _ = self._client.news_ts(src='cls', start_date=_start, end_date=_end)
                    else:
                        _ = self._client.news(src='cls', start_date=_start, end_date=_end)
                    self.is_available = True
                    logger.info('NewsProvider: 接口连通性检查通过')
                except Exception as _e:
                    logger.warning(f'NewsProvider连通性检查失败: {_e}')
                    self.is_available = False
            except Exception as e:
                logger.warning(f'NewsProvider 初始化失败: {e}')

    # ------------------------- 辅助：时间区间 -------------------------
    def _range(self, start_date: str | None = None, end_date: str | None = None, days: int = 3):
        def _parse(s):
            for fmt in ('%Y-%m-%d %H:%M:%S', '%Y%m%d', '%Y-%m-%d'):
                try:
                    return _dt.strptime(s, fmt)
                except Exception:
                    continue
            return None

        end_dt = _parse(end_date) if end_date else _dt.now()
        start_dt = _parse(start_date) if start_date else (end_dt - _td(days=days))
        return (
            start_dt.strftime('%Y-%m-%d %H:%M:%S'),
            end_dt.strftime('%Y-%m-%d %H:%M:%S'),
        )

    # ------------------------- 快讯：news / news_ts -------------------------
    def fetch_news(self, src: str = 'cls', start_date: str | None = None, end_date: str | None = None,
                   days: int = 3) -> _pd.DataFrame:
        """获取主流网站快讯。默认抓取最近3天；支持 src 指定来源。"""
        if not self._client:
            return _pd.DataFrame()
        s, e = self._range(start_date, end_date, days)
        try:
            df = self._client.news_ts(src=src, start_date=s, end_date=e)
            logger.info(f"使用news_ts接口获取快讯数据: {src}, {s} 到 {e}")
            return df if isinstance(df, _pd.DataFrame) else _pd.DataFrame(df)
        except Exception as e:
            logger.error(f'fetch_news失败: {e}')
            return _pd.DataFrame()

    # ------------------------- 通讯长文：major_news -------------------------
    def fetch_major_news(self, src: str | None = None, start_date: str | None = None,
                         end_date: str | None = None, days: int = 3, fields: list[str] | None = None) -> _pd.DataFrame:
        if not self._client:
            return _pd.DataFrame()
        s, e = self._range(start_date, end_date, days)
        kwargs = {'start_date': s, 'end_date': e}
        if src:
            kwargs['src'] = src
        if fields:
            kwargs['fields'] = fields
        try:
            df = self._client.major_news_ts(**kwargs)
            return df if isinstance(df, _pd.DataFrame) else _pd.DataFrame(df)
        except Exception as e:
            logger.error(f'fetch_major_news失败: {e}')
            return _pd.DataFrame()

    # ------------------------- 新闻联播文字稿：cctv_news -------------------------
    def fetch_cctv_news_recent(self, days: int = 3) -> _pd.DataFrame:
        if not self._client:
            return _pd.DataFrame()
        rows = []
        for i in range(days):
            d = (_dt.now() - _td(days=i)).strftime('%Y%m%d')
            try:
                df = self._client.cctv_news_ts(date=d)
                if isinstance(df, _pd.DataFrame) and not df.empty:
                    rows.append(df)
            except Exception as e:
                logger.warning(f'cctv_news获取 {d} 失败: {e}')
        return _pd.concat(rows, ignore_index=True) if rows else _pd.DataFrame()

