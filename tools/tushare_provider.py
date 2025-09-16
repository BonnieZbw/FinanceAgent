# 文件: tools/tushare_provider.py (新增)
# 描述: 封装所有 Tushare API 调用，作为唯一的数据源提供者。
# -----------------------------------------------------------------
import pandas as pd
from datetime import datetime, timedelta
import logging
import inspect
import tushare as ts

logger = logging.getLogger(__name__)


class TushareProvider:
    def __init__(self, pro_client=None):
        """
        初始化TushareProvider，只使用tushare
        
        Args:
            pro_client: Tushare pro客户端
        """
        self.pro = pro_client
        self.is_available = False
        
        if self.pro:
            logger.info("--- TushareProvider: 使用tushare作为主要数据源 ---")
            # 进行接口测试验证可用性
            self.is_available = self._test_availability()
            if self.is_available:
                logger.info("--- TushareProvider: 接口测试成功，数据源可用 ---")
            else:
                logger.warning("--- TushareProvider: 接口测试失败，数据源不可用 ---")
        else:
            logger.warning("--- TushareProvider: 未提供tushare客户端 ---")
    
    def _test_availability(self):
        """测试TushareProvider是否真正可用"""
        try:
            # 使用一个简单的接口调用测试
            test_stock = "000001.SZ"  # 平安银行作为测试股票
            
            # 尝试获取日线数据
            test_data = self.fetch_daily_basic_data(test_stock)
            if test_data is not None and not test_data.empty:
                logger.info(f"TushareProvider 接口测试成功，获取到 {len(test_data)} 条数据")
                return True
            else:
                logger.warning("TushareProvider 接口测试返回空数据")
                return False
        except Exception as e:
            logger.warning(f"TushareProvider 接口测试失败: {e}")
            return False
    
    def _call_tushare_api(self, method_name, *args, **kwargs):
        """
        调用tushare API的方法
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

        # 1) tushare 优先
        if self.pro:
            try:
                logger.info(f"尝试使用tushare调用 {method_name}")
                method = getattr(self.pro, method_name)
                result = _normalize_result(method(*args, **kwargs))
                if result is not None and not result.empty:
                    logger.info(f"✅ tushare调用 {method_name} 成功，返回 {result.shape}")
                    return result
                else:
                    logger.info(f"tushare调用 {method_name} 返回空数据（正常情况）")
                    return pd.DataFrame()
            except Exception as e:
                logger.warning(f"❌ tushare调用 {method_name} 失败: {e}")
                # 异常情况下，继续执行到最后的错误处理
        else:
            # 如果没有 pro 客户端，直接记录错误
            logger.error(f"❌ {method_name} 调用失败，tushare无法获取数据")
            return pd.DataFrame()

        # 只有在异常情况下才执行到这里
        logger.error(f"❌ {method_name} 调用失败，tushare无法获取数据")
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

    def _try_fallback_dates(self, api_method: str, start_date: str, max_days: int = 5) -> pd.DataFrame:
        """尝试回退到前几个交易日获取数据"""
        try:
            start_date_dt = datetime.strptime(start_date, '%Y%m%d')
            
            for i in range(1, max_days + 1):
                # 往前推i天
                fallback_date = (start_date_dt - timedelta(days=i)).strftime('%Y%m%d')
                logger.info(f"尝试回退到 {fallback_date} (第{i}天)")
                
                # 调用API获取数据
                result = self._call_tushare_api(api_method, trade_date=fallback_date)
                
                if result is not None and not result.empty:
                    logger.info(f"✅ 在 {fallback_date} 找到数据，共 {len(result)} 条")
                    return result
                else:
                    logger.info(f"❌ {fallback_date} 没有数据，继续回退")
            
            logger.warning(f"尝试了 {max_days} 个交易日都没有找到数据")
            return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"日期回退过程中出错: {e}")
            return pd.DataFrame()

    """--------------------------------- 基本面数据 ---------------------------------"""

    def fetch_fina_indicator_data(self, stock_code: str, end_date: str = None) -> pd.DataFrame:
        """调用Tushare API获取财务指标数据。"""
        try:
            start_date, end_date = self._get_date_range(end_date)
            result = self._call_tushare_api(
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
        """调用Tushare API获取日线行情数据（基本面分析使用）。"""
        try:
            start_date, end_date = self._get_date_range(end_date)
            result = self._call_tushare_api(
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
        """调用Tushare API获取分红数据。"""
        try:
            result = self._call_tushare_api('dividend', ts_code=stock_code)
            logger.info(f"fetch_dividend_data: {result}")
            return result
        except Exception as e:
            logger.error(f"fetch_dividend_data error: {e}")
            return pd.DataFrame()

    def fetch_income_data(self, stock_code: str, end_date: str = None) -> pd.DataFrame:
        """调用Tushare API获取营业收入数据。"""
        try:
            start_date, end_date = self._get_date_range(end_date)
            result = self._call_tushare_api(
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
        """调用Tushare API获取资产负债表数据。"""
        try:
            start_date, end_date = self._get_date_range(end_date)
            result = self._call_tushare_api(
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
        """调用Tushare API获取现金流量表数据。"""
        try:
            start_date, end_date = self._get_date_range(end_date)
            result = self._call_tushare_api(
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
        """调用Tushare API获取业绩预告数据。"""
        try:
            start_date, end_date = self._get_date_range(end_date)
            result = self._call_tushare_api(
                'forecast',
                ts_code=stock_code,
                # start_date=start_date,
                end_date=end_date
            )
            logger.info(f"fetch_forecast_data: {result}")
            return result
        except Exception as e:
            logger.error(f"fetch_forecast_data error: {e}")
            return pd.DataFrame()

    
    def fetch_express_data(self, stock_code: str, end_date: str = None) -> pd.DataFrame:
        """调用Tushare API获取业绩快报数据。"""
        try:
            start_date, end_date = self._get_date_range(end_date)
            result = self._call_tushare_api(
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
        """调用Tushare API获取主营业务数据。"""
        try:
            start_date, end_date = self._get_date_range(end_date)
            result = self._call_tushare_api(
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
        只使用 tushare 模块函数 ts.pro_bar（需要全局 set_token）。
        """
        try:
            start_date, end_date = self._get_date_range(end_date, years=5)

            # 使用 tushare: 模块函数 ts.pro_bar（依赖全局 ts.set_token）
            try:
                logger.info("尝试使用tushare调用 ts.pro_bar")
                result = ts.pro_bar(
                    ts_code=stock_code,
                    start_date=start_date,
                    end_date=end_date,
                    adj=adj,
                    ma=ma,
                    asset="E",
                    freq=freq,
                )
                if result is not None and not result.empty:
                    logger.info("✅ tushare调用 ts.pro_bar 成功")
                    return result
                else:
                    logger.info("tushare调用 ts.pro_bar 返回空数据（正常情况）")
                    return pd.DataFrame()
            except Exception as e:
                logger.warning(f"❌ tushare调用 ts.pro_bar 失败: {e}")

            logger.error("❌ pro_bar 调用失败，tushare无法获取数据")
            return pd.DataFrame()

        except Exception as e:
            logger.error(f"fetch_pro_bar_data error: {e}")
            return pd.DataFrame()

    def fetch_stk_factor_data(self, stock_code: str, end_date: str = None) -> pd.DataFrame:
        """技术指标数据（MACD/KDJ/RSI等）"""
        try:
            start_date, end_date = self._get_date_range(end_date)
            result = self._call_tushare_api(
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
            result = self._call_tushare_api(
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
            result = self._call_tushare_api(
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
            result = self._call_tushare_api(
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
            result = self._call_tushare_api(
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
            result = self._call_tushare_api(
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

    def fetch_moneyflow_hsgt_data(self, end_date: str = None) -> pd.DataFrame:
        """
        获取【北向资金】数据：
        1) 优先使用 AkShare: ak.stock_hsgt_hist_em(symbol="北向资金")
        2) 失败则回退 Tushare: pro.moneyflow_hsgt
        3) 统一输出字段：trade_date, 当日成交净买额, 买入成交额, 卖出成交额, 历史累计净买额, 当日资金流入
        4) 自动处理中文单位（亿/万）、千分位符号等为 float
        5) 支持以 end_date 为截止的一年窗口，并带 1~5 天回退兜底
        """
        import math
        from datetime import datetime as _dt, timedelta as _td

        def _coerce_number(x):
            """将 '12.3亿'、'5,678.9'、'1.2 万' 等字符串安全转换为 float。"""
            if x is None or (isinstance(x, float) and pd.isna(x)):
                return math.nan
            if isinstance(x, (int, float)):
                return float(x)
            s = str(x).strip()
            if s == "" or s.lower() == "nan":
                return math.nan
            mul = 1.0
            # 去除空格
            s = s.replace(" ", "")
            # 单位处理
            if s.endswith("亿"):
                mul = 1e8
                s = s[:-1]
            elif s.endswith("万"):
                mul = 1e4
                s = s[:-1]
            # 千分位符
            s = s.replace(",", "").replace("，", "")
            try:
                return float(s) * mul
            except Exception:
                try:
                    return float(pd.to_numeric(s, errors="coerce")) * mul
                except Exception:
                    return math.nan

        def _normalize_akshare_df(df: pd.DataFrame) -> pd.DataFrame:
            """将 AkShare 返回表统一到内部标准列名与数值格式。"""
            if df is None or df.empty:
                return pd.DataFrame()

            out = df.copy()

            # 统一日期列
            if "trade_date" not in out.columns:
                if "日期" in out.columns:
                    out["trade_date"] = pd.to_datetime(out["日期"]).dt.strftime("%Y%m%d")
                elif "交易日期" in out.columns:
                    out["trade_date"] = pd.to_datetime(out["交易日期"]).dt.strftime("%Y%m%d")

            candidates = {
                # 当日成交净买额
                "当日成交净买额": [
                    "当日成交净买额", "当日净买额",
                    "北向资金-净流入", "北向资金-净流入(亿)",
                    "净流入", "净流入(亿)", "净买额", "净买额(亿)"
                ],
                # 买入成交额
                "买入成交额": [
                    "买入成交额", "买入额", "买入总额", "买入成交金额",
                    "北向资金-买入金额", "北向资金-买入金额(亿)"
                ],
                # 卖出成交额
                "卖出成交额": [
                    "卖出成交额", "卖出额", "卖出总额", "卖出成交金额",
                    "北向资金-卖出金额", "北向资金-卖出金额(亿)"
                ],
                # 历史累计净买额
                "历史累计净买额": [
                    "历史累计净买额", "历史净买额",
                    "北向资金-净流入-历史累计",
                    "北向资金-累计净买额", "北向资金-累计净买额(亿)",
                    "历史累计净流入", "累计净买额", "累计净买额(亿)"
                ],
                # 当日资金流入（如有）
                "当日资金流入": [
                    "当日资金流入", "资金净流入", "当日净流入",
                    "北向资金净流入", "北向资金净流入(亿)"
                ],
            }

            # 精确映射
            rename_map = {}
            for target, cands in candidates.items():
                for c in cands:
                    if c in out.columns:
                        rename_map[c] = target
                        break
                if target not in out.columns and target not in rename_map.values():
                    out[target] = pd.NA

            # 模糊兜底：若精确匹配失败，用包含关系粗抓
            if out.columns.size > 0:
                cols = list(out.columns)
                def _find_like(keys):
                    for col in cols:
                        s = str(col)
                        if all(k in s for k in keys):
                            return col
                    return None
                fuzzy_map = {
                    "当日成交净买额": _find_like(["净", "入"]),
                    "买入成交额": _find_like(["买", "入"]),
                    "卖出成交额": _find_like(["卖", "出"]),
                    "历史累计净买额": _find_like(["累计", "净"]),
                    "当日资金流入": _find_like(["资", "金", "流"]),
                }
                for tgt, src in fuzzy_map.items():
                    if src and tgt not in out.columns:
                        rename_map[src] = tgt

            if rename_map:
                out = out.rename(columns=rename_map)

            # 只保留关心列
            keep = ["trade_date"] + list(candidates.keys())
            keep = [c for c in keep if c in out.columns]
            if "trade_date" not in keep and "trade_date" in out.columns:
                keep = ["trade_date"] + keep
            out = out[keep].copy()

            # 数值化
            for col in ["当日成交净买额", "买入成交额", "卖出成交额", "历史累计净买额", "当日资金流入"]:
                if col in out.columns:
                    out[col] = out[col].apply(_coerce_number)

            # 兜底：若仍未命中任何目标列，再做一次基于关键字的填充
            numeric_targets = ["当日成交净买额", "买入成交额", "卖出成交额", "历史累计净买额", "当日资金流入"]
            if all(col not in out.columns for col in numeric_targets):
                like_cols = [c for c in df.columns if "北向资金" in str(c) or "净" in str(c)]
                for c in like_cols:
                    if "累计" in c and "历史累计净买额" not in out.columns:
                        out["历史累计净买额"] = df[c].apply(_coerce_number)
                    elif "买入" in c and "买入成交额" not in out.columns:
                        out["买入成交额"] = df[c].apply(_coerce_number)
                    elif "卖出" in c and "卖出成交额" not in out.columns:
                        out["卖出成交额"] = df[c].apply(_coerce_number)
                    elif ("净流入" in c or "净买额" in c) and "当日成交净买额" not in out.columns:
                        out["当日成交净买额"] = df[c].apply(_coerce_number)

            # 去重排序
            if "trade_date" in out.columns:
                out = (
                    out.dropna(subset=["trade_date"])
                       .drop_duplicates(subset=["trade_date"])
                       .sort_values("trade_date")
                       .reset_index(drop=True)
                )
            return out

        # ---------- 1) AkShare 优先 ----------
        ak_df = pd.DataFrame()
        try:
            logger.info("尝试使用 AkShare 获取北向资金...")
            import akshare as ak
            ak_df = ak.stock_hsgt_hist_em(symbol="北向资金")
            if ak_df is None:
                ak_df = pd.DataFrame()
        except Exception as e:
            logger.warning(f"AkShare 获取北向资金失败: {e}")
            ak_df = pd.DataFrame()

        if ak_df is not None and not ak_df.empty:
            norm = _normalize_akshare_df(ak_df)
            # 一年窗口过滤 + 最多5天回退
            if end_date:
                try:
                    start_date, end_date_filter = self._get_date_range(end_date, years=1)
                    sub = norm[(norm["trade_date"] >= start_date) & (norm["trade_date"] <= end_date_filter)]
                    if sub.empty:
                        end_dt = _dt.strptime(end_date, "%Y%m%d")
                        for i in range(1, 6):
                            fb = (end_dt - _td(days=i)).strftime("%Y%m%d")
                            fb_start = (end_dt - _td(days=i) - _td(days=365)).strftime("%Y%m%d")
                            tmp = _normalize_akshare_df(ak_df)
                            tmp = tmp[(tmp["trade_date"] >= fb_start) & (tmp["trade_date"] <= fb)]
                            if not tmp.empty:
                                logger.info(f"✅ AkShare 回退到 {fb} 找到 {len(tmp)} 条北向资金")
                                sub = tmp
                                break
                    if sub is not None and not sub.empty:
                        logger.info(f"AkShare 规范化后范围: {sub['trade_date'].min()} ~ {sub['trade_date'].max()}，共 {len(sub)} 条")
                        return sub
                    else:
                        raw_norm = _normalize_akshare_df(ak_df)
                        if raw_norm is not None and not raw_norm.empty:
                            logger.warning("AkShare 过滤后为空，返回未过滤的规范化数据作为兜底")
                            return raw_norm
                except Exception as e:
                    logger.warning(f"AkShare 窗口过滤失败: {e}")
                    if norm is not None and not norm.empty:
                        return norm
            else:
                if norm is not None and not norm.empty:
                    logger.info(f"AkShare 规范化后共 {len(norm)} 条")
                    return norm

        # ---------- 2) 回退 Tushare ----------
        try:
            if end_date is not None:
                today = _dt.now().date()
                try:
                    end_date_dt = _dt.strptime(end_date, "%Y%m%d").date()
                    if end_date_dt == today:
                        end_date = (today - _td(days=1)).strftime("%Y%m%d")
                        logger.info(f"为规避当日延迟，将 end_date 回退到 {end_date}")
                except ValueError:
                    pass

            start_date, end_date_calc = self._get_date_range(end_date, years=1)
            ts_df = self._call_tushare_api(
                "moneyflow_hsgt",
                start_date=start_date,
                end_date=end_date_calc
            )

            if ts_df is not None and not ts_df.empty:
                rename_ts = {
                    "north_net_buy": "当日成交净买额",
                    "buy_value": "买入成交额",
                    "sell_value": "卖出成交额",
                    "north_net_buy_cum": "历史累计净买额",
                    "north_money": "当日资金流入",
                }
                # 别名兜底
                alias = {
                    "north_net_buy": ["north_net_buy", "net_buy", "net_amount"],
                    "buy_value": ["buy_value", "buy_amount", "buy"],
                    "sell_value": ["sell_value", "sell_amount", "sell"],
                    "north_net_buy_cum": ["north_net_buy_cum", "acc_net_buy", "cum_net_buy"],
                    "north_money": ["north_money", "north_inflow", "amount", "money"],
                }
                for k, v in list(rename_ts.items()):
                    if k not in ts_df.columns:
                        hit = None
                        for a in alias.get(k, []):
                            if a in ts_df.columns:
                                hit = a
                                break
                        if hit:
                            rename_ts[hit] = v
                            rename_ts.pop(k, None)
                        else:
                            rename_ts.pop(k, None)

                ts_df = ts_df.rename(columns=rename_ts)

                keep_ts = ["trade_date", "当日成交净买额", "买入成交额", "卖出成交额", "历史累计净买额", "当日资金流入"]
                keep_ts = [c for c in keep_ts if c in ts_df.columns]
                ts_df = ts_df[keep_ts].copy()

                for col in ["当日成交净买额", "买入成交额", "卖出成交额", "历史累计净买额", "当日资金流入"]:
                    if col in ts_df.columns:
                        ts_df[col] = pd.to_numeric(ts_df[col], errors="coerce")

                if "trade_date" in ts_df.columns:
                    ts_df = (
                        ts_df.dropna(subset=["trade_date"])
                             .drop_duplicates(subset=["trade_date"])
                             .sort_values("trade_date")
                             .reset_index(drop=True)
                    )
                logger.info(f"Tushare 北向资金时间范围: {ts_df['trade_date'].min()} ~ {ts_df['trade_date'].max()}，共 {len(ts_df)} 条")
                return ts_df
        except Exception as e:
            logger.warning(f"Tushare 获取北向资金失败: {e}")

        logger.warning("AkShare 与 Tushare 均未得到有效的北向资金数据")
        return pd.DataFrame()
        
    def fetch_moneyflow_ths_data(self, stock_code: str, end_date: str = None) -> pd.DataFrame:
        """获取个股主力动向"""
        try:
            # 个股主力动向接口不支持日期范围查询，只能获取所有数据后本地过滤
            result = self._call_tushare_api(
                'moneyflow_ths',
                ts_code=stock_code
            )
            
            # 如果指定了end_date，进行本地日期过滤
            if end_date is not None and not result.empty:
                start_date, end_date_filter = self._get_date_range(end_date, years=1)
                # 过滤数据
                filtered_result = result[(result['trade_date'] >= start_date) & (result['trade_date'] <= end_date_filter)]
                
                # 如果过滤后为空，记录数据可用性信息
                if filtered_result.empty:
                    actual_start = result['trade_date'].min()
                    actual_end = result['trade_date'].max()
                    logger.warning(f"个股主力动向数据在指定时间范围内为空。数据可用范围: {actual_start} 到 {actual_end}，查询范围: {start_date} 到 {end_date_filter}")
                
                result = filtered_result
            
            logger.info(f"fetch_moneyflow_ths_data: {result}")
            return result
        except Exception as e:
            logger.error(f"fetch_moneyflow_ths_data error: {e}")
            return pd.DataFrame()
    
    def fetch_moneyflow_cnt_ths_data(self, stock_code: str, end_date: str = None) -> pd.DataFrame:
        """获取板块主力动向 - 优先使用moneyflow_ind_dc接口，支持自动日期回退"""
        try:
            # 优先使用moneyflow_ind_dc接口，支持日期范围查询
            logger.info("尝试使用moneyflow_ind_dc接口获取板块资金流向数据")
            result = self.fetch_moneyflow_ind_dc_data(stock_code, end_date, content_type=None)
            
            if result is not None and not result.empty:
                logger.info(f"moneyflow_ind_dc接口成功获取 {len(result)} 条板块资金流向数据")
                return result
            else:
                logger.warning("moneyflow_ind_dc接口返回空数据，回退到moneyflow_cnt_ths接口")
                
                # 回退到原始接口（单日查询）
                start_date, end_date_calc = self._get_date_range(end_date, years=1)
                trade_date = end_date if end_date else end_date_calc
                
                # 尝试获取指定日期的数据
                result = self._call_tushare_api(
                    'moneyflow_cnt_ths',
                    trade_date=trade_date
                )
                
                # 如果指定日期没有数据，尝试自动回退到前几个交易日
                if result is None or result.empty:
                    logger.info(f"指定日期 {trade_date} 没有数据，尝试自动回退到前几个交易日")
                    result = self._try_fallback_dates('moneyflow_cnt_ths', trade_date, max_days=5)
                
                logger.info(f"fetch_moneyflow_cnt_ths_data: 最终获取到 {len(result) if result is not None else 0} 条数据")
                return result
        except Exception as e:
            logger.error(f"fetch_moneyflow_cnt_ths_data error: {e}")
            return pd.DataFrame()   
    
    def fetch_moneyflow_ind_ths_data(self, stock_code: str, end_date: str = None) -> pd.DataFrame:
        """获取行业主力动向"""
        try:
            # 行业主力动向接口支持start_date和end_date参数
            start_date, end_date_calc = self._get_date_range(end_date, years=1)
            result = self._call_tushare_api(
                'moneyflow_ind_ths',
                start_date=start_date,
                end_date=end_date if end_date else end_date_calc
            )
            
            # 如果指定了股票代码，尝试根据股票行业筛选相关数据
            if stock_code and not result.empty:
                # 获取股票基本信息
                stock_basic = self.fetch_stock_basic_data(stock_code)
                if not stock_basic.empty and 'industry' in stock_basic.columns:
                    stock_industry = stock_basic.iloc[0]['industry']
                    logger.info(f"股票 {stock_code} 所属行业: {stock_industry}")
                    
                    # 筛选相关行业数据
                    if 'industry' in result.columns:
                        # 精确匹配
                        exact_match = result[result['industry'] == stock_industry]
                        if not exact_match.empty:
                            logger.info(f"找到精确匹配的行业数据: {len(exact_match)} 条")
                            result = exact_match
                        else:
                            # 智能模糊匹配
                            # 1. 直接包含匹配
                            direct_match = result[result['industry'].str.contains(stock_industry, na=False)]
                            if not direct_match.empty:
                                logger.info(f"找到直接包含匹配的行业数据: {len(direct_match)} 条")
                                result = direct_match
                            else:
                                # 2. 反向匹配（行业数据包含股票行业关键词）
                                reverse_match = result[result['industry'].str.contains(stock_industry, na=False)]
                                if not reverse_match.empty:
                                    logger.info(f"找到反向匹配的行业数据: {len(reverse_match)} 条")
                                    result = reverse_match
                                else:
                                    # 3. 关键词匹配（提取核心关键词进行匹配）
                                    # 定义行业关键词映射
                                    industry_keywords = {
                                        '全国地产': '房地产',
                                        '区域地产': '房地产',
                                        '地产': '房地产',
                                        '白酒': '白酒',
                                        '啤酒': '饮料制造',
                                        '葡萄酒': '饮料制造',
                                        '银行': '银行',
                                        '保险': '保险',
                                        '证券': '证券',
                                        '信托': '多元金融',
                                        '基金': '多元金融',
                                        '汽车': '汽车整车',
                                        '家电': '白色家电',
                                        '医药': '化学制药',
                                        '化工': '化学制品',
                                        '钢铁': '钢铁',
                                        '煤炭': '煤炭开采加工',
                                        '石油': '石油石化',
                                        '电力': '电力',
                                        '通信': '通信设备',
                                        '电子': '电子',
                                        '计算机': '计算机设备',
                                        '软件': '软件开发',
                                        '互联网': '互联网电商',
                                        '传媒': '文化传媒',
                                        '教育': '教育',
                                        '旅游': '旅游及酒店',
                                        '餐饮': '酒店及餐饮',
                                        '零售': '零售',
                                        '贸易': '贸易',
                                        '物流': '物流',
                                        '建筑': '建筑装饰',
                                        '建材': '建筑材料',
                                        '机械': '专用设备',
                                        '设备': '专用设备',
                                        '军工': '国防军工',
                                        '航空': '航空装备',
                                        '船舶': '船舶制造',
                                        '铁路': '铁路设备',
                                        '新能源': '光伏设备',
                                        '光伏': '光伏设备',
                                        '风电': '风电设备',
                                        '核电': '核电',
                                        '环保': '环保',
                                        '农业': '农产品加工',
                                        '食品': '食品加工制造',
                                        '服装': '服装家纺',
                                        '纺织': '纺织制造',
                                        '家居': '家居用品',
                                        '游戏': '游戏',
                                        '影视': '影视院线',
                                        '体育': '体育',
                                        '娱乐': '文化传媒'
                                    }
                                    
                                    # 尝试关键词匹配
                                    matched_industries = []
                                    for keyword, target_industry in industry_keywords.items():
                                        if keyword in stock_industry:
                                            matched_industries.append(target_industry)
                                    
                                    if matched_industries:
                                        # 使用第一个匹配的行业
                                        target_industry = matched_industries[0]
                                        keyword_match = result[result['industry'] == target_industry]
                                        if not keyword_match.empty:
                                            logger.info(f"通过关键词映射找到行业数据: {target_industry}, {len(keyword_match)} 条")
                                            result = keyword_match
                                        else:
                                            logger.warning(f"未找到与股票行业 '{stock_industry}' 相关的行业数据")
                                    else:
                                        logger.warning(f"未找到与股票行业 '{stock_industry}' 相关的行业数据")
                    else:
                        logger.warning("行业主力动向数据中缺少industry字段")
                else:
                    logger.warning(f"无法获取股票 {stock_code} 的基本信息或行业信息")
            
            logger.info(f"fetch_moneyflow_ind_ths_data: {result}")
            return result
        except Exception as e:
            logger.error(f"fetch_moneyflow_ind_ths_data error: {e}")
            return pd.DataFrame()

    def fetch_moneyflow_mkt_dc_data(self, stock_code: str, end_date: str = None) -> pd.DataFrame:
        """获取大盘资金流向"""
        try:
            # 大盘资金流向接口支持start_date和end_date参数
            start_date, end_date_calc = self._get_date_range(end_date, years=1)
            result = self._call_tushare_api(
                'moneyflow_mkt_dc',
                start_date=start_date,
                end_date=end_date if end_date else end_date_calc
            )
            logger.info(f"fetch_moneyflow_mkt_dc_data: {result}")
            return result
        except Exception as e:
            logger.error(f"fetch_moneyflow_mkt_dc_data error: {e}")
            return pd.DataFrame()


    def fetch_top_list_data(self, stock_code: str, end_date: str = None) -> pd.DataFrame:
        """获取龙虎榜每日统计(代码)"""
        try:
            start_date, end_date_calc = self._get_date_range(end_date, years=1)
            trade_date = end_date if end_date else end_date_calc
            result = self._call_tushare_api(
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
        """获取龙虎榜机构明细(代码)"""
        try:
            start_date, end_date_calc = self._get_date_range(end_date, years=1)
            trade_date = end_date if end_date else end_date_calc
            result = self._call_tushare_api(
                'top_inst',
                ts_code=stock_code,
                trade_date=trade_date
            )
            logger.info(f"fetch_top_inst_data: {result}")
            return result
        except Exception as e:
            logger.error(f"fetch_top_inst_data error: {e}")
            return pd.DataFrame()
        
    
    def fetch_moneyflow_ind_dc_data(self, stock_code: str = None, end_date: str = None, 
                                   content_type: str = None) -> pd.DataFrame:
        """获取板块资金流向数据
        
        Args:
            stock_code: 股票代码（可选）
            end_date: 结束日期（可选）
            content_type: 资金类型，可选值：行业、概念、地域（可选）
        """
        try:
            # 构建参数
            params = {}
            
            if stock_code:
                params['ts_code'] = stock_code
            if end_date:
                params['trade_date'] = end_date
            if content_type:
                params['content_type'] = content_type
            
            # 如果没有指定日期，使用默认日期范围
            if not end_date:
                start_date, end_date = self._get_date_range(None, years=1)
                params['start_date'] = start_date
                params['end_date'] = end_date
            
            result = self._call_tushare_api('moneyflow_ind_dc', **params)
            logger.info(f"fetch_moneyflow_ind_dc_data: {result}")
            return result
        except Exception as e:
            logger.error(f"fetch_moneyflow_ind_dc_data error: {e}")
            return pd.DataFrame()

    def fetch_moneyflow_hsgt_data(self, end_date: str = None) -> pd.DataFrame:
        """获取北向资金数据 - 优先使用akshare，Tushare作为备用，并统一成内部字段
        统一后的字段：
        - trade_date: YYYYMMDD
        - 当日成交净买额
        - 买入成交额
        - 卖出成交额
        - 历史累计净买额
        - 当日资金流入
        """
        def _coerce_number(x):
            """
            将带有中文单位/逗号/空格的字符串转换为float。
            支持：'亿'->1e8, '万'->1e4；无单位按原值；无法解析返回NaN。
            """
            import math
            if x is None or (isinstance(x, float) and pd.isna(x)):
                return math.nan
            if isinstance(x, (int, float)):
                return float(x)
            s = str(x).strip()
            if s == "" or s.lower() == "nan":
                return math.nan
            mul = 1.0
            if s.endswith("亿"):
                mul = 1e8
                s = s[:-1]
            elif s.endswith("万"):
                mul = 1e4
                s = s[:-1]
            # 去掉中文与英文逗号、空格
            s = s.replace(",", "").replace("，", "").replace(" ", "")
            try:
                return float(s) * mul
            except Exception:
                # 再尝试去除可能的单位或符号
                try:
                    return float(pd.to_numeric(s, errors="coerce")) * mul
                except Exception:
                    return math.nan

        def _normalize_akshare_df(df: pd.DataFrame) -> pd.DataFrame:
            """
            将 AkShare 的北向资金 DataFrame 统一为内部字段。
            AkShare 可能出现的字段名（不同版本会略有差异），这里做了"多对一"的鲁棒映射。
            """
            if df is None or df.empty:
                return pd.DataFrame()

            # 标准目标列
            target_cols = [
                "trade_date",
                "当日成交净买额",
                "买入成交额",
                "卖出成交额",
                "历史累计净买额",
                "当日资金流入",
            ]

            # 复制以免修改原对象
            out = df.copy()

            # 统一日期列 -> trade_date
            # 常见：'日期'
            if "trade_date" not in out.columns:
                if "日期" in out.columns:
                    out["trade_date"] = pd.to_datetime(out["日期"]).dt.strftime("%Y%m%d")
                elif "交易日期" in out.columns:
                    out["trade_date"] = pd.to_datetime(out["交易日期"]).dt.strftime("%Y%m%d")

            # 可能出现在 AkShare 的列名集合（根据不同数据源版本）
            # 你可以根据你本地 AkShare 返回的实际列名再扩充下面的候选集合
            candidates = {
                "当日成交净买额": ["当日成交净买额", "当日净买额", "北向资金-净流入", "净流入"],
                "买入成交额": ["买入成交额", "买入额", "买入总额", "买入成交金额"],
                "卖出成交额": ["卖出成交额", "卖出额", "卖出总额", "卖出成交金额"],
                "历史累计净买额": ["历史累计净买额", "历史净买额", "北向资金-净流入-历史累计", "历史累计净流入"],
                "当日资金流入": ["当日资金流入", "资金净流入", "当日净流入", "北向资金净流入"],
            }

            # 建立映射
            rename_map = {}
            for target, cands in candidates.items():
                for c in cands:
                    if c in out.columns:
                        rename_map[c] = target
                        break
                # 如果未找到匹配列，则创建空列，后续填充为 NaN
                if target not in rename_map.values() and target not in out.columns:
                    out[target] = pd.NA

            if rename_map:
                out = out.rename(columns=rename_map)

            # 只保留目标列 + trade_date
            keep = ["trade_date"] + [c for c in candidates.keys()]
            keep = [c for c in keep if c in out.columns]
            out = out[keep].copy()

            # 数值化处理
            for col in ["当日成交净买额", "买入成交额", "卖出成交额", "历史累计净买额", "当日资金流入"]:
                if col in out.columns:
                    out[col] = out[col].apply(_coerce_number)

            # 按日期排序并去重
            if "trade_date" in out.columns:
                out = out.dropna(subset=["trade_date"]).drop_duplicates(subset=["trade_date"]).sort_values("trade_date")

            return out.reset_index(drop=True)

        try:
            # ---------- 优先：AkShare ----------
            logger.info("尝试使用akshare获取北向资金数据...")
            try:
                import akshare as ak
                ak_df = ak.stock_hsgt_hist_em(symbol="北向资金")
            except Exception as ak_err:
                logger.warning(f"akshare获取北向资金数据失败: {ak_err}")
                ak_df = pd.DataFrame()

            if ak_df is not None and not ak_df.empty:
                norm = _normalize_akshare_df(ak_df)

                # 如果指定 end_date，则限制为 end_date 往前一年的窗口
                if end_date:
                    try:
                        start_date, end_date_filter = self._get_date_range(end_date, years=1)
                        norm = norm[(norm["trade_date"] >= start_date) & (norm["trade_date"] <= end_date_filter)]
                        # 如果窗口内没数据，尝试向前回退几天
                        if norm.empty:
                            end_dt = datetime.strptime(end_date, "%Y%m%d")
                            for i in range(1, 6):
                                fb = (end_dt - timedelta(days=i)).strftime("%Y%m%d")
                                fb_start = (end_dt - timedelta(days=i) - timedelta(days=365)).strftime("%Y%m%d")
                                tmp = _normalize_akshare_df(ak_df)
                                tmp = tmp[(tmp["trade_date"] >= fb_start) & (tmp["trade_date"] <= fb)]
                                if not tmp.empty:
                                    logger.info(f"✅ akshare回退到 {fb} 找到 {len(tmp)} 条数据")
                                    norm = tmp
                                    break
                    except Exception as f_err:
                        logger.warning(f"akshare窗口过滤失败: {f_err}")

                if norm is not None and not norm.empty:
                    logger.info(f"akshare北向资金规范化后数据范围: {norm['trade_date'].min()} ~ {norm['trade_date'].max()}，共 {len(norm)} 条")
                    return norm
                else:
                    logger.warning("akshare返回数据但在规范化/过滤后为空，尝试使用Tushare。")

            # ---------- 备用：Tushare ----------
            if end_date is not None:
                # 如果 end_date 是今天，避开当日延迟：回退一天
                today = datetime.now().date()
                try:
                    end_date_dt = datetime.strptime(end_date, "%Y%m%d").date()
                    if end_date_dt == today:
                        end_date = (today - timedelta(days=1)).strftime("%Y%m%d")
                        logger.info(f"为规避当日延迟，将 end_date 回退到 {end_date}")
                except ValueError:
                    pass

            start_date, end_date_calc = self._get_date_range(end_date, years=1)
            ts_df = self._call_tushare_api("moneyflow_hsgt", start_date=start_date, end_date=end_date_calc)

            if ts_df is not None and not ts_df.empty:
                # 尽量对 Tushare 字段进行同名对齐（若存在）
                # 假设 tushare 列：trade_date, north_money, buy_value, sell_value, north_net_flow 等（请按你本地的实际列名增补）
                rename_ts = {
                    "north_net_buy": "当日成交净买额",
                    "buy_value": "买入成交额",
                    "sell_value": "卖出成交额",
                    "north_net_buy_cum": "历史累计净买额",
                    "north_money": "当日资金流入",
                }
                for k in list(rename_ts.keys()):
                    if k not in ts_df.columns:
                        # 根据你本地的列名实际情况补充别名，这里提供一些常见别名
                        aliases = {
                            "north_net_buy": ["north_net_buy", "net_buy", "net_amount"],
                            "buy_value": ["buy_value", "buy_amount"],
                            "sell_value": ["sell_value", "sell_amount"],
                            "north_net_buy_cum": ["north_net_buy_cum", "acc_net_buy"],
                            "north_money": ["north_money", "north_inflow", "amount"],
                        }.get(k, [])
                        for a in aliases:
                            if a in ts_df.columns:
                                rename_ts[a] = rename_ts.pop(k)
                                break
                        else:
                            rename_ts.pop(k, None)
                ts_df = ts_df.rename(columns=rename_ts)

                # 只保留标准列
                keep_ts = ["trade_date", "当日成交净买额", "买入成交额", "卖出成交额", "历史累计净买额", "当日资金流入"]
                keep_ts = [c for c in keep_ts if c in ts_df.columns]
                ts_df = ts_df[keep_ts].copy()

                # 数值化
                for col in ["当日成交净买额", "买入成交额", "卖出成交额", "历史累计净买额", "当日资金流入"]:
                    if col in ts_df.columns:
                        ts_df[col] = pd.to_numeric(ts_df[col], errors="coerce")

                ts_df = ts_df.dropna(subset=["trade_date"]).drop_duplicates(subset=["trade_date"]).sort_values("trade_date").reset_index(drop=True)
                logger.info(f"Tushare北向资金数据时间范围: {ts_df['trade_date'].min()} ~ {ts_df['trade_date'].max()}，共 {len(ts_df)} 条")
                return ts_df

            # 两个源都无数据
            logger.warning("Tushare与AkShare北向资金接口均未得到有效数据")
            return pd.DataFrame()

        except Exception as e:
            logger.error(f"fetch_moneyflow_hsgt_data error: {e}")
            return pd.DataFrame()

    def fetch_moneyflow_hsgt_data_with_akshare_fallback(self, end_date: str = None) -> pd.DataFrame:
        """获取北向资金数据，优先使用Tushare，失败时使用akshare作为备用
        
        这个方法结合了Tushare和akshare的优势：
        - Tushare：历史数据完整，格式统一
        - akshare：最新数据更新及时
        """
        try:
            # 首先尝试使用Tushare获取数据
            tushare_result = self.fetch_moneyflow_hsgt_data(end_date)
            
            # 如果Tushare有数据，检查数据的新鲜度
            if tushare_result is not None and not tushare_result.empty:
                latest_date = tushare_result['trade_date'].max()
                cutoff_date = '20240816'
                
                # 如果数据足够新（晚于2024年8月16日），直接返回
                if latest_date >= cutoff_date:
                    logger.info(f"使用Tushare数据，最新日期: {latest_date}")
                    return tushare_result
                else:
                    logger.warning(f"Tushare数据过时（最新: {latest_date}），尝试使用akshare获取最新数据")
            else:
                logger.warning("Tushare返回空数据，尝试使用akshare获取数据")
            
            # 尝试使用akshare作为备用数据源
            try:
                import akshare as ak
                logger.info("尝试使用akshare获取北向资金数据...")
                
                # 获取北向资金历史数据
                akshare_result = ak.stock_hsgt_hist_em(symbol="北向资金")
                
                if akshare_result is not None and not akshare_result.empty:
                    logger.info(f"akshare获取到 {len(akshare_result)} 条北向资金数据")
                    
                    # 转换日期格式以匹配Tushare格式
                    if '日期' in akshare_result.columns:
                        akshare_result['trade_date'] = pd.to_datetime(akshare_result['日期']).dt.strftime('%Y%m%d')
                    
                    # 如果指定了日期范围，进行过滤
                    if end_date is not None:
                        start_date, end_date_filter = self._get_date_range(end_date, years=1)
                        akshare_result = akshare_result[
                            (akshare_result['trade_date'] >= start_date) & 
                            (akshare_result['trade_date'] <= end_date_filter)
                        ]
                    
                    logger.info(f"akshare北向资金数据时间范围: {akshare_result['trade_date'].min()} 到 {akshare_result['trade_date'].max()}")
                    return akshare_result
                else:
                    logger.warning("akshare也返回空数据")
                    return tushare_result  # 返回Tushare的结果（即使是空的）
                    
            except Exception as akshare_error:
                logger.error(f"akshare获取北向资金数据失败: {akshare_error}")
                return tushare_result  # 返回Tushare的结果
            
        except Exception as e:
            logger.error(f"fetch_moneyflow_hsgt_data_with_akshare_fallback error: {e}")
            return pd.DataFrame()

    def fetch_cyq_perf_data(self, stock_code: str, end_date: str = None) -> pd.DataFrame:
        """每日筹码及胜率"""
        try:
            start_date, end_date = self._get_date_range(end_date, years=1)
            result = self._call_tushare_api(
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
    
    def fetch_stock_basic_data(self, stock_code: str = None) -> pd.DataFrame:
        """获取股票基本信息"""
        try:
            if stock_code:
                result = self._call_tushare_api('stock_basic', ts_code=stock_code)
            else:
                result = self._call_tushare_api('stock_basic')
            logger.info(f"fetch_stock_basic_data: {result}")
            return result
        except Exception as e:
            logger.error(f"fetch_stock_basic_data error: {e}")
            return pd.DataFrame()
    
    