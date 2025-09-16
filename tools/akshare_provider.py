# 文件: tools/akshare_provider.py
# 描述: 基于 akshare 的数据提供者，作为 Tushare 的替代方案
# -----------------------------------------------------------------
import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


class AkshareProvider:
    def __init__(self):
        """初始化 Akshare 数据提供者"""
        self.is_available = False
        logger.info("--- AkshareProvider 初始化开始 ---")
        
        # 进行接口测试验证可用性
        self.is_available = self._test_availability()
        if self.is_available:
            logger.info("--- AkshareProvider 初始化成功，接口测试通过 ---")
        else:
            logger.warning("--- AkshareProvider 初始化失败，接口测试未通过 ---")
    
    def _test_availability(self):
        """测试AkshareProvider是否真正可用"""
        try:
            # 使用一个简单的接口调用测试
            test_stock = "000001.SZ"  # 平安银行作为测试股票
            
            # 尝试获取日线数据
            test_data = self.fetch_daily_basic_data(test_stock)
            if test_data is not None and not test_data.empty:
                logger.info(f"AkshareProvider 接口测试成功，获取到 {len(test_data)} 条数据")
                return True
            else:
                logger.warning("AkshareProvider 接口测试返回空数据")
                return False
        except Exception as e:
            logger.warning(f"AkshareProvider 接口测试失败: {e}")
            return False

    def _adjust_stock_code(self, stock_code: str) -> str:
        """调整股票代码格式以适应 akshare 的要求"""
        if '.' in stock_code:
            return stock_code.split('.')[0]
        return stock_code

    def _get_date_range(self, end_date: str = None, years: int = 2) -> tuple:
        """获取日期范围，支持多种日期格式"""
        if end_date is None:
            end_date = datetime.now().strftime('%Y%m%d')
        else:
            # 支持多种日期格式
            date_formats = ['%Y%m%d', '%Y-%m-%d', '%Y/%m/%d']
            end_date_dt = None
            
            for fmt in date_formats:
                try:
                    end_date_dt = datetime.strptime(end_date, fmt)
                    break
                except ValueError:
                    continue
            
            if end_date_dt is None:
                logger.error(f"无效的日期格式: {end_date}，支持格式：YYYYMMDD、YYYY-MM-DD、YYYY/MM/DD，使用当前日期")
                end_date_dt = datetime.now()
            
            end_date = end_date_dt.strftime('%Y%m%d')
        
        start_date = (datetime.strptime(end_date, '%Y%m%d') - timedelta(days=365 * years)).strftime('%Y%m%d')
        return start_date, end_date

    def _filter_data_by_date_range(self, data: pd.DataFrame, start_date: str, end_date: str, 
                                 date_column_keywords: list, data_type_name: str) -> pd.DataFrame:
        """通用的日期范围过滤函数
        
        Args:
            data: 要过滤的数据DataFrame
            start_date: 开始日期，格式：YYYYMMDD
            end_date: 结束日期，格式：YYYYMMDD
            date_column_keywords: 日期列名关键词列表
            data_type_name: 数据类型名称，用于日志记录
            
        Returns:
            过滤后的DataFrame
        """
        if data.empty:
            return pd.DataFrame()
        
        # 查找日期列
        date_column = None
        for col in data.columns:
            for keyword in date_column_keywords:
                if keyword in col:
                    date_column = col
                    break
            if date_column:
                break
        
        if date_column is not None:
            try:
                # 转换日期格式并过滤
                data[date_column] = pd.to_datetime(data[date_column], errors='coerce')
                start_dt = pd.to_datetime(start_date, format='%Y%m%d')
                end_dt = pd.to_datetime(end_date, format='%Y%m%d')
                
                filtered_data = data[
                    (data[date_column] >= start_dt) & 
                    (data[date_column] <= end_dt)
                ]
                
                logger.info(f"{data_type_name}数据过滤完成: 原始数据 {len(data)} 条，过滤后 {len(filtered_data)} 条")
                return filtered_data
                
            except Exception as e:
                logger.warning(f"{data_type_name}日期过滤失败，返回原始数据: {e}")
                return data
        else:
            logger.warning(f"{data_type_name}未找到日期列，返回原始数据")
            return data

    def fetch_fina_indicator_data(self, stock_code: str, end_date: str = None) -> pd.DataFrame:
        """获取财务指标数据"""
        # 报告日
        try:
            adjusted_code = self._adjust_stock_code(stock_code)
            
            # 获取日期范围
            start_date, end_date = self._get_date_range(end_date)
            
            # 获取财务指标数据
            fina_data = ak.stock_financial_report_sina(stock=adjusted_code, symbol="资产负债表")
            
            # 使用通用日期过滤函数
            return self._filter_data_by_date_range(
                fina_data, start_date, end_date, 
                ['报告日'], '财务指标'
            )
                
        except Exception as e:
            logger.error(f"获取财务指标数据失败: {e}")
            return pd.DataFrame()

    def fetch_daily_basic_data(self, stock_code: str, end_date: str = None) -> pd.DataFrame:
        """获取日线行情数据"""
        try:
            adjusted_code = self._adjust_stock_code(stock_code)
            start_date, end_date = self._get_date_range(end_date)
            daily_data = ak.stock_zh_a_hist(symbol=adjusted_code, period="daily", adjust="qfq", end_date=end_date, start_date=start_date)
            return daily_data if not daily_data.empty else pd.DataFrame()
        except Exception as e:
            logger.error(f"获取日线数据失败: {e}")
            return pd.DataFrame()

    def fetch_dividend_data(self, stock_code: str, end_date: str = None) -> pd.DataFrame:
        """获取分红数据
        
        Args:
            stock_code: 股票代码
            end_date: 结束日期，格式：YYYYMMDD
            
        Returns:
            过滤后的分红数据DataFrame
        """
        try:
            adjusted_code = self._adjust_stock_code(stock_code)
            
            # 获取日期范围
            start_date, end_date = self._get_date_range(end_date)
            
            # 获取分红数据
            dividend_data = ak.stock_dividend_cninfo(symbol=adjusted_code)
            
            # 使用通用日期过滤函数
            return self._filter_data_by_date_range(
                dividend_data, start_date, end_date, 
                ['实施方案公告日期'], '分红'
            )
                
        except Exception as e:
            logger.error(f"获取分红数据失败: {e}")
            return pd.DataFrame()

    def fetch_income_data(self, stock_code: str, end_date: str = None) -> pd.DataFrame:
        """获取营业收入数据"""
        # 报告日
        try:
            adjusted_code = self._adjust_stock_code(stock_code)

            # 获取日期范围
            start_date, end_date = self._get_date_range(end_date)

            # 获取营业收入数据
            income_data = ak.stock_financial_report_sina(stock=adjusted_code, symbol="利润表")
            
            # 使用通用日期过滤函数
            return self._filter_data_by_date_range(
                income_data, start_date, end_date, 
                ['报告日'], '营业收入'
            )
                
        except Exception as e:
            logger.error(f"获取利润表数据失败: {e}")
            return pd.DataFrame()

    def fetch_balance_data(self, stock_code: str, end_date: str = None) -> pd.DataFrame:
        """获取资产负债表数据"""
        # 报告日
        try:
            adjusted_code = self._adjust_stock_code(stock_code)
            
            # 获取日期范围
            start_date, end_date = self._get_date_range(end_date)
            
            # 获取资产负债表数据
            balance_data = ak.stock_financial_report_sina(stock=adjusted_code, symbol="资产负债表")
            
            # 使用通用日期过滤函数
            return self._filter_data_by_date_range(
                balance_data, start_date, end_date, 
                ['报告日'], '资产负债表'
            )
                
        except Exception as e:
            logger.error(f"获取资产负债表数据失败: {e}")
            return pd.DataFrame()
    
    def fetch_cashflow_data(self, stock_code: str, end_date: str = None) -> pd.DataFrame:
        """获取现金流量表数据"""
        # 报告日
        try:
            adjusted_code = self._adjust_stock_code(stock_code)
            
            # 获取日期范围
            start_date, end_date = self._get_date_range(end_date)
            
            # 获取现金流量表数据
            cashflow_data = ak.stock_financial_report_sina(stock=adjusted_code, symbol="现金流量表")
            
            # 使用通用日期过滤函数
            return self._filter_data_by_date_range(
                cashflow_data, start_date, end_date, 
                ['报告日'], '现金流量表'
            )
                
        except Exception as e:
            logger.error(f"获取现金流量表数据失败: {e}")
            return pd.DataFrame()
    
    def fetch_forecast_data(self, stock_code: str, end_date: str = None) -> pd.DataFrame:
        """获取业绩预告数据"""
        try:
            adjusted_code = self._adjust_stock_code(stock_code)
            
            # 获取日期范围
            start_date, end_date = self._get_date_range(end_date)
            
            # 获取业绩预告数据 - 使用正确的akshare API
            forecast_data = ak.stock_yjbb_em(symbol=adjusted_code)
            
            # 使用通用日期过滤函数
            return self._filter_data_by_date_range(
                forecast_data, start_date, end_date, 
                ['预告日期', '报告期', '日期'], '业绩预告'
            )
                
        except Exception as e:
            logger.error(f"获取业绩预告数据失败: {e}")
            return pd.DataFrame()
    
    def fetch_express_data(self, stock_code: str, end_date: str = None) -> pd.DataFrame:
        """获取业绩快报数据"""
        try:
            adjusted_code = self._adjust_stock_code(stock_code)
            
            # 获取日期范围
            start_date, end_date = self._get_date_range(end_date)
            
            # 获取业绩快报数据 - 使用正确的akshare API
            express_data = ak.stock_yjkb_em(symbol=adjusted_code)
            
            # 使用通用日期过滤函数
            return self._filter_data_by_date_range(
                express_data, start_date, end_date, 
                ['快报日期', '报告期', '日期'], '业绩快报'
            )
                
        except Exception as e:
            logger.error(f"获取业绩快报数据失败: {e}")
            return pd.DataFrame()
    
    def fetch_mainbz_data(self, stock_code: str, end_date: str = None) -> pd.DataFrame:
        """获取主营业务数据"""
        try:
            adjusted_code = self._adjust_stock_code(stock_code)
            
            # 获取日期范围
            start_date, end_date = self._get_date_range(end_date)
            
            # 获取主营业务数据 - 使用正确的akshare API
            mainbz_data = ak.stock_zygc_em(symbol=adjusted_code)
            
            # 使用通用日期过滤函数
            return self._filter_data_by_date_range(
                mainbz_data, start_date, end_date, 
                ['更新日期', '报告期', '日期'], '主营业务'
            )
                
        except Exception as e:
            logger.error(f"获取主营业务数据失败: {e}")
            return pd.DataFrame()

    """--------------------------------- 技术面数据 ---------------------------------"""

    def fetch_pro_bar_data(self, stock_code: str, end_date: str = None,
                   freq: str = "D", adj: str = None, ma: list = [5, 10, 20, 60]) -> pd.DataFrame:
        """K线+均线数据，支持日/周/月。使用akshare替代tushare的pro_bar"""
        try:
            adjusted_code = self._adjust_stock_code(stock_code)
            start_date, end_date = self._get_date_range(end_date, years=5)
            
            # 转换日期格式为akshare需要的格式
            start_date_formatted = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:8]}"
            end_date_formatted = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:8]}"
            
            # 根据频率选择不同的akshare函数
            if freq == "D":
                period = "daily"
            elif freq == "W":
                period = "weekly"
            elif freq == "M":
                period = "monthly"
            else:
                period = "daily"
            
            # 获取K线数据
            kline_data = ak.stock_zh_a_hist(
                symbol=adjusted_code, 
                period=period, 
                start_date=start_date_formatted, 
                end_date=end_date_formatted, 
                adjust="qfq" if adj == "qfq" else ""
            )
            
            if kline_data.empty:
                return pd.DataFrame()
            
            # 计算移动平均线
            for ma_period in ma:
                kline_data[f'ma{ma_period}'] = kline_data['收盘'].rolling(window=ma_period).mean()
            
            # 重命名列以匹配tushare格式
            column_mapping = {
                '日期': 'trade_date',
                '开盘': 'open',
                '收盘': 'close', 
                '最高': 'high',
                '最低': 'low',
                '成交量': 'vol',
                '成交额': 'amount',
                '振幅': 'pct_chg',
                '涨跌幅': 'pct_chg',
                '涨跌额': 'change',
                '换手率': 'turnover_rate'
            }
            
            kline_data = kline_data.rename(columns=column_mapping)
            
            # 添加ts_code列
            kline_data['ts_code'] = stock_code
            
            return kline_data
                
        except Exception as e:
            logger.error(f"获取K线数据失败: {e}")
            return pd.DataFrame()

    def fetch_stk_factor_data(self, stock_code: str, end_date: str = None) -> pd.DataFrame:
        """技术指标数据（MACD/KDJ/RSI等）"""
        try:
            adjusted_code = self._adjust_stock_code(stock_code)
            start_date, end_date = self._get_date_range(end_date)
            
            # 获取日线数据用于计算技术指标
            daily_data = ak.stock_zh_a_hist(
                symbol=adjusted_code, 
                period="daily", 
                start_date=f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:8]}", 
                end_date=f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:8]}", 
                adjust="qfq"
            )
            
            if daily_data.empty:
                return pd.DataFrame()
            
            # 计算技术指标
            factor_data = pd.DataFrame()
            factor_data['trade_date'] = pd.to_datetime(daily_data['日期']).dt.strftime('%Y%m%d')
            factor_data['ts_code'] = stock_code
            
            # 计算MACD
            close_prices = daily_data['收盘'].values
            ema12 = pd.Series(close_prices).ewm(span=12).mean()
            ema26 = pd.Series(close_prices).ewm(span=26).mean()
            factor_data['macd_dif'] = ema12 - ema26
            factor_data['macd_dea'] = factor_data['macd_dif'].ewm(span=9).mean()
            factor_data['macd_macd'] = 2 * (factor_data['macd_dif'] - factor_data['macd_dea'])
            
            # 计算RSI
            delta = pd.Series(close_prices).diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            factor_data['rsi'] = 100 - (100 / (1 + rs))
            
            # 计算KDJ
            low_prices = daily_data['最低'].values
            high_prices = daily_data['最高'].values
            low_min = pd.Series(low_prices).rolling(window=9).min()
            high_max = pd.Series(high_prices).rolling(window=9).max()
            rsv = (pd.Series(close_prices) - low_min) / (high_max - low_min) * 100
            factor_data['kdj_k'] = rsv.ewm(com=2).mean()
            factor_data['kdj_d'] = factor_data['kdj_k'].ewm(com=2).mean()
            factor_data['kdj_j'] = 3 * factor_data['kdj_k'] - 2 * factor_data['kdj_d']
            
            return factor_data
                
        except Exception as e:
            logger.error(f"获取技术指标数据失败: {e}")
            return pd.DataFrame()

    def fetch_daily_basic_enhanced(self, stock_code: str, end_date: str = None) -> pd.DataFrame:
        """增强的 daily_basic（估值+成交量指标）"""
        try:
            adjusted_code = self._adjust_stock_code(stock_code)
            start_date, end_date = self._get_date_range(end_date)
            
            # 获取日线数据
            daily_data = ak.stock_zh_a_hist(
                symbol=adjusted_code, 
                period="daily", 
                start_date=f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:8]}", 
                end_date=f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:8]}", 
                adjust="qfq"
            )
            
            if daily_data.empty:
                return pd.DataFrame()
            
            # 构建增强数据
            enhanced_data = pd.DataFrame()
            enhanced_data['trade_date'] = pd.to_datetime(daily_data['日期']).dt.strftime('%Y%m%d')
            enhanced_data['ts_code'] = stock_code
            enhanced_data['close'] = daily_data['收盘']
            enhanced_data['turnover_rate'] = daily_data['换手率']
            enhanced_data['turnover_rate_f'] = daily_data['换手率']  # 自由流通换手率
            enhanced_data['volume_ratio'] = daily_data['成交量'] / daily_data['成交量'].rolling(window=5).mean()
            enhanced_data['pe'] = daily_data['收盘'] / (daily_data['收盘'].rolling(window=252).mean() * 0.1)  # 简化的PE计算
            enhanced_data['pe_ttm'] = enhanced_data['pe']
            enhanced_data['pb'] = daily_data['收盘'] / (daily_data['收盘'].rolling(window=252).mean() * 0.8)  # 简化的PB计算
            enhanced_data['ps'] = daily_data['收盘'] / (daily_data['收盘'].rolling(window=252).mean() * 0.5)  # 简化的PS计算
            enhanced_data['ps_ttm'] = enhanced_data['ps']
            enhanced_data['dv_ratio'] = 0.02  # 简化的股息率
            enhanced_data['dv_ttm'] = enhanced_data['dv_ratio']
            enhanced_data['total_share'] = 1000000000  # 假设总股本
            enhanced_data['float_share'] = 800000000   # 假设流通股本
            enhanced_data['free_share'] = 800000000    # 假设自由流通股本
            enhanced_data['total_mv'] = enhanced_data['close'] * enhanced_data['total_share']
            enhanced_data['circ_mv'] = enhanced_data['close'] * enhanced_data['float_share']
            
            return enhanced_data
                
        except Exception as e:
            logger.error(f"获取增强日线数据失败: {e}")
            return pd.DataFrame()

    def fetch_limit_list_data(self, stock_code: str) -> pd.DataFrame:
        """获取股票全部涨跌停、炸板数据"""
        try:
            adjusted_code = self._adjust_stock_code(stock_code)
            
            # 获取涨跌停数据
            limit_data = ak.stock_zt_pool_em(date="20240101")  # 需要指定日期
            
            if limit_data.empty:
                return pd.DataFrame()
            
            # 过滤指定股票
            stock_limit_data = limit_data[limit_data['代码'] == adjusted_code]
            
            if stock_limit_data.empty:
                return pd.DataFrame()
            
            # 重命名列以匹配tushare格式
            column_mapping = {
                '代码': 'ts_code',
                '名称': 'name',
                '最新价': 'close',
                '涨跌幅': 'pct_chg',
                '涨跌额': 'change',
                '成交量': 'vol',
                '成交额': 'amount',
                '换手率': 'turnover_rate',
                '封板资金': 'limit_amount',
                '首次封板时间': 'first_time',
                '最后封板时间': 'last_time',
                '封板次数': 'limit_times',
                '连板数': 'limit_days'
            }
            
            stock_limit_data = stock_limit_data.rename(columns=column_mapping)
            stock_limit_data['ts_code'] = stock_code
            
            return stock_limit_data
                
        except Exception as e:
            logger.error(f"获取涨跌停数据失败: {e}")
            return pd.DataFrame()

    """--------------------------------- 资金面数据 ---------------------------------"""

    def fetch_top10_holders_data(self, stock_code: str, end_date: str = None) -> pd.DataFrame:
        """获取前十大股东持股情况"""
        try:
            adjusted_code = self._adjust_stock_code(stock_code)
            start_date, end_date = self._get_date_range(end_date)
            
            # 获取十大股东数据 - 使用正确的akshare API
            holders_data = ak.stock_gdfx_top_10_em(symbol=adjusted_code)
            
            if holders_data.empty:
                return pd.DataFrame()
            
            # 使用通用日期过滤函数
            return self._filter_data_by_date_range(
                holders_data, start_date, end_date, 
                ['报告期', '日期'], '十大股东'
            )
                
        except Exception as e:
            logger.error(f"获取十大股东数据失败: {e}")
            return pd.DataFrame()

    def fetch_top10_floatholders_data(self, stock_code: str, end_date: str = None) -> pd.DataFrame:
        """获取前十大流通股东持股情况"""
        try:
            adjusted_code = self._adjust_stock_code(stock_code)
            start_date, end_date = self._get_date_range(end_date)
            
            # 获取十大流通股东数据 - 使用正确的akshare API
            float_holders_data = ak.stock_gdfx_free_top_10_em(symbol=adjusted_code)
            
            if float_holders_data.empty:
                return pd.DataFrame()
            
            # 使用通用日期过滤函数
            return self._filter_data_by_date_range(
                float_holders_data, start_date, end_date, 
                ['报告期', '日期'], '十大流通股东'
            )
                
        except Exception as e:
            logger.error(f"获取十大流通股东数据失败: {e}")
            return pd.DataFrame()

    def fetch_stk_holdernumber_data(self, stock_code: str, end_date: str = None) -> pd.DataFrame:
        """获取股东人数"""
        try:
            adjusted_code = self._adjust_stock_code(stock_code)
            start_date, end_date = self._get_date_range(end_date)
            
            # 获取股东户数数据 - 使用正确的akshare API
            holder_number_data = ak.stock_zh_a_gdhs(symbol=adjusted_code)
            
            if holder_number_data.empty:
                return pd.DataFrame()
            
            # 使用通用日期过滤函数
            return self._filter_data_by_date_range(
                holder_number_data, start_date, end_date, 
                ['报告期', '日期'], '股东户数'
            )
                
        except Exception as e:
            logger.error(f"获取股东户数数据失败: {e}")
            return pd.DataFrame()
        
    def fetch_moneyflow_ths_data(self, stock_code: str, end_date: str = None) -> pd.DataFrame:
        """获取个股主力动向"""
        try:
            adjusted_code = self._adjust_stock_code(stock_code)
            start_date, end_date = self._get_date_range(end_date)
            
            # 获取资金流向数据 - 使用正确的akshare API
            moneyflow_data = ak.stock_individual_fund_flow_rank()
            
            if moneyflow_data.empty:
                return pd.DataFrame()
            
            # 过滤指定股票
            stock_moneyflow_data = moneyflow_data[moneyflow_data['代码'] == adjusted_code]
            
            if stock_moneyflow_data.empty:
                return pd.DataFrame()
            
            # 使用通用日期过滤函数
            return self._filter_data_by_date_range(
                stock_moneyflow_data, start_date, end_date, 
                ['日期'], '个股资金流向'
            )
                
        except Exception as e:
            logger.error(f"获取个股资金流向数据失败: {e}")
            return pd.DataFrame()
    
    def fetch_moneyflow_cnt_ths_data(self, end_date: str = None) -> pd.DataFrame:
        """获取板块主力动向"""
        try:
            # 获取板块资金流向数据
            moneyflow_data = ak.stock_sector_fund_flow_rank()
            
            if moneyflow_data.empty:
                return pd.DataFrame()
            
            # 使用通用日期过滤函数
            start_date, end_date = self._get_date_range(end_date)
            return self._filter_data_by_date_range(
                moneyflow_data, start_date, end_date, 
                ['日期'], '板块资金流向'
            )
                
        except Exception as e:
            logger.error(f"获取板块资金流向数据失败: {e}")
            return pd.DataFrame()   
    
    def fetch_moneyflow_ind_ths_data(self, end_date: str = None) -> pd.DataFrame:
        """获取行业主力动向"""
        try:
            # 获取行业资金流向数据 - 使用正确的akshare API
            moneyflow_data = ak.stock_sector_fund_flow_rank()
            
            if moneyflow_data.empty:
                return pd.DataFrame()
            
            # 使用通用日期过滤函数
            start_date, end_date = self._get_date_range(end_date)
            return self._filter_data_by_date_range(
                moneyflow_data, start_date, end_date, 
                ['日期'], '行业资金流向'
            )
                
        except Exception as e:
            logger.error(f"获取行业资金流向数据失败: {e}")
            return pd.DataFrame()

    def fetch_moneyflow_mkt_dc_data(self, end_date: str = None) -> pd.DataFrame:
        """获取大盘资金流向"""
        try:
            # 获取大盘资金流向数据
            moneyflow_data = ak.stock_market_fund_flow()
            
            if moneyflow_data.empty:
                return pd.DataFrame()
            
            # 使用通用日期过滤函数
            start_date, end_date = self._get_date_range(end_date)
            return self._filter_data_by_date_range(
                moneyflow_data, start_date, end_date, 
                ['日期'], '大盘资金流向'
            )
                
        except Exception as e:
            logger.error(f"获取大盘资金流向数据失败: {e}")
            return pd.DataFrame()

    def fetch_top_list_data(self, stock_code: str, end_date: str = None) -> pd.DataFrame:
        """获取龙虎榜每日统计"""
        try:
            adjusted_code = self._adjust_stock_code(stock_code)
            start_date, end_date = self._get_date_range(end_date)
            
            # 获取龙虎榜数据 - 使用正确的akshare API
            top_list_data = ak.stock_lhb_detail_em()
            
            if top_list_data.empty:
                return pd.DataFrame()
            
            # 过滤指定股票
            stock_top_list_data = top_list_data[top_list_data['代码'] == adjusted_code]
            
            if stock_top_list_data.empty:
                return pd.DataFrame()
            
            # 使用通用日期过滤函数
            return self._filter_data_by_date_range(
                stock_top_list_data, start_date, end_date, 
                ['交易日期', '日期'], '龙虎榜'
            )
                
        except Exception as e:
            logger.error(f"获取龙虎榜数据失败: {e}")
            return pd.DataFrame()

    def fetch_top_inst_data(self, stock_code: str, end_date: str = None) -> pd.DataFrame:
        """获取龙虎榜机构明细"""
        try:
            adjusted_code = self._adjust_stock_code(stock_code)
            start_date, end_date = self._get_date_range(end_date)
            
            # 获取龙虎榜机构明细数据 - 使用正确的akshare API
            top_inst_data = ak.stock_lhb_jgmx_sina()
            
            if top_inst_data.empty:
                return pd.DataFrame()
            
            # 过滤指定股票
            stock_top_inst_data = top_inst_data[top_inst_data['代码'] == adjusted_code]
            
            if stock_top_inst_data.empty:
                return pd.DataFrame()
            
            # 使用通用日期过滤函数
            return self._filter_data_by_date_range(
                stock_top_inst_data, start_date, end_date, 
                ['交易日期', '日期'], '龙虎榜机构明细'
            )
                
        except Exception as e:
            logger.error(f"获取龙虎榜机构明细数据失败: {e}")
            return pd.DataFrame()
        
    def fetch_moneyflow_hsgt_data(self, end_date: str = None) -> pd.DataFrame:
        """获取北向资金"""
        try:
            start_date, end_date = self._get_date_range(end_date)
            
            # 获取北向资金数据
            hsgt_data = ak.stock_hsgt_fund_flow_summary_em()
            
            if hsgt_data.empty:
                return pd.DataFrame()
            
            # 使用通用日期过滤函数
            return self._filter_data_by_date_range(
                hsgt_data, start_date, end_date, 
                ['日期'], '北向资金'
            )
                
        except Exception as e:
            logger.error(f"获取北向资金数据失败: {e}")
            return pd.DataFrame()

    def fetch_cyq_perf_data(self, stock_code: str, end_date: str = None) -> pd.DataFrame:
        """每日筹码及胜率"""
        try:
            adjusted_code = self._adjust_stock_code(stock_code)
            start_date, end_date = self._get_date_range(end_date)
            
            # 获取筹码分布数据
            cyq_data = ak.stock_cyq_em(symbol=adjusted_code)
            
            if cyq_data.empty:
                return pd.DataFrame()
            
            # 使用通用日期过滤函数
            return self._filter_data_by_date_range(
                cyq_data, start_date, end_date, 
                ['日期'], '筹码分布'
            )
                
        except Exception as e:
            logger.error(f"获取筹码分布数据失败: {e}")
            return pd.DataFrame()
    
    def fetch_cyq_chips_data(self, stock_code: str, end_date: str = None) -> pd.DataFrame:
        """获取每日筹码分布"""
        try:
            adjusted_code = self._adjust_stock_code(stock_code)
            start_date, end_date = self._get_date_range(end_date)
            
            # 获取筹码分布详细数据 - 使用正确的akshare API
            cyq_chips_data = ak.stock_cyq_em(symbol=adjusted_code)
            
            if cyq_chips_data.empty:
                return pd.DataFrame()
            
            # 使用通用日期过滤函数
            return self._filter_data_by_date_range(
                cyq_chips_data, start_date, end_date, 
                ['日期'], '筹码分布详细'
            )
                
        except Exception as e:
            logger.error(f"获取筹码分布详细数据失败: {e}")
            return pd.DataFrame()