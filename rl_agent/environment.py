# in rl_agent/environment.py
import os
import json
import gymnasium as gym
from gymnasium import spaces
import numpy as np
import pandas as pd

class StockTradingEnv(gym.Env):
    """
    实盘风格的A股交易环境：
    - **连续动作**：目标仓位(权重) ∈ [0,1]（可配置做空为[-1,1]）
    - **成交规则**：T+1、100股一手、涨跌停拦截、次日开盘成交+滑点+手续费/印花税
    - **状态**：现金、持仓比例、价格，以及多面report特征（fund/tech/sentiment/news/fundflow + 观点数值化）
    - **奖励**：步进收益、Alpha、Sharpe形项、回撤增量惩罚、换手惩罚

    数据要求 df 至少包含：['date','open','high','low','close']；基准 benchmark_df 同日期、'close'列。
    报告目录：result/<ticker>/<YYYYMMDD>/*_report.json
    """

    metadata = {"render.modes": ["human"]}

    def __init__(
        self,
        df: pd.DataFrame,
        stock_code: str,
        reports_root: str,
        benchmark_df: pd.DataFrame | None = None,
        initial_balance: float = 1_000_000.0,
        # 交易细节（可按券商/交易所微调）
        lot_size: int = 100,            # 一手=100股
        min_tick: float = 0.01,         # 最小价位
        limit_pct: float = 0.10,        # 涨跌停幅 10%（ST可调为0.05）
        slippage_rate: float = 0.0005,  # 滑点（万5）
        commission_rate: float = 0.0003,# 手续费（万3），双边
        stamp_duty_sell: float = 0.0005,# 印花税（仅卖出）默认万5（政策会变，外置参数）
        transfer_fee_rate: float = 0.0, # 过户费（沪市万0.6，可按需设置）
        trans_cost_rate: float | None = None,  # 兼容旧参数，不再使用
        reward_weights: dict | None = None,
        sharpe_window: int = 20,
        allow_short: bool = False,
    ):
        super().__init__()

        # ---- 数据校验 ----
        req_cols = {"date","open","high","low","close"}
        assert req_cols.issubset(df.columns), f"df must contain {req_cols}"
        self.df = df.reset_index(drop=True).copy()
        self.stock_code = stock_code
        self.reports_root = reports_root

        # 交易参数
        self.initial_balance = float(initial_balance)
        self.lot_size = int(lot_size)
        self.min_tick = float(min_tick)
        self.limit_pct = float(limit_pct)
        self.slippage_rate = float(slippage_rate)
        self.commission_rate = float(commission_rate)
        self.stamp_duty_sell = float(stamp_duty_sell)
        self.transfer_fee_rate = float(transfer_fee_rate)
        self.allow_short = allow_short

        # 基准
        if benchmark_df is not None:
            assert {"date","close"}.issubset(benchmark_df.columns), "benchmark_df must contain 'date','close'"
            bmk = benchmark_df[["date","close"]].rename(columns={"close":"bmk_close"}).copy()
            self.mkt = pd.merge(self.df[["date","close"]], bmk, on="date", how="left").ffill()
        else:
            self.mkt = self.df[["date","close"]].copy()
            self.mkt["bmk_close"] = self.mkt["close"].values

        # 动作空间：目标仓位
        if self.allow_short:
            self.action_space = spaces.Box(low=-1.0, high=1.0, shape=(1,), dtype=np.float32)
        else:
            self.action_space = spaces.Box(low=0.0, high=1.0, shape=(1,), dtype=np.float32)

        # 观测：现金、仓位比例、价格 + 多面特征
        self.obs_dim = 14
        self.observation_space = spaces.Box(low=-np.inf, high=np.inf, shape=(self.obs_dim,), dtype=np.float32)

        # 奖励权重
        default_weights = dict(step_return=1.0, alpha=0.5, sharpe=0.2, drawdown_penalty=0.6, turnover_penalty=0.02)
        self.w = default_weights if reward_weights is None else {**default_weights, **reward_weights}
        self.sharpe_window = int(sharpe_window)

        # 内部状态
        self.current_step = 0
        self.balance = self.initial_balance
        self.position_shares = 0
        self.prev_portfolio_value = self.initial_balance
        self.high_watermark = self.initial_balance
        self.equity_curve = [self.initial_balance]

        # T+1：记录当日新买入数量（当天不可卖）
        self.bought_today = 0
        # 挂单：动作在t下达，t+1开盘执行
        self.next_target_weight = None
        self.prev_action_target = 0.0

        # 报告缓存
        self._signal_cache = {}

    # ----------------- 报告读取 -----------------
    def _date_str(self, ts) -> str:
        ts = pd.to_datetime(ts)
        return ts.strftime("%Y%m%d")

    def _report_dir_for_date(self, date_ts):
        return os.path.join(self.reports_root, self.stock_code, self._date_str(date_ts))

    def _view_to_num(self, v: str) -> float:
        if not isinstance(v, str):
            return 0.0
        if "看多" in v:
            return 1.0
        if "看空" in v:
            return -1.0
        return 0.0

    def _load_signals_for_date(self, date_ts):
        key = self._date_str(date_ts)
        if key in self._signal_cache:
            return self._signal_cache[key]
        sig = {k:0.0 for k in [
            "fund_score","tech_score","senti_score","news_score",
            "main_capital_score","inst_capital_score","retail_capital_score",
            "fund_view","tech_view","senti_view","news_view"
        ]}
        d = self._report_dir_for_date(date_ts)
        try:
            if os.path.isdir(d):
                for fname in os.listdir(d):
                    if not fname.endswith("_report.json"):
                        continue
                    with open(os.path.join(d, fname), "r", encoding="utf-8") as f:
                        data = json.load(f)
                    data = data.get("data", {})
                    if "fundamental" in fname:
                        s = data.get("scores", {})
                        sig["fund_score"] = float(s.get("profitability",0))+float(s.get("solvency",0))+float(s.get("growth_potential",0))
                        sig["fund_view"] = self._view_to_num(data.get("viewpoint"))
                    elif "technical" in fname:
                        s = data.get("scores", {})
                        vals = [float(s.get(k,0)) for k in ["trend_strength","momentum","support_resistance","volume_analysis","pattern_analysis"]]
                        sig["tech_score"] = float(np.mean(vals)) if len(vals)>0 else 0.0
                        sig["tech_view"] = self._view_to_num(data.get("viewpoint"))
                    elif "sentiment" in fname:
                        s = data.get("scores", {})
                        vals = [float(s.get(k,0)) for k in ["market_heat","investor_sentiment","institution_opinion"]]
                        sig["senti_score"] = float(np.mean(vals)) if len(vals)>0 else 0.0
                        sig["senti_view"] = self._view_to_num(data.get("viewpoint"))
                    elif "news" in fname:
                        s = data.get("scores", {})
                        vals = [float(s.get(k,0)) for k in ["sentiment_score","news_impact","market_attention"]]
                        sig["news_score"] = float(np.mean(vals)) if len(vals)>0 else 0.0
                        sig["news_view"] = self._view_to_num(data.get("viewpoint"))
                    elif "fund" in fname:
                        s = data.get("scores", {})
                        sig["main_capital_score"] = float(s.get("main_capital",0))
                        sig["inst_capital_score"] = float(s.get("institution_capital",0))
                        sig["retail_capital_score"] = float(s.get("retail_capital",0))
        except Exception:
            pass
        self._signal_cache[key] = sig
        return sig

    # ----------------- 工具 -----------------
    def _portfolio_value(self, price: float) -> float:
        return float(self.balance + self.position_shares * price)

    def _round_to_lot(self, shares: float) -> int:
        shares = int(shares // self.lot_size * self.lot_size)
        return max(0, shares) if not self.allow_short else shares

    def _apply_tick(self, price: float) -> float:
        return np.round(price / self.min_tick) * self.min_tick

    # ----------------- Gym API -----------------
    def reset(self, seed=None, options=None):
        self.current_step = 0
        self.balance = self.initial_balance
        self.position_shares = 0
        self.prev_portfolio_value = self.initial_balance
        self.high_watermark = self.initial_balance
        self.equity_curve = [self.initial_balance]
        self.bought_today = 0
        self.next_target_weight = None
        self.prev_action_target = 0.0
        return self._next_observation(), {}

    def _next_observation(self) -> np.ndarray:
        row = self.df.loc[self.current_step]
        price = float(row["close"])  # 观测用收盘
        sig = self._load_signals_for_date(row["date"])
        pos_val = self.position_shares * price
        total_val = self.balance + pos_val
        pos_frac = 0.0 if total_val<=0 else pos_val/total_val
        obs = np.array([
            self.balance,
            pos_frac,
            price,
            sig["fund_score"],sig["tech_score"],sig["senti_score"],sig["news_score"],
            sig["main_capital_score"],sig["inst_capital_score"],sig["retail_capital_score"],
            sig["fund_view"],sig["tech_view"],sig["senti_view"],sig["news_view"],
        ], dtype=np.float32)
        return obs

    def _execute_open_orders(self, step_idx: int):
        """在 step_idx 当天的**开盘价**执行上一交易日下达的目标仓位。考虑T+1、涨跌停、滑点与费用。"""
        if self.next_target_weight is None:
            # 今日无待执行
            self.bought_today = 0
            return

        row = self.df.loc[step_idx]
        prev_close = float(self.df.loc[step_idx-1, "close"]) if step_idx>0 else float(row["open"])
        o = float(row["open"])  # 成交基价（开盘）

        # 涨跌停判断：
        up_limit = prev_close * (1.0 + self.limit_pct)
        down_limit = prev_close * (1.0 - self.limit_pct)
        is_up_limit_open = o >= np.round(up_limit / self.min_tick) * self.min_tick
        is_down_limit_open = o <= np.round(down_limit / self.min_tick) * self.min_tick

        # 目标持仓（以开盘价计）
        total_before = self.balance + self.position_shares * o
        tgt_value = self.next_target_weight * total_before
        tgt_shares_float = tgt_value / o
        tgt_shares = self._round_to_lot(tgt_shares_float)

        delta = tgt_shares - self.position_shares
        side = np.sign(delta)

        # T+1 可卖数量限制
        sellable = self.position_shares - self.bought_today
        if side < 0:
            # 卖出不超过可卖
            delta = -min(abs(delta), max(0, sellable))
        
        # 涨跌停拦截：
        if side > 0 and is_up_limit_open:
            delta = 0  # 一字板涨停，买不到
        if side < 0 and is_down_limit_open:
            delta = 0  # 一字板跌停，卖不出

        if delta == 0:
            self.bought_today = 0
            self.next_target_weight = None
            return

        # 成交价 = 开盘价 ± 滑点
        px = o * (1 + self.slippage_rate if side>0 else 1 - self.slippage_rate)
        px = float(self._apply_tick(px))

        trade_shares = abs(delta)
        trade_value = trade_shares * px

        # 费用：佣金（双边）、过户费（可选）、印花税（仅卖）
        commission = max(trade_value * self.commission_rate, 0.0)
        transfer_fee = trade_value * self.transfer_fee_rate
        stamp = trade_value * self.stamp_duty_sell if side < 0 else 0.0
        cost = commission + transfer_fee + stamp

        if side > 0:
            # 现金充足性（买入）
            max_affordable = int(((self.balance) // (px * self.lot_size)) * self.lot_size)
            trade_shares = min(trade_shares, max_affordable)
            if trade_shares <= 0:
                self.bought_today = 0
                self.next_target_weight = None
                return
            trade_value = trade_shares * px
            commission = max(trade_value * self.commission_rate, 0.0)
            transfer_fee = trade_value * self.transfer_fee_rate
            cost = commission + transfer_fee
            cash_out = trade_value + cost
            self.balance -= cash_out
            self.position_shares += trade_shares
            self.bought_today = trade_shares  # 当天买入的锁定数量
        else:
            # 卖出：现金流入
            trade_value = trade_shares * px
            commission = max(trade_value * self.commission_rate, 0.0)
            transfer_fee = trade_value * self.transfer_fee_rate
            stamp = trade_value * self.stamp_duty_sell
            cost = commission + transfer_fee + stamp
            cash_in = trade_value - cost
            self.balance += cash_in
            self.position_shares -= trade_shares
            self.bought_today = 0  # 卖出日不新增锁定

        self.next_target_weight = None

    def step(self, action):
        # 1) 解析动作（目标仓位），裁剪到动作空间
        a = float(np.clip(action[0], self.action_space.low[0], self.action_space.high[0]))

        # 2) 前进到**下一交易日**并在开盘执行昨日动作
        next_idx = min(self.current_step + 1, len(self.df) - 1)
        # 在 next_idx 的开盘执行上一个 next_target_weight（若有）
        self._execute_open_orders(next_idx)

        # 3) 记录今天的新目标（将在下一步开盘执行）
        self.next_target_weight = a

        # 4) 以**当日收盘**做市值评估，并计算收益/奖励
        row = self.df.loc[next_idx]
        close_px = float(row["close"])
        pv = self._portfolio_value(close_px)

        # 基准步收益（close-to-close）
        if next_idx == 0:
            bret = 0.0
        else:
            b_prev = float(self.mkt.loc[next_idx - 1, "bmk_close"]) 
            b_curr = float(self.mkt.loc[next_idx, "bmk_close"]) 
            bret = 0.0 if b_prev == 0 else (b_curr - b_prev) / b_prev

        prev_pv = self.prev_portfolio_value
        step_ret = 0.0 if prev_pv <= 0 else (pv - prev_pv) / prev_pv

        # Sharpe形项
        self.equity_curve.append(pv)
        if len(self.equity_curve) > 1:
            rets = np.diff(self.equity_curve[-(self.sharpe_window+1):]) / np.array(self.equity_curve[-(self.sharpe_window+1):-1])
            rolling_std = float(np.std(rets)) if len(rets) > 1 else 0.0
        else:
            rolling_std = 0.0
        sharpe_term = 0.0 if rolling_std == 0.0 else step_ret / (rolling_std + 1e-8)

        # Alpha
        alpha_term = step_ret - bret

        # 回撤增量
        self.high_watermark = max(self.high_watermark, pv)
        dd = 0.0 if self.high_watermark == 0 else (self.high_watermark - pv) / self.high_watermark
        if len(self.equity_curve) >= 2:
            prev_pv_for_dd = self.equity_curve[-2]
            prev_dd = 0.0 if self.high_watermark == 0 else (self.high_watermark - prev_pv_for_dd) / self.high_watermark
        else:
            prev_dd = 0.0
        dd_increase = max(0.0, dd - prev_dd)

        # 换手惩罚：基于目标变化
        turnover_penalty = abs(a - self.prev_action_target)
        self.prev_action_target = a

        reward = (
            self.w["step_return"] * step_ret +
            self.w["alpha"] * alpha_term +
            self.w["sharpe"] * sharpe_term -
            self.w["drawdown_penalty"] * dd_increase -
            self.w["turnover_penalty"] * turnover_penalty
        )

        # 推进
        self.prev_portfolio_value = pv
        self.current_step = next_idx
        done = self.current_step >= len(self.df) - 1

        info = {
            "portfolio_value": pv,
            "step_return": step_ret,
            "benchmark_return": bret,
            "alpha": alpha_term,
            "drawdown": dd,
            "position_shares": int(self.position_shares),
            "position_frac": 0.0 if pv == 0 else (self.position_shares * close_px) / pv,
        }

        return self._next_observation(), float(reward), bool(done), False, info

    def render(self, mode="human"):
        if len(self.equity_curve) == 0:
            return
        pv = self.equity_curve[-1]
        print(f"Step {self.current_step} | PV: {pv:,.2f}")