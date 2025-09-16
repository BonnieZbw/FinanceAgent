# in backtest.py
import os
import pandas as pd
import numpy as np
from rl_agent.environment import StockTradingEnv
from rl_agent.agent import CIOAgent

# ---- Tushare loader ----
# 使用前请设置环境变量 TUSHARE_TOKEN 或自行传参

def load_ohlcv_from_tushare(ts_code: str, start_date: str, end_date: str):
    import tushare as ts
    token = os.getenv("TUSHARE_TOKEN")
    if not token:
        raise RuntimeError("请先在环境变量中设置 TUSHARE_TOKEN")
    pro = ts.pro_api(token)
    # 使用 daily 接口，返回 open/high/low/close/vol，日期格式 YYYYMMDD
    df = pro.daily(ts_code=ts_code, start_date=start_date.replace("-",""), end_date=end_date.replace("-",""))
    if df.empty:
        raise RuntimeError(f"Tushare无数据: {ts_code} {start_date}~{end_date}")
    df = df[["trade_date","open","high","low","close","vol"]].rename(columns={"trade_date":"date"})
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').reset_index(drop=True)
    return df


def run_backtesting(stock_code: str, start_date: str, end_date: str, reports_root: str = "result", benchmark_code: str | None = None):
    # 1. 实盘数据（OHLC）
    historical_data_df = load_ohlcv_from_tushare(stock_code, start_date, end_date)

    # 2. 基准（可选）
    benchmark_df = None
    if benchmark_code:
        benchmark_df = load_ohlcv_from_tushare(benchmark_code, start_date, end_date)[["date","close"]]

    # 3. 创建环境（含A股撮合与风控）
    env = StockTradingEnv(
        df=historical_data_df,
        stock_code=stock_code,
        reports_root=reports_root,
        benchmark_df=benchmark_df,
        initial_balance=1_000_000.0,
        lot_size=100,
        min_tick=0.01,
        limit_pct=0.10,
        slippage_rate=0.0005,
        commission_rate=0.0003,
        stamp_duty_sell=0.0005,
        transfer_fee_rate=0.0,
        reward_weights={
            "step_return": 1.0,
            "alpha": 0.7,
            "sharpe": 0.2,
            "drawdown_penalty": 0.6,
            "turnover_penalty": 0.02,
        },
        sharpe_window=20,
        allow_short=False,
    )

    # 4. 训练
    agent = CIOAgent(env)
    agent.train(total_timesteps=len(historical_data_df) * 20)

    # 5. 评估
    obs = env.reset()
    episode_info = []
    for _ in range(len(historical_data_df)-1):
        action = agent.predict(obs)
        obs, reward, done, info = env.step(action)
        episode_info.append({"reward": reward, **info})
        if done:
            break

    # 6. 总结
    df_info = pd.DataFrame(episode_info)
    if not df_info.empty:
        total_ret = (df_info["portfolio_value"].iloc[-1] / df_info["portfolio_value"].iloc[0]) - 1 if len(df_info) > 1 else 0.0
        print("Backtest summary:")
        print(f"  Final PV: {df_info['portfolio_value'].iloc[-1]:,.2f}")
        print(f"  Total Return: {total_ret:.2%}")
        print(f"  Max Drawdown: {df_info['drawdown'].max():.2%}")

if __name__ == '__main__':
    # 示例：平安银行，以上证指数作基准（如需用沪深300，传 000300.SH）
    run_backtesting(stock_code="000001.SZ", start_date="2023-01-01", end_date="2025-09-16", reports_root="result", benchmark_code="000001.SH")