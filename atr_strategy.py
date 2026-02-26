import pandas as pd
import numpy as np
import os
from datetime import datetime

# 配置参数
CSV_FILE = 'fund_data'  # 替换为你的文件名
K_FACTOR = 2.5               # 波动系数，怕卖飞设2.5-3.0
ATR_PERIOD = 14

def calculate_atr_strategy(df):
    # 确保日期排序
    df['日期'] = pd.to_datetime(df['日期'])
    df = df.sort_values('日期')

    # 计算 TR (True Range)
    df['h_l'] = df['最高'] - df['最低']
    df['h_pc'] = abs(df['最高'] - df['收盘'].shift(1))
    df['l_pc'] = abs(df['最低'] - df['收盘'].shift(1))
    df['tr'] = df[['h_l', 'h_pc', 'l_pc']].max(axis=1)

    # 计算 ATR
    df['atr'] = df['tr'].rolling(window=ATR_PERIOD).mean()
    
    # 这里的 Highest High 是指持仓期间的最高价，脚本简化为近期 20 日最高价
    df['rolling_high'] = df['最高'].rolling(window=20).max()
    
    # 计算吊灯止损位
    df['exit_price'] = df['rolling_high'] - (K_FACTOR * df['atr'])
    
    return df.iloc[-1]

if __name__ == "__main__":
    if os.path.exists(CSV_FILE):
        latest = calculate_atr_strategy(pd.read_csv(CSV_FILE))
        
        print(f"--- 策略执行指令 ({datetime.now().strftime('%Y-%m-%d')}) ---")
        print(f"当前价格支撑参考: {latest['收盘']}")
        print(f"近期最高价: {latest['rolling_high']:.3f}")
        print(f"动态波动(ATR): {latest['atr']:.3f}")
        print(f"【非人性止损线】: {latest['exit_price']:.3f}")
        print("------------------------------------------")
        print("规则：收盘价不破止损线，绝对不准清仓！")
    else:
        print("未找到数据文件，请检查路径。")
