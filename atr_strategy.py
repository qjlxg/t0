import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime

# 配置参数
DATA_DIR = 'fund_data'
K_FACTOR = 2.5  # 建议：怕卖飞用2.5-3.0，想保利润用2.0
ATR_PERIOD = 14

def calculate_atr_strategy(file_path):
    try:
        # 读取数据，根据你的格式，CSV是以制表符或逗号分隔，这里兼容处理
        df = pd.read_csv(file_path, sep=None, engine='python')
        
        # 统一列名（去掉空格）
        df.columns = [c.strip() for c in df.columns]
        df['日期'] = pd.to_datetime(df['日期'])
        df = df.sort_values('日期')

        # 计算 TR
        df['h_l'] = df['最高'] - df['最低']
        df['h_pc'] = abs(df['最高'] - df['收盘'].shift(1))
        df['l_pc'] = abs(df['最低'] - df['收盘'].shift(1))
        df['tr'] = df[['h_l', 'h_pc', 'l_pc']].max(axis=1)

        # 计算 ATR 和 20日滚动最高价
        df['atr'] = df['tr'].rolling(window=ATR_PERIOD).mean()
        df['rolling_high'] = df['最高'].rolling(window=20).max()
        
        # 计算 动态止损线 (Chandelier Exit)
        df['exit_price'] = df['rolling_high'] - (K_FACTOR * df['atr'])
        
        latest = df.iloc[-1]
        stock_code = latest['股票代码']
        
        return {
            "code": stock_code,
            "close": latest['收盘'],
            "high": latest['rolling_high'],
            "atr": latest['atr'],
            "exit": latest['exit_price']
        }
    except Exception as e:
        return f"文件 {file_path} 处理失败: {e}"

if __name__ == "__main__":
    print(f"========== 策略回测报告 ({datetime.now().strftime('%Y-%m-%d %H:%M')}) ==========")
    
    # 获取目录下所有csv文件
    files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    
    if not files:
        print(f"错误：在 {DATA_DIR} 目录下未找到任何 CSV 文件。")
    else:
        for f in files:
            res = calculate_atr_strategy(f)
            if isinstance(res, dict):
                print(f"\n标的代码: {res['code']}")
                print(f"  当前价格: {res['close']:.3f}")
                print(f"  区间最高: {res['high']:.3f}")
                print(f"  波动(ATR): {res['atr']:.4f}")
                print(f"  >>> 【止损/止盈线】: {res['exit']:.3f} <<<")
                
                status = "✅ 趋势安全，死拿！" if res['close'] > res['exit'] else "⚠️ 跌破阈值，考虑离场！"
                print(f"  状态判断: {status}")
    print("\n============================================================")
