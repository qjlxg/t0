import pandas as pd
import akshare as ak
import os
import time
import random
from datetime import datetime

# --- 配置 ---
SAVE_DIR = "fund_data"
TODAY_STR = datetime.now().strftime('%Y-%m-%d')
TODAY_PARAM = datetime.now().strftime('%Y%m%d')

def get_last_date(file_path):
    """读取本地文件最后日期"""
    try:
        df = pd.read_csv(file_path, encoding='utf-8-sig', usecols=['日期'])
        if df.empty: return None
        return str(df['日期'].iloc[-1]).replace("/", "-")
    except:
        return None

def fetch_data_logic(fund_code, start_date):
    """双保险抓取逻辑"""
    # 策略 A: 东方财富
    try:
        # print(f"  -> 尝试东方财富接口...")
        df = ak.fund_etf_hist_em(
            symbol=fund_code, 
            period="daily", 
            start_date=start_date, 
            end_date=TODAY_PARAM, 
            adjust="qfq"
        )
        if df is not None and not df.empty:
            return df, "EM"
    except Exception:
        pass # 失败则进入策略 B

    # 策略 B: 百度股市通 (作为备选)
    try:
        # print(f"  -> 切换至百度接口...")
        # 注意：百度接口通常使用 stock_zh_a_hist 也能抓取 ETF
        df = ak.stock_zh_a_hist(
            symbol=fund_code, 
            period="daily", 
            start_date=start_date, 
            end_date=TODAY_PARAM, 
            adjust="qfq"
        )
        if df is not None and not df.empty:
            return df, "Baidu"
    except Exception as e:
        return None, str(e)

    return None, "All sources failed"

def update_fund_data(fund_code):
    fund_code = str(fund_code).strip().zfill(6)
    file_path = os.path.join(SAVE_DIR, f"{fund_code}.csv")
    
    if not os.path.exists(file_path):
        return "SKIP"

    last_date = get_last_date(file_path)
    if last_date == TODAY_STR:
        return "ALREADY_NEW"

    # 模拟人类行为随机休眠
    time.sleep(random.uniform(1.5, 3.5))
    
    start_param = last_date.replace("-", "") if last_date else "20000101"
    new_df, source = fetch_data_logic(fund_code, start_param)

    if isinstance(new_df, pd.DataFrame):
        # 格式化日期对齐
        new_df['日期'] = pd.to_datetime(new_df['日期']).dt.strftime('%Y-%m-%d')
        if last_date:
            new_df = new_df[new_df['日期'] > last_date]
        
        if new_df.empty:
            return "UP_TO_DATE"

        # 11 列严格对齐
        target_cols = ['日期', '开盘', '收盘', '最高', '最低', '成交量', '成交额', '振幅', '涨跌幅', '涨跌额', '换手率']
        new_df = new_df[target_cols]

        new_df.to_csv(file_path, mode='a', index=False, header=False, encoding='utf-8-sig')
        return f"SUCCESS({source}, +{len(new_df)})"
    
    return f"ERROR({source})"

def main():
    if not os.path.exists("etf.txt"): return
    codes = pd.read_csv("etf.txt")['code'].sort_values().unique().tolist()
    
    print(f"🚀 双保险任务启动 | 今日: {TODAY_STR}")
    
    success_count = 0
    for i, code in enumerate(codes):
        res = update_fund_data(code)
        
        if res == "ALREADY_NEW": continue
        
        print(f"[{i+1}/{len(codes)}] ETF {code}: {res}")
        
        if "SUCCESS" in res:
            success_count += 1
            
        # 针对 GitHub Actions 的单次保护阈值
        if success_count >= 1500:
            print("🏁 本次达到 150 只限额，存盘退出。")
            break

    print(f"📊 本次处理完成: {success_count} 只基金。")

if __name__ == "__main__":
    main()
