import pandas as pd
import akshare as ak
import os
import time
import random
from datetime import datetime

SAVE_DIR = "fund_data"
TODAY_STR = datetime.now().strftime('%Y-%m-%d')
TODAY_PARAM = datetime.now().strftime('%Y%m%d')

# 标准化列名，确保与你之前的 CSV 格式一致
TARGET_COLS = ['日期', '开盘', '收盘', '最高', '最低', '成交量', '成交额', '振幅', '涨跌幅', '涨跌额', '换手率']

def update_fund_data(fund_code):
    fund_code = str(fund_code).strip().zfill(6)
    file_path = os.path.join(SAVE_DIR, f"{fund_code}.csv")
    
    if not os.path.exists(file_path): return "SKIP_NO_FILE"

    # 1. 获取现有数据的最后日期
    try:
        df_old = pd.read_csv(file_path, encoding='utf-8-sig')
        if df_old.empty:
            last_date = "2010-01-01"
        else:
            last_date = str(df_old['日期'].iloc[-1]).replace("/", "-")
    except Exception as e:
        return f"READ_ERR({str(e)[:20]})"

    if last_date == TODAY_STR: return "ALREADY_NEW"

    # 2. 爬取逻辑
    start_param = last_date.replace("-", "")
    
    # 增加随机延迟，模拟真实行为
    time.sleep(random.uniform(3.0, 8.0)) 

    for attempt in range(3): # 增加到3次重试
        try:
            # 统一使用 EM 基金接口，这是目前最稳的
            df = ak.fund_etf_hist_em(
                symbol=fund_code, 
                period="daily", 
                start_date=start_param, 
                end_date=TODAY_PARAM, 
                adjust="qfq"
            )

            if df is None or df.empty:
                return "EMPTY_DATA"

            # 关键：动态对齐列名（防止 Akshare 字段名变动）
            # 假设返回的前11列就是我们要的，强制重命名
            df = df.iloc[:, :11] 
            df.columns = TARGET_COLS
            
            # 格式化日期并过滤旧数据
            df['日期'] = pd.to_datetime(df['日期']).dt.strftime('%Y-%m-%d')
            df = df[df['日期'] > last_date]

            if df.empty: return "UP_TO_DATE"

            # 追加写入
            df.to_csv(file_path, mode='a', index=False, header=False, encoding='utf-8-sig')
            return "SUCCESS"

        except Exception as e:
            wait_time = (attempt + 1) * 15
            print(f"  - Attempt {attempt+1} failed for {fund_code}: {e}. Waiting {wait_time}s...")
            time.sleep(wait_time)
            continue
            
    return "FAILED_ALL"

def main():
    if not os.path.exists(SAVE_DIR): os.makedirs(SAVE_DIR)
    if not os.path.exists("etf.txt"): 
        print("❌ 找不到 etf.txt"); return
    
    # 读取代码列表
    df_codes = pd.read_csv("etf.txt", dtype={'code': str})
    codes = df_codes['code'].unique().tolist()
    
    print(f"🚀 启动更新 | 今日: {TODAY_STR} | 目标数: {len(codes)}")
    
    success_cnt = 0
    fail_streak = 0
    
    for i, code in enumerate(codes):
        res = update_fund_data(code)
        
        # 打印进度
        if res not in ["ALREADY_NEW", "UP_TO_DATE"]:
            print(f"[{i+1}/{len(codes)}] ETF {code}: {res}", flush=True)
        
        if res == "SUCCESS":
            success_cnt += 1
            fail_streak = 0
        elif res == "FAILED_ALL":
            fail_streak += 1
        
        # 保险丝：连续失败 10 次，可能是被封 IP 了
        if fail_streak >= 10:
            print("🛑 连续失败过多，触发熔断。")
            break
            
        # 每天只更新一部分，细水长流防止 GitHub 账号被警告
        if success_cnt >= 50: 
            print("🏁 本次 50 只任务完成，撤退。")
            break

if __name__ == "__main__":
    main()
