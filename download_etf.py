import pandas as pd
import akshare as ak
import os
import time
import random
from datetime import datetime

SAVE_DIR = "fund_data"
TODAY_STR = datetime.now().strftime('%Y-%m-%d')
TODAY_PARAM = datetime.now().strftime('%Y%m%d')

def update_fund_data(fund_code):
    fund_code = str(fund_code).strip().zfill(6)
    file_path = os.path.join(SAVE_DIR, f"{fund_code}.csv")
    
    if not os.path.exists(file_path): return "SKIP"

    # 读取最后日期
    try:
        df_tmp = pd.read_csv(file_path, encoding='utf-8-sig', usecols=['日期'])
        last_date = str(df_tmp['日期'].iloc[-1]).replace("/", "-")
    except:
        last_date = None

    if last_date == TODAY_STR: return "ALREADY_NEW"

    # --- 核心改动：原地重试逻辑 ---
    for attempt in range(2): # 两次大机会
        try:
            # 增加极其随机的休眠，GitHub上必须慢
            time.sleep(random.uniform(5.0, 10.0)) 
            
            start_param = last_date.replace("-", "") if last_date else "20100101"
            
            # 轮流使用数据源
            if attempt == 0:
                df = ak.fund_etf_hist_em(symbol=fund_code, period="daily", start_date=start_param, end_date=TODAY_PARAM, adjust="qfq")
                src = "EM"
            else:
                df = ak.stock_zh_a_hist(symbol=fund_code, period="daily", start_date=start_param, end_date=TODAY_PARAM, adjust="qfq")
                src = "BD"

            if df is not None and not df.empty:
                df['日期'] = pd.to_datetime(df['日期']).dt.strftime('%Y-%m-%d')
                if last_date: df = df[df['日期'] > last_date]
                if df.empty: return "UP_TO_DATE"

                target_cols = ['日期', '开盘', '收盘', '最高', '最低', '成交量', '成交额', '振幅', '涨跌幅', '涨跌额', '换手率']
                df[target_cols].to_csv(file_path, mode='a', index=False, header=False, encoding='utf-8-sig')
                return f"SUCCESS({src})"
        except Exception as e:
            if "RemoteDisconnected" in str(e):
                time.sleep(30) # 遇到封锁死等30秒
            continue
            
    return "FAILED_ALL"

def main():
    if not os.path.exists("etf.txt"): return
    codes = pd.read_csv("etf.txt")['code'].sort_values().unique().tolist()
    
    print(f"🚀 启动生存模式 | 今日: {TODAY_STR}", flush=True)
    
    success_cnt = 0
    err_cnt = 0
    
    for i, code in enumerate(codes):
        res = update_fund_data(code)
        if res == "ALREADY_NEW": continue
        
        print(f"[{i+1}/{len(codes)}] ETF {code}: {res}", flush=True)
        
        if "SUCCESS" in res:
            success_cnt += 1
            err_cnt = 0 # 成功了重置错误计数
        else:
            err_cnt += 1
            
        # 针对当前 IP 状态的保险丝
        if err_cnt >= 5:
            print("🛑 连续 5 只失败，IP 已死，提前结束任务。", flush=True)
            break
            
        # 每次只敢下 30 只，多了必封
        if success_cnt >= 30:
            print("🏁 本次 30 只达成，撤退。", flush=True)
            break

if __name__ == "__main__":
    main()
