import os
import pandas as pd
import glob
from datetime import datetime, timedelta
import multiprocessing

# ==========================================
# 战法名称：【连跌回调·精简推送版】
# 更新说明：
# 1. 自动归档：存入年月目录（如 202601/），文件名带时间戳。
# 2. 宁缺毋滥：只保留 🔴红色 和 🔵蓝色 信号，过滤掉无意义的垃圾信息。
# 3. 排序置顶：最强的顺势信号（星级最高）排在最前面。
# ==========================================

def get_stats(df):
    stats = []
    for d in [2, 3, 4, 5]:
        target_idx = df[df['down_count'] == d].index + 1
        target_idx = [i for i in target_idx if i < len(df)]
        if not target_idx:
            stats.extend([0, 0])
            continue
        changes = (df.iloc[target_idx]['收盘'].values - df.iloc[[i-1 for i in target_idx]]['收盘'].values) / df.iloc[[i-1 for i in target_idx]]['收盘'].values * 100
        stats.extend([round(changes.mean(), 2), round((changes > 0).mean() * 100, 2)])
    return stats

def analyze_single_file(file_path, etf_names):
    try:
        df = pd.read_csv(file_path)
        if len(df) < 120: return None
        
        df['日期'] = pd.to_datetime(df['日期'])
        df = df.sort_values('日期').reset_index(drop=True)
        symbol = os.path.basename(file_path).split('.')[0].zfill(6)
        name = etf_names.get(symbol, "未知")

        df['is_down'] = df['收盘'].diff() < 0
        counts, cur = [], 0
        for val in df['is_down']:
            if val: cur += 1
            else: cur = 0
            counts.append(cur)
        df['down_count'] = counts
        
        ma250 = df['收盘'].rolling(250).mean().iloc[-1] if len(df)>=250 else df['收盘'].rolling(60).mean().iloc[-1]
        curr_price = df['收盘'].iloc[-1]
        is_bull = curr_price > ma250
        
        ma20 = df['收盘'].rolling(20).mean().iloc[-1]
        bias20 = ((curr_price - ma20) / ma20) * 100
        
        curr_down = counts[-1]
        rating = "过滤"
        prio = 0
        
        if curr_down >= 2:
            if is_bull:
                # 顺势信号：只要有连跌且趋势好，就是机会
                score = curr_down + (2 if bias20 < -4 else 0)
                rating = f"🔴顺势 {'⭐'*score}"
                prio = 10 + score
            elif curr_down >= 4 or bias20 < -8:
                # 逆势信号：趋势不好时，必须极端超跌（巴西ETF模式）
                rating = f"🔵逆势抢反弹 {'⚡'*curr_down}"
                prio = 5 + curr_down
        
        if rating == "过滤": return None

        stats_all = get_stats(df)
        return [symbol, name, rating, "多头" if is_bull else "空头", round(bias20, 2), curr_down] + stats_all, prio
    except:
        return None

def main():
    # 1. 加载名称
    etf_names = {}
    if os.path.exists('ETF列表.xlsx'):
        m_df = pd.read_excel('ETF列表.xlsx', dtype={'证券代码': str})
        etf_names = dict(zip(m_df['证券代码'].str.zfill(6), m_df['证券简称']))

    # 2. 并行分析
    csv_files = glob.glob('fund_data/*.csv')
    with multiprocessing.Pool(multiprocessing.cpu_count()) as pool:
        raw_results = pool.starmap(analyze_single_file, [(f, etf_names) for f in csv_files])

    # 3. 过滤与排序
    valid_results = [r[0] for r in raw_results if r is not None]
    prios = [r[1] for r in raw_results if r is not None]
    
    if not valid_results:
        print("今日无符合条件的优质信号。")
        return

    cols = ['代码', '名称', '操作建议', '大趋势', '偏离度%', '当前连跌', 
            '2跌均涨', '2跌胜率%', '3跌均涨', '3跌胜率%', '4跌均涨', '4跌胜率%', '5跌均涨', '5跌胜率%']
    
    res_df = pd.DataFrame(valid_results, columns=cols)
    res_df['prio'] = prios
    res_df = res_df.sort_values('prio', ascending=False).drop(columns=['prio'])

    # 4. 目录创建与保存
    now = datetime.now()
    dir_name = now.strftime('%Y%m')
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)
    
    file_name = f"etf_final_strategy_{now.strftime('%Y%m%d_%H%M%S')}.csv"
    save_path = os.path.join(dir_name, file_name)
    
    res_df.to_csv(save_path, index=False, encoding='utf_8_sig')
    print(f"信号报告已推送至: {save_path} (共 {len(res_df)} 条优质信号)")

if __name__ == '__main__':
    main()