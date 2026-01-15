import os
import pandas as pd
import glob
from datetime import datetime, timedelta
import multiprocessing

# ==========================================
# 战法名称：【连跌回调·终极智能归档修正版】
# 修正说明：严格匹配 DataFrame 的列数（30列数据对30个列名）
# ==========================================

def get_stats(df):
    """返回 2/3/4/5 连跌后的 均涨 和 胜率，共 8 个值"""
    stats = []
    for d in [2, 3, 4, 5]:
        target_idx = df[df['down_count'] == d].index + 1
        target_idx = [i for i in target_idx if i < len(df)]
        if not target_idx:
            stats.extend([0.0, 0.0]) # 均涨, 胜率
            continue
        changes = (df.iloc[target_idx]['收盘'].values - df.iloc[[i-1 for i in target_idx]]['收盘'].values) / df.iloc[[i-1 for i in target_idx]]['收盘'].values * 100
        stats.extend([round(changes.mean(), 2), round((changes > 0).mean() * 100, 2)])
    return stats

def analyze_single_file(file_path, etf_names):
    try:
        df = pd.read_csv(file_path)
        if len(df) < 60: return None
        
        df['日期'] = pd.to_datetime(df['日期'])
        df = df.sort_values('日期').reset_index(drop=True)
        symbol = os.path.basename(file_path).split('.')[0].zfill(6)
        name = etf_names.get(symbol, "未知")

        # 1. 连跌计算
        df['is_down'] = df['收盘'].diff() < 0
        counts, cur = [], 0
        for val in df['is_down']:
            if val: cur += 1
            else: cur = 0
            counts.append(cur)
        df['down_count'] = counts
        
        # 2. 趋势与偏离度
        ma_period = 250 if len(df) >= 250 else 60
        df['ma_trend'] = df['收盘'].rolling(window=ma_period).mean()
        curr_price = df['收盘'].iloc[-1]
        is_bull = curr_price > df['ma_trend'].iloc[-1]
        
        df['ma20'] = df['收盘'].rolling(20).mean()
        bias20 = ((curr_price - df['ma20'].iloc[-1]) / df['ma20'].iloc[-1]) * 100
        
        # 3. 评分分级
        curr_down = counts[-1]
        rating = "过滤"
        prio = 0
        
        if curr_down >= 2:
            if is_bull:
                score = curr_down + (2 if bias20 < -3 else 0)
                rating = f"🔴顺势 {'⭐'*score}"
                prio = 100 + score
            elif curr_down >= 4 or bias20 < -8:
                rating = f"🔵逆势抢反弹 {'⚡'*curr_down}"
                prio = 50 + curr_down
        
        if rating == "过滤": return None

        # 4. 数据采集 (核心：确保这里返回的数据量与 cols 长度一致)
        # 基础 6 列: 代码, 名称, 操作建议, 大趋势, 偏离度%, 当前连跌
        base_info = [symbol, name, rating, "多头" if is_bull else "空头", round(bias20, 2), curr_down]
        
        # 全量统计 8 列
        full_stats = get_stats(df)
        
        # 近3年统计 8 列
        three_years_ago = datetime.now() - timedelta(days=1095)
        df_3y = df[df['日期'] >= three_years_ago].copy()
        three_year_stats = get_stats(df_3y) if not df_3y.empty else [0.0]*8
        
        # 总计: 6 + 8 + 8 = 22 列
        return (base_info + full_stats + three_year_stats, prio, bias20)
    except:
        return None

def main():
    etf_names = {}
    if os.path.exists('ETF列表.xlsx'):
        try:
            m_df = pd.read_excel('ETF列表.xlsx', dtype={'证券代码': str})
            etf_names = dict(zip(m_df['证券代码'].str.zfill(6), m_df['证券简称']))
        except:
            # 兼容CSV格式
            try:
                m_df = pd.read_csv('ETF列表.xlsx', dtype={'证券代码': str})
                etf_names = dict(zip(m_df['证券代码'].str.zfill(6), m_df['证券简称']))
            except: pass

    csv_files = glob.glob('fund_data/*.csv')
    with multiprocessing.Pool(multiprocessing.cpu_count()) as pool:
        raw_results = pool.starmap(analyze_single_file, [(f, etf_names) for f in csv_files])

    valid_results = [r[0] for r in raw_results if r is not None]
    prios = [r[1] for r in raw_results if r is not None]
    biases = [r[2] for r in raw_results if r is not None]
    
    if not valid_results:
        print("今日无优质信号。")
        return

    # 严格定义 22 列名
    cols = ['代码', '名称', '操作建议', '大趋势', '偏离度%', '当前连跌', 
            '全2均涨', '全2胜率%', '全3均涨', '全3胜率%', '全4均涨', '全4胜率%', '全5均涨', '全5胜率%',
            '3年2均涨', '3年2胜率%', '3年3均涨', '3年3胜率%', '3年4均涨', '3年4胜率%', '3年5均涨', '3年5胜率%']
    
    res_df = pd.DataFrame(valid_results, columns=cols)
    res_df['prio'] = prios
    res_df['bias_val'] = biases
    
    # 排序并识别今日之星
    res_df = res_df.sort_values(['prio', 'bias_val'], ascending=[False, True])
    top_prio = res_df['prio'].max()
    res_df.loc[res_df['prio'] == top_prio, '操作建议'] = res_df.loc[res_df['prio'] == top_prio, '操作建议'].apply(lambda x: "👑今日之星 " + x)
    
    res_df = res_df.drop(columns=['prio', 'bias_val'])

    # 归档保存
    now = datetime.now()
    month_dir = now.strftime('%Y%m')
    if not os.path.exists(month_dir): os.makedirs(month_dir)
    
    file_name = f"etf_final_strategy_{now.strftime('%Y%m%d_%H%M%S')}.csv"
    save_path = os.path.join(month_dir, file_name)
    res_df.to_csv(save_path, index=False, encoding='utf_8_sig')
    print(f"成功！信号已归档至: {save_path}")

if __name__ == '__main__':
    main()