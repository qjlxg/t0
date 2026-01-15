import os
import pandas as pd
import glob
from datetime import datetime, timedelta
import multiprocessing

# ==========================================
# 战法名称：【连跌回调·五星评分战法】
# 排序逻辑：有信号标的（⭐）绝对置顶，按星级和近3年胜率双重排序
# ==========================================

def get_stats(df):
    stats = []
    for d in [2, 3, 4, 5]:
        target_idx = df[df['down_count'] == d].index + 1
        target_idx = [i for i in target_idx if i < len(df)]
        if not target_idx:
            stats.extend([0, 0])
            continue
        next_days = df.iloc[target_idx]
        prev_days = df.iloc[[i-1 for i in target_idx]]
        changes = (next_days['收盘'].values - prev_days['收盘'].values) / prev_days['收盘'].values * 100
        win_rate = (changes > 0).mean()
        avg_change = changes.mean()
        stats.extend([round(win_rate * 100, 2), round(avg_change, 2)])
    return stats

def calculate_score(row):
    score = 0
    # 3连跌基础评分
    if row['近1周3连跌'] == 1:
        score = 3
        if row['全量3跌胜率%'] > 55: score += 1
        if row['近3年3跌胜率%'] > row['全量3跌胜率%']: score += 1
    # 2连跌基础评分
    elif row['近1周2连跌'] == 1:
        score = 2
        if row['全量2跌胜率%'] > 55: score += 1
    
    # 风险惩罚
    if row['近3年3跌胜率%'] > 0 and row['近3年3跌胜率%'] < 45:
        score = max(0, score - 2)
        
    return "⭐" * score if score > 0 else "无信号"

def analyze_single_file(file_path, etf_names):
    try:
        df = pd.read_csv(file_path)
        if df.empty or len(df) < 5: return None
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
        
        # 信号提取
        last_5 = df.tail(5)
        recent_2d = 1 if any(last_5['down_count'] == 2) else 0
        recent_3d = 1 if any(last_5['down_count'] == 3) else 0
        
        full_stats = get_stats(df)
        three_years_ago = datetime.now() - timedelta(days=1095)
        df_3y = df[df['日期'] >= three_years_ago].copy()
        three_year_stats = get_stats(df_3y) if not df_3y.empty else [0]*8
            
        return [symbol, name, recent_2d, recent_3d] + full_stats + three_year_stats
    except:
        return None

def main():
    etf_names = {}
    if os.path.exists('ETF列表.xlsx'):
        try:
            m_df = pd.read_excel('ETF列表.xlsx', dtype={'证券代码': str})
            etf_names = dict(zip(m_df['证券代码'].str.zfill(6), m_df['证券简称']))
        except: pass

    csv_files = glob.glob('fund_data/*.csv')
    with multiprocessing.Pool(multiprocessing.cpu_count()) as pool:
        results = pool.starmap(analyze_single_file, [(f, etf_names) for f in csv_files])

    results = [r for r in results if r is not None]
    cols = ['代码', '名称', '近1周2连跌', '近1周3连跌', 
            '全量2跌胜率%', '全量2跌均涨', '全量3跌胜率%', '全量3跌均涨', 
            '全量4跌胜率%', '全量4跌均涨', '全量5跌胜率%', '全量5跌均涨',
            '近3年2跌胜率%', '近3年2跌均涨', '近3年3跌胜率%', '近3年3跌均涨',
            '近3年4跌胜率%', '近3年4跌均涨', '近3年5跌胜率%', '近3年5跌均涨']
    
    res_df = pd.DataFrame(results, columns=cols)
    res_df['战法评分'] = res_df.apply(calculate_score, axis=1)
    
    # --- 关键排序逻辑优化 ---
    # 定义星级长度作为排序列
    res_df['rank_score'] = res_df['战法评分'].apply(lambda x: len(x) if '⭐' in x else 0)
    
    # 按照评分等级从高到低，再按近3年3跌胜率从高到低
    res_df = res_df.sort_values(by=['rank_score', '近3年3跌胜率%'], ascending=False)
    
    # 移除辅助排序列，调整列顺序
    res_df = res_df.drop(columns=['rank_score'])
    new_cols = ['代码', '名称', '战法评分'] + [c for c in cols if c not in ['代码', '名称']]
    res_df = res_df[new_cols]

    folder = datetime.now().strftime('%Y%m')
    if not os.path.exists(folder): os.makedirs(folder)
    out = f"{folder}/etf_analysis_logic_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    res_df.to_csv(out, index=False, encoding='utf_8_sig')
    print(f"信号已置顶！报告生成: {out}")

if __name__ == '__main__':
    main()
