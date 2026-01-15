import os
import pandas as pd
import glob
from datetime import datetime, timedelta
import multiprocessing

# ==========================================
# 战法名称：【连跌回调·五星评分战法】
# 逻辑说明：
# 1. 扫描 fund_data 历史数据，对比【全量胜率】与【近3年胜率】。
# 2. 评分系统：
#    - ⭐⭐⭐⭐⭐：近期触发3连跌，且近3年反弹胜率 > 全量胜率 > 55%（逻辑加强，反弹意愿极强）。
#    - ⭐⭐⭐⭐：近期触发3连跌，历史全量胜率 > 55%（长期基因优秀）。
#    - ⭐⭐⭐：近期触发2连跌，历史胜率稳定（短线机会）。
# 3. 操作要领：选星级高的品种，博弈次日收阳；若近3年胜率明显退化，即使连跌也不碰。
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
        # 计算次日涨跌幅
        changes = (next_days['收盘'].values - prev_days['收盘'].values) / prev_days['收盘'].values * 100
        win_rate = (changes > 0).mean()
        avg_change = changes.mean()
        stats.extend([round(win_rate * 100, 2), round(avg_change, 2)])
    return stats

def calculate_score(row):
    """根据数据计算星级评分"""
    score = 0
    # 基础：是否有近期信号
    if row['近1周3连跌'] == 1:
        score = 3
        # 基因加分：全量胜率高
        if row['全量3跌胜率%'] > 55: score += 1
        # 时效加分：近3年表现更好（逻辑增强）
        if row['近3年3跌胜率%'] > row['全量3跌胜率%']: score += 1
    elif row['近1周2连跌'] == 1:
        score = 2
        if row['全量2跌胜率%'] > 55: score += 1
    
    # 逻辑退化惩罚：如果近3年胜率低于45%且明显低于全量，降级
    if row['近3年3跌胜率%'] > 0 and row['近3年3跌胜率%'] < 45:
        score = max(0, score - 2)
        
    return "⭐" * score if score > 0 else "无信号/观察"

def analyze_single_file(file_path, etf_names):
    try:
        df = pd.read_csv(file_path)
        if df.empty or len(df) < 5: return None
        df['日期'] = pd.to_datetime(df['日期'])
        df = df.sort_values('日期').reset_index(drop=True)
        
        symbol = os.path.basename(file_path).split('.')[0].zfill(6)
        name = etf_names.get(symbol, "未知")
        
        # 核心逻辑：计算连跌
        df['is_down'] = df['收盘'].diff() < 0
        counts, cur = [], 0
        for val in df['is_down']:
            if val: cur += 1
            else: cur = 0
            counts.append(cur)
        df['down_count'] = counts
        
        # 1. 信号：最近一周
        last_5 = df.tail(5)
        recent_2d = 1 if any(last_5['down_count'] == 2) else 0
        recent_3d = 1 if any(last_5['down_count'] == 3) else 0
        
        # 2. 统计
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
    
    # 计算综合评分列
    res_df['战法评分'] = res_df.apply(calculate_score, axis=1)
    
    # 重新排列：评分列放在最前面
    new_cols = ['代码', '名称', '战法评分'] + [c for c in cols if c not in ['代码', '名称']]
    res_df = res_df[new_cols]
    
    # 排序：星级高的在前
    res_df['score_len'] = res_df['战法评分'].str.len()
    res_df = res_df.sort_values(by=['score_len', '近3年3跌胜率%'], ascending=False).drop('score_len', axis=1)

    folder = datetime.now().strftime('%Y%m')
    if not os.path.exists(folder): os.makedirs(folder)
    out = f"{folder}/etf_analysis_logic_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    res_df.to_csv(out, index=False, encoding='utf_8_sig')
    print(f"分析报告已生成: {out}")

if __name__ == '__main__':
    main()
