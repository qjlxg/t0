import os
import pandas as pd
import glob
from datetime import datetime, timedelta
import multiprocessing

# ==========================================
# 战法名称：【连跌回调捕捉战法 (时效增强版)】
# 备注：
# 1. 扫描 fund_data 下所有 CSV 数据。
# 2. 统计最近一周触发 2-3 连跌的标的（实时信号）。
# 3. 统计对比：全量历史胜率 vs 近3年胜率。
# 4. 操作要领：
#    - 优先选择：全量胜率 > 55% 且 近3年胜率未明显退化的品种。
#    - 预警：若近3年胜率远低于全量胜率，说明该品种近期“跌起来不回头”，需放弃博弈。
# ==========================================

def get_stats(df, days_count, period_name=""):
    """
    计算连跌后的胜率和均涨幅
    """
    stats = []
    for d in [2, 3, 4, 5]:
        # 寻找连跌 d 天的索引
        target_idx = df[df['down_count'] == d].index + 1
        target_idx = [i for i in target_idx if i < len(df)]
        
        if not target_idx:
            stats.extend([0, 0])
            continue
            
        next_days = df.iloc[target_idx]
        prev_days = df.iloc[[i-1 for i in target_idx]]
        
        # 计算次日收益率
        changes = (next_days['收盘'].values - prev_days['收盘'].values) / prev_days['收盘'].values * 100
        
        win_rate = (changes > 0).mean()
        avg_change = changes.mean()
        stats.extend([round(win_rate * 100, 2), round(avg_change, 2)])
    return stats

def analyze_single_file(file_path, etf_names):
    try:
        df = pd.read_csv(file_path)
        if df.empty or len(df) < 5: return None
            
        df['日期'] = pd.to_datetime(df['日期'])
        df = df.sort_values('日期').reset_index(drop=True)
        
        raw_symbol = os.path.basename(file_path).split('.')[0]
        symbol = raw_symbol.zfill(6) 
        name = etf_names.get(symbol, "未知")
        
        # 识别下跌逻辑
        df['is_down'] = df['收盘'].diff() < 0
        
        def count_consecutive(series):
            counts, cur = [], 0
            for val in series:
                if val: cur += 1
                else: cur = 0
                counts.append(cur)
            return counts
        
        df['down_count'] = count_consecutive(df['is_down'])
        
        # A. 最近一周信号
        last_5 = df.tail(5)
        recent_2d = 1 if any(last_5['down_count'] == 2) else 0
        recent_3d = 1 if any(last_5['down_count'] == 3) else 0
        
        # B. 全量历史统计
        full_stats = get_stats(df, [2,3,4,5])
        
        # C. 近3年统计 (1095天)
        three_years_ago = datetime.now() - timedelta(days=1095)
        df_3y = df[df['日期'] >= three_years_ago].copy()
        three_year_stats = get_stats(df_3y, [2,3,4,5]) if not df_3y.empty else [0]*8
            
        return [symbol, name, recent_2d, recent_3d] + full_stats + three_year_stats

    except Exception:
        return None

def main():
    etf_names = {}
    mapping_file = 'ETF列表.xlsx' # 确保文件名准确
    if os.path.exists(mapping_file):
        try:
            m_df = pd.read_excel(mapping_file, dtype={'证券代码': str})
            m_df['证券代码'] = m_df['证券代码'].str.zfill(6)
            etf_names = dict(zip(m_df['证券代码'], m_df['证券简称']))
        except: pass

    csv_files = glob.glob('fund_data/*.csv')
    if not csv_files: return

    with multiprocessing.Pool(multiprocessing.cpu_count()) as pool:
        results = pool.starmap(analyze_single_file, [(f, etf_names) for f in csv_files])

    results = [r for r in results if r is not None]
    
    cols = ['代码', '名称', '近1周2连跌', '近1周3连跌', 
            '全量2跌胜率%', '全量2跌均涨', '全量3跌胜率%', '全量3跌均涨', 
            '全量4跌胜率%', '全量4跌均涨', '全量5跌胜率%', '全量5跌均涨',
            '近3年2跌胜率%', '近3年2跌均涨', '近3年3跌胜率%', '近3年3跌均涨',
            '近3年4跌胜率%', '近3年4跌均涨', '近3年5跌胜率%', '近3年5跌均涨']
    
    res_df = pd.DataFrame(results, columns=cols)
    # 优先展示近期有信号的，再按近3年3连跌胜率排
    res_df = res_df.sort_values(by=['近1周3连跌', '近1周2连跌', '近3年3跌胜率%'], ascending=False)

    folder = datetime.now().strftime('%Y%m')
    if not os.path.exists(folder): os.makedirs(folder)
    
    output_path = f"{folder}/etf_analysis_logic_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    res_df.to_csv(output_path, index=False, encoding='utf_8_sig')
    print(f"分析完成: {output_path}")

if __name__ == '__main__':
    main()
