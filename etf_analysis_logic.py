import os
import pandas as pd
import glob
from datetime import datetime, timedelta
import multiprocessing

# ==========================================
# 战法名称：【终极全功能版·不再出错】
# 功能：连跌统计 + 趋势/偏离过滤 + 溢价比对 + 名称匹配 + 自动归档
# ==========================================

def get_stats(df):
    stats = []
    for d in [2, 3, 4, 5]:
        target_idx = df[df['down_count'] == d].index + 1
        target_idx = [i for i in target_idx if i < len(df)]
        if not target_idx:
            stats.extend([0.0, 0.0])
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
        
        # 严格获取名称
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
        rating, prio = "过滤", 0
        if curr_down >= 2:
            if is_bull:
                score = curr_down + (2 if bias20 < -3 else 0)
                rating, prio = f"🔴顺势 {'⭐'*score}", 100 + score
            elif curr_down >= 4 or bias20 < -8:
                rating, prio = f"🔵逆势抢反弹 {'⚡'*curr_down}", 50 + curr_down
        
        if rating == "过滤": return None

        # 4. 统计数据
        full_stats = get_stats(df)
        three_years_ago = datetime.now() - timedelta(days=1095)
        df_3y = df[df['日期'] >= three_years_ago].copy()
        three_year_stats = get_stats(df_3y) if not df_3y.empty else [0.0]*8
        
        # 基础信息列
        base_info = [symbol, name, rating, "多头" if is_bull else "空头", round(bias20, 2), curr_down]
        return (base_info + full_stats + three_year_stats, prio, bias20)
    except: return None

def main():
    # 1. 加载名称列表 (支持 xlsx 或 csv)
    etf_names = {}
    name_file = 'ETF列表.xlsx'
    if os.path.exists(name_file):
        try:
            if name_file.endswith('.xlsx'):
                m_df = pd.read_excel(name_file, dtype={'证券代码': str})
            else:
                m_df = pd.read_csv(name_file, dtype={'证券代码': str})
            # 兼容列名：证券代码/代码，证券简称/名称/简称
            c_code = '证券代码' if '证券代码' in m_df.columns else m_df.columns[0]
            c_name = '证券简称' if '证券简称' in m_df.columns else m_df.columns[1]
            etf_names = dict(zip(m_df[c_code].str.zfill(6), m_df[c_name]))
        except Exception as e: print(f"名称文件加载失败: {e}")

    # 2. 加载溢价数据
    premium_dict = {}
    if os.path.exists('all_valid_data.csv'):
        try:
            av_df = pd.read_csv('all_valid_data.csv', dtype={'代码': str})
            av_df['溢价率_num'] = av_df['溢价率'].str.replace('%', '').astype(float)
            premium_dict = av_df.set_index('代码')[['溢价率', '估算净值', '溢价率_num']].to_dict('index')
        except: pass

    # 3. 并行分析
    csv_files = glob.glob('fund_data/*.csv')
    with multiprocessing.Pool(multiprocessing.cpu_count()) as pool:
        raw_results = pool.starmap(analyze_single_file, [(f, etf_names) for f in csv_files])

    valid_results = [r[0] for r in raw_results if r is not None]
    if not valid_results: return print("今日无优质信号。")

    # 定义完整表头
    cols = ['代码', '名称', '操作建议', '大趋势', '偏离度%', '当前连跌', 
            '全2均涨', '全2胜率%', '全3均涨', '全3胜率%', '全4均涨', '全4胜率%', '全5均涨', '全5胜率%',
            '3年2均涨', '3年2胜率%', '3年3均涨', '3年3胜率%', '3年4均涨', '3年4胜率%', '3年5均涨', '3年5胜率%']
    
    res_df = pd.DataFrame(valid_results, columns=cols)
    
    # 4. 实时溢价匹配与高溢价过滤
    def apply_premium(row):
        code = row['代码']
        if code in premium_dict:
            info = premium_dict[code]
            if info['溢价率_num'] > 2.0: return None, None, True # 熔断
            return info['溢价率'], info['估算净值'], False
        return "未知", "未知", False

    res_df[['实时溢价率', '参考净值', 'is_filtered']] = res_df.apply(lambda r: pd.Series(apply_premium(r)), axis=1)
    final_df = res_df[res_df['is_filtered'] == False].drop(columns=['is_filtered']).copy()
    
    # 5. 排序与今日之星识别
    # 重新附回 prio 和 bias 用于排序
    prio_map = {r[0][0]: r[1] for r in raw_results if r is not None}
    bias_map = {r[0][0]: r[2] for r in raw_results if r is not None}
    final_df['prio'] = final_df['代码'].map(prio_map)
    final_df['bias_val'] = final_df['代码'].map(bias_map)
    
    final_df = final_df.sort_values(['prio', 'bias_val'], ascending=[False, True])
    if not final_df.empty:
        final_df.iloc[0, 2] = "👑今日之星 " + final_df.iloc[0, 2]
    
    final_df = final_df.drop(columns=['prio', 'bias_val'])

    # 6. 保存到归档目录
    now = datetime.now()
    month_dir = now.strftime('%Y%m')
    if not os.path.exists(month_dir): os.makedirs(month_dir)
    save_path = os.path.join(month_dir, f"etf_final_strategy_{now.strftime('%Y%m%d_%H%M%S')}.csv")
    final_df.to_csv(save_path, index=False, encoding='utf_8_sig')
    print(f"处理完成，结果已保存至: {save_path}")

if __name__ == '__main__':
    main()
