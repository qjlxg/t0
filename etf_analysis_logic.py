import os
import pandas as pd
import glob
from datetime import datetime, timedelta
import multiprocessing

# ==========================================
# 战法名称：【连跌回调·溢价风控终极版】
# 核心逻辑：
# 1. 战法触发：连跌 + 趋势 + 偏离度。
# 2. 溢价熔断：比对 all_valid_data.csv，剔除溢价 > 2% 的标的。
# 3. 结果精选：只输出低溢价的顺势红标和逆势蓝标。
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
        
        # 3. 评分
        curr_down = counts[-1]
        rating, prio = "过滤", 0
        if curr_down >= 2:
            if is_bull:
                score = curr_down + (2 if bias20 < -3 else 0)
                rating, prio = f"🔴顺势 {'⭐'*score}", 100 + score
            elif curr_down >= 4 or bias20 < -8:
                rating, prio = f"🔵逆势抢反弹 {'⚡'*curr_down}", 50 + curr_down
        
        if rating == "过滤": return None
        return ([symbol, name, rating, "多头" if is_bull else "空头", round(bias20, 2), curr_down] + get_stats(df) + get_stats(df[df['日期'] >= datetime.now() - timedelta(days=1095)]), prio, bias20)
    except: return None

def main():
    # 1. 加载溢价数据 (all_valid_data.csv)
    premium_data = {}
    if os.path.exists('all_valid_data.csv'):
        av_df = pd.read_csv('all_valid_data.csv', dtype={'代码': str})
        # 将溢价率字符串 "-0.13%" 转为浮点数 -0.13
        av_df['溢价率_num'] = av_df['溢价率'].str.replace('%', '').astype(float)
        premium_data = av_df.set_index('代码')[['溢价率', '估算净值', '溢价率_num']].to_dict('index')

    # 2. 加载名称
    etf_names = {}
    if os.path.exists('ETF列表.xlsx - Sheet1.csv'):
        m_df = pd.read_csv('ETF列表.xlsx - Sheet1.csv', dtype={'证券代码': str})
        etf_names = dict(zip(m_df['证券代码'].str.zfill(6), m_df['证券简称']))

    # 3. 分析
    csv_files = glob.glob('fund_data/*.csv')
    with multiprocessing.Pool(multiprocessing.cpu_count()) as pool:
        raw_results = pool.starmap(analyze_single_file, [(f, etf_names) for f in csv_files])

    valid_results = [r[0] for r in raw_results if r is not None]
    prios = [r[1] for r in raw_results if r is not None]
    biases = [r[2] for r in raw_results if r is not None]
    
    if not valid_results: return print("今日无信号。")

    cols = ['代码', '名称', '操作建议', '大趋势', '偏离度%', '当前连跌', 
            '全2均涨', '全2胜率%', '全3均涨', '全3胜率%', '全4均涨', '全4胜率%', '全5均涨', '全5胜率%',
            '3年2均涨', '3年2胜率%', '3年3均涨', '3年3胜率%', '3年4均涨', '3年4胜率%', '3年5均涨', '3年5胜率%']
    
    res_df = pd.DataFrame(valid_results, columns=cols)
    
    # 4. 核心：比对溢价数据并过滤
    def check_premium(row):
        code = row['代码']
        if code in premium_data:
            p_info = premium_data[code]
            if p_info['溢价率_num'] > 2.0: # 溢价熔断阈值：2.0%
                return None, None, True # 标记为被过滤
            return p_info['溢价率'], p_info['估算净值'], False
        return "未知", "未知", False

    res_df[['实时溢价率', '参考净值', 'is_filtered']] = res_df.apply(lambda r: pd.Series(check_premium(r)), axis=1)
    
    # 剔除高溢价标的
    final_df = res_df[res_df['is_filtered'] == False].drop(columns=['is_filtered']).copy()
    
    # 5. 排序与今日之星
    final_df['prio'] = [prios[i] for i in final_df.index]
    final_df['bias_val'] = [biases[i] for i in final_df.index]
    final_df = final_df.sort_values(['prio', 'bias_val'], ascending=[False, True])
    if not final_df.empty:
        final_df.iloc[0, 2] = "👑今日之星 " + final_df.iloc[0, 2]
    
    final_df = final_df.drop(columns=['prio', 'bias_val'])

    # 6. 保存
    now = datetime.now()
    month_dir = now.strftime('%Y%m')
    if not os.path.exists(month_dir): os.makedirs(month_dir)
    save_path = os.path.join(month_dir, f"etf_final_strategy_{now.strftime('%Y%m%d_%H%M%S')}.csv")
    final_df.to_csv(save_path, index=False, encoding='utf_8_sig')
    print(f"过滤高溢价后，剩余 {len(final_df)} 只标的。结果存至: {save_path}")

if __name__ == '__main__':
    main()
