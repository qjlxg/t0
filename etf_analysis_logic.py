import os
import pandas as pd
import glob
from datetime import datetime, timedelta
import multiprocessing

# ==========================================
# 战法名称：【连跌回调·全防御增强版】
# 核心逻辑：
# 1. 趋势过滤：计算MA250（年线）。若价格在年线下，评分强降级。
# 2. 偏离过滤：计算BIAS20（乖离率）。偏离不够，反弹无力。
# 3. 量能过滤：对比成交量。缩量阴跌不进场，放量恐慌/地量衰竭才是底。
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
        stats.extend([round(changes.mean(), 2), round((changes > 0).mean() * 100, 2)])
    return stats

def analyze_single_file(file_path, etf_names):
    try:
        df = pd.read_csv(file_path)
        if df.empty or len(df) < 250: return None # 数据不足一年不分析大趋势
        
        df['日期'] = pd.to_datetime(df['日期'])
        df = df.sort_values('日期').reset_index(drop=True)
        symbol = os.path.basename(file_path).split('.')[0].zfill(6)
        name = etf_names.get(symbol, "未知")

        # --- 计算技术指标 ---
        # 1. 连跌天数
        df['is_down'] = df['收盘'].diff() < 0
        counts, cur = [], 0
        for val in df['is_down']:
            if val: cur += 1
            else: cur = 0
            counts.append(cur)
        df['down_count'] = counts
        
        # 2. 趋势项：250日均线
        df['ma250'] = df['收盘'].rolling(window=250).mean()
        current_price = df['收盘'].iloc[-1]
        ma250_val = df['ma250'].iloc[-1]
        trend = "多头" if current_price > ma250_val else "空头(危险)"
        
        # 3. 乖离率：(收盘-MA20)/MA20
        df['ma20'] = df['收盘'].rolling(window=20).mean()
        bias20 = ((current_price - df['ma20'].iloc[-1]) / df['ma20'].iloc[-1]) * 100
        
        # 4. 成交量：对比5日均量
        vol_ratio = df['成交量'].iloc[-1] / df['成交量'].rolling(5).mean().iloc[-1]

        # --- 信号与评分 ---
        current_down = counts[-1]
        last_5 = df.tail(5)
        recent_3d = 1 if any(last_5['down_count'] == 3) else 0
        
        # 基础胜率统计
        full_stats = get_stats(df)
        three_years_ago = datetime.now() - timedelta(days=1095)
        df_3y = get_stats(df[df['日期'] >= three_years_ago]) if not df[df['日期'] >= three_years_ago].empty else [0]*8
        
        # 综合评分逻辑
        score = 0
        if current_down >= 3:
            score = 3
            if trend == "多头": score += 1      # 趋势加成
            if bias20 < -5: score += 1          # 超跌加成
            if vol_ratio < 0.8: score += 1      # 缩量地量加成
            if trend == "空头(危险)": score -= 2 # 空头市场严厉减分
        
        rating = "⭐" * max(0, score) if score > 0 else "观察"
        if current_down >= 4: rating = "❗极端 " + rating

        return [symbol, name, rating, trend, round(bias20, 2), current_down, recent_3d] + full_stats + three_year_stats
    except:
        return None

def main():
    etf_names = {}
    if os.path.exists('ETF列表.xlsx'):
        m_df = pd.read_excel('ETF列表.xlsx', dtype={'证券代码': str})
        etf_names = dict(zip(m_df['证券代码'].str.zfill(6), m_df['证券简称']))

    csv_files = glob.glob('fund_data/*.csv')
    with multiprocessing.Pool(multiprocessing.cpu_count()) as pool:
        results = pool.starmap(analyze_single_file, [(f, etf_names) for f in csv_files])

    results = [r for r in results if r is not None]
    cols = ['代码', '名称', '战法评分', '趋势状态', '乖离率20', '当前连跌', '近1周3连跌',
            '全量2跌均涨', '全量2跌胜率%', '全量3跌均涨', '全量3跌胜率%', '全量4跌均涨', '全量4跌胜率%', '全量5跌均涨', '全量5跌胜率%',
            '3年2跌均涨', '3年2跌胜率%', '3年3跌均涨', '3年3跌胜率%', '3年4跌均涨', '3年4跌胜率%', '3年5跌均涨', '3年5跌胜率%']
    
    res_df = pd.DataFrame(results, columns=cols)
    res_df['sort_key'] = res_df['战法评分'].apply(lambda x: len(x) if '⭐' in x else 0)
    res_df = res_df.sort_values(['sort_key', '3年3跌胜率%'], ascending=False).drop(columns=['sort_key'])

    folder = datetime.now().strftime('%Y%m')
    if not os.path.exists(folder): os.makedirs(folder)
    out = f"{folder}/etf_full_defense_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    res_df.to_csv(out, index=False, encoding='utf_8_sig')
    print(f"完整防御版报告已生成: {out}")

if __name__ == '__main__':
    main()
