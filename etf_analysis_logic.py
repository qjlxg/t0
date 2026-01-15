import os
import pandas as pd
import glob
from datetime import datetime, timedelta
import multiprocessing

# ==========================================
# 战法名称：【连跌回调·完整版（智能分级）】
# 优化点：
# 1. 评分分级：区分“顺势金叉”和“逆势超跌”，不放过任何机会。
# 2. 乖离阈值：Bias20 < -3% 即视为具备反弹空间。
# 3. 结果展示：将核心判断指标（趋势、乖离、量比）全部前置。
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
        if df.empty or len(df) < 30: return None
        
        df['日期'] = pd.to_datetime(df['日期'])
        df = df.sort_values('日期').reset_index(drop=True)
        symbol = os.path.basename(file_path).split('.')[0].zfill(6)
        name = etf_names.get(symbol, "未知")

        # --- 技术指标计算 ---
        df['is_down'] = df['收盘'].diff() < 0
        counts, cur = [], 0
        for val in df['is_down']:
            if val: cur += 1
            else: cur = 0
            counts.append(cur)
        df['down_count'] = counts
        
        # 趋势：MA250 (如果数据不够，用MA60替代判断短趋势)
        ma_period = 250 if len(df) >= 250 else 60
        df['ma_trend'] = df['收盘'].rolling(window=ma_period).mean()
        current_price = df['收盘'].iloc[-1]
        is_bull = current_price > df['ma_trend'].iloc[-1]
        
        # 乖离：Bias20
        df['ma20'] = df['收盘'].rolling(window=20).mean()
        bias20 = ((current_price - df['ma20'].iloc[-1]) / df['ma20'].iloc[-1]) * 100
        
        # 量比：今日成交量 / 5日均量
        vol_ratio = df['成交量'].iloc[-1] / df['成交量'].rolling(5).mean().iloc[-1]

        # --- 评分逻辑 ---
        current_down = counts[-1]
        score = 0
        signal_type = "观察"
        
        if current_down >= 2:
            score = current_down # 2连跌2分，3连跌3分
            if is_bull: score += 1      # 趋势好+1
            if bias20 < -4: score += 1  # 跌得深+1
            if vol_ratio < 0.8: score += 1 # 缩量衰竭+1
            
            # 信号分类
            if is_bull:
                signal_type = "⭐顺势捡钱"
            else:
                signal_type = "⚡逆势博弈"
        
        rating = signal_type + " " + ("⭐" * score)
        if current_down >= 4: rating = "❗" + rating

        # 获取统计数据
        full_stats = get_stats(df)
        three_years_ago = datetime.now() - timedelta(days=1095)
        df_3y = get_stats(df[df['日期'] >= three_years_ago]) if not df[df['日期'] >= three_years_ago].empty else [0]*8
        
        return [symbol, name, rating, "多头" if is_bull else "空头", round(bias20, 2), round(vol_ratio, 2), current_down] + full_stats + three_year_stats
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
    cols = ['代码', '名称', '综合评价', '趋势', '乖离率20', '量比', '连跌天数',
            '全量2跌均涨', '全量2跌胜率%', '全量3跌均涨', '全量3跌胜率%', '全量4跌均涨', '全量4跌胜率%', '全量5跌均涨', '全量5跌胜率%',
            '3年2跌均涨', '3年2跌胜率%', '3年3跌均涨', '3年3跌胜率%', '3年4跌均涨', '3年4跌胜率%', '3年5跌均涨', '3年5跌胜率%']
    
    res_df = pd.DataFrame(results, columns=cols)
    
    # 排序逻辑：带信号的全部置顶，然后按评分和胜率排
    res_df['is_signal'] = res_df['综合评价'].apply(lambda x: 1 if "⭐" in x or "⚡" in x else 0)
    res_df = res_df.sort_values(['is_signal', '3年3跌胜率%'], ascending=False).drop(columns=['is_signal'])

    folder = datetime.now().strftime('%Y%m')
    if not os.path.exists(folder): os.makedirs(folder)
    out = f"{folder}/etf_comprehensive_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    res_df.to_csv(out, index=False, encoding='utf_8_sig')
    print(f"报告已生成（包含顺势与逆势信号）: {out}")

if __name__ == '__main__':
    main()
