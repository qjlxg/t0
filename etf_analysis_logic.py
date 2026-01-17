import os
import pandas as pd
import glob
from datetime import datetime, timedelta
import multiprocessing

# ==========================================
# 战法：【全功能·数据对齐终极版】
# 1. 自动修复 all_valid_data.csv 代码匹配问题
# 2. 严格执行 2% 溢价熔断，剔除深坑标的
# 3. 完整保留全量/3年连跌统计逻辑
# 4. 自动按年月归档结果
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
        
        # 匹配名称
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
                rating, prio = f"🔵反弹 {'⚡'*curr_down}", 50 + curr_down
        
        if rating == "过滤": return None

        # 4. 获取历史统计
        full_stats = get_stats(df)
        three_years_ago = datetime.now() - timedelta(days=1095)
        df_3y = df[df['日期'] >= three_years_ago].copy()
        three_year_stats = get_stats(df_3y) if not df_3y.empty else [0.0]*8
        
        base_info = [symbol, name, rating, "多头" if is_bull else "空头", round(bias20, 2), curr_down]
        return (base_info + full_stats + three_year_stats, prio, bias20)
    except: return None

def main():
    # 1. 加载 ETF 名称字典 (强制 6 位对齐)
    etf_names = {}
    name_file = 'ETF列表.xlsx'
    if os.path.exists(name_file):
        try:
            m_df = pd.read_excel(name_file, dtype={0: str})
            etf_names = dict(zip(m_df.iloc[:,0].str.zfill(6), m_df.iloc[:,1]))
        except:
            try:
                m_df = pd.read_csv(name_file, dtype={0: str})
                etf_names = dict(zip(m_df.iloc[:,0].str.zfill(6), m_df.iloc[:,1]))
            except: pass

    # 2. 加载溢价数据并修复格式
    premium_dict = {}
    prem_file = 'all_valid_data.csv'
    if os.path.exists(prem_file):
        try:
            # 自动识别分隔符并清理数据
            av_df = pd.read_csv(prem_file, sep=None, engine='python')
            av_df.columns = [c.strip() for c in av_df.columns]
            
            # 关键：修复代码字段，转为 6 位字符串
            av_df['代码'] = av_df['代码'].astype(str).str.replace('.0', '', regex=False).str.zfill(6)
            
            # 关键：修复溢价率字段，转为浮点数
            av_df['溢价率_val'] = av_df['溢价率'].astype(str).str.replace('%', '').replace('nan', '0').astype(float)
            
            premium_dict = av_df.set_index('代码')[['溢价率', '估算净值', '溢价率_val']].to_dict('index')
            print(f"成功载入 {len(premium_dict)} 条实时溢价参考。")
        except Exception as e:
            print(f"溢价文件匹配失败: {e}")

    # 3. 并行计算
    csv_files = glob.glob('fund_data/*.csv')
    with multiprocessing.Pool(multiprocessing.cpu_count()) as pool:
        raw_results = pool.starmap(analyze_single_file, [(f, etf_names) for f in csv_files])

    valid_results = [r[0] for r in raw_results if r is not None]
    if not valid_results:
        print("今日无符合条件信号。")
        return

    # 4. 生成 DataFrame 并执行实时过滤
    cols = ['代码', '名称', '操作建议', '大趋势', '偏离度%', '当前连跌', 
            '全2均涨', '全2胜率%', '全3均涨', '全3胜率%', '全4均涨', '全4胜率%', '全5均涨', '全5胜率%',
            '3年2均涨', '3年2胜率%', '3年3均涨', '3年3胜率%', '3年4均涨', '3年4胜率%', '3年5均涨', '3年5胜率%']
    
    res_df = pd.DataFrame(valid_results, columns=cols)

    def apply_premium_safe(row):
        code = row['代码']
        if code in premium_dict:
            p = premium_dict[code]
            # 执行 2% 溢价熔断
            if p['溢价率_val'] > 2.0: return None, None, True
            return p['溢价率'], p['估算净值'], False
        return "未知", "未知", False

    res_df[['实时溢价率', '参考净值', 'is_filtered']] = res_df.apply(lambda r: pd.Series(apply_premium_safe(r)), axis=1)
    
    # 剔除高溢价标的
    final_df = res_df[res_df['is_filtered'] == False].drop(columns=['is_filtered']).copy()

    # 5. 排序与识别“今日之星”
    prio_map = {r[0][0]: r[1] for r in raw_results if r is not None}
    bias_map = {r[0][0]: r[2] for r in raw_results if r is not None}
    final_df['prio'] = final_df['代码'].map(prio_map)
    final_df['bias_val'] = final_df['代码'].map(bias_map)
    
    final_df = final_df.sort_values(['prio', 'bias_val'], ascending=[False, True])
    if not final_df.empty:
        final_df.iloc[0, 2] = "👑今日之星 " + final_df.iloc[0, 2]
    
    final_df = final_df.drop(columns=['prio', 'bias_val'])

    # 6. 归档
    now = datetime.now()
    month_dir = now.strftime('%Y%m')
    if not os.path.exists(month_dir): os.makedirs(month_dir)
    save_path = os.path.join(month_dir, f"etf_final_strategy_{now.strftime('%Y%m%d_%H%M%S')}.csv")
    final_df.to_csv(save_path, index=False, encoding='utf_8_sig')
    print(f"处理完成！已自动剔除高溢价风险项。最终名单保存至: {save_path}")

if __name__ == '__main__':
    main()
