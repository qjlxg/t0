import os
import pandas as pd
import glob
from datetime import datetime
import multiprocessing

# 战法名称：连跌回调捕捉战法
# 操作要领：
# 1. 筛选连续下跌2-3天的品种，寻找短期超跌机会。
# 2. 参考历史同类连跌后的次日上涨概率（胜率），胜率越高，信号越可靠。
# 3. 结果按最近一周是否有信号排序，优先展示。

def analyze_etf(file_path, etf_names):
    try:
        df = pd.read_csv(file_path, encoding='utf-8')
        # 兼容两种日期格式
        df['日期'] = pd.to_datetime(df['日期'])
        df = df.sort_values('日期').reset_index(drop=True)
        
        symbol = os.path.basename(file_path).replace('.csv', '')
        name = etf_names.get(symbol, "未知")
        
        # 计算涨跌标志 (收盘价低于前一日收盘价记为下跌)
        df['is_down'] = df['收盘'].diff() < 0
        
        # 计算连续下跌天数
        def get_consecutive_downs(series):
            downs = []
            count = 0
            for val in series:
                if val:
                    count += 1
                else:
                    count = 0
                downs.append(count)
            return downs

        df['consecutive_downs'] = get_consecutive_downs(df['is_down'])
        
        # 1. 统计最近信号
        last_week = df.tail(5)
        recent_2d = 1 if any(last_week['consecutive_downs'] == 2) else 0
        recent_3d = 1 if any(last_week['consecutive_downs'] == 3) else 0
        
        # 2. 统计历史规律
        stats = []
        for d in [2, 3, 4, 5]:
            # 找到所有连跌d天的索引
            indices = df[df['consecutive_downs'] == d].index
            # 过滤掉最后一天（无法计算次日）
            indices = [i for i in indices if i + 1 < len(df)]
            
            if not indices:
                stats.extend([0, 0]) # 胜率, 平均涨幅
                continue
            
            next_day_changes = df.loc[[i + 1 for i in indices], '涨跌幅']
            win_rate = (next_day_changes > 0).mean()
            avg_change = next_day_changes.mean()
            stats.extend([round(win_rate, 4), round(avg_change, 4)])

        return [symbol, name, recent_2d, recent_3d] + stats
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return None

def main():
    # 读取ETF列表映射
    etf_list_file = 'ETF列表.xlsx - Sheet1.csv'
    etf_names = {}
    if os.path.exists(etf_list_file):
        mapping_df = pd.read_csv(etf_list_file)
        etf_names = dict(zip(mapping_df['证券代码'].astype(str), mapping_df['证券简称']))

    # 并行处理所有CSV
    files = glob.glob('fund_data/*.csv')
    with multiprocessing.Pool() as pool:
        results = pool.starmap(analyze_etf, [(f, etf_names) for f in files])

    # 过滤空结果
    results = [r for r in results if r is not None]
    
    # 构建DataFrame
    columns = ['代码', '名称', '近一周连跌2天', '近一周连跌3天', 
               '2连跌后胜率', '2连跌后均涨幅', '3连跌后胜率', '3连跌后均涨幅',
               '4连跌后胜率', '4连跌后均涨幅', '5连跌后胜率', '5连跌后均涨幅']
    
    res_df = pd.DataFrame(results, columns=columns)
    
    # 排序：近一周有信号的排在前面
    res_df = res_df.sort_values(by=['近一周连跌3天', '近一周连跌2天'], ascending=False)
    
    # 保存结果
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    dir_path = datetime.now().strftime('%Y%m')
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
        
    output_name = f"{dir_path}/etf_analysis_logic_{timestamp}.csv"
    res_df.to_csv(output_name, index=False, encoding='utf_8_sig')
    print(f"Analysis saved to {output_name}")

if __name__ == "__main__":
    main()
