import os
import pandas as pd
import glob
from datetime import datetime
import multiprocessing

# ==========================================
# 战法名称：【连跌回调捕捉战法】
# 逻辑说明：
# 1. 核心假设：优质品种（ETF）连续下跌后存在均值回归的需求。
# 2. 筛选条件：最近一周内出现 2 连跌或 3 连跌的品种（左侧预警）。
# 3. 统计支持：通过历史数据计算 2/3/4/5 连跌后，“次日”上涨的胜率。
# 4. 买卖要领：当近期出现信号，且历史同类信号胜率 > 55% 时，视为高胜率机会。
# ==========================================

def analyze_single_file(file_path, etf_names):
    """
    单个文件的处理函数，将被并行调用
    """
    try:
        # 1. 加载数据
        df = pd.read_csv(file_path)
        if df.empty or len(df) < 10:
            return None
            
        df['日期'] = pd.to_datetime(df['日期'])
        df = df.sort_values('日期').reset_index(drop=True)
        
        symbol = os.path.basename(file_path).split('.')[0]
        name = etf_names.get(symbol, "未知")
        
        # 2. 识别连跌逻辑 (收盘价低于前日收盘)
        df['is_down'] = df['涨跌幅'] < 0
        
        # 计算连续下跌天数
        def count_consecutive(series):
            res = []
            count = 0
            for val in series:
                if val: count += 1
                else: count = 0
                res.append(count)
            return res
        
        df['down_count'] = count_consecutive(df['is_down'])
        
        # 3. 最近一周信号统计 (最近5个交易日)
        last_5 = df.tail(5)
        recent_2d = 1 if any(last_5['down_count'] == 2) else 0
        recent_3d = 1 if any(last_5['down_count'] == 3) else 0
        
        # 4. 历史回测规律统计
        hist_stats = []
        for d in [2, 3, 4, 5]:
            # 找到历史所有连跌 d 天后的下一天索引
            target_indices = df[df['down_count'] == d].index + 1
            # 确保索引不越界
            target_indices = [i for i in target_indices if i < len(df)]
            
            if not target_indices:
                hist_stats.extend([0, 0])
                continue
            
            next_day_returns = df.iloc[target_indices]['涨跌幅']
            win_rate = (next_day_returns > 0).mean() # 胜率
            avg_ret = next_day_returns.mean()        # 平均涨幅
            hist_stats.extend([round(win_rate * 100, 2), round(avg_ret, 2)])
            
        return [symbol, name, recent_2d, recent_3d] + hist_stats

    except Exception as e:
        print(f"解析 {file_path} 失败: {e}")
        return None

def main():
    # A. 获取 ETF 名称映射 (处理本地 CSV)
    etf_names = {}
    mapping_file = 'ETF列表.xlsx'
    if os.path.exists(mapping_file):
        mapping_df = pd.read_csv(mapping_file)
        # 强制转换代码为6位字符串防止丢失前导0
        etf_names = dict(zip(mapping_df['证券代码'].astype(str).str.zfill(6), mapping_df['证券简称']))

    # B. 准备待扫描的文件列表
    data_dir = 'fund_data'
    csv_files = glob.glob(f'{data_dir}/*.csv')
    
    if not csv_files:
        print("未找到数据文件，请检查 fund_data 目录")
        return

    # C. 并行计算执行
    # 使用所有可用 CPU 核心
    cpus = multiprocessing.cpu_count()
    print(f"启动并行分析，核心数: {cpus}，总任务数: {len(csv_files)}")
    
    with multiprocessing.Pool(processes=cpus) as pool:
        # 使用 starmap 传递多参数
        results = pool.starmap(analyze_single_file, [(f, etf_names) for f in csv_files])

    # D. 结果汇总与排序
    results = [r for r in results if r is not None]
    cols = ['代码', '名称', '近1周2连跌', '近1周3连跌', 
            '2连跌胜率%', '2连跌均涨', '3连跌胜率%', '3连跌均涨', 
            '4连跌胜率%', '4连跌均涨', '5连跌胜率%', '5连跌均涨']
    
    res_df = pd.DataFrame(results, columns=cols)
    
    # 排序逻辑：优先展示最近出现 3 连跌和 2 连跌的品种
    res_df = res_df.sort_values(by=['近1周3连跌', '近1周2连跌', '3连跌胜率%'], ascending=False)

    # E. 保存结果
    bj_time = datetime.now().strftime('%Y%m%d_%H%M%S')
    folder = datetime.now().strftime('%Y%m')
    if not os.path.exists(folder): os.makedirs(folder)
    
    file_path = f"{folder}/etf_analysis_logic_{bj_time}.csv"
    res_df.to_csv(file_path, index=False, encoding='utf_8_sig')
    print(f"分析完成，输出至: {file_path}")

if __name__ == '__main__':
    main()
