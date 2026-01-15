import os
import pandas as pd
import glob
from datetime import datetime
import multiprocessing

# ==========================================
# 战法名称：【连跌回调捕捉战法】
# 备注：
# 1. 扫描 fund_data 下所有历史 CSV 格式数据。
# 2. 统计最近一周内触发 2 连跌、3 连跌的标的（前置显示）。
# 3. 统计历史规律：计算连续下跌 2,3,4,5 天后，次日的上涨概率（胜率）与平均涨幅。
# 4. 操作要领：寻找历史胜率高且近期触发连跌的品种，博弈超跌反弹。
# ==========================================

def analyze_single_file(file_path, etf_names):
    try:
        # 加载数据
        df = pd.read_csv(file_path)
        if df.empty or len(df) < 5:
            return None
            
        df['日期'] = pd.to_datetime(df['日期'])
        df = df.sort_values('日期').reset_index(drop=True)
        
        # 提取并标准化代码 (补齐6位)
        raw_symbol = os.path.basename(file_path).split('.')[0]
        symbol = raw_symbol.zfill(6) 
        name = etf_names.get(symbol, "未知")
        
        # 识别下跌逻辑 (当日收盘价 < 前日收盘价)
        df['is_down'] = df['收盘'].diff() < 0
        
        # 计算连续下跌天数 (向量化逻辑)
        def count_consecutive_downs(series):
            counts = []
            cur = 0
            for is_down in series:
                if is_down: cur += 1
                else: cur = 0
                counts.append(cur)
            return counts
        
        df['down_count'] = count_consecutive_downs(df['is_down'])
        
        # 统计最近一周信号 (最近5个交易日)
        last_5 = df.tail(5)
        recent_2d = 1 if any(last_5['down_count'] == 2) else 0
        recent_3d = 1 if any(last_5['down_count'] == 3) else 0
        
        # 历史规律统计
        hist_stats = []
        for d in [2, 3, 4, 5]:
            # 获取连跌d天后的“下一天”索引
            target_idx = df[df['down_count'] == d].index + 1
            # 过滤越界索引
            target_idx = [i for i in target_idx if i < len(df)]
            
            if not target_idx:
                hist_stats.extend([0, 0])
                continue
            
            # 计算这些次日的涨跌幅
            # (当前收盘 - 前日收盘) / 前日收盘
            next_days = df.iloc[target_idx]
            prev_days = df.iloc[[i-1 for i in target_idx]]
            
            changes = (next_days['收盘'].values - prev_days['收盘'].values) / prev_days['收盘'].values * 100
            
            win_rate = (changes > 0).mean()
            avg_change = changes.mean()
            hist_stats.extend([round(win_rate * 100, 2), round(avg_change, 2)])
            
        return [symbol, name, recent_2d, recent_3d] + hist_stats

    except Exception:
        return None

def main():
    # A. 加载 ETF 名称映射 (直接读取 .xlsx)
    etf_names = {}
    mapping_file = 'ETF列表.xlsx'
    if os.path.exists(mapping_file):
        try:
            # 强制将证券代码列作为字符串读取，防止前导0丢失
            m_df = pd.read_excel(mapping_file, dtype={'证券代码': str})
            m_df['证券代码'] = m_df['证券代码'].str.zfill(6)
            etf_names = dict(zip(m_df['证券代码'], m_df['证券简称']))
        except Exception as e:
            print(f"读取 Excel 失败: {e}，请确保安装了 openpyxl")

    # B. 准备待扫描文件
    data_dir = 'fund_data'
    csv_files = glob.glob(f'{data_dir}/*.csv')
    
    if not csv_files:
        print("未找到数据文件，请检查 fund_data 目录。")
        return

    # C. 并行计算 (并行核心数 = CPU核心数)
    with multiprocessing.Pool(multiprocessing.cpu_count()) as pool:
        results = pool.starmap(analyze_single_file, [(f, etf_names) for f in csv_files])

    # D. 结果整理与排序
    results = [r for r in results if r is not None]
    cols = ['代码', '名称', '近1周2连跌', '近1周3连跌', 
            '2连跌胜率%', '2连跌均涨', '3连跌胜率%', '3连跌均涨', 
            '4连跌胜率%', '4连跌均涨', '5连跌胜率%', '5连跌均涨']
    
    res_df = pd.DataFrame(results, columns=cols)
    # 优先展示近期有信号的，再按胜率降序
    res_df = res_df.sort_values(by=['近1周3连跌', '近1周2连跌', '3连跌胜率%'], ascending=False)

    # E. 保存结果到年月目录
    now = datetime.now()
    folder = now.strftime('%Y%m')
    if not os.path.exists(folder):
        os.makedirs(folder)
    
    timestamp = now.strftime('%Y%m%d_%H%M%S')
    output_path = f"{folder}/etf_analysis_logic_{timestamp}.csv"
    
    # 使用 utf_8_sig 解决 Excel 打开中文乱码
    res_df.to_csv(output_path, index=False, encoding='utf_8_sig')
    print(f"分析完成！结果已存至: {output_path}")

if __name__ == '__main__':
    main()
