# main.py
from twse_scraper import fetch_twse_data
from otc_scraper import fetch_otc_data
from utils import save_to_csv, get_today_str
import os
import pandas as pd
from datetime import datetime, timedelta

def crawl_data(date_str):
    # 建立 data 資料夾，如果已存在就跳過
    os.makedirs("data", exist_ok=True)

    # 取得今天日期字串 (YYYY-MM-DD)
    #date_str = "2025-06-05"
    #date_str = get_today_str()

    # 抓取證交所資料
    twse_fp = f"data/{date_str}_twse.csv"
    if not os.path.exists(twse_fp):
    	twse_df = fetch_twse_data(date_str)
    	save_to_csv(twse_df, twse_fp)

    # 抓取櫃買中心資料
    otc_fp = f"data/{date_str}_otc.csv"
    if not os.path.exists(otc_fp):
    	otc_df = fetch_otc_data(date_str)
    	save_to_csv(otc_df, otc_fp)

    print("資料抓取完成\n")

def merge_data(date_str):
    twse_fp = f"data/{date_str}_twse.csv"
    otc_fp = f"data/{date_str}_otc.csv"

    twse = pd.read_csv(twse_fp)
    otc = pd.read_csv(otc_fp)

    print("TWSE 欄位：", twse.columns.tolist())
    print("OTC 原始欄位：", otc.columns.tolist())

    # 欄位對應表（otc 欄位 -> twse 欄位）
    column_mapping = {
        '代號': '證券代號',
        '名稱': '證券名稱',
        '收盤': '收盤價',
        '漲跌幅(%)': '漲跌幅(%)',
        '開盤': '開盤價',
        '最高': '最高價',
        '最低': '最低價',
        '成交股數': '成交股數',
        '成交金額': '成交金額',
        '昨收': '昨收價',
        # 其他需要比對的欄位可以繼續加
    }

    # OTC 欄位改名對應 TWSE
    otc_renamed = otc.rename(columns={k: v for k, v in column_mapping.items() if k in otc.columns})

    # 只保留 TWSE 中有的欄位（忽略 OTC 專屬欄）
    common_columns = [col for col in twse.columns if col in otc_renamed.columns]
    twse_clean = twse[common_columns]
    otc_clean = otc_renamed[common_columns]

    # 合併資料
    merged = pd.concat([twse_clean, otc_clean], ignore_index=True)

    print(merged.columns) # ['證券代號', '證券名稱', '成交股數', '成交筆數', '開盤價', '最高價', '最低價', '收盤價', '漲跌價差']
    merged['成交股數'] = pd.to_numeric(merged['成交股數'].astype(str).str.replace(',', ''), errors='coerce')
    merged.insert(2, '成交張數', (merged['成交股數'] / 1000).round(0).astype(int))

    #print(merged.dtypes)
    for col in ["開盤價", "最高價", "最低價", "收盤價"]:
        merged[col] = pd.to_numeric(
            merged[col].astype(str).str.replace(',', ''),
            errors='coerce'
        )
    #print(merged.dtypes)

    merged["當日漲跌幅(%)"] = 100 * (merged["收盤價"] - merged["開盤價"]) / merged["開盤價"]

    merged = merged[['證券代號', '證券名稱', '成交張數', '開盤價', '最高價', '最低價', '收盤價', '當日漲跌幅(%)']]
    print(merged.dtypes)

    # 儲存
    output_fp = f"data/{date_str}_merged.csv"
    merged.to_csv(output_fp, index=False, encoding='utf-8-sig')
    print(f"已輸出合併後檔案：{output_fp}\n")
'''
def filter_data(date_str):
    merged_fp = f"data/{date_str}_merged.csv"
    merged = pd.read_csv(merged_fp)
    filterd = merged[(merged["成交張數"]>1000)&(merged["當日漲跌幅(%)"]>3)]

    # 儲存
    output_fp = f"data/{date_str}_filterd.csv"
    filterd.to_csv(output_fp, index=False, encoding='utf-8-sig')
    print(f"已輸出合併後檔案：{output_fp}\n")

    # 便宜股
    cheaps = filterd[filterd["收盤價"]<50]
    output_fp = f"data/{date_str}_cheaps.csv"
    cheaps.to_csv(output_fp, index=False, encoding='utf-8-sig')
    print(f"已輸出合併後檔案：{output_fp}\n")
'''
def filter_data(date_str):
    merged_fp = f"data/{date_str}_merged.csv"
    merged = pd.read_csv(merged_fp)

    # 取得昨日日期字串
    date = datetime.strptime(date_str, "%Y-%m-%d")
    prev_date_str = (date - timedelta(days=1)).strftime("%Y-%m-%d")
    prev_fp = f"data/{prev_date_str}_merged.csv"
    #prev_fp = f"data/2025-06-06_merged.csv"

    if os.path.exists(prev_fp):
        prev = pd.read_csv(prev_fp)
        prev = prev[["證券代號", "成交張數"]].rename(columns={"成交張數": "昨日成交張數"})
        merged = merged.merge(prev, on="證券代號", how="left")
        merged["昨日成交張數"] = pd.to_numeric(merged["昨日成交張數"], errors="coerce").fillna(0)
        merged["爆量比"] = merged["成交張數"] / merged["昨日成交張數"]
        # 加入三條件過濾
        filterd = merged[
            (merged["成交張數"] > 1000) &
            (merged["當日漲跌幅(%)"] > 3) &
            (merged["爆量比"] > 1.5)
        ]
    else:
        print(f"⚠ 找不到昨日檔案：{prev_fp}，略過昨日成交量比對")
        filterd = merged[
            (merged["成交張數"] > 1000) &
            (merged["當日漲跌幅(%)"] > 3)
        ]

    # 儲存
    output_fp = f"data/{date_str}_filterd.csv"
    filterd.to_csv(output_fp, index=False, encoding='utf-8-sig')
    print(f"已輸出篩選檔案：{output_fp}")

    # 便宜股
    cheaps = filterd[filterd["收盤價"] < 50]
    output_fp = f"data/{date_str}_cheaps.csv"
    cheaps.to_csv(output_fp, index=False, encoding='utf-8-sig')
    print(f"已輸出便宜股檔案：{output_fp}")

if __name__ == "__main__":
    date_str = "2025-05-08"
    crawl_data(date_str)
    merge_data(date_str)
    filter_data(date_str)
