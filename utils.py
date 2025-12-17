# utils.py
import os
import pandas as pd
from datetime import datetime

# 取得今天日期字串
def get_today_str():
    return datetime.today().strftime('%Y-%m-%d')

# 儲存 DataFrame 為 CSV
def save_to_csv(df, filename):
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    df.to_csv(filename, index=False, encoding='utf-8-sig')
