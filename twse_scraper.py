
import os
import time
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

def smart_read_csv_auto_encoding(filepath):
    encodings = ['utf-8-sig', 'cp950', 'big5']
    for enc in encodings:
        try:
            with open(filepath, 'r', encoding=enc) as f:
                lines = f.readlines()
            for idx, line in enumerate(lines):
                if '證券代號' in line and '證券名稱' in line:
                    header_idx = idx
                    df = pd.read_csv(filepath, encoding=enc, skiprows=header_idx)
                    df = df.loc[~df.iloc[:, 0].astype(str).str.contains("備註|漲跌|當證券|除境外|本統計", na=False)]
                    df = df.loc[:, ~df.columns.str.contains("Unnamed")]
                    return df
        except Exception:
            continue
    raise Exception("無法解析 CSV 檔案")

def fetch_twse_data(date_str):
    date = datetime.strptime(date_str, "%Y-%m-%d")
    y, m, d = date.year, date.month, date.day
    ymd_str = f"{y}{m:02}{d:02}"

    download_dir = "./data"
    os.makedirs(download_dir, exist_ok=True)
    target_filename = f"MI_INDEX_ALLBUT0999_{ymd_str}.csv"
    target_filepath = os.path.join(download_dir, target_filename)

    if not os.path.exists(target_filepath):
        url = "https://www.twse.com.tw/zh/page/trading/exchange/MI_INDEX.html"
        options = Options()
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        prefs = {"download.default_directory": os.path.abspath(download_dir)}
        options.add_experimental_option("prefs", prefs)

        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.get(url)

        # 設定年、月、日
        Select(driver.find_element(By.NAME, "yy")).select_by_value(str(y))
        Select(driver.find_element(By.NAME, "mm")).select_by_value(str(m))
        Select(driver.find_element(By.NAME, "dd")).select_by_value(str(d))

                # 選擇分類「全部(不含權證...)」
        Select(driver.find_element(By.NAME, "type")).select_by_value("ALLBUT0999")

        # 點擊查詢
        driver.find_element(By.CSS_SELECTOR, "button.search").click()

        # 點擊 CSV 下載按鈕
        csv_btn = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "button.csv"))
        )
        csv_btn.click()

        # 等待檔案完成下載
        for _ in range(30):
            if os.path.exists(target_filepath) and not os.path.exists(target_filepath + ".crdownload"):
                break
            time.sleep(1)
        else:
            driver.quit()
            raise Exception("CSV 下載逾時")
        driver.quit()

    df = smart_read_csv_auto_encoding(target_filepath)
    df = df.loc[~df.iloc[:, 0].astype(str).str.contains("備註|漲跌|當證券|除境外|本統計", na=False)]
    df = df.loc[:, ~df.columns.str.contains("Unnamed")]

    os.remove(target_filepath)

    return df
