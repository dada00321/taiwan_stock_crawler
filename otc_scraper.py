# otc_scraper.py
import os
import time
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
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
                if '代號' in line and '名稱' in line:
                    header_idx = idx
                    df = pd.read_csv(filepath, encoding=enc, skiprows=header_idx)
                    df = df.loc[~df.iloc[:, 0].astype(str).str.contains("備註|漲跌|當證券|除境外|本統計", na=False)]
                    df = df.loc[:, ~df.columns.str.contains("Unnamed")]
                    return df
        except Exception:
            continue
    raise Exception("CSV檔案無法正確解析，請檢查格式")

def fetch_otc_data(date_str):
    date = datetime.strptime(date_str, "%Y-%m-%d")
    roc_year = date.year - 1911
    #ymd = f"{date.year}{date.month:02}{date.day:02}"

    download_dir = "./data"
    os.makedirs(download_dir, exist_ok=True)
    target_filename = f"RSTA3104_{roc_year}{date.month:02}{date.day:02}.csv"
    target_filepath = os.path.join(download_dir, target_filename)

    if not os.path.exists(target_filepath):
        url = "https://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote.php?l=zh-tw"
        options = Options()
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        prefs = {"download.default_directory": os.path.abspath(download_dir)}
        options.add_experimental_option("prefs", prefs)

        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.get(url)

        time.sleep(3)
        # 點開日曆
        date_input = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, 'input[name="date"].date'))
        )
        date_input.click()
        time.sleep(1)

        # 選擇年份與月份
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "yearselect"))
        )
        Select(driver.find_element(By.CLASS_NAME, "yearselect")).select_by_value(str(date.year))
        Select(driver.find_element(By.CLASS_NAME, "monthselect")).select_by_value(str(date.month - 1))

        # 點選對應的日期（不補0）
        day_cell = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, f"//td[@class='available' and text()='{date.day}']"))
        )
        day_cell.click()

        # 點擊 CSV 下載按鈕
        csv_btn = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, 'button.response[data-format="csv"]'))
        )
        csv_btn.click()

        for _ in range(30):
            if os.path.exists(target_filepath) and not os.path.exists(target_filepath + ".crdownload"):
                break
            time.sleep(1)
        else:
            driver.quit()
            raise Exception("CSV 檔案下載失敗")
        driver.quit()

    df = smart_read_csv_auto_encoding(target_filepath)
    df['代號'] = df['代號'].astype(str).str.strip()
    df = df[df['代號'].str.match(r'^\d{4}$')]
    df['收盤'] = pd.to_numeric(df['收盤'].astype(str).str.replace(',', ''), errors='coerce')
    df['漲跌'] = pd.to_numeric(df['漲跌'].astype(str).str.replace(',', '').replace('--', '0').str.replace('+', ''), errors='coerce')
    df['昨收'] = df['收盤'] - df['漲跌']
    df['漲跌幅(%)'] = (df['漲跌'] / df['昨收'] * 100).round(2)

    os.remove(target_filepath)

    return df#[['代號', '名稱', '收盤', '漲跌', '漲跌幅(%)']]
