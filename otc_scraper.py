import datetime as dt
import time
import requests
import pandas as pd

def _to_roc_date_str(date_str: str) -> str:
    d = dt.datetime.strptime(date_str, "%Y-%m-%d").date()
    roc_year = d.year - 1911
    if roc_year <= 0:
        raise ValueError(f"Invalid ROC year for date_str={date_str}")
    return f"{roc_year:03d}/{d.month:02d}/{d.day:02d}"

def fetch_otc_data(date_str: str, timeout: int = 20) -> pd.DataFrame:
    roc_date = _to_roc_date_str(date_str)

    url = "https://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_result.php"
    params = {
        "l": "zh-tw",
        "d": roc_date,
        "_": int(time.time() * 1000),  # cache buster
    }
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www.tpex.org.tw/zh-tw/mainboard/trading/info/pricing.html",
        "Accept": "application/json,text/plain,*/*",
        "X-Requested-With": "XMLHttpRequest",
    }
    r = requests.get(url, params=params, headers=headers, timeout=timeout)
    r.raise_for_status()

    try:
        payload = r.json()
    except Exception:
        head = (r.text or "")[:500]
        raise ValueError(f"Response is not JSON. Head:\n{head}")

    # ---- 1) 舊格式：aaData ----
    if isinstance(payload, dict) and payload.get("aaData"):
        rows = payload["aaData"]
        # 沒有 fields 就用常見欄位（依實際長度截斷）
        default_cols = [
            "代號","名稱","收盤","漲跌","開盤","最高","最低","均價",
            "成交股數","成交金額","成交筆數","最後買價","最後買量",
            "最後賣價","最後賣量","發行股數","次日參考價","次日漲停價","次日跌停價"
        ]
        df = pd.DataFrame(rows, columns=default_cols[:len(rows[0])])
        if "代號" in df.columns:
            df["代號"] = df["代號"].astype(str).str.zfill(4)
        return df

    # ---- 2) 新格式：tables ----
    if isinstance(payload, dict) and payload.get("tables"):
        tables = payload["tables"]
        if not tables:
            raise ValueError(f"No tables returned for {date_str} (ROC={roc_date}). stat={payload.get('stat')}")

        # 多數情況 tables[0] 就是每日收盤行情；但保險起見：找 data 最多的那張
        best = max(tables, key=lambda t: len(t.get("data") or []))
        fields = best.get("fields") or []
        rows = best.get("data") or []

        if not rows:
            raise ValueError(
                f"tables present but empty data for {date_str} (ROC={roc_date}). "
                f"stat={payload.get('stat')}, date={payload.get('date')}"
            )

        # fields 若是 dict（有 name/title/key），做一次 normalize
        if fields and isinstance(fields[0], dict):
            # 常見 key：'name'/'title'/'field'
            norm = []
            for f in fields:
                norm.append(f.get("name") or f.get("title") or f.get("field") or "")
            fields = norm

        # 欄位數跟資料長度可能不同，這裡自動截斷/補齊
        col_n = len(rows[0])
        if len(fields) >= col_n:
            cols = fields[:col_n]
        else:
            cols = fields + [f"col_{i}" for i in range(len(fields), col_n)]

        df = pd.DataFrame(rows, columns=cols)

        # 代號欄位名稱可能是「代號」或「股票代號」等，做個保守處理
        code_col = None
        for c in df.columns:
            if "代號" in str(c):
                code_col = c
                break
        if code_col:
            df[code_col] = df[code_col].astype(str).str.zfill(4)
        
        # 只篩選四位數的上櫃股票 (不包含上櫃 ETF)
        # df = df[df["代號"].astype(str).str.fullmatch(r"\d{4}")]
        
        # 排除沒有收盤價的標的
        df = df[df["收盤"] != "---"]
        
        df["代號"] = df["代號"].astype(str)
        
        return df

    raise ValueError(
        f"Unrecognized payload format for {date_str} (ROC={roc_date}). "
        f"payload keys={list(payload.keys()) if isinstance(payload, dict) else type(payload)}"
    )

'''
# usage
if __name__ == "__main__":
    df = fetch_otc_data("2025-12-17")
    print(df.head())
    print("rows:", len(df))
    print("cols:", list(df.columns))
'''