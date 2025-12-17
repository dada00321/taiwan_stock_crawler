import re
import requests
import pandas as pd

def fetch_twse_data(date_str: str) -> pd.DataFrame:
    """
    Fetch TWSE daily close data (MI_INDEX, type=ALLBUT0999) for a given date.

    Args:
        date_str: "YYYY-MM-DD", e.g. "2025-12-17"

    Returns:
        pd.DataFrame with at least one table that contains '證券代號' column.
        Adds a 'date' column (YYYY-MM-DD).
    """
    # 1) normalize date
    yyyymmdd = date_str.replace("-", "")
    if not re.fullmatch(r"\d{8}", yyyymmdd):
        raise ValueError(f"Invalid date_str: {date_str}. Expect 'YYYY-MM-DD'.")

    url = "https://www.twse.com.tw/exchangeReport/MI_INDEX"
    params = {
        "response": "json",
        "date": yyyymmdd,
        "type": "ALLBUT0999",  # all but warrants / CBBC etc.
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; twse-scraper/1.0; +https://www.twse.com.tw/)"
    }

    r = requests.get(url, params=params, headers=headers, timeout=30)
    r.raise_for_status()
    payload = r.json()

    # 2) basic validation
    tables = payload.get("tables", [])
    if not tables:
        # TWSE sometimes returns empty tables on holidays/non-trading days
        raise ValueError(f"No tables returned for {date_str}. payload keys={list(payload.keys())}")

    # 3) helper: clean stock id like '="020039"' -> '020039'
    def clean_stock_id(x):
        if x is None:
            return None
        s = str(x).strip()
        # remove Excel-style ="...."
        if s.startswith('="') and s.endswith('"'):
            s = s[2:-1]
        # keep only digits
        m = re.search(r"\d+", s)
        return m.group(0) if m else s

    # 4) pick the table that looks like "每日收盤行情" and contains "證券代號"
    chosen = None
    for t in tables:
        fields = t.get("fields") or []
        title = (t.get("title") or "")
        if ("證券代號" in fields) and (("每日收盤行情" in title) or ("收盤" in title)):
            chosen = t
            break
    if chosen is None:
        # fallback: first table that contains 證券代號
        for t in tables:
            fields = t.get("fields") or []
            if "證券代號" in fields:
                chosen = t
                break

    if chosen is None:
        raise ValueError(f"Cannot find a table with '證券代號' for {date_str}.")

    fields = chosen.get("fields") or []
    data = chosen.get("data") or []
    if not data:
        raise ValueError(f"Table found but no data rows for {date_str}. title={chosen.get('title')}")

    df = pd.DataFrame(data, columns=fields)

    # 5) standardize stock id and (optionally) keep only 4-digit stock codes
    if "證券代號" in df.columns:
        df["證券代號_raw"] = df["證券代號"]
        df["證券代號"] = df["證券代號"].map(clean_stock_id)

        # 只保留「代號=四位數」(上市一般股票常用)
        # df = df[df["證券代號"].astype(str).str.fullmatch(r"\d{4}", na=False)].reset_index(drop=True)

    # 6) add date column
    df.insert(0, "date", date_str)
    
    # 7) remove invalid (without open price) stocks
    df = df[df["收盤價"] != "--"]
    
    df["證券代號"] = df["證券代號"].astype(str)
    return df

# Example:
# df = fetch_twse_data("2025-12-17")
# print(df.head())
