"""
MoatScan - 台股全市場篩選腳本
每天收盤後由 GitHub Actions 自動執行
輸出：data/tw_results.json
"""

import yfinance as yf
import pandas as pd
import json
import requests
import time
import math
from datetime import datetime

# ── 取得台股全部上市/上櫃代號 ─────────────────────────────────────────────
def get_tw_tickers():
    tickers = []

    # 上市 (TSE) - TWSE 開放資料
    try:
        url = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"
        r = requests.get(url, timeout=15)
        data = r.json()
        for row in data:
            code = row.get("Code", "")
            if code.isdigit() and len(code) == 4:
                tickers.append(f"{code}.TW")
    except Exception as e:
        print(f"[警告] TSE 清單抓取失敗: {e}")

    # 上櫃 (OTC) - TPEx 開放資料
    try:
        url = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes"
        r = requests.get(url, timeout=15)
        data = r.json()
        for row in data:
            code = row.get("SecuritiesCompanyCode", "")
            if code.isdigit() and len(code) == 4:
                tickers.append(f"{code}.TWO")
    except Exception as e:
        print(f"[警告] OTC 清單抓取失敗: {e}")

    tickers = list(set(tickers))
    print(f"[INFO] 共取得 {len(tickers)} 支台股代號")
    return tickers


# ── 單支股票評分 ───────────────────────────────────────────────────────────
def score_ticker(ticker_str, retries=2):
    for attempt in range(retries):
        try:
            tk = yf.Ticker(ticker_str)
            info = tk.info

            # 基本資訊
            name = info.get("longName") or info.get("shortName") or ticker_str
            sector = info.get("sector", "未知")
            industry = info.get("industry", "未知")
            mkt_cap = info.get("marketCap", 0)
            price = info.get("currentPrice") or info.get("regularMarketPrice") or 0
            summary = (info.get("longBusinessSummary") or "")[:300]

            # 若市值 < 50億台幣（~1.5億美元）跳過，避免微型股雜訊
            if mkt_cap and mkt_cap < 150_000_000:
                return None

            scores = {}
            details = {}

            # ── 財務報表 ────────────────────────────────────────────────
            fin = tk.financials          # 損益表 (年, index=科目, columns=日期)
            cf  = tk.cashflow            # 現金流量表
            bs  = tk.balance_sheet       # 資產負債表

            # 轉為「年份由新到舊」的 list
            def col_values(df, *keys):
                for key in keys:
                    if df is not None and not df.empty:
                        for idx in df.index:
                            if key.lower() in str(idx).lower():
                                return df.loc[idx].dropna().tolist()
                return []

            # 1. EPS ─────────────────────────────────────────────────────
            eps_vals = col_values(fin, "Basic EPS", "Diluted EPS", "EPS")
            if not eps_vals:
                # fallback: trailingEPS from info
                te = info.get("trailingEps")
                eps_vals = [te] if te else []

            eps_pass = False
            if len(eps_vals) >= 2:
                positive_count = sum(1 for v in eps_vals if v and v > 0)
                # 趨勢：最新 >= 最舊的 80%
                trend_ok = eps_vals[0] >= eps_vals[-1] * 0.8 if eps_vals[-1] and eps_vals[-1] > 0 else False
                eps_pass = (positive_count >= len(eps_vals) * 0.75) and trend_ok
            scores["eps"] = 1 if eps_pass else 0
            details["eps"] = f"{len(eps_vals)}年資料，最新${round(eps_vals[0],2) if eps_vals else 'N/A'}"

            # 2. FCF ──────────────────────────────────────────────────────
            op_cf   = col_values(cf, "Operating Cash Flow", "Cash From Operations")
            cap_ex  = col_values(cf, "Capital Expenditure", "Purchase Of PPE")
            fcf_vals = []
            for i in range(min(len(op_cf), len(cap_ex))):
                cap = cap_ex[i] if cap_ex[i] else 0
                fcf_vals.append(op_cf[i] - abs(cap))

            fcf_pass = False
            if len(fcf_vals) >= 2:
                positive_count = sum(1 for v in fcf_vals if v > 0)
                fcf_pass = positive_count >= len(fcf_vals) * 0.8
            scores["fcf"] = 1 if fcf_pass else 0
            details["fcf"] = f"{sum(1 for v in fcf_vals if v>0)}/{len(fcf_vals)}年為正"

            # 3. ROIC > 10% ───────────────────────────────────────────────
            ebit_vals  = col_values(fin, "EBIT", "Operating Income")
            tax_rate   = info.get("effectiveTaxRate", 0.2)
            total_eq   = col_values(bs, "Stockholders Equity", "Total Equity")
            total_debt = col_values(bs, "Total Debt", "Long Term Debt")

            roic_vals = []
            for i in range(min(len(ebit_vals), len(total_eq), len(total_debt))):
                nopat = ebit_vals[i] * (1 - tax_rate)
                invested = (total_eq[i] or 0) + (total_debt[i] or 0)
                if invested > 0:
                    roic_vals.append(nopat / invested)

            roic_pass = False
            if roic_vals:
                above_10 = sum(1 for r in roic_vals if r > 0.10)
                roic_pass = above_10 >= len(roic_vals) * 0.7
            scores["roic"] = 1 if roic_pass else 0
            details["roic"] = f"最新{round(roic_vals[0]*100,1) if roic_vals else 'N/A'}%"

            # 4. D/E < 0.5 ────────────────────────────────────────────────
            de = info.get("debtToEquity")
            if de is None and total_eq and total_debt:
                de = (total_debt[0] / total_eq[0]) * 100 if total_eq[0] else None
            # yfinance debtToEquity 以百分比表示 (e.g. 45 = 0.45)
            de_ratio = de / 100 if de is not None else None
            de_pass  = de_ratio is not None and de_ratio < 0.5
            scores["de"] = 1 if de_pass else 0
            details["de"] = f"D/E={round(de_ratio,2) if de_ratio is not None else 'N/A'}"

            # 5. Net Margin > 20% ─────────────────────────────────────────
            rev_vals    = col_values(fin, "Total Revenue", "Revenue")
            ni_vals     = col_values(fin, "Net Income")
            nm_vals = []
            for i in range(min(len(rev_vals), len(ni_vals))):
                if rev_vals[i] and rev_vals[i] != 0:
                    nm_vals.append(ni_vals[i] / rev_vals[i])

            nm_pass = False
            if nm_vals:
                above_20 = sum(1 for m in nm_vals if m > 0.20)
                nm_pass  = above_20 >= len(nm_vals) * 0.6
            scores["netmargin"] = 1 if nm_pass else 0
            details["netmargin"] = f"最新{round(nm_vals[0]*100,1) if nm_vals else 'N/A'}%"

            # 6. 配息 ─────────────────────────────────────────────────────
            div_rate  = info.get("dividendRate") or 0
            div_yield = info.get("dividendYield") or 0
            div_hist  = tk.dividends
            div_years = 0
            if div_hist is not None and not div_hist.empty:
                div_years = div_hist.resample("Y").sum().astype(bool).sum()
            div_pass = div_years >= 3 or (div_rate > 0 and div_yield > 0)
            scores["dividend"] = 1 if div_pass else 0
            details["dividend"] = f"近{div_years}年有配息"

            # ── 總分計算 ──────────────────────────────────────────────────
            fin_score = sum(scores.values())

            return {
                "ticker":     ticker_str,
                "name":       name,
                "sector":     sector,
                "industry":   industry,
                "market_cap": mkt_cap,
                "price":      price,
                "summary":    summary,
                "fin_score":  fin_score,
                "moat_score": 0,          # 前端手動設定
                "total_score": fin_score, # 護城河加上去後更新
                "scores":     scores,
                "details":    details,
                "updated":    datetime.now().strftime("%Y-%m-%d"),
            }

        except Exception as e:
            if attempt < retries - 1:
                time.sleep(3)
            else:
                return None

    return None


# ── 主流程 ────────────────────────────────────────────────────────────────
def main():
    import os
    os.makedirs("data", exist_ok=True)

    tickers = get_tw_tickers()

    results = []
    failed  = []
    total   = len(tickers)

    for i, ticker in enumerate(tickers):
        print(f"[{i+1}/{total}] {ticker}", end=" ... ")
        result = score_ticker(ticker)
        if result:
            results.append(result)
            print(f"✓ fin={result['fin_score']}/6")
        else:
            failed.append(ticker)
            print("skip")

        # 每50支暫停一下，避免被 Yahoo 封鎖
        if (i + 1) % 50 == 0:
            time.sleep(5)

    # 依財務分數排序
    results.sort(key=lambda x: x["fin_score"], reverse=True)

    # 存檔
    output = {
        "updated":  datetime.now().strftime("%Y-%m-%d %H:%M"),
        "market":   "TW",
        "total":    len(results),
        "failed":   len(failed),
        "results":  results,
    }

    with open("data/tw_results.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n✅ 完成！共 {len(results)} 支，失敗 {len(failed)} 支")
    print(f"財務6分：{sum(1 for r in results if r['fin_score']==6)} 支")
    print(f"財務5分：{sum(1 for r in results if r['fin_score']==5)} 支")
    print(f"財務4分：{sum(1 for r in results if r['fin_score']==4)} 支")


if __name__ == "__main__":
    main()
