"""
MoatScan - 美股全市場篩選腳本
輸出：data/us_results.json
"""

import yfinance as yf
import pandas as pd
import json
import requests
import time
from datetime import datetime
import os

# ── 取得美股代號清單（S&P500 + NASDAQ100 + 其他） ────────────────────────
def get_us_tickers():
    tickers = set()

    # S&P 500 (Wikipedia)
    try:
        sp500 = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")[0]
        for t in sp500["Symbol"].tolist():
            tickers.add(str(t).replace(".", "-"))
        print(f"[INFO] S&P500: {len(tickers)} 支")
    except Exception as e:
        print(f"[警告] S&P500 清單失敗: {e}")

    # NASDAQ 100
    try:
        nq100 = pd.read_html("https://en.wikipedia.org/wiki/Nasdaq-100")[4]
        col = [c for c in nq100.columns if "ticker" in c.lower() or "symbol" in c.lower()]
        if col:
            for t in nq100[col[0]].tolist():
                tickers.add(str(t))
        print(f"[INFO] 加入 NASDAQ100 後: {len(tickers)} 支")
    except Exception as e:
        print(f"[警告] NASDAQ100 清單失敗: {e}")

    # 若有本地清單（進階版可加入全 8000 支）
    if os.path.exists("data/us_tickers_full.txt"):
        with open("data/us_tickers_full.txt") as f:
            for line in f:
                t = line.strip()
                if t:
                    tickers.add(t)

    result = sorted(list(tickers))
    print(f"[INFO] 共取得 {len(result)} 支美股代號")
    return result


# ── 單支股票評分（與台股相同邏輯）─────────────────────────────────────────
def score_ticker(ticker_str, retries=2):
    for attempt in range(retries):
        try:
            tk = yf.Ticker(ticker_str)
            info = tk.info

            name     = info.get("longName") or info.get("shortName") or ticker_str
            sector   = info.get("sector", "未知")
            industry = info.get("industry", "未知")
            mkt_cap  = info.get("marketCap", 0)
            price    = info.get("currentPrice") or info.get("regularMarketPrice") or 0
            summary  = (info.get("longBusinessSummary") or "")[:400]

            # 市值 < 5億美元跳過
            if mkt_cap and mkt_cap < 500_000_000:
                return None

            scores  = {}
            details = {}

            fin = tk.financials
            cf  = tk.cashflow
            bs  = tk.balance_sheet

            def col_values(df, *keys):
                for key in keys:
                    if df is not None and not df.empty:
                        for idx in df.index:
                            if key.lower() in str(idx).lower():
                                return df.loc[idx].dropna().tolist()
                return []

            # 1. EPS
            eps_vals = col_values(fin, "Basic EPS", "Diluted EPS")
            if not eps_vals:
                te = info.get("trailingEps")
                eps_vals = [te] if te else []
            eps_pass = False
            eps_g3, eps_g5 = None, None
            if len(eps_vals) >= 2:
                pos = sum(1 for v in eps_vals if v and v > 0)
                trend = eps_vals[0] >= eps_vals[-1] * 0.8 if eps_vals[-1] and eps_vals[-1] > 0 else False
                eps_pass = (pos >= len(eps_vals) * 0.75) and trend
                def safe_cagr(cur, old, yrs):
                    try:
                        if cur and old and old > 0 and cur > 0:
                            return round((pow(cur / old, 1.0 / yrs) - 1) * 100, 2)
                    except: pass
                    return None
                if len(eps_vals) >= 4:
                    eps_g3 = safe_cagr(eps_vals[0], eps_vals[3], 3)
                if len(eps_vals) >= 6:
                    eps_g5 = safe_cagr(eps_vals[0], eps_vals[5], 5)
            scores["eps"]   = 1 if eps_pass else 0
            details["eps"]  = f"最新EPS ${round(eps_vals[0],2) if eps_vals else 'N/A'}"

            # 2. FCF
            op_cf  = col_values(cf, "Operating Cash Flow", "Cash From Operations")
            cap_ex = col_values(cf, "Capital Expenditure", "Purchase Of PPE")
            fcf_vals = [op_cf[i] - abs(cap_ex[i] if cap_ex[i] else 0) for i in range(min(len(op_cf), len(cap_ex)))]
            fcf_pass = len(fcf_vals) >= 2 and sum(1 for v in fcf_vals if v > 0) >= len(fcf_vals) * 0.8
            scores["fcf"]   = 1 if fcf_pass else 0
            details["fcf"]  = f"{sum(1 for v in fcf_vals if v>0)}/{len(fcf_vals)}年FCF為正"

            # 3. ROIC
            ebit_vals  = col_values(fin, "EBIT", "Operating Income")
            tax_rate   = info.get("effectiveTaxRate", 0.21)
            total_eq   = col_values(bs, "Stockholders Equity", "Total Equity")
            total_debt = col_values(bs, "Total Debt", "Long Term Debt")
            roic_vals  = []
            for i in range(min(len(ebit_vals), len(total_eq), len(total_debt))):
                nopat    = ebit_vals[i] * (1 - tax_rate)
                invested = (total_eq[i] or 0) + (total_debt[i] or 0)
                if invested > 0:
                    roic_vals.append(nopat / invested)
            roic_pass = bool(roic_vals) and sum(1 for r in roic_vals if r > 0.10) >= len(roic_vals) * 0.7
            scores["roic"]   = 1 if roic_pass else 0
            details["roic"]  = f"最新ROIC {round(roic_vals[0]*100,1) if roic_vals else 'N/A'}%"

            # 4. D/E
            de = info.get("debtToEquity")
            de_ratio = de / 100 if de is not None else None
            if de_ratio is None and total_eq and total_debt:
                de_ratio = total_debt[0] / total_eq[0] if total_eq[0] else None
            de_pass = de_ratio is not None and de_ratio < 0.5
            scores["de"]   = 1 if de_pass else 0
            details["de"]  = f"D/E={round(de_ratio,2) if de_ratio is not None else 'N/A'}"

            # 5. Net Margin
            rev_vals = col_values(fin, "Total Revenue", "Revenue")
            ni_vals  = col_values(fin, "Net Income")
            nm_vals  = [ni_vals[i]/rev_vals[i] for i in range(min(len(rev_vals),len(ni_vals))) if rev_vals[i]]
            nm_pass  = bool(nm_vals) and sum(1 for m in nm_vals if m > 0.20) >= len(nm_vals) * 0.6
            scores["netmargin"]   = 1 if nm_pass else 0
            details["netmargin"]  = f"最新Net Margin {round(nm_vals[0]*100,1) if nm_vals else 'N/A'}%"

            # 6. 配息
            div_rate  = info.get("dividendRate") or 0
            div_hist  = tk.dividends
            div_years = 0
            if div_hist is not None and not div_hist.empty:
                div_years = div_hist.resample("Y").sum().astype(bool).sum()
            div_pass = div_years >= 3 or div_rate > 0
            scores["dividend"]   = 1 if div_pass else 0
            details["dividend"]  = f"近{div_years}年有配息"

            fin_score = sum(scores.values())

            return {
                "ticker":      ticker_str,
                "name":        name,
                "sector":      sector,
                "industry":    industry,
                "market_cap":  mkt_cap,
                "price":       price,
                "summary":     summary,
                "fin_score":   fin_score,
                "moat_score":  0,
                "total_score": fin_score,
                "scores":      scores,
                "details":     details,
                "eps_current": round(eps_vals[0], 2) if eps_vals else None,
                "eps_g3":      eps_g3,
                "eps_g5":      eps_g5,
                "div_rate":    round(div_rate, 2) if div_rate else None,
                "bvps":        round(info.get("bookValuePerShare", 0), 2) if info.get("bookValuePerShare") else None,
                "updated":     datetime.now().strftime("%Y-%m-%d"),
            }

        except Exception as e:
            if attempt < retries - 1:
                time.sleep(3)

    return None


# ── 主流程 ────────────────────────────────────────────────────────────────
def main():
    os.makedirs("data", exist_ok=True)

    tickers = get_us_tickers()
    results, failed = [], []

    for i, ticker in enumerate(tickers):
        print(f"[{i+1}/{len(tickers)}] {ticker}", end=" ... ")
        r = score_ticker(ticker)
        if r:
            results.append(r)
            print(f"✓ fin={r['fin_score']}/6")
        else:
            failed.append(ticker)
            print("skip")
        if (i + 1) % 50 == 0:
            time.sleep(5)

    results.sort(key=lambda x: x["fin_score"], reverse=True)

    output = {
        "updated": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "market":  "US",
        "total":   len(results),
        "failed":  len(failed),
        "results": results,
    }

    with open("data/us_results.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n✅ 完成！共 {len(results)} 支，失敗 {len(failed)} 支")


if __name__ == "__main__":
    main()
