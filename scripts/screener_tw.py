"""
MoatScan - 台股全市場篩選腳本
輸出：data/tw_results.json
"""
import sys, os, json, time
import yfinance as yf
import pandas as pd
import requests as req
from datetime import datetime

sys.stdout.reconfigure(line_buffering=True)

SECTOR_ZH = {
    "Technology":"科技","Financial Services":"金融服務","Healthcare":"醫療保健",
    "Consumer Cyclical":"非必需消費","Consumer Defensive":"必需消費",
    "Industrials":"工業","Basic Materials":"基礎材料","Energy":"能源",
    "Utilities":"公用事業","Real Estate":"不動產",
    "Communication Services":"通訊服務","Communication":"通訊服務",
}
INDUSTRY_ZH = {
    "Semiconductors":"半導體","Semiconductor Equipment & Materials":"半導體設備",
    "Electronic Components":"電子零組件","Electronics & Computer Distribution":"電子通路",
    "Consumer Electronics":"消費電子","Computer Hardware":"電腦硬體",
    "Information Technology Services":"資訊服務","Software—Application":"應用軟體",
    "Software—Infrastructure":"基礎軟體","Internet Content & Information":"網路內容",
    "Communication Equipment":"通訊設備","Telecom Services":"電信服務",
    "Banks—Regional":"區域銀行","Banks—Diversified":"多元銀行",
    "Insurance—Life":"人壽保險","Insurance—Diversified":"多元保險",
    "Asset Management":"資產管理","Capital Markets":"資本市場",
    "Drug Manufacturers—General":"製藥","Biotechnology":"生技",
    "Medical Devices":"醫療器材","Diagnostics & Research":"診斷與研究",
    "Specialty Retail":"特殊零售","Discount Stores":"折扣零售",
    "Grocery Stores":"超市","Beverages—Non-Alcoholic":"飲料（非酒精）",
    "Food Distribution":"食品通路","Packaged Foods":"包裝食品","Restaurants":"餐飲",
    "Auto Manufacturers":"汽車製造","Auto Parts":"汽車零件",
    "Aerospace & Defense":"航太與國防","Industrial Conglomerates":"工業集團",
    "Specialty Chemicals":"特殊化學","Chemicals":"化學","Steel":"鋼鐵",
    "Oil & Gas Integrated":"石油天然氣（整合）","Oil & Gas Refining & Marketing":"石油煉製",
    "Oil & Gas E&P":"石油探勘開採","Utilities—Regulated Electric":"電力公用事業",
    "REIT—Industrial":"工業型不動產","REIT—Office":"辦公型不動產",
    "Electronic Gaming & Multimedia":"電子遊戲","Pollution & Treatment Controls":"環保",
    "Waste Management":"廢棄物處理","Engineering & Construction":"工程建設",
    "Electrical Equipment & Parts":"電氣設備","Scientific & Technical Instruments":"科學儀器",
    "Contract Manufacturers":"代工製造","Printed Circuit Boards":"印刷電路板",
    "Electronic Distribution":"電子通路",
}

def get_tw_tickers():
    tickers = []
    name_map = {}
    try:
        r = req.get("https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL", timeout=15)
        for row in r.json():
            code = row.get("Code", "")
            if code.isdigit() and len(code) == 4:
                key = f"{code}.TW"
                tickers.append(key)
                if row.get("Name"): name_map[key] = row["Name"]
    except Exception as e:
        print(f"[警告] TSE 失敗: {e}", flush=True)
    try:
        r = req.get("https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes", timeout=15)
        for row in r.json():
            code = row.get("SecuritiesCompanyCode", "")
            if code.isdigit() and len(code) == 4:
                key = f"{code}.TWO"
                tickers.append(key)
                if row.get("CompanyName"): name_map[key] = row["CompanyName"]
    except Exception as e:
        print(f"[警告] OTC 失敗: {e}", flush=True)
    tickers = list(set(tickers))
    print(f"[INFO] 共取得 {len(tickers)} 支，中文名稱 {len(name_map)} 支", flush=True)
    return tickers, name_map

def pre_filter(tickers):
    print(f"[初篩] 批次下載 {len(tickers)} 支行情...", flush=True)
    passed = []
    for i in range(0, len(tickers), 200):
        batch = tickers[i:i+200]
        try:
            df = yf.download(batch, period="5d", auto_adjust=True, progress=False)
            if df.empty:
                passed.extend(batch); continue
            close  = df["Close"]
            volume = df["Volume"]
            for t in batch:
                try:
                    ac = close[t].dropna().mean()  if t in close.columns  else 0
                    av = volume[t].dropna().mean() if t in volume.columns else 0
                    if ac >= 5 and av >= 200_000: passed.append(t)
                except Exception:
                    passed.append(t)
        except Exception as e:
            print(f"[初篩批次失敗] {e}", flush=True)
            passed.extend(batch)
        time.sleep(2)
    print(f"[初篩] 通過 {len(passed)} 支（過濾 {len(tickers)-len(passed)} 支）", flush=True)
    # 初篩後等久一點，讓 Yahoo 速率限制冷卻
    print("[暫停] 初篩完畢，等待 60 秒讓 Yahoo 冷卻...", flush=True)
    time.sleep(60)
    return passed

def col_values(df, *keys):
    for key in keys:
        if df is not None and not df.empty:
            for idx in df.index:
                if key.lower() in str(idx).lower():
                    return df.loc[idx].dropna().tolist()
    return []

def calc_fcf_conversion(fcf_vals, nm_vals):
    rates = []
    for i in range(min(len(fcf_vals), len(nm_vals))):
        if nm_vals[i] and nm_vals[i] > 0:
            rates.append(fcf_vals[i] / nm_vals[i])
    if not rates: return None, "N/A"
    avg = sum(rates) / len(rates)
    return round(avg, 2), f"平均{round(avg,2)}x（{len(rates)}年）"

def calc_gm_stability(fin):
    gp_vals  = col_values(fin, "Gross Profit")
    rev_vals = col_values(fin, "Total Revenue", "Revenue")
    margins  = []
    for i in range(min(len(gp_vals), len(rev_vals))):
        if rev_vals[i] and rev_vals[i] != 0:
            margins.append(gp_vals[i] / rev_vals[i] * 100)
    if len(margins) < 2: return None, None, "N/A"
    avg_gm = round(sum(margins) / len(margins), 1)
    std_gm = round(pd.Series(margins).std(), 1)
    grade  = "🟢 穩定" if std_gm < 3 else ("🟡 普通" if std_gm < 7 else "🔴 波動")
    return avg_gm, std_gm, f"均{avg_gm}% σ={std_gm}% {grade}"

def calc_pe_assessment(info, eps_g3):
    pe = info.get("trailingPE") or info.get("forwardPE")
    if not pe or pe <= 0: return None, None, "N/A"
    pe = round(pe, 1)
    if eps_g3 and eps_g3 > 0:
        peg = round(pe / eps_g3, 2)
        grade = "🟢 偏低" if peg < 1 else ("🟡 合理" if peg < 2 else "🔴 偏高")
        return pe, peg, f"PE={pe} PEG={peg} {grade}"
    return pe, None, f"PE={pe}"

def score_ticker(ticker_str, name_zh, retries=3):
    for attempt in range(retries):
        try:
            tk   = yf.Ticker(ticker_str)
            info = tk.info
            name = name_zh or info.get("longName") or info.get("shortName") or ticker_str
            sector_en   = info.get("sector")   or "未知"
            industry_en = info.get("industry") or "未知"
            sector   = SECTOR_ZH.get(sector_en,   sector_en)
            industry = INDUSTRY_ZH.get(industry_en, industry_en)
            mkt_cap  = info.get("marketCap") or 0
            price    = info.get("currentPrice") or info.get("regularMarketPrice") or 0
            summary  = (info.get("longBusinessSummary") or "")[:300]
            if mkt_cap and mkt_cap < 150_000_000: return None

            scores, details = {}, {}
            fin = tk.financials
            cf  = tk.cashflow
            bs  = tk.balance_sheet

            # 1. EPS
            eps_vals = col_values(fin, "Basic EPS", "Diluted EPS", "EPS")
            if not eps_vals:
                te = info.get("trailingEps")
                eps_vals = [te] if te else []
            eps_pass = False
            eps_g3, eps_g5 = None, None
            if len(eps_vals) >= 2:
                pos      = sum(1 for v in eps_vals if v and v > 0)
                trend_ok = eps_vals[0] >= eps_vals[-1] * 0.8 if (eps_vals[-1] and eps_vals[-1] > 0) else False
                eps_pass = (pos >= len(eps_vals) * 0.75) and trend_ok
                def safe_cagr(cur, old, yrs):
                    try:
                        if cur and old and old > 0 and cur > 0:
                            return round((pow(cur / old, 1.0 / yrs) - 1) * 100, 2)
                    except: pass
                    return None
                if len(eps_vals) >= 4: eps_g3 = safe_cagr(eps_vals[0], eps_vals[3], 3)
                if len(eps_vals) >= 6: eps_g5 = safe_cagr(eps_vals[0], eps_vals[5], 5)
            scores["eps"]  = 1 if eps_pass else 0
            details["eps"] = f"{len(eps_vals)}年資料，最新${round(eps_vals[0],2) if eps_vals else 'N/A'}"

            # 2. FCF
            op_cf    = col_values(cf, "Operating Cash Flow", "Cash From Operations")
            cap_ex   = col_values(cf, "Capital Expenditure", "Purchase Of PPE")
            nm_vals  = col_values(fin, "Net Income")
            fcf_vals = [op_cf[i] - abs(cap_ex[i] or 0) for i in range(min(len(op_cf), len(cap_ex)))]
            fcf_pass = len(fcf_vals) >= 2 and sum(1 for v in fcf_vals if v > 0) >= len(fcf_vals) * 0.8
            scores["fcf"]  = 1 if fcf_pass else 0
            details["fcf"] = f"{sum(1 for v in fcf_vals if v>0)}/{len(fcf_vals)}年FCF為正"

            # 3. ROIC
            ebit_vals  = col_values(fin, "EBIT", "Operating Income")
            tax_rate   = (info.get("effectiveTaxRate") or 0.2)
            total_eq   = col_values(bs, "Stockholders Equity", "Total Equity")
            total_debt = col_values(bs, "Total Debt", "Long Term Debt")
            roic_vals  = []
            for i in range(min(len(ebit_vals), len(total_eq), len(total_debt))):
                nopat    = ebit_vals[i] * (1 - tax_rate)
                invested = (total_eq[i] or 0) + (total_debt[i] or 0)
                if invested > 0: roic_vals.append(nopat / invested)
            roic_pass = bool(roic_vals) and sum(1 for r in roic_vals if r > 0.10) >= len(roic_vals) * 0.7
            scores["roic"]  = 1 if roic_pass else 0
            details["roic"] = f"最新ROIC {round(roic_vals[0]*100,1) if roic_vals else 'N/A'}%"

            # 4. D/E
            de       = info.get("debtToEquity")
            de_ratio = de / 100 if de is not None else None
            if de_ratio is None and total_eq and total_debt:
                de_ratio = total_debt[0] / total_eq[0] if total_eq[0] else None
            de_pass = de_ratio is not None and de_ratio < 0.5
            scores["de"]  = 1 if de_pass else 0
            details["de"] = f"D/E={round(de_ratio,2) if de_ratio is not None else 'N/A'}"

            # 5. Net Margin
            rev_vals = col_values(fin, "Total Revenue", "Revenue")
            nm_margins = []
            for i in range(min(len(nm_vals), len(rev_vals))):
                if rev_vals[i] and rev_vals[i] != 0:
                    nm_margins.append(nm_vals[i] / rev_vals[i])
            nm_pass = bool(nm_margins) and sum(1 for m in nm_margins if m > 0.20) >= len(nm_margins) * 0.6
            scores["netmargin"]  = 1 if nm_pass else 0
            details["netmargin"] = f"最新{round(nm_margins[0]*100,1) if nm_margins else 'N/A'}%"

            # 6. 配息
            div_rate  = (info.get("dividendRate")  or 0)
            div_yield = (info.get("dividendYield") or 0)
            div_hist  = tk.dividends
            div_years = 0
            if div_hist is not None and not div_hist.empty:
                div_years = div_hist.resample("YE").sum().astype(bool).sum()
            div_pass = div_years >= 3 or (div_rate > 0 and div_yield > 0)
            scores["dividend"]  = 1 if div_pass else 0
            details["dividend"] = f"近{div_years}年有配息"

            fin_score = sum(scores.values())

            # 新指標
            fcf_conv, fcf_conv_txt = calc_fcf_conversion(fcf_vals, nm_vals)
            avg_gm, std_gm, gm_txt = calc_gm_stability(fin)
            pe_val, peg_val, pe_txt = calc_pe_assessment(info, eps_g3)

            # BVPS
            bvps = info.get("bookValuePerShare")
            if not bvps and bs is not None and not bs.empty:
                eq_vals = col_values(bs, "Stockholders Equity", "Total Equity", "Common Stock Equity")
                shares  = info.get("sharesOutstanding") or info.get("impliedSharesOutstanding")
                if eq_vals and shares and shares > 0:
                    bvps = round(eq_vals[0] / shares, 2)
            if bvps: bvps = round(float(bvps), 2)

            return {
                "ticker":ticker_str,"name":name,"sector":sector,"industry":industry,
                "market_cap":mkt_cap,"price":price,"summary":summary,
                "fin_score":fin_score,"moat_score":0,"total_score":fin_score,
                "scores":scores,"details":details,
                "eps_current":round(eps_vals[0],2) if eps_vals else None,
                "eps_g3":eps_g3,"eps_g5":eps_g5,
                "div_rate":round(div_rate,2) if div_rate else None,"bvps":bvps,
                "fcf_conv":fcf_conv,"fcf_conv_txt":fcf_conv_txt,
                "avg_gm":avg_gm,"std_gm":std_gm,"gm_txt":gm_txt,
                "pe":pe_val,"peg":peg_val,"pe_txt":pe_txt,
                "updated":datetime.now().strftime("%Y-%m-%d"),
            }

        except Exception as e:
            err = str(e)
            if "Too Many Requests" in err or "RateLimit" in err:
                wait = 30 * (attempt + 1)
                print(f"[限流] 等待 {wait} 秒後重試...", end=" ", flush=True)
                time.sleep(wait)
            elif attempt < retries - 1:
                time.sleep(5)
            else:
                print(f"[錯誤] {e.__class__.__name__}: {err[:80]}", end=" ", flush=True)
                return None
    return None

def main():
    os.makedirs("data", exist_ok=True)
    all_tickers, name_map = get_tw_tickers()
    tickers = pre_filter(all_tickers)

    results, failed = [], []
    total = len(tickers)
    print(f"[開始] 深度分析 {total} 支台股...", flush=True)

    for i, ticker in enumerate(tickers):
        print(f"[{i+1}/{total}] {ticker}", end=" ... ", flush=True)
        result = score_ticker(ticker, name_map.get(ticker, ""))
        if result:
            results.append(result)
            print(f"✓ fin={result['fin_score']}/6", flush=True)
        else:
            failed.append(ticker)
            print("skip", flush=True)

        time.sleep(1.2)
        if (i + 1) % 100 == 0:
            print(f"[暫停] 已處理 {i+1}/{total}，休息 20 秒...", flush=True)
            time.sleep(20)

    results.sort(key=lambda x: x["fin_score"], reverse=True)
    output = {
        "updated":datetime.now().strftime("%Y-%m-%d %H:%M"),
        "market":"TW","total":len(results),"failed":len(failed),"results":results,
    }
    with open("data/tw_results.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"\n✅ 台股完成！共 {len(results)} 支，失敗 {len(failed)} 支", flush=True)
    print(f"6分:{sum(1 for r in results if r['fin_score']==6)}  5分:{sum(1 for r in results if r['fin_score']==5)}  4分:{sum(1 for r in results if r['fin_score']==4)}", flush=True)

if __name__ == "__main__":
    main()
