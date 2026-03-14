"""
MoatScan - 美股篩選腳本
輸出：data/us_results.json

新增指標：FCF轉換率、毛利率穩定性、P/E評估
"""
import sys, os, json, time, io
import yfinance as yf
import pandas as pd
import requests as req
from datetime import datetime

sys.stdout.reconfigure(line_buffering=True)

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}

FALLBACK_TICKERS = [
    "AAPL","MSFT","NVDA","AMZN","GOOGL","META","TSLA","BRK-B","LLY","AVGO",
    "JPM","V","UNH","XOM","MA","COST","HD","PG","JNJ","ORCL","ABBV","WMT",
    "BAC","MRK","KO","CVX","NFLX","AMD","CRM","PEP","TMO","MCD","ADBE","ACN",
    "LIN","CSCO","ABT","WFC","GE","DHR","TXN","PM","ISRG","AMGN","NEE","RTX",
    "IBM","QCOM","SPGI","GS","CAT","INTU","VZ","T","NOW","BKNG","MS","AXP",
    "UBER","LOW","AMAT","GILD","BLK","ETN","MDT","SYK","TJX","ADI","C","DE",
    "PLD","CB","VRTX","REGN","MO","MMC","BSX","ZTS","PANW","LRCX","CME",
    "CI","SO","AON","ITW","SHW","DUK","NOC","APH","KLAC","USB","PNC","ICE",
    "GD","EMR","MCO","ECL","HUM","F","GM","FDX","NSC","COP","EOG","SLB",
    "OXY","PSX","VLO","MPC","HAL","DVN","FANG","APA",
    "HON","MMM","GWW","ROK","PH","DOV","XYL","AME","IEX","VRSK",
    "MCK","CAH","CVS","HCA","UHS","THC","CNC","MOH",
    "WM","RSG","IQV","DGX","LH",
    "SBUX","YUM","DRI","CMG","QSR","WING",
    "DIS","CMCSA","CHTR","WBD","FOX","FOXA",
    "NKE","RL","PVH","TPR","VFC","UAA",
    "BA","LMT","NOC","RTX","TDG","HWM",
    "D","EXC","AEP","ED","EIX","FE","PCG","XEL","WEC","ES",
    "AMT","EQIX","CCI","SPG","O","AVB","EQR","PSA","WELL",
    "BX","KKR","APO","CG","BAM","RVTY","COR",
    "MRVL","CDNS","SNPS","FTNT","CRWD","ZS","OKTA","NET","DDOG","MDB",
    "SNOW","PLTR","ABNB","DASH","DUOL","NTNX","PSTG","SMCI",
    "ON","MPWR","ENPH","SEDG","FSLR",
    "ILMN","IDXX","ALGN","HOLX","DXCM","PODD","NVCR","EXAS",
    "MELI","SE","JD","PDD","NIO","LI","XPEV",
]

# ── 主題關鍵字字典（與 screener_tw.py 共用邏輯）────────────────────────
THEME_KEYWORDS = {
    "光通訊":     ["optical transceiver","optical fiber","photonic","fiber optic"],
    "散熱":       ["thermal management","heat sink","heat pipe","vapor chamber","cooling solution"],
    "PCB":        ["printed circuit board","pcb substrate","circuit board","multilayer"],
    "伺服器":     ["server","data center","datacenter","hyperscale","rack server"],
    "AI/HPC":     ["artificial intelligence","machine learning","gpu computing","high performance computing","hpc"],
    "車用電子":   ["automotive","electric vehicle"," ev ","adas","vehicle electronics","in-vehicle"],
    "5G":         ["5g network","5g base station","millimeter wave","mmwave","5g infrastructure"],
    "IC設計":     ["fabless","ic design","system on chip","soc design","integrated circuit design"],
    "記憶體":     ["dram","nand flash","memory module","storage memory"],
    "封測":       ["semiconductor packaging","chip testing","assembly and test","advanced packaging"],
    "被動元件":   ["capacitor","resistor","inductor","passive component","mlcc"],
    "網通":       ["networking equipment","ethernet switch","router","wi-fi","wireless lan"],
    "電商":       ["e-commerce","online retail","marketplace platform","digital commerce"],
    "金融科技":   ["fintech","digital payment","mobile payment","digital banking"],
    "生技新藥":   ["drug development","pharmaceutical","clinical trial","fda approval","new drug"],
    "醫材":       ["medical device","diagnostic equipment","surgical instrument","implantable"],
    "再生能源":   ["solar energy","wind energy","renewable energy","photovoltaic","energy storage"],
    "機器人":     ["industrial robot","automation system","servo motor","motion control"],
    "航太國防":   ["aerospace","defense system","missile","satellite","military"],
    "電信":       ["telecommunications","mobile network","broadband service","telecom"],
    "零售/超商":  ["convenience store","supermarket","retail chain","department store"],
    "食品飲料":   ["food products","beverage","snack food","dairy product"],
    "金融保險":   ["life insurance","property insurance","bancassurance","insurance products"],
    "REITs":      ["real estate investment trust","reit","property leasing","rental income"],
    "雲端運算":   ["cloud computing","cloud service","saas","iaas","cloud platform"],
    "電動車":     ["electric vehicle","battery electric","ev battery","charging station"],
    "半導體設備": ["semiconductor equipment","etch","deposition","lithography","wafer processing"],
    "串流媒體":   ["streaming","subscription video","content platform","digital media"],
    "社群媒體":   ["social media","social network","user generated","advertising platform"],
    "支付":       ["payment processing","payment network","card network","merchant acquiring"],
}

def extract_themes(summary: str, industry: str) -> list:
    """從公司簡介和產業分類抽取主題標籤"""
    text = (summary + " " + industry).lower()
    return [t for t, kws in THEME_KEYWORDS.items() if any(kw in text for kw in kws)]

def get_us_tickers():
    tickers = set()
    print("[INFO] 嘗試從 Wikipedia 取得最新清單...", flush=True)

    # 1. S&P 500
    try:
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        res = req.get(url, headers=HEADERS, timeout=15)
        df  = pd.read_html(io.StringIO(res.text))[0]
        sp  = df['Symbol'].str.replace('.', '-', regex=False).tolist()
        tickers.update(sp)
        print(f"✓ S&P500: {len(sp)} 支", flush=True)
    except Exception as e:
        print(f"[警告] S&P500 失敗: {e}", flush=True)

    # 2. NASDAQ-100
    try:
        url = "https://en.wikipedia.org/wiki/Nasdaq-100"
        res = req.get(url, headers=HEADERS, timeout=15)
        df  = pd.read_html(io.StringIO(res.text), match='Ticker')[0]
        ndx = df['Ticker'].tolist()
        tickers.update(ndx)
        print(f"✓ NASDAQ100: {len(ndx)} 支", flush=True)
    except Exception as e:
        print(f"[警告] NASDAQ100 失敗: {e}", flush=True)

    # 3. 手動補充：S&P500 以外的重要中大型股
    EXTRA = [
        # 金融科技 / 支付
        "SQ","PYPL","SOFI","AFRM","COIN","HOOD","IBKR","LPLA","RJF","MKTX",
        # 雲端 / 軟體
        "TWLO","ZM","DOCN","PATH","AI","GTLB","HUBS","BILL","DDOG","MDB","SNOW",
        "PLTR","NTNX","PSTG","CFLT","SMAR","APPF","PCTY","PAYC",
        # 半導體（中小型）
        "WOLF","OLED","ONTO","MKSI","COHU","ACLS","FORM","UCTT","AMBA","CRUS",
        # 生技 / 醫療
        "MRNA","BNTX","BEAM","NTLA","CRSP","EDIT","ALNY","IONS","BMRN","RARE",
        "ACAD","INCY","SGEN","PRGO","JAZZ","EXAS","NVCR","DXCM","PODD","IRTC",
        # 消費 / 電商
        "ETSY","W","CHWY","CVNA","RVLV","LULU","RH","WSM","ELF","ULTA","POST",
        # 媒體 / 串流
        "SPOT","PARA","WMG","LYV","IMAX",
        # 能源轉型
        "BE","PLUG","ARRY","RUN","NOVA","FSLR","ENPH","SEDG",
        # 工業 / 國防
        "LDOS","SAIC","BAH","CACI","MANT","DRS","TDG","HWM",
        # REITs
        "VICI","STAG","COLD","IIPR","CUBE","LSI","NNN","ADC","EPRT","MPW",
        # 廣告科技
        "TTD","ROKU","DV","IAS","MGNI","PUBM","APPS",
        # 中型金融
        "SF","EVR","HLI","GBCI","CVBF","FNB","WTFC",
        # 其他成長股
        "ABNB","DASH","DUOL","CELH","AXON","GNRC","TREX","POOL",
    ]
    before = len(tickers)
    tickers.update(EXTRA)
    print(f"✓ 手動補充: {len(tickers)-before} 支新增", flush=True)

    if not tickers:
        print("[INFO] 使用保底清單", flush=True)
        return FALLBACK_TICKERS
    tickers.update(FALLBACK_TICKERS)
    result = list(tickers)
    print(f"[INFO] 美股清單共 {len(result)} 支（市值篩選前）", flush=True)
    return result

def pre_filter(tickers):
    print(f"[初篩] 批次下載 {len(tickers)} 支行情...", flush=True)
    passed = []
    for i in range(0, len(tickers), 50):
        batch = tickers[i:i+50]
        for attempt in range(3):
            try:
                df = yf.download(batch, period="5d", auto_adjust=True,
                                 progress=False, timeout=30)
                if df.empty:
                    passed.extend(batch); break
                close  = df["Close"]
                volume = df["Volume"]
                for t in batch:
                    try:
                        ac = close[t].dropna().mean()  if t in close.columns  else 0
                        av = volume[t].dropna().mean() if t in volume.columns else 0
                        if ac >= 5 and av >= 100_000: passed.append(t)
                    except Exception:
                        passed.append(t)
                break
            except Exception as e:
                err = str(e)
                if attempt < 2:
                    wait = 15 * (attempt + 1)
                    print(f"[初篩重試] {err[:60]}，等 {wait}s...", flush=True)
                    time.sleep(wait)
                else:
                    print(f"[初篩放棄此批] {err[:60]}", flush=True)
                    passed.extend(batch)
        time.sleep(3)
    print(f"[初篩] 通過 {len(passed)} 支（過濾 {len(tickers)-len(passed)} 支）", flush=True)
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
    if not rates:
        return None, "N/A"
    avg = sum(rates) / len(rates)
    return round(avg, 2), f"平均{round(avg,2)}x（{len(rates)}年）"

def calc_gm_stability(fin):
    gp_vals  = col_values(fin, "Gross Profit")
    rev_vals = col_values(fin, "Total Revenue", "Revenue")
    margins  = []
    for i in range(min(len(gp_vals), len(rev_vals))):
        if rev_vals[i] and rev_vals[i] != 0:
            margins.append(gp_vals[i] / rev_vals[i] * 100)
    if len(margins) < 2:
        return None, None, "N/A"
    avg_gm = round(sum(margins) / len(margins), 1)
    std_gm = round(pd.Series(margins).std(), 1)
    grade  = "🟢 穩定" if std_gm < 3 else ("🟡 普通" if std_gm < 7 else "🔴 波動")
    return avg_gm, std_gm, f"均{avg_gm}% σ={std_gm}% {grade}"

def calc_pe_assessment(info, eps_g3):
    pe = info.get("trailingPE") or info.get("forwardPE")
    if not pe or pe <= 0:
        return None, None, "N/A"
    pe = round(pe, 1)
    if eps_g3 and eps_g3 > 0:
        peg = round(pe / eps_g3, 2)
        grade = "🟢 偏低" if peg < 1 else ("🟡 合理" if peg < 2 else "🔴 偏高")
        return pe, peg, f"PE={pe} PEG={peg} {grade}"
    return pe, None, f"PE={pe}"

def score_ticker(ticker_str, retries=3):
    for attempt in range(retries):
        try:
            tk   = yf.Ticker(ticker_str)
            info = tk.info

            name     = info.get("longName") or info.get("shortName") or ticker_str
            sector   = info.get("sector",   "Unknown")
            industry = info.get("industry", "Unknown")
            mkt_cap  = info.get("marketCap") or 0
            price      = info.get("currentPrice") or info.get("regularMarketPrice") or 0
            prev_close = info.get("previousClose") or info.get("regularMarketPreviousClose") or 0
            change_pct = round((price - prev_close) / prev_close * 100, 2) if prev_close and price else None
            summary  = (info.get("longBusinessSummary") or "")[:300]
            themes   = extract_themes(summary, info.get("industry") or "")

            if mkt_cap and mkt_cap < 500_000_000:
                return None

            scores  = {}
            details = {}
            fin = tk.financials
            cf  = tk.cashflow
            bs  = tk.balance_sheet

            # 1. EPS
            eps_vals = col_values(fin, "Basic EPS", "Diluted EPS")
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
            details["eps"] = f"最新EPS ${round(eps_vals[0],2) if eps_vals else 'N/A'}"

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
            tax_rate   = (info.get("effectiveTaxRate") or 0.21)
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
            if bvps: bvps = round(float(bvps), 2)

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
                "bvps":        bvps,
                "fcf_conv":    fcf_conv,
                "fcf_conv_txt":fcf_conv_txt,
                "avg_gm":      avg_gm,
                "std_gm":      std_gm,
                "gm_txt":      gm_txt,
                "pe":          pe_val,
                "peg":         peg_val,
                "pe_txt":      pe_txt,
                "updated":     datetime.now().strftime("%Y-%m-%d"),
                "change_pct":  change_pct,
                "themes":       themes,
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
    all_tickers = get_us_tickers()
    tickers     = pre_filter(all_tickers)

    results, failed = [], []
    total = len(tickers)
    print(f"[開始] 深度分析 {total} 支美股...", flush=True)

    for i, ticker in enumerate(tickers):
        print(f"[{i+1}/{total}] {ticker}", end=" ... ", flush=True)
        r = score_ticker(ticker)
        if r:
            results.append(r)
            print(f"✓ fin={r['fin_score']}/6", flush=True)
        else:
            failed.append(ticker)
            print("skip", flush=True)
        time.sleep(1.2)
        if (i + 1) % 100 == 0:
            print(f"[暫停] 已處理 {i+1}/{total}，休息 20 秒...", flush=True)
            time.sleep(20)

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
    print(f"\n✅ 美股完成！共 {len(results)} 支，失敗 {len(failed)} 支", flush=True)

if __name__ == "__main__":
    main()
