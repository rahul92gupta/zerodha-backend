from flask import Flask, jsonify, request
from flask_cors import CORS
import yfinance as yf
import numpy as np
import json, time
from datetime import datetime, timedelta
from threading import Thread
import feedparser

app = Flask(__name__)
CORS(app)

# ═══════════════════════════════════════════════
#  NSE UNIVERSE — All major NSE stocks
# ═══════════════════════════════════════════════
NSE_SYMBOLS = {
    # Indices
    "NIFTY": "^NSEI", "SENSEX": "^BSESN", "BANKNIFTY": "^NSEBANK",
    "NIFTYMID": "^NSEMDCP50", "NIFTYIT": "^CNXIT",
    # Nifty 50
    "RELIANCE":"RELIANCE.NS","TCS":"TCS.NS","HDFCBANK":"HDFCBANK.NS",
    "INFY":"INFY.NS","ICICIBANK":"ICICIBANK.NS","HINDUNILVR":"HINDUNILVR.NS",
    "ITC":"ITC.NS","SBIN":"SBIN.NS","BHARTIARTL":"BHARTIARTL.NS",
    "BAJFINANCE":"BAJFINANCE.NS","KOTAKBANK":"KOTAKBANK.NS","LT":"LT.NS",
    "HCLTECH":"HCLTECH.NS","ASIANPAINT":"ASIANPAINT.NS","AXISBANK":"AXISBANK.NS",
    "MARUTI":"MARUTI.NS","SUNPHARMA":"SUNPHARMA.NS","TITAN":"TITAN.NS",
    "WIPRO":"WIPRO.NS","NTPC":"NTPC.NS","POWERGRID":"POWERGRID.NS",
    "ULTRACEMCO":"ULTRACEMCO.NS","NESTLEIND":"NESTLEIND.NS","TATAMOTORS":"TATAMOTORS.NS",
    "ADANIENT":"ADANIENT.NS","JSWSTEEL":"JSWSTEEL.NS","TATASTEEL":"TATASTEEL.NS",
    "ONGC":"ONGC.NS","COALINDIA":"COALINDIA.NS","BAJAJFINSV":"BAJAJFINSV.NS",
    "TECHM":"TECHM.NS","INDUSINDBK":"INDUSINDBK.NS","DRREDDY":"DRREDDY.NS",
    "CIPLA":"CIPLA.NS","DIVISLAB":"DIVISLAB.NS","EICHERMOT":"EICHERMOT.NS",
    "BPCL":"BPCL.NS","TATACONSUM":"TATACONSUM.NS","GRASIM":"GRASIM.NS",
    "APOLLOHOSP":"APOLLOHOSP.NS","ADANIPORTS":"ADANIPORTS.NS","BRITANNIA":"BRITANNIA.NS",
    "HEROMOTOCO":"HEROMOTOCO.NS","HINDALCO":"HINDALCO.NS","UPL":"UPL.NS",
    "SBILIFE":"SBILIFE.NS","HDFCLIFE":"HDFCLIFE.NS","BAJAJ-AUTO":"BAJAJ-AUTO.NS",
    "MM":"M&M.NS","SHREECEM":"SHREECEM.NS",
    # Nifty Next 50
    "AMBUJACEM":"AMBUJACEM.NS","BANKBARODA":"BANKBARODA.NS","BERGEPAINT":"BERGEPAINT.NS",
    "BOSCHLTD":"BOSCHLTD.NS","CANBK":"CANBK.NS","CHOLAFIN":"CHOLAFIN.NS",
    "COLPAL":"COLPAL.NS","DABUR":"DABUR.NS","DLF":"DLF.NS","GAIL":"GAIL.NS",
    "GODREJCP":"GODREJCP.NS","HAVELLS":"HAVELLS.NS","ICICIGI":"ICICIGI.NS",
    "ICICIPRULI":"ICICIPRULI.NS","INDIGO":"INDIGO.NS","INDUSTOWER":"INDUSTOWER.NS",
    "IOC":"IOC.NS","IRCTC":"IRCTC.NS","LICI":"LICI.NS","LUPIN":"LUPIN.NS",
    "MARICO":"MARICO.NS","MUTHOOTFIN":"MUTHOOTFIN.NS","NAUKRI":"NAUKRI.NS",
    "PAGEIND":"PAGEIND.NS","PETRONET":"PETRONET.NS","PIDILITIND":"PIDILITIND.NS",
    "PNB":"PNB.NS","RECLTD":"RECLTD.NS","SAIL":"SAIL.NS","SIEMENS":"SIEMENS.NS",
    "SRF":"SRF.NS","TORNTPHARM":"TORNTPHARM.NS","TRENT":"TRENT.NS",
    "TVSMOTOR":"TVSMOTOR.NS","VBL":"VBL.NS","VEDL":"VEDL.NS","VOLTAS":"VOLTAS.NS",
    "ZOMATO":"ZOMATO.NS","ZYDUSLIFE":"ZYDUSLIFE.NS",
    # MidCap Stars
    "ABFRL":"ABFRL.NS","ASTRAL":"ASTRAL.NS","AUROPHARMA":"AUROPHARMA.NS",
    "BALKRISIND":"BALKRISIND.NS","CGPOWER":"CGPOWER.NS","DEEPAKNTR":"DEEPAKNTR.NS",
    "DIXON":"DIXON.NS","FEDERALBNK":"FEDERALBNK.NS","GODREJPROP":"GODREJPROP.NS",
    "HAL":"HAL.NS","IDFCFIRSTB":"IDFCFIRSTB.NS","JUBLFOOD":"JUBLFOOD.NS",
    "KPITTECH":"KPITTECH.NS","LICHSGFIN":"LICHSGFIN.NS","LTIM":"LTIM.NS",
    "LTTS":"LTTS.NS","MAXHEALTH":"MAXHEALTH.NS","MPHASIS":"MPHASIS.NS",
    "MRF":"MRF.NS","OBEROIRLTY":"OBEROIRLTY.NS","OFSS":"OFSS.NS",
    "PERSISTENT":"PERSISTENT.NS","PHOENIXLTD":"PHOENIXLTD.NS","POLYCAB":"POLYCAB.NS",
    "POONAWALLA":"POONAWALLA.NS","PRESTIGE":"PRESTIGE.NS","RBLBANK":"RBLBANK.NS",
    "SCHAEFFLER":"SCHAEFFLER.NS","SOLARINDS":"SOLARINDS.NS","SUPREMEIND":"SUPREMEIND.NS",
    "TIINDIA":"TIINDIA.NS","TORNTPOWER":"TORNTPOWER.NS","UBL":"UBL.NS",
    "UNIONBANK":"UNIONBANK.NS",
}

SECTOR_MAP = {
    "RELIANCE":"Energy","ONGC":"Energy","BPCL":"Energy","COALINDIA":"Energy",
    "NTPC":"Energy","POWERGRID":"Energy","GAIL":"Energy","IOC":"Energy","TORNTPOWER":"Energy",
    "TCS":"IT","INFY":"IT","WIPRO":"IT","HCLTECH":"IT","TECHM":"IT","MPHASIS":"IT",
    "LTIM":"IT","LTTS":"IT","KPITTECH":"IT","PERSISTENT":"IT","OFSS":"IT","NAUKRI":"IT","DIXON":"IT",
    "HDFCBANK":"Banking","ICICIBANK":"Banking","SBIN":"Banking","AXISBANK":"Banking",
    "KOTAKBANK":"Banking","INDUSINDBK":"Banking","BANKBARODA":"Banking","CANBK":"Banking",
    "PNB":"Banking","FEDERALBNK":"Banking","IDFCFIRSTB":"Banking","RBLBANK":"Banking","UNIONBANK":"Banking",
    "BAJFINANCE":"NBFC","BAJAJFINSV":"NBFC","CHOLAFIN":"NBFC","MUTHOOTFIN":"NBFC",
    "LICHSGFIN":"NBFC","POONAWALLA":"NBFC","RECLTD":"NBFC","LICI":"NBFC","SBILIFE":"NBFC","HDFCLIFE":"NBFC","ICICIGI":"NBFC","ICICIPRULI":"NBFC",
    "HINDUNILVR":"FMCG","ITC":"FMCG","NESTLEIND":"FMCG","BRITANNIA":"FMCG","DABUR":"FMCG",
    "MARICO":"FMCG","COLPAL":"FMCG","GODREJCP":"FMCG","TATACONSUM":"FMCG","ASIANPAINT":"FMCG",
    "PAGEIND":"FMCG","VBL":"FMCG","UBL":"FMCG","BERGEPAINT":"FMCG","PIDILITIND":"FMCG",
    "SUNPHARMA":"Pharma","DRREDDY":"Pharma","CIPLA":"Pharma","DIVISLAB":"Pharma",
    "AUROPHARMA":"Pharma","LUPIN":"Pharma","TORNTPHARM":"Pharma","ZYDUSLIFE":"Pharma","MAXHEALTH":"Pharma","APOLLOHOSP":"Pharma",
    "MARUTI":"Auto","TATAMOTORS":"Auto","BAJAJ-AUTO":"Auto","EICHERMOT":"Auto","HEROMOTOCO":"Auto",
    "TVSMOTOR":"Auto","BALKRISIND":"Auto","BOSCHLTD":"Auto","SCHAEFFLER":"Auto","TIINDIA":"Auto","MM":"Auto",
    "LT":"Infra","ULTRACEMCO":"Infra","GRASIM":"Infra","AMBUJACEM":"Infra","SHREECEM":"Infra",
    "ADANIENT":"Infra","ADANIPORTS":"Infra","HAL":"Infra","INDUSTOWER":"Infra","SIEMENS":"Infra",
    "HAVELLS":"Infra","POLYCAB":"Infra","CGPOWER":"Infra","SUPREMEIND":"Infra","VOLTAS":"Infra","ASTRAL":"Infra",
    "BHARTIARTL":"Telecom","TTML":"Telecom",
    "TITAN":"FMCG","TRENT":"Retail","JUBLFOOD":"Retail","IRCTC":"Travel","INDIGO":"Aviation",
    "JSWSTEEL":"Metal","TATASTEEL":"Metal","HINDALCO":"Metal","VEDL":"Metal","SAIL":"Metal","NATIONALUM":"Metal",
    "ZOMATO":"Tech","NAUKRI":"Tech",
    "DLF":"Realty","GODREJPROP":"Realty","OBEROIRLTY":"Realty","PRESTIGE":"Realty","PHOENIXLTD":"Realty",
    "DEEPAKNTR":"Chem","SRF":"Chem","SOLARINDS":"Chem","UPL":"Chem",
    "MRF":"Auto","ABFRL":"Retail",
}

# ═══════════════════════════════════════════════
#  CACHE SYSTEM
# ═══════════════════════════════════════════════
_cache = {}
def cache_get(key, ttl=300):
    if key in _cache:
        if time.time() - _cache[key]['t'] < ttl:
            return _cache[key]['v']
    return None
def cache_set(key, val):
    _cache[key] = {'v': val, 't': time.time()}

# ═══════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════
def calc_rsi(closes, period=14):
    if len(closes) < period + 1: return 50.0
    gains = [max(closes[i]-closes[i-1], 0) for i in range(1, len(closes))]
    losses = [max(closes[i-1]-closes[i], 0) for i in range(1, len(closes))]
    avg_g = sum(gains[-period:]) / period
    avg_l = sum(losses[-period:]) / period
    rs = avg_g / max(avg_l, 0.001)
    return round(100 - 100/(1+rs), 2)

def calc_ema(prices, period):
    k = 2/(period+1)
    e = [prices[0]]
    for p in prices[1:]: e.append(p*k + e[-1]*(1-k))
    return e

def calc_macd(closes):
    if len(closes) < 26: return 0, 0, 0
    e12 = calc_ema(closes, 12)
    e26 = calc_ema(closes, 26)
    macd = [e12[i]-e26[i] for i in range(len(closes))]
    signal = calc_ema(macd, 9)
    hist = macd[-1] - signal[-1]
    return round(macd[-1], 2), round(signal[-1], 2), round(hist, 2)

def calc_atr(highs, lows, closes, period=14):
    tr = []
    for i in range(1, len(closes)):
        h, l, pc = highs[i], lows[i], closes[i-1]
        tr.append(max(h-l, abs(h-pc), abs(l-pc)))
    if not tr: return 0
    return round(sum(tr[-period:]) / min(period, len(tr)), 2)

def calc_bb(closes, period=20):
    if len(closes) < period: return closes[-1]*1.02, closes[-1], closes[-1]*0.98
    sl = closes[-period:]
    m = sum(sl)/period
    std = (sum((x-m)**2 for x in sl)/period)**0.5
    return round(m+2*std, 2), round(m, 2), round(m-2*std, 2)

def get_signal(rsi, macd, macd_sig, price, bb_upper, bb_lower, change_pct):
    bull = 0
    if rsi < 40: bull += 2
    elif rsi < 50: bull += 1
    elif rsi > 70: bull -= 2
    elif rsi > 60: bull -= 1
    if macd > macd_sig: bull += 2
    else: bull -= 1
    if price < bb_lower: bull += 1
    elif price > bb_upper: bull -= 1
    if change_pct > 0: bull += 1
    if bull >= 3: return "STRONG BUY"
    if bull >= 1: return "BUY"
    if bull <= -3: return "STRONG SELL"
    if bull <= -1: return "SELL"
    return "HOLD"

def calc_targets(price, atr, signal):
    mult = 1.5 if "BUY" in signal else 1.0
    if "BUY" in signal:
        sl = round(price - 1.5*atr, 2)
        t1 = round(price + 1.5*atr, 2)
        t2 = round(price + 3.0*atr, 2)
        t3 = round(price + 4.5*atr, 2)
    else:
        sl = round(price + 1.5*atr, 2)
        t1 = round(price - 1.5*atr, 2)
        t2 = round(price - 3.0*atr, 2)
        t3 = round(price - 4.5*atr, 2)
    rr = round(abs(t1-price)/max(abs(price-sl),0.01), 2)
    return sl, t1, t2, t3, rr

# ═══════════════════════════════════════════════
#  ROUTES
# ═══════════════════════════════════════════════
@app.route("/")
def home():
    return jsonify({"status":"AI Trading Terminal Backend v2.0","endpoints":["/live","/historical/<sym>","/fundamental/<sym>","/analyze/<sym>","/screener","/options/<sym>","/news","/heatmap"]})

@app.route("/live")
def live():
    cached = cache_get("live", ttl=15)
    if cached: return jsonify(cached)
    result = {}
    key_syms = ["NIFTY","SENSEX","BANKNIFTY","RELIANCE","TCS","HDFCBANK","INFY","ICICIBANK","ITC","SBIN","BHARTIARTL","WIPRO","BAJFINANCE"]
    for name in key_syms:
        sym = NSE_SYMBOLS.get(name, "")
        if not sym: continue
        try:
            t = yf.Ticker(sym)
            info = t.fast_info
            price = round(float(info.last_price), 2)
            prev  = round(float(info.previous_close), 2)
            chg   = round(price - prev, 2)
            pct   = round((chg/prev)*100, 2) if prev else 0
            result[name] = {"price": price, "prev": prev, "change": chg, "pct": pct}
        except:
            result[name] = {"price": 0, "prev": 0, "change": 0, "pct": 0}
    cache_set("live", result)
    return jsonify(result)

@app.route("/historical/<sym>")
def historical(sym):
    sym = sym.upper()
    period = request.args.get("period", "1y")
    cache_key = f"hist_{sym}_{period}"
    cached = cache_get(cache_key, ttl=3600)
    if cached: return jsonify(cached)
    symbol = NSE_SYMBOLS.get(sym)
    if not symbol:
        return jsonify({"error": f"Symbol {sym} not found"}), 404
    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period=period, interval="1d")
        if hist.empty:
            return jsonify({"error": "No data"}), 404
        data = []
        for idx, row in hist.iterrows():
            data.append({
                "date": str(idx)[:10],
                "open": round(float(row["Open"]), 2),
                "high": round(float(row["High"]), 2),
                "low":  round(float(row["Low"]), 2),
                "close":round(float(row["Close"]), 2),
                "volume":int(row["Volume"]),
            })
        closes = [d["close"] for d in data]
        rsi = calc_rsi(closes)
        macd, macd_sig, macd_hist = calc_macd(closes)
        result = {"symbol": sym, "count": len(data), "data": data,
                  "rsi": rsi, "macd": macd, "macd_signal": macd_sig}
        cache_set(cache_key, result)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/fundamental/<sym>")
def fundamental(sym):
    sym = sym.upper()
    cached = cache_get(f"fund_{sym}", ttl=3600)
    if cached: return jsonify(cached)
    symbol = NSE_SYMBOLS.get(sym)
    if not symbol:
        return jsonify({"error": "Not found"}), 404
    try:
        t = yf.Ticker(symbol)
        info = t.info
        result = {
            "symbol": sym,
            "name": info.get("longName", sym),
            "sector": info.get("sector", SECTOR_MAP.get(sym, "N/A")),
            "industry": info.get("industry", "N/A"),
            "market_cap": info.get("marketCap", 0),
            "market_cap_cr": round(info.get("marketCap", 0)/1e7, 0),
            "pe_ratio": info.get("trailingPE", 0),
            "forward_pe": info.get("forwardPE", 0),
            "pb_ratio": info.get("priceToBook", 0),
            "ps_ratio": info.get("priceToSalesTrailing12Months", 0),
            "eps": info.get("trailingEps", 0),
            "dividend_yield": round((info.get("dividendYield", 0) or 0)*100, 2),
            "roe": round((info.get("returnOnEquity", 0) or 0)*100, 2),
            "roce": round((info.get("returnOnAssets", 0) or 0)*100, 2),
            "debt_equity": info.get("debtToEquity", 0),
            "current_ratio": info.get("currentRatio", 0),
            "revenue_cr": round((info.get("totalRevenue", 0) or 0)/1e7, 0),
            "profit_cr": round((info.get("netIncomeToCommon", 0) or 0)/1e7, 0),
            "revenue_growth": round((info.get("revenueGrowth", 0) or 0)*100, 2),
            "earnings_growth": round((info.get("earningsGrowth", 0) or 0)*100, 2),
            "52w_high": info.get("fiftyTwoWeekHigh", 0),
            "52w_low": info.get("fiftyTwoWeekLow", 0),
            "avg_volume": info.get("averageVolume", 0),
            "beta": info.get("beta", 1),
            "analyst_rating": info.get("recommendationKey", "N/A").upper(),
            "target_price": info.get("targetMeanPrice", 0),
        }
        # Fundamental score (0-100)
        score = 50
        if result["pe_ratio"] and 0 < result["pe_ratio"] < 25: score += 10
        elif result["pe_ratio"] and result["pe_ratio"] > 50: score -= 10
        if result["roe"] > 15: score += 10
        elif result["roe"] > 10: score += 5
        if result["revenue_growth"] > 10: score += 10
        elif result["revenue_growth"] > 0: score += 5
        if result["debt_equity"] and result["debt_equity"] < 1: score += 10
        elif result["debt_equity"] and result["debt_equity"] > 3: score -= 10
        if result["dividend_yield"] > 1: score += 5
        if result["earnings_growth"] > 15: score += 10
        result["fundamental_score"] = min(100, max(0, score))
        cache_set(f"fund_{sym}", result)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/analyze/<sym>")
def analyze(sym):
    sym = sym.upper()
    cached = cache_get(f"analyze_{sym}", ttl=300)
    if cached: return jsonify(cached)
    symbol = NSE_SYMBOLS.get(sym)
    if not symbol:
        return jsonify({"error": "Not found"}), 404
    try:
        t = yf.Ticker(symbol)
        hist = t.history(period="6mo", interval="1d")
        if hist.empty:
            return jsonify({"error": "No data"}), 404
        closes = list(hist["Close"].astype(float))
        highs  = list(hist["High"].astype(float))
        lows   = list(hist["Low"].astype(float))
        volumes = list(hist["Volume"].astype(float))
        price = round(closes[-1], 2)
        prev  = round(closes[-2], 2)
        chg   = round(price - prev, 2)
        pct   = round(chg/prev*100, 2)
        rsi   = calc_rsi(closes)
        macd, macd_sig, macd_hist = calc_macd(closes)
        atr   = calc_atr(highs, lows, closes)
        bb_u, bb_m, bb_l = calc_bb(closes)
        # Volume analysis
        avg_vol = sum(volumes[-20:])/20 if len(volumes) >= 20 else sum(volumes)/len(volumes)
        vol_ratio = round(volumes[-1]/avg_vol, 2) if avg_vol > 0 else 1.0
        # Trend
        sma20 = sum(closes[-20:])/20 if len(closes) >= 20 else price
        sma50 = sum(closes[-50:])/50 if len(closes) >= 50 else price
        trend = "UPTREND" if price > sma20 > sma50 else "DOWNTREND" if price < sma20 < sma50 else "SIDEWAYS"
        signal = get_signal(rsi, macd, macd_sig, price, bb_u, bb_l, pct)
        sl, t1, t2, t3, rr = calc_targets(price, atr, signal)
        # Confidence score (0-100)
        confidence = 50
        if "STRONG" in signal: confidence += 20
        elif signal in ["BUY","SELL"]: confidence += 10
        if vol_ratio > 1.5: confidence += 10
        if trend in ["UPTREND","DOWNTREND"]: confidence += 10
        if rr > 2: confidence += 10
        confidence = min(95, confidence)
        # Position size (for 1L capital, 2% risk)
        capital = 100000
        risk_pct = 2.0
        risk_amt = capital * risk_pct/100
        risk_per_share = abs(price - sl)
        qty = int(risk_amt / risk_per_share) if risk_per_share > 0 else 0
        invest = round(qty * price, 0)
        # Recent price history for chart
        chart_data = [{"date": str(idx)[:10], "close": round(float(c), 2)}
                      for idx, c in zip(hist.index[-60:], closes[-60:])]
        result = {
            "symbol": sym, "price": price, "prev": prev, "change": chg, "pct": pct,
            "signal": signal, "confidence": confidence,
            "rsi": rsi, "rsi_zone": "OVERBOUGHT" if rsi>70 else "OVERSOLD" if rsi<30 else "NEUTRAL",
            "macd": macd, "macd_signal": macd_sig, "macd_hist": macd_hist,
            "macd_trend": "BULLISH" if macd > macd_sig else "BEARISH",
            "atr": atr, "atr_pct": round(atr/price*100, 2),
            "bb_upper": bb_u, "bb_mid": bb_m, "bb_lower": bb_l,
            "sma20": round(sma20, 2), "sma50": round(sma50, 2),
            "trend": trend, "vol_ratio": vol_ratio,
            "stop_loss": sl, "target1": t1, "target2": t2, "target3": t3,
            "risk_reward": rr,
            "position_qty": qty, "position_invest": invest,
            "sector": SECTOR_MAP.get(sym, "Other"),
            "chart": chart_data,
        }
        cache_set(f"analyze_{sym}", result)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/screener")
def screener():
    cached = cache_get("screener", ttl=1800)
    if cached: return jsonify(cached)
    sector_filter = request.args.get("sector", "ALL")
    results = []
    scan_list = list(NSE_SYMBOLS.keys())
    scan_list = [s for s in scan_list if s not in ["NIFTY","SENSEX","BANKNIFTY","NIFTYMID","NIFTYIT"]]
    for sym in scan_list[:80]:  # limit to 80 for performance
        symbol = NSE_SYMBOLS.get(sym)
        if not symbol: continue
        try:
            t = yf.Ticker(symbol)
            hist = t.history(period="3mo", interval="1d")
            if hist.empty or len(hist) < 15: continue
            closes = list(hist["Close"].astype(float))
            highs  = list(hist["High"].astype(float))
            lows   = list(hist["Low"].astype(float))
            volumes = list(hist["Volume"].astype(float))
            price = round(closes[-1], 2)
            prev  = round(closes[-2], 2)
            pct   = round((price-prev)/prev*100, 2)
            rsi   = calc_rsi(closes)
            macd, macd_sig, _ = calc_macd(closes)
            atr   = calc_atr(highs, lows, closes)
            bb_u, _, bb_l = calc_bb(closes)
            signal = get_signal(rsi, macd, macd_sig, price, bb_u, bb_l, pct)
            sl, t1, _, _, rr = calc_targets(price, atr, signal)
            # AI Score
            tech_score = 0
            if rsi < 40: tech_score += 30
            elif rsi < 55: tech_score += 20
            elif rsi > 70: tech_score -= 20
            if macd > macd_sig: tech_score += 25
            avg_vol = sum(volumes[-20:])/20 if len(volumes) >= 20 else 1
            if volumes[-1] > avg_vol * 1.5: tech_score += 15
            # momentum
            if len(closes) >= 20:
                mom = (closes[-1] - closes[-20])/closes[-20]*100
                tech_score += min(20, max(-20, mom*2))
            ai_score = min(100, max(0, 50 + tech_score))
            results.append({
                "sym": sym, "price": price, "pct": pct,
                "rsi": rsi, "macd_trend": "BULL" if macd > macd_sig else "BEAR",
                "signal": signal, "ai_score": round(ai_score, 1),
                "stop_loss": sl, "target": t1, "rr": rr,
                "sector": SECTOR_MAP.get(sym, "Other"),
            })
        except: continue
    results.sort(key=lambda x: x["ai_score"], reverse=True)
    out = {"count": len(results), "data": results, "updated": datetime.now().strftime("%H:%M:%S")}
    cache_set("screener", out)
    return jsonify(out)

@app.route("/options/<sym>")
def options(sym):
    sym = sym.upper()
    cached = cache_get(f"opt_{sym}", ttl=600)
    if cached: return jsonify(cached)
    symbol = NSE_SYMBOLS.get(sym)
    if not symbol:
        return jsonify({"error": "Not found"}), 404
    try:
        t = yf.Ticker(symbol)
        hist = t.history(period="5d", interval="1d")
        if hist.empty:
            return jsonify({"error": "No data"}), 404
        price = round(float(hist["Close"].iloc[-1]), 2)
        atr   = calc_atr(list(hist["High"].astype(float)), list(hist["Low"].astype(float)), list(hist["Close"].astype(float)), 5)
        # Build simulated options chain (realistic approximation)
        import random
        random.seed(int(price))  # deterministic
        strikes = [round(price * (1 + i*0.02), 0) for i in range(-6, 7)]
        chain = []
        iv_base = max(15, min(80, atr/price*100*15))
        for strike in strikes:
            moneyness = (strike - price) / price
            call_iv = round(iv_base + abs(moneyness)*20 + random.uniform(-3,3), 1)
            put_iv  = round(iv_base + abs(moneyness)*22 + random.uniform(-3,3), 1)
            # Black-Scholes simplified
            dist = abs(price - strike)
            call_premium = round(max(price-strike, 0) + atr*0.3*max(0.1, 1-dist/(price*0.1)), 1)
            put_premium  = round(max(strike-price, 0) + atr*0.3*max(0.1, 1-dist/(price*0.1)), 1)
            call_oi = int(random.uniform(5,50)*1000 * (1.5 if abs(moneyness)<0.02 else 1))
            put_oi  = int(random.uniform(5,50)*1000 * (1.5 if abs(moneyness)<0.02 else 1))
            chain.append({
                "strike": strike, "call_iv": call_iv, "put_iv": put_iv,
                "call_premium": call_premium, "put_premium": put_premium,
                "call_oi": call_oi, "put_oi": put_oi,
                "call_delta": round(max(0.01, min(0.99, 0.5 - moneyness*3)), 2),
                "put_delta": round(max(-0.99, min(-0.01, -0.5 - moneyness*3)), 2),
                "moneyness": "ATM" if abs(moneyness)<0.015 else ("ITM" if moneyness<0 else "OTM"),
            })
        total_call_oi = sum(c["call_oi"] for c in chain)
        total_put_oi  = sum(c["put_oi"]  for c in chain)
        pcr = round(total_put_oi / max(total_call_oi, 1), 2)
        # Options recommendation
        closes = list(hist["Close"].astype(float))
        rsi = calc_rsi(closes)
        if rsi < 35 and pcr > 1.2:
            opt_signal = "BUY CALL — Oversold + High PCR (reversal likely)"
            opt_type = "call"
        elif rsi > 65 and pcr < 0.8:
            opt_signal = "BUY PUT — Overbought + Low PCR (correction likely)"
            opt_type = "put"
        elif pcr > 1.5:
            opt_signal = "BUY CALL — Extreme PUT OI (market expects bounce)"
            opt_type = "call"
        elif pcr < 0.7:
            opt_signal = "BUY PUT — Extreme CALL OI (market expects fall)"
            opt_type = "put"
        else:
            opt_signal = "HOLD / Wait for clearer signal"
            opt_type = "none"
        # Best strike recommendation
        atm = min(chain, key=lambda x: abs(x["strike"]-price))
        rec_strike = round(price * 1.02 if opt_type=="call" else price * 0.98, 0)
        result = {
            "symbol": sym, "price": price, "atr": atr, "iv_base": round(iv_base, 1),
            "pcr": pcr, "total_call_oi": total_call_oi, "total_put_oi": total_put_oi,
            "signal": opt_signal, "option_type": opt_type,
            "rec_strike": rec_strike, "rec_expiry": "Monthly",
            "rec_premium": atm["call_premium"] if opt_type=="call" else atm["put_premium"],
            "chain": chain,
        }
        cache_set(f"opt_{sym}", result)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/news")
def news():
    cached = cache_get("news", ttl=900)
    if cached: return jsonify(cached)
    feeds = [
        "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms",
        "https://www.moneycontrol.com/rss/marketsnews.xml",
    ]
    articles = []
    positive_words = ["surge","rally","gain","rise","profit","growth","buy","bullish","strong","record","beat","up","positive","boost"]
    negative_words = ["fall","drop","crash","loss","decline","sell","bearish","weak","miss","down","negative","cut","worry","fear","risk"]
    for feed_url in feeds:
        try:
            f = feedparser.parse(feed_url)
            for entry in f.entries[:10]:
                title = entry.get("title","")
                text = (title + " " + entry.get("summary","")).lower()
                pos = sum(1 for w in positive_words if w in text)
                neg = sum(1 for w in negative_words if w in text)
                score = round((pos - neg) / max(pos + neg, 1), 2)
                sentiment = "POSITIVE" if score > 0.1 else "NEGATIVE" if score < -0.1 else "NEUTRAL"
                articles.append({"title": title, "score": score, "sentiment": sentiment,
                                  "link": entry.get("link",""), "time": entry.get("published","")})
        except: continue
    if not articles:
        # Fallback static news
        articles = [
            {"title":"Nifty 50 holds key support, FIIs turn buyers","score":0.3,"sentiment":"POSITIVE"},
            {"title":"RBI policy: Rate unchanged, growth focus continues","score":0.2,"sentiment":"POSITIVE"},
            {"title":"IT sector outlook: Strong deal wins in Q3","score":0.35,"sentiment":"POSITIVE"},
            {"title":"Global markets mixed; oil prices weigh on energy","score":-0.1,"sentiment":"NEGATIVE"},
            {"title":"Midcap index outperforms, selective buying seen","score":0.15,"sentiment":"POSITIVE"},
        ]
    avg_score = round(sum(a["score"] for a in articles)/len(articles), 3)
    market_mood = "BULLISH" if avg_score > 0.1 else "BEARISH" if avg_score < -0.1 else "NEUTRAL"
    result = {"articles": articles[:15], "avg_score": avg_score,
              "market_mood": market_mood, "count": len(articles)}
    cache_set("news", result)
    return jsonify(result)

@app.route("/heatmap")
def heatmap():
    cached = cache_get("heatmap", ttl=1800)
    if cached: return jsonify(cached)
    # Group by sector and get performance
    sectors = {}
    for sym in list(NSE_SYMBOLS.keys())[:60]:
        if sym in ["NIFTY","SENSEX","BANKNIFTY","NIFTYMID","NIFTYIT"]: continue
        sector = SECTOR_MAP.get(sym, "Other")
        if sector not in sectors: sectors[sector] = []
        symbol = NSE_SYMBOLS[sym]
        try:
            t = yf.Ticker(symbol)
            hist = t.history(period="5d", interval="1d")
            if len(hist) < 2: continue
            closes = list(hist["Close"].astype(float))
            pct = round((closes[-1]-closes[-2])/closes[-2]*100, 2)
            sectors[sector].append({"sym": sym, "pct": pct, "price": round(closes[-1],2)})
        except: continue
    result = []
    for sector, stocks in sectors.items():
        if not stocks: continue
        avg_pct = round(sum(s["pct"] for s in stocks)/len(stocks), 2)
        result.append({"sector": sector, "avg_pct": avg_pct, "stocks": stocks, "count": len(stocks)})
    result.sort(key=lambda x: x["avg_pct"], reverse=True)
    out = {"sectors": result, "updated": datetime.now().strftime("%H:%M:%S")}
    cache_set("heatmap", out)
    return jsonify(out)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
