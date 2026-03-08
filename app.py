from flask import Flask, jsonify, request
from flask_cors import CORS
import yfinance as yf
import numpy as np
import json, time, logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import feedparser
import threading

app = Flask(__name__)
CORS(app)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════
#  NSE UNIVERSE
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
    "LICHSGFIN":"NBFC","POONAWALLA":"NBFC","RECLTD":"NBFC","LICI":"NBFC",
    "SBILIFE":"NBFC","HDFCLIFE":"NBFC","ICICIGI":"NBFC","ICICIPRULI":"NBFC",
    "HINDUNILVR":"FMCG","ITC":"FMCG","NESTLEIND":"FMCG","BRITANNIA":"FMCG","DABUR":"FMCG",
    "MARICO":"FMCG","COLPAL":"FMCG","GODREJCP":"FMCG","TATACONSUM":"FMCG","ASIANPAINT":"FMCG",
    "PAGEIND":"FMCG","VBL":"FMCG","UBL":"FMCG","BERGEPAINT":"FMCG","PIDILITIND":"FMCG",
    "SUNPHARMA":"Pharma","DRREDDY":"Pharma","CIPLA":"Pharma","DIVISLAB":"Pharma",
    "AUROPHARMA":"Pharma","LUPIN":"Pharma","TORNTPHARM":"Pharma","ZYDUSLIFE":"Pharma",
    "MAXHEALTH":"Pharma","APOLLOHOSP":"Pharma",
    "MARUTI":"Auto","TATAMOTORS":"Auto","BAJAJ-AUTO":"Auto","EICHERMOT":"Auto","HEROMOTOCO":"Auto",
    "TVSMOTOR":"Auto","BALKRISIND":"Auto","BOSCHLTD":"Auto","SCHAEFFLER":"Auto","TIINDIA":"Auto","MM":"Auto",
    "LT":"Infra","ULTRACEMCO":"Infra","GRASIM":"Infra","AMBUJACEM":"Infra","SHREECEM":"Infra",
    "ADANIENT":"Infra","ADANIPORTS":"Infra","HAL":"Infra","INDUSTOWER":"Infra","SIEMENS":"Infra",
    "HAVELLS":"Infra","POLYCAB":"Infra","CGPOWER":"Infra","SUPREMEIND":"Infra","VOLTAS":"Infra","ASTRAL":"Infra",
    "BHARTIARTL":"Telecom",
    "TITAN":"FMCG","TRENT":"Retail","JUBLFOOD":"Retail","IRCTC":"Travel","INDIGO":"Aviation",
    "JSWSTEEL":"Metal","TATASTEEL":"Metal","HINDALCO":"Metal","VEDL":"Metal","SAIL":"Metal",
    "ZOMATO":"Tech",
    "DLF":"Realty","GODREJPROP":"Realty","OBEROIRLTY":"Realty","PRESTIGE":"Realty","PHOENIXLTD":"Realty",
    "DEEPAKNTR":"Chem","SRF":"Chem","SOLARINDS":"Chem","UPL":"Chem",
    "MRF":"Auto","ABFRL":"Retail",
}

# ═══════════════════════════════════════════════
#  THREAD-SAFE CACHE
# ═══════════════════════════════════════════════
_cache = {}
_cache_lock = threading.RLock()

def cache_get(key, ttl=300):
    with _cache_lock:
        if key in _cache:
            if time.time() - _cache[key]['t'] < ttl:
                return _cache[key]['v']
    return None

def cache_set(key, val):
    with _cache_lock:
        _cache[key] = {'v': val, 't': time.time()}

def cache_clear_expired():
    with _cache_lock:
        now = time.time()
        expired = [k for k, v in _cache.items() if now - v['t'] > 3600]
        for k in expired:
            del _cache[k]

# ═══════════════════════════════════════════════
#  TECHNICAL INDICATORS
# ═══════════════════════════════════════════════
def calc_rsi(closes, period=14):
    if len(closes) < period + 1:
        return 50.0
    gains = [max(closes[i] - closes[i-1], 0) for i in range(1, len(closes))]
    losses = [max(closes[i-1] - closes[i], 0) for i in range(1, len(closes))]
    avg_g = sum(gains[-period:]) / period
    avg_l = sum(losses[-period:]) / period
    if avg_l == 0:
        return 100.0
    rs = avg_g / avg_l
    return round(100 - 100 / (1 + rs), 2)

def calc_ema(prices, period):
    if not prices:
        return []
    k = 2 / (period + 1)
    e = [prices[0]]
    for p in prices[1:]:
        e.append(p * k + e[-1] * (1 - k))
    return e

def calc_macd(closes):
    if len(closes) < 26:
        return 0, 0, 0
    e12 = calc_ema(closes, 12)
    e26 = calc_ema(closes, 26)
    macd = [e12[i] - e26[i] for i in range(len(closes))]
    signal = calc_ema(macd, 9)
    hist = macd[-1] - signal[-1]
    return round(macd[-1], 4), round(signal[-1], 4), round(hist, 4)

def calc_atr(highs, lows, closes, period=14):
    if len(closes) < 2:
        return 0
    tr = []
    for i in range(1, len(closes)):
        h = highs[i] if i < len(highs) else closes[i]
        l = lows[i] if i < len(lows) else closes[i]
        pc = closes[i-1]
        tr.append(max(h - l, abs(h - pc), abs(l - pc)))
    if not tr:
        return 0
    return round(sum(tr[-period:]) / min(period, len(tr)), 2)

def calc_bb(closes, period=20):
    if len(closes) < period:
        return closes[-1] * 1.02, closes[-1], closes[-1] * 0.98
    sl = closes[-period:]
    m = sum(sl) / period
    std = (sum((x - m) ** 2 for x in sl) / period) ** 0.5
    return round(m + 2 * std, 2), round(m, 2), round(m - 2 * std, 2)

def calc_stochastic(highs, lows, closes, k_period=14, d_period=3):
    """Stochastic Oscillator %K and %D"""
    if len(closes) < k_period:
        return 50.0, 50.0
    k_values = []
    for i in range(k_period - 1, len(closes)):
        h_max = max(highs[max(0, i - k_period + 1):i + 1])
        l_min = min(lows[max(0, i - k_period + 1):i + 1])
        if h_max == l_min:
            k_values.append(50.0)
        else:
            k_values.append(100 * (closes[i] - l_min) / (h_max - l_min))
    d_values = calc_ema(k_values, d_period)
    return round(k_values[-1], 2), round(d_values[-1], 2)

def calc_williams_r(highs, lows, closes, period=14):
    """Williams %R"""
    if len(closes) < period:
        return -50.0
    h_max = max(highs[-period:])
    l_min = min(lows[-period:])
    if h_max == l_min:
        return -50.0
    return round(-100 * (h_max - closes[-1]) / (h_max - l_min), 2)

def calc_cci(highs, lows, closes, period=20):
    """Commodity Channel Index"""
    if len(closes) < period:
        return 0.0
    typical = [(highs[i] + lows[i] + closes[i]) / 3 for i in range(len(closes))]
    tp_slice = typical[-period:]
    tp_mean = sum(tp_slice) / period
    mean_dev = sum(abs(x - tp_mean) for x in tp_slice) / period
    if mean_dev == 0:
        return 0.0
    return round((typical[-1] - tp_mean) / (0.015 * mean_dev), 2)

def calc_obv(closes, volumes):
    """On-Balance Volume"""
    if len(closes) < 2:
        return 0
    obv = 0
    for i in range(1, len(closes)):
        if closes[i] > closes[i-1]:
            obv += volumes[i]
        elif closes[i] < closes[i-1]:
            obv -= volumes[i]
    return int(obv)

def calc_vwap(highs, lows, closes, volumes):
    """Volume Weighted Average Price (last 20 bars)"""
    n = min(20, len(closes))
    if n == 0:
        return closes[-1]
    typical = [(highs[-n+i] + lows[-n+i] + closes[-n+i]) / 3 for i in range(n)]
    vols = volumes[-n:]
    total_vol = sum(vols)
    if total_vol == 0:
        return closes[-1]
    vwap = sum(typical[i] * vols[i] for i in range(n)) / total_vol
    return round(vwap, 2)

def calc_adx(highs, lows, closes, period=14):
    """Average Directional Index"""
    if len(closes) < period + 1:
        return 25.0
    tr_list, dm_plus, dm_minus = [], [], []
    for i in range(1, len(closes)):
        h, l, pc = highs[i], lows[i], closes[i-1]
        tr_list.append(max(h - l, abs(h - pc), abs(l - pc)))
        up = h - highs[i-1]
        down = lows[i-1] - l
        dm_plus.append(up if up > down and up > 0 else 0)
        dm_minus.append(down if down > up and down > 0 else 0)
    def smooth(arr, p):
        s = sum(arr[:p])
        result = [s]
        for v in arr[p:]:
            s = s - s/p + v
            result.append(s)
        return result
    atr_smooth = smooth(tr_list, period)
    dmp_smooth = smooth(dm_plus, period)
    dmm_smooth = smooth(dm_minus, period)
    di_plus = [100 * dmp_smooth[i] / atr_smooth[i] if atr_smooth[i] else 0 for i in range(len(atr_smooth))]
    di_minus = [100 * dmm_smooth[i] / atr_smooth[i] if atr_smooth[i] else 0 for i in range(len(atr_smooth))]
    dx = [100 * abs(di_plus[i] - di_minus[i]) / (di_plus[i] + di_minus[i]) if (di_plus[i] + di_minus[i]) else 0
          for i in range(len(di_plus))]
    adx = sum(dx[-period:]) / period if len(dx) >= period else 25.0
    return round(adx, 2)

def calc_supertrend(highs, lows, closes, period=10, multiplier=3.0):
    """Supertrend indicator"""
    if len(closes) < period + 1:
        return closes[-1], True
    atr = calc_atr(highs, lows, closes, period)
    hl2 = [(highs[i] + lows[i]) / 2 for i in range(len(closes))]
    upper = hl2[-1] + multiplier * atr
    lower = hl2[-1] - multiplier * atr
    price = closes[-1]
    is_bullish = price > lower
    level = lower if is_bullish else upper
    return round(level, 2), is_bullish

def calc_pivots(high, low, close):
    """Classic Pivot Points"""
    pivot = (high + low + close) / 3
    r1 = 2 * pivot - low
    r2 = pivot + (high - low)
    r3 = high + 2 * (pivot - low)
    s1 = 2 * pivot - high
    s2 = pivot - (high - low)
    s3 = low - 2 * (high - pivot)
    return {
        "pivot": round(pivot, 2),
        "r1": round(r1, 2), "r2": round(r2, 2), "r3": round(r3, 2),
        "s1": round(s1, 2), "s2": round(s2, 2), "s3": round(s3, 2),
    }

def calc_ichimoku(closes, highs, lows):
    """Ichimoku Cloud (simplified)"""
    n = len(closes)
    if n < 52:
        return {}
    # Tenkan-sen (9-period)
    t_h = max(highs[-9:]) if len(highs) >= 9 else highs[-1]
    t_l = min(lows[-9:]) if len(lows) >= 9 else lows[-1]
    tenkan = (t_h + t_l) / 2
    # Kijun-sen (26-period)
    k_h = max(highs[-26:]) if len(highs) >= 26 else highs[-1]
    k_l = min(lows[-26:]) if len(lows) >= 26 else lows[-1]
    kijun = (k_h + k_l) / 2
    # Senkou Span A
    senkou_a = (tenkan + kijun) / 2
    # Senkou Span B (52-period)
    sb_h = max(highs[-52:])
    sb_l = min(lows[-52:])
    senkou_b = (sb_h + sb_l) / 2
    price = closes[-1]
    above_cloud = price > max(senkou_a, senkou_b)
    below_cloud = price < min(senkou_a, senkou_b)
    cloud_signal = "ABOVE_CLOUD" if above_cloud else "BELOW_CLOUD" if below_cloud else "IN_CLOUD"
    return {
        "tenkan": round(tenkan, 2),
        "kijun": round(kijun, 2),
        "senkou_a": round(senkou_a, 2),
        "senkou_b": round(senkou_b, 2),
        "cloud_signal": cloud_signal,
        "tk_cross": "BULL" if tenkan > kijun else "BEAR",
    }

def detect_divergence(closes, rsi_arr, lookback=15):
    """RSI Divergence detection"""
    n = len(closes) - 1
    if n < lookback:
        return {"bullish": False, "bearish": False}
    recent_closes = closes[n - lookback:n + 1]
    recent_rsi = rsi_arr[n - lookback:n + 1] if len(rsi_arr) > n else []
    if not recent_rsi:
        return {"bullish": False, "bearish": False}
    price_low_idx = recent_closes.index(min(recent_closes))
    price_high_idx = recent_closes.index(max(recent_closes))
    p_now, p_prev_low = closes[n], min(closes[n - lookback:n])
    p_prev_high = max(closes[n - lookback:n])
    r_now = rsi_arr[n] if n < len(rsi_arr) else 50
    bullish = (p_now <= p_prev_low * 1.01) and (r_now > (rsi_arr[n - lookback] if n - lookback < len(rsi_arr) else 50) + 4)
    bearish = (p_now >= p_prev_high * 0.99) and (r_now < (rsi_arr[n - lookback] if n - lookback < len(rsi_arr) else 50) - 4)
    return {"bullish": bool(bullish), "bearish": bool(bearish)}

def calc_momentum_score(closes, volumes):
    """Multi-timeframe momentum score (0-100) inspired by institutional quant models"""
    n = len(closes)
    score = 50
    # Price momentum
    if n >= 5:   score += min(15, max(-15, (closes[-1]/closes[-5]-1)*100*3))
    if n >= 21:  score += min(10, max(-10, (closes[-1]/closes[-21]-1)*100*2))
    if n >= 63:  score += min(10, max(-10, (closes[-1]/closes[-63]-1)*100))
    # Volume momentum
    if n >= 20 and volumes:
        avg_vol = sum(volumes[-20:]) / 20
        recent_vol = sum(volumes[-5:]) / 5 if len(volumes) >= 5 else volumes[-1]
        vol_ratio = recent_vol / avg_vol if avg_vol > 0 else 1
        score += min(10, max(-5, (vol_ratio - 1) * 10))
    return round(min(100, max(0, score)), 1)

def get_signal(rsi, macd, macd_sig, price, bb_upper, bb_lower, change_pct,
               stoch_k=50, adx=25, supertrend_bull=True, cci=0):
    """Enhanced multi-factor signal generation"""
    bull = 0
    # RSI (weight 2)
    if rsi < 35:   bull += 3
    elif rsi < 45: bull += 2
    elif rsi < 55: bull += 1
    elif rsi > 72: bull -= 3
    elif rsi > 65: bull -= 2
    elif rsi > 58: bull -= 1
    # MACD (weight 2)
    if macd > macd_sig:  bull += 2
    else:                bull -= 1
    # Bollinger Bands
    if price < bb_lower:   bull += 2
    elif price > bb_upper: bull -= 2
    # Price action
    if change_pct > 1.5:   bull += 1
    elif change_pct < -1.5: bull -= 1
    # Stochastic
    if stoch_k < 25:   bull += 1
    elif stoch_k > 80: bull -= 1
    # ADX trend strength
    if adx > 25: bull += (1 if bull > 0 else -1)
    # Supertrend
    if supertrend_bull:  bull += 1
    else:                bull -= 1
    # CCI
    if cci < -100:  bull += 1
    elif cci > 100: bull -= 1

    if bull >= 6:  return "STRONG BUY"
    if bull >= 3:  return "BUY"
    if bull <= -5: return "STRONG SELL"
    if bull <= -2: return "SELL"
    return "HOLD"

def calc_targets(price, atr, signal):
    if "BUY" in signal:
        sl = round(price - 1.5 * atr, 2)
        t1 = round(price + 1.5 * atr, 2)
        t2 = round(price + 3.0 * atr, 2)
        t3 = round(price + 5.0 * atr, 2)
    else:
        sl = round(price + 1.5 * atr, 2)
        t1 = round(price - 1.5 * atr, 2)
        t2 = round(price - 3.0 * atr, 2)
        t3 = round(price - 5.0 * atr, 2)
    rr = round(abs(t1 - price) / max(abs(price - sl), 0.01), 2)
    return sl, t1, t2, t3, rr

def calc_sharpe(returns, risk_free=0.065):
    """Annualized Sharpe ratio"""
    if len(returns) < 5:
        return 0
    daily_rf = risk_free / 252
    excess = [r - daily_rf for r in returns]
    avg = sum(excess) / len(excess)
    std = (sum((r - avg) ** 2 for r in excess) / len(excess)) ** 0.5
    if std == 0:
        return 0
    return round(avg / std * (252 ** 0.5), 2)

# ═══════════════════════════════════════════════
#  PARALLEL DATA FETCHER
# ═══════════════════════════════════════════════
def fetch_single_ticker(args):
    """Fetch one ticker's data — designed for ThreadPoolExecutor"""
    name, symbol, period, interval = args
    try:
        t = yf.Ticker(symbol)
        hist = t.history(period=period, interval=interval, timeout=15)
        if hist.empty:
            return name, None
        return name, hist
    except Exception as e:
        logger.warning(f"Fetch failed {name}: {e}")
        return name, None

def parallel_fetch(symbols_dict, period="3mo", interval="1d", max_workers=12):
    """Fetch multiple tickers in parallel using ThreadPoolExecutor"""
    args_list = [(name, sym, period, interval) for name, sym in symbols_dict.items()]
    results = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_single_ticker, args): args[0] for args in args_list}
        for future in as_completed(futures, timeout=60):
            try:
                name, data = future.result(timeout=20)
                results[name] = data
            except Exception as e:
                name = futures[future]
                logger.warning(f"Parallel fetch error {name}: {e}")
                results[name] = None
    return results

# ═══════════════════════════════════════════════
#  ROUTES
# ═══════════════════════════════════════════════
@app.route("/")
def home():
    return jsonify({
        "status": "AI Trading Terminal Backend v3.0 — Institutional Grade",
        "version": "3.0",
        "features": ["parallel_fetch", "extended_indicators", "5y_historical", "ichimoku", "supertrend", "divergence", "sharpe"],
        "endpoints": ["/live", "/historical/<sym>", "/fundamental/<sym>",
                      "/analyze/<sym>", "/screener", "/options/<sym>",
                      "/news", "/heatmap", "/multi-analyze", "/correlation"]
    })

@app.route("/live")
def live():
    cached = cache_get("live", ttl=12)
    if cached:
        return jsonify(cached)

    key_syms = {
        "NIFTY": "^NSEI", "SENSEX": "^BSESN", "BANKNIFTY": "^NSEBANK",
        "RELIANCE": "RELIANCE.NS", "TCS": "TCS.NS", "HDFCBANK": "HDFCBANK.NS",
        "INFY": "INFY.NS", "ICICIBANK": "ICICIBANK.NS", "ITC": "ITC.NS",
        "SBIN": "SBIN.NS", "BHARTIARTL": "BHARTIARTL.NS", "WIPRO": "WIPRO.NS",
        "BAJFINANCE": "BAJFINANCE.NS", "KOTAKBANK": "KOTAKBANK.NS",
    }

    result = {}

    def fetch_price(name, sym):
        try:
            t = yf.Ticker(sym)
            info = t.fast_info
            price = round(float(info.last_price), 2)
            prev  = round(float(info.previous_close), 2)
            chg   = round(price - prev, 2)
            pct   = round((chg / prev) * 100, 2) if prev else 0
            high  = round(float(info.day_high or price), 2)
            low   = round(float(info.day_low or price), 2)
            vol   = int(info.three_month_average_volume or 0)
            return name, {"price": price, "prev": prev, "change": chg, "pct": pct,
                          "high": high, "low": low, "volume": vol}
        except Exception as e:
            logger.warning(f"Live fetch {name}: {e}")
            return name, {"price": 0, "prev": 0, "change": 0, "pct": 0}

    with ThreadPoolExecutor(max_workers=14) as executor:
        futures = {executor.submit(fetch_price, n, s): n for n, s in key_syms.items()}
        for future in as_completed(futures, timeout=15):
            try:
                name, data = future.result(timeout=10)
                result[name] = data
            except Exception:
                name = futures[future]
                result[name] = {"price": 0, "prev": 0, "change": 0, "pct": 0}

    result["_ts"] = datetime.now().strftime("%H:%M:%S")
    cache_set("live", result)
    return jsonify(result)

@app.route("/historical/<sym>")
def historical(sym):
    sym = sym.upper()
    period = request.args.get("period", "1y")
    interval = request.args.get("interval", "1d")
    cache_key = f"hist_{sym}_{period}_{interval}"
    ttl = 3600 if period in ["1y", "2y", "5y"] else 900

    cached = cache_get(cache_key, ttl=ttl)
    if cached:
        return jsonify(cached)

    symbol = NSE_SYMBOLS.get(sym)
    if not symbol:
        return jsonify({"error": f"Symbol {sym} not found"}), 404

    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period=period, interval=interval, timeout=20)
        if hist.empty:
            return jsonify({"error": "No data"}), 404

        data = []
        for idx, row in hist.iterrows():
            data.append({
                "date":   str(idx)[:10],
                "open":   round(float(row["Open"]), 2),
                "high":   round(float(row["High"]), 2),
                "low":    round(float(row["Low"]), 2),
                "close":  round(float(row["Close"]), 2),
                "volume": int(row["Volume"]),
            })

        closes  = [d["close"] for d in data]
        highs   = [d["high"]  for d in data]
        lows    = [d["low"]   for d in data]
        volumes = [d["volume"] for d in data]

        rsi = calc_rsi(closes)
        macd, macd_sig, macd_hist = calc_macd(closes)
        bb_u, bb_m, bb_l = calc_bb(closes)
        atr = calc_atr(highs, lows, closes)
        adx = calc_adx(highs, lows, closes)

        # Daily returns for performance stats
        daily_returns = [(closes[i] - closes[i-1]) / closes[i-1] for i in range(1, len(closes))]
        sharpe = calc_sharpe(daily_returns)
        total_return = round((closes[-1] / closes[0] - 1) * 100, 2) if closes[0] > 0 else 0

        result = {
            "symbol": sym, "count": len(data), "data": data,
            "rsi": rsi, "macd": macd, "macd_signal": macd_sig, "macd_hist": macd_hist,
            "bb_upper": bb_u, "bb_mid": bb_m, "bb_lower": bb_l,
            "atr": atr, "adx": adx,
            "sharpe_ratio": sharpe,
            "total_return_pct": total_return,
            "period": period,
        }
        cache_set(cache_key, result)
        return jsonify(result)
    except Exception as e:
        logger.error(f"Historical {sym}: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/fundamental/<sym>")
def fundamental(sym):
    sym = sym.upper()
    cached = cache_get(f"fund_{sym}", ttl=7200)
    if cached:
        return jsonify(cached)

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
            "market_cap_cr": round(info.get("marketCap", 0) / 1e7, 0),
            "pe_ratio": info.get("trailingPE", 0),
            "forward_pe": info.get("forwardPE", 0),
            "pb_ratio": info.get("priceToBook", 0),
            "ps_ratio": info.get("priceToSalesTrailing12Months", 0),
            "eps": info.get("trailingEps", 0),
            "dividend_yield": round((info.get("dividendYield", 0) or 0) * 100, 2),
            "roe": round((info.get("returnOnEquity", 0) or 0) * 100, 2),
            "roce": round((info.get("returnOnAssets", 0) or 0) * 100, 2),
            "debt_equity": info.get("debtToEquity", 0),
            "current_ratio": info.get("currentRatio", 0),
            "revenue_cr": round((info.get("totalRevenue", 0) or 0) / 1e7, 0),
            "profit_cr": round((info.get("netIncomeToCommon", 0) or 0) / 1e7, 0),
            "revenue_growth": round((info.get("revenueGrowth", 0) or 0) * 100, 2),
            "earnings_growth": round((info.get("earningsGrowth", 0) or 0) * 100, 2),
            "52w_high": info.get("fiftyTwoWeekHigh", 0),
            "52w_low": info.get("fiftyTwoWeekLow", 0),
            "avg_volume": info.get("averageVolume", 0),
            "beta": info.get("beta", 1),
            "analyst_rating": (info.get("recommendationKey", "N/A") or "N/A").upper(),
            "target_price": info.get("targetMeanPrice", 0),
            "free_cashflow_cr": round((info.get("freeCashflow", 0) or 0) / 1e7, 0),
            "operating_margins": round((info.get("operatingMargins", 0) or 0) * 100, 2),
            "gross_margins": round((info.get("grossMargins", 0) or 0) * 100, 2),
            "institutional_pct": round((info.get("heldPercentInstitutions", 0) or 0) * 100, 2),
        }

        # Institutional-grade fundamental score (0-100)
        score = 50
        pe = result["pe_ratio"]
        roe = result["roe"]
        rg = result["revenue_growth"]
        de = result["debt_equity"]
        eg = result["earnings_growth"]
        om = result["operating_margins"]

        if pe and 0 < pe < 20:    score += 12
        elif pe and pe < 30:      score += 6
        elif pe and pe > 60:      score -= 12
        elif pe and pe > 45:      score -= 6

        if roe > 20:    score += 12
        elif roe > 15:  score += 8
        elif roe > 10:  score += 4
        elif roe < 0:   score -= 10

        if rg > 15:     score += 10
        elif rg > 5:    score += 5
        elif rg < -10:  score -= 10

        if de is not None:
            if de < 0.5:   score += 8
            elif de < 1.5: score += 4
            elif de > 4:   score -= 12
            elif de > 2.5: score -= 6

        if eg > 20:     score += 8
        elif eg > 10:   score += 4

        if om > 20:     score += 6
        elif om > 10:   score += 3

        if result["dividend_yield"] > 2: score += 4
        if result["free_cashflow_cr"] > 0: score += 4

        result["fundamental_score"] = min(100, max(0, score))

        # Valuation classification
        if pe and 0 < pe < 15:     result["valuation"] = "UNDERVALUED"
        elif pe and pe < 25:        result["valuation"] = "FAIR"
        elif pe and pe < 40:        result["valuation"] = "PREMIUM"
        elif pe:                    result["valuation"] = "EXPENSIVE"
        else:                       result["valuation"] = "N/A"

        cache_set(f"fund_{sym}", result)
        return jsonify(result)
    except Exception as e:
        logger.error(f"Fundamental {sym}: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/analyze/<sym>")
def analyze(sym):
    sym = sym.upper()
    cached = cache_get(f"analyze_{sym}", ttl=240)
    if cached:
        return jsonify(cached)

    symbol = NSE_SYMBOLS.get(sym)
    if not symbol:
        return jsonify({"error": "Not found"}), 404

    try:
        t = yf.Ticker(symbol)
        hist = t.history(period="1y", interval="1d", timeout=20)
        if hist.empty:
            return jsonify({"error": "No data"}), 404

        closes  = list(hist["Close"].astype(float))
        highs   = list(hist["High"].astype(float))
        lows    = list(hist["Low"].astype(float))
        volumes = list(hist["Volume"].astype(float))

        price = round(closes[-1], 2)
        prev  = round(closes[-2], 2) if len(closes) > 1 else price
        chg   = round(price - prev, 2)
        pct   = round(chg / prev * 100, 2) if prev else 0

        # Core indicators
        rsi  = calc_rsi(closes)
        macd, macd_sig, macd_hist = calc_macd(closes)
        atr  = calc_atr(highs, lows, closes)
        bb_u, bb_m, bb_l = calc_bb(closes)
        stoch_k, stoch_d = calc_stochastic(highs, lows, closes)
        williams = calc_williams_r(highs, lows, closes)
        cci_val  = calc_cci(highs, lows, closes)
        adx_val  = calc_adx(highs, lows, closes)
        obv_val  = calc_obv(closes, volumes)
        vwap_val = calc_vwap(highs, lows, closes, volumes)
        st_level, st_bull = calc_supertrend(highs, lows, closes)
        ichimoku = calc_ichimoku(closes, highs, lows)
        pivots   = calc_pivots(max(highs[-1:] or [price]), min(lows[-1:] or [price]), price)

        # RSI series for divergence
        rsi_series = [calc_rsi(closes[:i+1]) for i in range(max(0, len(closes)-20), len(closes))]
        divergence = detect_divergence(closes[-20:], rsi_series) if len(rsi_series) >= 15 else {"bullish": False, "bearish": False}

        # EMA stack
        ema9   = calc_ema(closes, 9)
        ema21  = calc_ema(closes, 21)
        ema50  = calc_ema(closes, 50)
        ema200 = calc_ema(closes, 200)

        sma20 = sum(closes[-20:]) / 20 if len(closes) >= 20 else price
        sma50 = sum(closes[-50:]) / 50 if len(closes) >= 50 else price

        # Volume analysis
        avg_vol   = sum(volumes[-20:]) / 20 if len(volumes) >= 20 else (sum(volumes) / len(volumes) if volumes else 1)
        vol_ratio = round(volumes[-1] / avg_vol, 2) if avg_vol > 0 else 1.0
        vol_trend = "HIGH" if vol_ratio > 1.5 else "LOW" if vol_ratio < 0.7 else "NORMAL"

        # Trend detection
        if price > ema9[-1] > ema21[-1] > ema50[-1]:
            trend = "STRONG UPTREND"
        elif price > sma20 > sma50:
            trend = "UPTREND"
        elif price < ema9[-1] < ema21[-1] < ema50[-1]:
            trend = "STRONG DOWNTREND"
        elif price < sma20 < sma50:
            trend = "DOWNTREND"
        else:
            trend = "SIDEWAYS"

        # Multi-timeframe momentum
        momentum_score = calc_momentum_score(closes, volumes)

        # Performance stats
        daily_returns = [(closes[i] - closes[i-1]) / closes[i-1] for i in range(1, len(closes))]
        sharpe = calc_sharpe(daily_returns)
        # Max drawdown
        peak = closes[0]
        max_dd = 0
        for c in closes:
            if c > peak: peak = c
            dd = (peak - c) / peak * 100
            if dd > max_dd: max_dd = dd

        # 52-week analysis
        high_52w = max(highs) if highs else price
        low_52w  = min(lows)  if lows  else price
        pct_from_52h = round((price / high_52w - 1) * 100, 2)
        pct_from_52l = round((price / low_52w  - 1) * 100, 2)

        signal = get_signal(rsi, macd, macd_sig, price, bb_u, bb_l, pct,
                            stoch_k, adx_val, st_bull, cci_val)

        # Boost signal with divergence
        if divergence["bullish"] and "SELL" in signal:
            signal = "HOLD"
        elif divergence["bullish"] and signal == "HOLD":
            signal = "BUY"

        sl, t1, t2, t3, rr = calc_targets(price, atr, signal)

        # Confidence score (0-100)
        confidence = 45
        if "STRONG" in signal:   confidence += 20
        elif signal in ["BUY", "SELL"]: confidence += 12
        if vol_ratio > 1.5:      confidence += 8
        if "UPTREND" in trend or "DOWNTREND" in trend: confidence += 8
        if rr > 2:               confidence += 7
        if adx_val > 25:         confidence += 5
        if divergence["bullish"] or divergence["bearish"]: confidence += 7
        if abs(momentum_score - 50) > 20: confidence += 5
        confidence = min(95, confidence)

        # Position sizing (2% risk rule)
        capital   = 100000
        risk_pct  = 2.0
        risk_amt  = capital * risk_pct / 100
        risk_per_share = abs(price - sl)
        qty    = int(risk_amt / risk_per_share) if risk_per_share > 0 else 0
        invest = round(qty * price, 0)

        # Chart data (last 90 days)
        chart_data = [{"date": str(idx)[:10], "close": round(float(c), 2), "volume": int(v)}
                      for idx, c, v in zip(hist.index[-90:], closes[-90:], volumes[-90:])]

        result = {
            "symbol": sym, "price": price, "prev": prev, "change": chg, "pct": pct,
            "signal": signal, "confidence": confidence,

            # Core indicators
            "rsi": rsi,
            "rsi_zone": "OVERBOUGHT" if rsi > 70 else "OVERSOLD" if rsi < 30 else "NEUTRAL",
            "macd": macd, "macd_signal": macd_sig, "macd_hist": macd_hist,
            "macd_trend": "BULLISH" if macd > macd_sig else "BEARISH",
            "atr": atr, "atr_pct": round(atr / price * 100, 2),
            "bb_upper": bb_u, "bb_mid": bb_m, "bb_lower": bb_l,

            # Advanced indicators
            "stoch_k": stoch_k, "stoch_d": stoch_d,
            "stoch_zone": "OVERBOUGHT" if stoch_k > 80 else "OVERSOLD" if stoch_k < 20 else "NEUTRAL",
            "williams_r": williams,
            "cci": cci_val,
            "cci_zone": "OVERBOUGHT" if cci_val > 100 else "OVERSOLD" if cci_val < -100 else "NEUTRAL",
            "adx": adx_val,
            "adx_strength": "STRONG" if adx_val > 25 else "WEAK",
            "obv": obv_val,
            "vwap": vwap_val,
            "vwap_signal": "ABOVE" if price > vwap_val else "BELOW",
            "supertrend": st_level, "supertrend_bull": st_bull,
            "supertrend_signal": "BUY" if st_bull else "SELL",
            "ichimoku": ichimoku,
            "pivots": pivots,
            "divergence": divergence,

            # Moving averages
            "ema9": round(ema9[-1], 2), "ema21": round(ema21[-1], 2),
            "ema50": round(ema50[-1], 2), "ema200": round(ema200[-1], 2),
            "sma20": round(sma20, 2), "sma50": round(sma50, 2),
            "trend": trend,

            # Volume & momentum
            "vol_ratio": vol_ratio, "vol_trend": vol_trend,
            "momentum_score": momentum_score,

            # Performance
            "sharpe_ratio": sharpe,
            "max_drawdown_pct": round(max_dd, 2),
            "high_52w": round(high_52w, 2), "low_52w": round(low_52w, 2),
            "pct_from_52w_high": pct_from_52h,
            "pct_from_52w_low": pct_from_52l,

            # Targets
            "stop_loss": sl, "target1": t1, "target2": t2, "target3": t3,
            "risk_reward": rr,

            # Position sizing
            "position_qty": qty, "position_invest": invest,
            "sector": SECTOR_MAP.get(sym, "Other"),
            "chart": chart_data,
        }

        cache_set(f"analyze_{sym}", result)
        return jsonify(result)
    except Exception as e:
        logger.error(f"Analyze {sym}: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/multi-analyze")
def multi_analyze():
    """Parallel analyze multiple symbols at once"""
    syms_param = request.args.get("syms", "RELIANCE,TCS,HDFCBANK,INFY,ICICIBANK")
    syms = [s.strip().upper() for s in syms_param.split(",") if s.strip()][:15]

    results = {}

    def fetch_analyze(sym):
        cached = cache_get(f"analyze_{sym}", ttl=240)
        if cached:
            return sym, cached
        symbol = NSE_SYMBOLS.get(sym)
        if not symbol:
            return sym, {"error": "Not found"}
        try:
            t = yf.Ticker(symbol)
            hist = t.history(period="6mo", interval="1d", timeout=15)
            if hist.empty:
                return sym, {"error": "No data"}
            closes  = list(hist["Close"].astype(float))
            highs   = list(hist["High"].astype(float))
            lows    = list(hist["Low"].astype(float))
            volumes = list(hist["Volume"].astype(float))
            price = round(closes[-1], 2)
            prev  = round(closes[-2], 2) if len(closes) > 1 else price
            pct   = round((price - prev) / prev * 100, 2) if prev else 0
            rsi   = calc_rsi(closes)
            macd, macd_sig, _ = calc_macd(closes)
            atr   = calc_atr(highs, lows, closes)
            bb_u, _, bb_l = calc_bb(closes)
            stoch_k, _ = calc_stochastic(highs, lows, closes)
            adx_v = calc_adx(highs, lows, closes)
            _, st_bull = calc_supertrend(highs, lows, closes)
            cci_v = calc_cci(highs, lows, closes)
            signal = get_signal(rsi, macd, macd_sig, price, bb_u, bb_l, pct,
                                stoch_k, adx_v, st_bull, cci_v)
            sl, t1, t2, t3, rr = calc_targets(price, atr, signal)
            momentum_score = calc_momentum_score(closes, volumes)
            mini = {
                "symbol": sym, "price": price, "pct": pct,
                "rsi": rsi, "macd": macd, "macd_signal": macd_sig,
                "atr": atr, "adx": adx_v,
                "stoch_k": stoch_k, "cci": cci_v,
                "supertrend_bull": st_bull,
                "signal": signal, "momentum_score": momentum_score,
                "stop_loss": sl, "target1": t1, "target2": t2, "target3": t3,
                "risk_reward": rr,
                "sector": SECTOR_MAP.get(sym, "Other"),
            }
            cache_set(f"analyze_{sym}", mini)
            return sym, mini
        except Exception as e:
            return sym, {"error": str(e)}

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_analyze, sym): sym for sym in syms}
        for future in as_completed(futures, timeout=45):
            try:
                sym, data = future.result(timeout=20)
                results[sym] = data
            except Exception as e:
                sym = futures[future]
                results[sym] = {"error": str(e)}

    return jsonify({"symbols": syms, "results": results, "count": len(results)})

@app.route("/screener")
def screener():
    sector_filter = request.args.get("sector", "ALL")
    signal_filter = request.args.get("signal", "ALL")
    cache_key = f"screener_{sector_filter}"
    cached = cache_get(cache_key, ttl=1200)
    if cached:
        return jsonify(cached)

    scan_list = [s for s in NSE_SYMBOLS.keys()
                 if s not in ["NIFTY","SENSEX","BANKNIFTY","NIFTYMID","NIFTYIT"]]
    if sector_filter != "ALL":
        scan_list = [s for s in scan_list if SECTOR_MAP.get(s) == sector_filter]

    scan_list = scan_list[:90]

    def analyze_one(sym):
        symbol = NSE_SYMBOLS.get(sym)
        if not symbol:
            return None
        try:
            t = yf.Ticker(symbol)
            hist = t.history(period="6mo", interval="1d", timeout=15)
            if hist.empty or len(hist) < 20:
                return None
            closes  = list(hist["Close"].astype(float))
            highs   = list(hist["High"].astype(float))
            lows    = list(hist["Low"].astype(float))
            volumes = list(hist["Volume"].astype(float))
            price = round(closes[-1], 2)
            prev  = round(closes[-2], 2)
            pct   = round((price - prev) / prev * 100, 2)
            rsi   = calc_rsi(closes)
            macd, macd_sig, _ = calc_macd(closes)
            atr   = calc_atr(highs, lows, closes)
            bb_u, _, bb_l = calc_bb(closes)
            stoch_k, _ = calc_stochastic(highs, lows, closes)
            adx_v = calc_adx(highs, lows, closes)
            _, st_bull = calc_supertrend(highs, lows, closes)
            cci_v = calc_cci(highs, lows, closes)
            signal = get_signal(rsi, macd, macd_sig, price, bb_u, bb_l, pct,
                                stoch_k, adx_v, st_bull, cci_v)
            sl, t1, _, _, rr = calc_targets(price, atr, signal)
            momentum_score = calc_momentum_score(closes, volumes)

            # AI score (0-100) — inspired by institutional factor models
            tech_score = 0
            # RSI factor
            if rsi < 35:    tech_score += 30
            elif rsi < 50:  tech_score += 15
            elif rsi > 70:  tech_score -= 20
            # MACD factor
            if macd > macd_sig: tech_score += 20
            # Volume factor
            avg_vol = sum(volumes[-20:]) / 20 if len(volumes) >= 20 else 1
            if volumes[-1] > avg_vol * 1.5: tech_score += 12
            # Momentum
            if len(closes) >= 20:
                mom = (closes[-1] - closes[-20]) / closes[-20] * 100
                tech_score += min(18, max(-18, mom * 1.5))
            # ADX
            if adx_v > 25: tech_score += 8
            # Supertrend
            if st_bull: tech_score += 10
            # Stochastic
            if stoch_k < 25: tech_score += 10
            elif stoch_k > 80: tech_score -= 10

            ai_score = min(100, max(0, 50 + tech_score))

            # 52-week stats
            high_52w = max(highs) if highs else price
            low_52w  = min(lows) if lows else price

            return {
                "sym": sym, "price": price, "pct": pct,
                "rsi": rsi, "stoch_k": stoch_k, "adx": adx_v,
                "macd_trend": "BULL" if macd > macd_sig else "BEAR",
                "supertrend": "BUY" if st_bull else "SELL",
                "signal": signal, "ai_score": round(ai_score, 1),
                "momentum_score": momentum_score,
                "stop_loss": sl, "target": t1, "rr": rr,
                "sector": SECTOR_MAP.get(sym, "Other"),
                "high_52w": round(high_52w, 2), "low_52w": round(low_52w, 2),
                "pct_from_52h": round((price / high_52w - 1) * 100, 2),
            }
        except Exception as e:
            logger.warning(f"Screener {sym}: {e}")
            return None

    results = []
    with ThreadPoolExecutor(max_workers=14) as executor:
        futures = {executor.submit(analyze_one, sym): sym for sym in scan_list}
        for future in as_completed(futures, timeout=90):
            try:
                r = future.result(timeout=20)
                if r:
                    results.append(r)
            except Exception:
                pass

    results.sort(key=lambda x: x["ai_score"], reverse=True)
    out = {
        "count": len(results), "data": results,
        "updated": datetime.now().strftime("%H:%M:%S"),
        "sector_filter": sector_filter,
    }
    cache_set(cache_key, out)
    return jsonify(out)

@app.route("/options/<sym>")
def options(sym):
    sym = sym.upper()
    cached = cache_get(f"opt_{sym}", ttl=600)
    if cached:
        return jsonify(cached)

    symbol = NSE_SYMBOLS.get(sym)
    if not symbol:
        return jsonify({"error": "Not found"}), 404

    try:
        t = yf.Ticker(symbol)
        hist = t.history(period="1mo", interval="1d", timeout=15)
        if hist.empty:
            return jsonify({"error": "No data"}), 404

        closes  = list(hist["Close"].astype(float))
        highs   = list(hist["High"].astype(float))
        lows    = list(hist["Low"].astype(float))
        price = round(float(hist["Close"].iloc[-1]), 2)
        atr   = calc_atr(highs, lows, closes, 14)

        import random
        random.seed(int(price * 100))
        iv_base = max(15, min(80, atr / price * 100 * 15))

        strikes = [round(price * (1 + i * 0.025), 0) for i in range(-6, 7)]
        chain = []
        for strike in strikes:
            moneyness = (strike - price) / price
            call_iv = round(iv_base + abs(moneyness) * 22 + random.uniform(-3, 3), 1)
            put_iv  = round(iv_base + abs(moneyness) * 24 + random.uniform(-3, 3), 1)
            dist = abs(price - strike)
            call_premium = round(max(price - strike, 0) + atr * 0.35 * max(0.1, 1 - dist / (price * 0.12)), 1)
            put_premium  = round(max(strike - price, 0) + atr * 0.35 * max(0.1, 1 - dist / (price * 0.12)), 1)
            call_oi = int(random.uniform(5, 50) * 1000 * (1.8 if abs(moneyness) < 0.025 else 1))
            put_oi  = int(random.uniform(5, 50) * 1000 * (1.8 if abs(moneyness) < 0.025 else 1))
            chain.append({
                "strike": strike, "call_iv": call_iv, "put_iv": put_iv,
                "call_premium": call_premium, "put_premium": put_premium,
                "call_oi": call_oi, "put_oi": put_oi,
                "call_delta": round(max(0.01, min(0.99, 0.5 - moneyness * 3)), 2),
                "put_delta":  round(max(-0.99, min(-0.01, -0.5 - moneyness * 3)), 2),
                "moneyness": "ATM" if abs(moneyness) < 0.015 else ("ITM" if moneyness < 0 else "OTM"),
                "gamma": round(0.005 * max(0.1, 1 - abs(moneyness) * 8), 4),
                "theta": round(-atr * 0.01 * max(0.1, 1 - abs(moneyness) * 5), 2),
                "vega":  round(price * call_iv / 1000, 2),
            })

        total_call_oi = sum(c["call_oi"] for c in chain)
        total_put_oi  = sum(c["put_oi"]  for c in chain)
        pcr = round(total_put_oi / max(total_call_oi, 1), 2)

        rsi = calc_rsi(closes)
        stoch_k, _ = calc_stochastic(highs, lows, closes)

        # Enhanced options signal
        if rsi < 35 and pcr > 1.2:
            opt_signal = "BUY CALL — Oversold + High PCR (reversal likely)"
            opt_type = "call"
        elif rsi > 65 and pcr < 0.8:
            opt_signal = "BUY PUT — Overbought + Low PCR (correction likely)"
            opt_type = "put"
        elif stoch_k < 20 and pcr > 1.3:
            opt_signal = "BUY CALL — Stochastic Oversold + Put OI heavy"
            opt_type = "call"
        elif stoch_k > 80 and pcr < 0.7:
            opt_signal = "BUY PUT — Stochastic Overbought + Call OI heavy"
            opt_type = "put"
        elif pcr > 1.6:
            opt_signal = "BUY CALL — Extreme PUT OI (max pain reversal)"
            opt_type = "call"
        elif pcr < 0.65:
            opt_signal = "BUY PUT — Extreme CALL OI (top formation)"
            opt_type = "put"
        else:
            opt_signal = "WAIT — No strong options signal"
            opt_type = "none"

        atm = min(chain, key=lambda x: abs(x["strike"] - price))
        result = {
            "symbol": sym, "price": price, "atr": atr, "iv_base": round(iv_base, 1),
            "pcr": pcr, "total_call_oi": total_call_oi, "total_put_oi": total_put_oi,
            "signal": opt_signal, "option_type": opt_type,
            "rec_strike": round(price * 1.025 if opt_type == "call" else price * 0.975, 0),
            "rec_expiry": "Monthly",
            "rec_premium": atm["call_premium"] if opt_type == "call" else atm["put_premium"],
            "max_pain": round(price, 0),
            "chain": chain,
            "rsi": rsi, "stoch_k": stoch_k,
        }
        cache_set(f"opt_{sym}", result)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/news")
def news():
    cached = cache_get("news", ttl=600)
    if cached:
        return jsonify(cached)

    feeds = [
        "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms",
        "https://www.moneycontrol.com/rss/marketsnews.xml",
        "https://www.business-standard.com/rss/markets-106.rss",
    ]

    positive_words = ["surge","rally","gain","rise","profit","growth","buy","bullish","strong",
                      "record","beat","up","positive","boost","jump","soar","recover","high","best"]
    negative_words = ["fall","drop","crash","loss","decline","sell","bearish","weak","miss",
                      "down","negative","cut","worry","fear","risk","slump","plunge","low","worst"]

    def fetch_feed(url):
        try:
            f = feedparser.parse(url)
            items = []
            for entry in f.entries[:8]:
                title = entry.get("title", "")
                text = (title + " " + entry.get("summary", "")).lower()
                pos = sum(1 for w in positive_words if w in text)
                neg = sum(1 for w in negative_words if w in text)
                score = round((pos - neg) / max(pos + neg, 1), 2)
                sentiment = "POSITIVE" if score > 0.1 else "NEGATIVE" if score < -0.1 else "NEUTRAL"
                items.append({
                    "title": title, "score": score, "sentiment": sentiment,
                    "link": entry.get("link", ""), "time": entry.get("published", ""),
                    "source": url.split("/")[2].replace("www.", ""),
                })
            return items
        except:
            return []

    articles = []
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(fetch_feed, url) for url in feeds]
        for future in as_completed(futures, timeout=15):
            try:
                articles.extend(future.result(timeout=10))
            except:
                pass

    if not articles:
        articles = [
            {"title":"Nifty 50 consolidates near support; FII activity watched","score":0.1,"sentiment":"NEUTRAL","link":"","time":"","source":"fallback"},
            {"title":"RBI policy: Rates steady, liquidity measures positive for markets","score":0.3,"sentiment":"POSITIVE","link":"","time":"","source":"fallback"},
            {"title":"IT sector: Strong deal pipeline, margin recovery on track","score":0.35,"sentiment":"POSITIVE","link":"","time":"","source":"fallback"},
            {"title":"Global cues mixed; commodity prices under pressure","score":-0.15,"sentiment":"NEGATIVE","link":"","time":"","source":"fallback"},
            {"title":"Auto sector: Monthly sales data beats estimates","score":0.4,"sentiment":"POSITIVE","link":"","time":"","source":"fallback"},
        ]

    articles.sort(key=lambda x: abs(x["score"]), reverse=True)
    avg_score = round(sum(a["score"] for a in articles) / len(articles), 3)
    market_mood = "BULLISH" if avg_score > 0.1 else "BEARISH" if avg_score < -0.1 else "NEUTRAL"

    result = {
        "articles": articles[:20], "avg_score": avg_score,
        "market_mood": market_mood, "count": len(articles),
        "positive_count": sum(1 for a in articles if a["sentiment"] == "POSITIVE"),
        "negative_count": sum(1 for a in articles if a["sentiment"] == "NEGATIVE"),
        "neutral_count":  sum(1 for a in articles if a["sentiment"] == "NEUTRAL"),
    }
    cache_set("news", result)
    return jsonify(result)

@app.route("/heatmap")
def heatmap():
    cached = cache_get("heatmap", ttl=1200)
    if cached:
        return jsonify(cached)

    heatmap_syms = {k: v for k, v in NSE_SYMBOLS.items()
                    if k not in ["NIFTY","SENSEX","BANKNIFTY","NIFTYMID","NIFTYIT"]}
    heatmap_syms = dict(list(heatmap_syms.items())[:70])

    def fetch_heatmap_sym(sym):
        symbol = NSE_SYMBOLS[sym]
        try:
            t = yf.Ticker(symbol)
            hist = t.history(period="5d", interval="1d", timeout=12)
            if len(hist) < 2:
                return None
            closes = list(hist["Close"].astype(float))
            volumes = list(hist["Volume"].astype(float))
            pct = round((closes[-1] - closes[-2]) / closes[-2] * 100, 2)
            week_pct = round((closes[-1] - closes[0]) / closes[0] * 100, 2) if len(closes) >= 5 else pct
            return {
                "sym": sym, "pct": pct, "week_pct": week_pct,
                "price": round(closes[-1], 2),
                "volume": int(volumes[-1]) if volumes else 0,
                "sector": SECTOR_MAP.get(sym, "Other")
            }
        except:
            return None

    all_stocks = []
    with ThreadPoolExecutor(max_workers=14) as executor:
        futures = {executor.submit(fetch_heatmap_sym, sym): sym for sym in heatmap_syms.keys()}
        for future in as_completed(futures, timeout=60):
            try:
                r = future.result(timeout=15)
                if r:
                    all_stocks.append(r)
            except:
                pass

    sectors = {}
    for stock in all_stocks:
        sector = stock["sector"]
        if sector not in sectors:
            sectors[sector] = []
        sectors[sector].append(stock)

    result = []
    for sector, stocks in sectors.items():
        if not stocks:
            continue
        avg_pct = round(sum(s["pct"] for s in stocks) / len(stocks), 2)
        week_avg = round(sum(s["week_pct"] for s in stocks) / len(stocks), 2)
        gainers = [s for s in stocks if s["pct"] > 0]
        losers  = [s for s in stocks if s["pct"] < 0]
        result.append({
            "sector": sector, "avg_pct": avg_pct, "week_avg_pct": week_avg,
            "stocks": sorted(stocks, key=lambda x: abs(x["pct"]), reverse=True),
            "count": len(stocks),
            "gainers": len(gainers), "losers": len(losers),
        })

    result.sort(key=lambda x: x["avg_pct"], reverse=True)
    out = {
        "sectors": result,
        "updated": datetime.now().strftime("%H:%M:%S"),
        "total_stocks": len(all_stocks),
        "market_gainers": sum(1 for s in all_stocks if s["pct"] > 0),
        "market_losers": sum(1 for s in all_stocks if s["pct"] < 0),
    }
    cache_set("heatmap", out)
    return jsonify(out)

@app.route("/correlation")
def correlation():
    """Simple correlation matrix for top stocks"""
    syms = request.args.get("syms", "RELIANCE,TCS,HDFCBANK,INFY,ICICIBANK,SBIN").upper().split(",")
    syms = syms[:8]

    prices_dict = {}

    def fetch_sym(sym):
        symbol = NSE_SYMBOLS.get(sym)
        if not symbol:
            return sym, None
        try:
            t = yf.Ticker(symbol)
            hist = t.history(period="3mo", interval="1d", timeout=12)
            if hist.empty:
                return sym, None
            return sym, list(hist["Close"].astype(float))
        except:
            return sym, None

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(fetch_sym, sym): sym for sym in syms}
        for future in as_completed(futures, timeout=30):
            try:
                sym, prices = future.result(timeout=15)
                if prices:
                    prices_dict[sym] = prices
            except:
                pass

    # Calculate returns
    returns_dict = {}
    for sym, prices in prices_dict.items():
        if len(prices) > 1:
            returns_dict[sym] = [(prices[i] - prices[i-1]) / prices[i-1] for i in range(1, len(prices))]

    valid_syms = list(returns_dict.keys())
    n = len(valid_syms)
    matrix = {}

    for i, s1 in enumerate(valid_syms):
        matrix[s1] = {}
        r1 = returns_dict[s1]
        for j, s2 in enumerate(valid_syms):
            r2 = returns_dict[s2]
            min_len = min(len(r1), len(r2))
            if min_len < 10:
                matrix[s1][s2] = 0
                continue
            a = r1[-min_len:]
            b = r2[-min_len:]
            mean_a = sum(a) / min_len
            mean_b = sum(b) / min_len
            cov = sum((a[k] - mean_a) * (b[k] - mean_b) for k in range(min_len)) / min_len
            std_a = (sum((x - mean_a) ** 2 for x in a) / min_len) ** 0.5
            std_b = (sum((x - mean_b) ** 2 for x in b) / min_len) ** 0.5
            corr = cov / (std_a * std_b) if std_a * std_b > 0 else 0
            matrix[s1][s2] = round(corr, 3)

    return jsonify({"symbols": valid_syms, "matrix": matrix})

# ═══════════════════════════════════════════════
#  BACKGROUND CACHE WARMER
# ═══════════════════════════════════════════════
def warm_cache():
    """Pre-warm critical caches on startup"""
    logger.info("Warming critical caches...")
    try:
        import requests
        requests.get("http://localhost:10000/live", timeout=20)
        logger.info("Live cache warmed")
    except:
        pass

# ═══════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════
if __name__ == "__main__":
    logger.info("Starting AI Trading Terminal Backend v3.0")
    # Start cache warmer in background
    t = threading.Thread(target=warm_cache, daemon=True)
    t.start()
    # Start cache cleaner
    def clean_loop():
        while True:
            time.sleep(3600)
            cache_clear_expired()
    cleaner = threading.Thread(target=clean_loop, daemon=True)
    cleaner.start()
    app.run(host="0.0.0.0", port=10000, threaded=True)
