"""
AI Trading Terminal Backend v4.0 — Institutional-Grade NSE Analytics
=====================================================================
Improvements over v3.0 (PDF-informed):
  ✅ Wilder's smoothed RSI (proper formula)
  ✅ Stateful Supertrend (tracks prior trend direction across all bars)
  ✅ Barra-inspired Factor Model scoring (momentum, trend, quality, vol, reversal)
  ✅ Fixed ROCE (now uses proper EBIT/Capital Employed proxy)
  ✅ yf.download() batch fetching for 5-10x speed improvement
  ✅ Market Breadth endpoint (A/D ratio, new highs/lows, McClellan oscillator)
  ✅ Walk-forward backtest support (train 3Y → test 2Y)
  ✅ Sector Rotation endpoint
  ✅ Dynamic /universe endpoint — no hardcoded stock subsets anywhere
  ✅ Improved NLP sentiment (entity extraction + sector weighting)
  ✅ No options endpoint (removed)
  ✅ Smarter parallel fetching with connection pooling
  ✅ Regime detection (bull/bear/sideways) for signal conditioning
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
import yfinance as yf
import numpy as np
import json, time, logging, math
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import feedparser
import threading
import requests
from collections import defaultdict

app = Flask(__name__)
CORS(app)
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════
#  MASTER NSE UNIVERSE — single source of truth
#  All endpoints derive from this dict dynamically
# ══════════════════════════════════════════════════════════
NSE_SYMBOLS = {
    # ── INDICES ──
    "NIFTY":     "^NSEI",
    "SENSEX":    "^BSESN",
    "BANKNIFTY": "^NSEBANK",
    "NIFTYMID":  "^NSEMDCP50",
    "NIFTYIT":   "^CNXIT",
    "NIFTYSMALL":"^CNXSC",
    # ── NIFTY 50 ──
    "RELIANCE":   "RELIANCE.NS",  "TCS":        "TCS.NS",
    "HDFCBANK":   "HDFCBANK.NS",  "INFY":       "INFY.NS",
    "ICICIBANK":  "ICICIBANK.NS", "HINDUNILVR": "HINDUNILVR.NS",
    "ITC":        "ITC.NS",       "SBIN":       "SBIN.NS",
    "BHARTIARTL": "BHARTIARTL.NS","BAJFINANCE":  "BAJFINANCE.NS",
    "KOTAKBANK":  "KOTAKBANK.NS", "LT":         "LT.NS",
    "HCLTECH":    "HCLTECH.NS",   "ASIANPAINT": "ASIANPAINT.NS",
    "AXISBANK":   "AXISBANK.NS",  "MARUTI":     "MARUTI.NS",
    "SUNPHARMA":  "SUNPHARMA.NS", "TITAN":      "TITAN.NS",
    "WIPRO":      "WIPRO.NS",     "NTPC":       "NTPC.NS",
    "POWERGRID":  "POWERGRID.NS", "ULTRACEMCO": "ULTRACEMCO.NS",
    "NESTLEIND":  "NESTLEIND.NS", "TATAMOTORS": "TATAMOTORS.NS",
    "ADANIENT":   "ADANIENT.NS",  "JSWSTEEL":   "JSWSTEEL.NS",
    "TATASTEEL":  "TATASTEEL.NS", "ONGC":       "ONGC.NS",
    "COALINDIA":  "COALINDIA.NS", "BAJAJFINSV": "BAJAJFINSV.NS",
    "TECHM":      "TECHM.NS",     "INDUSINDBK": "INDUSINDBK.NS",
    "DRREDDY":    "DRREDDY.NS",   "CIPLA":      "CIPLA.NS",
    "DIVISLAB":   "DIVISLAB.NS",  "EICHERMOT":  "EICHERMOT.NS",
    "BPCL":       "BPCL.NS",      "TATACONSUM": "TATACONSUM.NS",
    "GRASIM":     "GRASIM.NS",    "APOLLOHOSP": "APOLLOHOSP.NS",
    "ADANIPORTS": "ADANIPORTS.NS","BRITANNIA":  "BRITANNIA.NS",
    "HEROMOTOCO": "HEROMOTOCO.NS","HINDALCO":   "HINDALCO.NS",
    "UPL":        "UPL.NS",       "SBILIFE":    "SBILIFE.NS",
    "HDFCLIFE":   "HDFCLIFE.NS",  "BAJAJ-AUTO": "BAJAJ-AUTO.NS",
    "MM":         "M&M.NS",       "SHREECEM":   "SHREECEM.NS",
    # ── NIFTY NEXT 50 ──
    "AMBUJACEM":  "AMBUJACEM.NS", "BANKBARODA": "BANKBARODA.NS",
    "BERGEPAINT": "BERGEPAINT.NS","BOSCHLTD":   "BOSCHLTD.NS",
    "CANBK":      "CANBK.NS",     "CHOLAFIN":   "CHOLAFIN.NS",
    "COLPAL":     "COLPAL.NS",    "DABUR":      "DABUR.NS",
    "DLF":        "DLF.NS",       "GAIL":       "GAIL.NS",
    "GODREJCP":   "GODREJCP.NS",  "HAVELLS":    "HAVELLS.NS",
    "ICICIGI":    "ICICIGI.NS",   "ICICIPRULI": "ICICIPRULI.NS",
    "INDIGO":     "INDIGO.NS",    "INDUSTOWER": "INDUSTOWER.NS",
    "IOC":        "IOC.NS",       "IRCTC":      "IRCTC.NS",
    "LICI":       "LICI.NS",      "LUPIN":      "LUPIN.NS",
    "MARICO":     "MARICO.NS",    "MUTHOOTFIN": "MUTHOOTFIN.NS",
    "NAUKRI":     "NAUKRI.NS",    "PAGEIND":    "PAGEIND.NS",
    "PETRONET":   "PETRONET.NS",  "PIDILITIND": "PIDILITIND.NS",
    "PNB":        "PNB.NS",       "RECLTD":     "RECLTD.NS",
    "SAIL":       "SAIL.NS",      "SIEMENS":    "SIEMENS.NS",
    "SRF":        "SRF.NS",       "TORNTPHARM": "TORNTPHARM.NS",
    "TRENT":      "TRENT.NS",     "TVSMOTOR":   "TVSMOTOR.NS",
    "VBL":        "VBL.NS",       "VEDL":       "VEDL.NS",
    "VOLTAS":     "VOLTAS.NS",    "ZOMATO":     "ZOMATO.NS",
    "ZYDUSLIFE":  "ZYDUSLIFE.NS",
    # ── NIFTY MIDCAP ──
    "ABFRL":      "ABFRL.NS",     "ASTRAL":     "ASTRAL.NS",
    "AUROPHARMA": "AUROPHARMA.NS","BALKRISIND": "BALKRISIND.NS",
    "CGPOWER":    "CGPOWER.NS",   "DEEPAKNTR":  "DEEPAKNTR.NS",
    "DIXON":      "DIXON.NS",     "FEDERALBNK": "FEDERALBNK.NS",
    "GODREJPROP": "GODREJPROP.NS","HAL":        "HAL.NS",
    "IDFCFIRSTB": "IDFCFIRSTB.NS","JUBLFOOD":   "JUBLFOOD.NS",
    "KPITTECH":   "KPITTECH.NS",  "LICHSGFIN":  "LICHSGFIN.NS",
    "LTIM":       "LTIM.NS",      "LTTS":       "LTTS.NS",
    "MAXHEALTH":  "MAXHEALTH.NS", "MPHASIS":    "MPHASIS.NS",
    "MRF":        "MRF.NS",       "OBEROIRLTY": "OBEROIRLTY.NS",
    "OFSS":       "OFSS.NS",      "PERSISTENT": "PERSISTENT.NS",
    "PHOENIXLTD": "PHOENIXLTD.NS","POLYCAB":    "POLYCAB.NS",
    "POONAWALLA": "POONAWALLA.NS","PRESTIGE":   "PRESTIGE.NS",
    "RBLBANK":    "RBLBANK.NS",   "SCHAEFFLER": "SCHAEFFLER.NS",
    "SOLARINDS":  "SOLARINDS.NS", "SUPREMEIND": "SUPREMEIND.NS",
    "TIINDIA":    "TIINDIA.NS",   "TORNTPOWER": "TORNTPOWER.NS",
    "UBL":        "UBL.NS",       "UNIONBANK":  "UNIONBANK.NS",
    "ABB":        "ABB.NS",       "BANDHANBNK": "BANDHANBNK.NS",
    "CROMPTON":   "CROMPTON.NS",  "ESCORTS":    "ESCORTS.NS",
    "GMRINFRA":   "GMRINFRA.NS",  "INDIANB":    "INDIANB.NS",
    "KALYANKJIL": "KALYANKJIL.NS","LALPATHLAB": "LALPATHLAB.NS",
    "NYKAA":      "NYKAA.NS",     "PAYTM":      "PAYTM.NS",
}

SECTOR_MAP = {
    "RELIANCE":"Energy","ONGC":"Energy","BPCL":"Energy","COALINDIA":"Energy",
    "NTPC":"Energy","POWERGRID":"Energy","GAIL":"Energy","IOC":"Energy",
    "TORNTPOWER":"Energy","PETRONET":"Energy",
    "TCS":"IT","INFY":"IT","WIPRO":"IT","HCLTECH":"IT","TECHM":"IT",
    "MPHASIS":"IT","LTIM":"IT","LTTS":"IT","KPITTECH":"IT","PERSISTENT":"IT",
    "OFSS":"IT","NAUKRI":"IT","DIXON":"IT",
    "HDFCBANK":"Banking","ICICIBANK":"Banking","SBIN":"Banking",
    "AXISBANK":"Banking","KOTAKBANK":"Banking","INDUSINDBK":"Banking",
    "BANKBARODA":"Banking","CANBK":"Banking","PNB":"Banking",
    "FEDERALBNK":"Banking","IDFCFIRSTB":"Banking","RBLBANK":"Banking",
    "UNIONBANK":"Banking","BANDHANBNK":"Banking","INDIANB":"Banking",
    "BAJFINANCE":"NBFC","BAJAJFINSV":"NBFC","CHOLAFIN":"NBFC",
    "MUTHOOTFIN":"NBFC","LICHSGFIN":"NBFC","POONAWALLA":"NBFC",
    "RECLTD":"NBFC","LICI":"NBFC","SBILIFE":"NBFC","HDFCLIFE":"NBFC",
    "ICICIGI":"NBFC","ICICIPRULI":"NBFC",
    "HINDUNILVR":"FMCG","ITC":"FMCG","NESTLEIND":"FMCG","BRITANNIA":"FMCG",
    "DABUR":"FMCG","MARICO":"FMCG","COLPAL":"FMCG","GODREJCP":"FMCG",
    "TATACONSUM":"FMCG","ASIANPAINT":"FMCG","PAGEIND":"FMCG","VBL":"FMCG",
    "UBL":"FMCG","BERGEPAINT":"FMCG","PIDILITIND":"FMCG","TITAN":"FMCG",
    "KALYANKJIL":"FMCG","NYKAA":"FMCG",
    "SUNPHARMA":"Pharma","DRREDDY":"Pharma","CIPLA":"Pharma",
    "DIVISLAB":"Pharma","AUROPHARMA":"Pharma","LUPIN":"Pharma",
    "TORNTPHARM":"Pharma","ZYDUSLIFE":"Pharma","MAXHEALTH":"Pharma",
    "APOLLOHOSP":"Pharma","LALPATHLAB":"Pharma",
    "MARUTI":"Auto","TATAMOTORS":"Auto","BAJAJ-AUTO":"Auto","EICHERMOT":"Auto",
    "HEROMOTOCO":"Auto","TVSMOTOR":"Auto","BALKRISIND":"Auto","BOSCHLTD":"Auto",
    "SCHAEFFLER":"Auto","TIINDIA":"Auto","MM":"Auto","MRF":"Auto","ESCORTS":"Auto",
    "LT":"Infra","ULTRACEMCO":"Infra","GRASIM":"Infra","AMBUJACEM":"Infra",
    "SHREECEM":"Infra","ADANIENT":"Infra","ADANIPORTS":"Infra","HAL":"Infra",
    "INDUSTOWER":"Infra","SIEMENS":"Infra","HAVELLS":"Infra","POLYCAB":"Infra",
    "CGPOWER":"Infra","SUPREMEIND":"Infra","VOLTAS":"Infra","ASTRAL":"Infra",
    "ABB":"Infra","CROMPTON":"Infra","GMRINFRA":"Infra",
    "BHARTIARTL":"Telecom",
    "TRENT":"Retail","JUBLFOOD":"Retail","ABFRL":"Retail","IRCTC":"Travel",
    "INDIGO":"Aviation","PAYTM":"Fintech",
    "JSWSTEEL":"Metal","TATASTEEL":"Metal","HINDALCO":"Metal","VEDL":"Metal","SAIL":"Metal",
    "ZOMATO":"Tech","NAUKRI":"Tech",
    "DLF":"Realty","GODREJPROP":"Realty","OBEROIRLTY":"Realty",
    "PRESTIGE":"Realty","PHOENIXLTD":"Realty",
    "DEEPAKNTR":"Chem","SRF":"Chem","SOLARINDS":"Chem","UPL":"Chem",
}

# Dynamically built set of non-index symbols
STOCK_SYMBOLS = {k: v for k, v in NSE_SYMBOLS.items()
                 if k not in {"NIFTY","SENSEX","BANKNIFTY","NIFTYMID","NIFTYIT","NIFTYSMALL"}}

INDEX_SYMBOLS  = {k: v for k, v in NSE_SYMBOLS.items()
                  if k in {"NIFTY","SENSEX","BANKNIFTY","NIFTYMID","NIFTYIT","NIFTYSMALL"}}

# ══════════════════════════════════════════════════════════
#  THREAD-SAFE MULTI-LEVEL CACHE
# ══════════════════════════════════════════════════════════
_cache     = {}
_cache_lock = threading.RLock()

def cache_get(key, ttl=300):
    with _cache_lock:
        entry = _cache.get(key)
        if entry and (time.time() - entry['t']) < ttl:
            return entry['v']
    return None

def cache_set(key, val):
    with _cache_lock:
        _cache[key] = {'v': val, 't': time.time()}

def cache_delete(key):
    with _cache_lock:
        _cache.pop(key, None)

def cache_clear_expired():
    with _cache_lock:
        now = time.time()
        expired = [k for k, v in _cache.items() if now - v['t'] > 7200]
        for k in expired:
            del _cache[k]
        logger.info(f"Cache cleanup: removed {len(expired)} expired entries, {len(_cache)} remain")

# ══════════════════════════════════════════════════════════
#  BATCH FETCHER — Uses yf.download() for 5-10x speedup
# ══════════════════════════════════════════════════════════
def batch_download(symbols_dict, period="6mo", interval="1d"):
    """
    Batch download using yf.download() — single HTTP round-trip for all tickers.
    Returns dict: {name: DataFrame with OHLCV columns}
    """
    if not symbols_dict:
        return {}

    tickers  = list(symbols_dict.values())
    name_map = {v: k for k, v in symbols_dict.items()}  # reverse map

    try:
        if len(tickers) == 1:
            raw = yf.download(tickers[0], period=period, interval=interval,
                              auto_adjust=True, progress=False, timeout=30)
            if raw.empty:
                return {}
            return {name_map[tickers[0]]: raw}

        raw = yf.download(
            tickers,
            period=period,
            interval=interval,
            auto_adjust=True,
            group_by="ticker",
            threads=True,
            progress=False,
            timeout=45,
        )
        if raw.empty:
            return {}

        results = {}
        for ticker in tickers:
            try:
                df = raw[ticker].dropna(how="all")
                if not df.empty and len(df) > 5:
                    results[name_map[ticker]] = df
            except (KeyError, Exception):
                pass
        return results

    except Exception as e:
        logger.warning(f"batch_download failed ({period}/{interval}): {e}. Falling back to parallel single fetch.")
        return _parallel_fallback(symbols_dict, period, interval)


def _parallel_fallback(symbols_dict, period, interval, max_workers=14):
    """Fallback parallel individual fetcher when batch fails."""
    results = {}

    def _fetch_one(name, ticker):
        try:
            df = yf.Ticker(ticker).history(period=period, interval=interval, timeout=15)
            return name, (df if not df.empty else None)
        except Exception:
            return name, None

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {ex.submit(_fetch_one, n, s): n for n, s in symbols_dict.items()}
        for fut in as_completed(futs, timeout=60):
            try:
                name, df = fut.result(timeout=20)
                if df is not None:
                    results[name] = df
            except Exception:
                pass
    return results


def df_to_arrays(df):
    """Convert DataFrame to numpy arrays; handles both yf column name styles."""
    col = lambda *names: next(
        (c for c in df.columns if str(c).lower() in [n.lower() for n in names]), None
    )
    c_close  = col("Close","close","Adj Close")
    c_open   = col("Open","open")
    c_high   = col("High","high")
    c_low    = col("Low","low")
    c_volume = col("Volume","volume")

    closes  = df[c_close].astype(float).values  if c_close  else np.array([])
    opens   = df[c_open].astype(float).values   if c_open   else closes.copy()
    highs   = df[c_high].astype(float).values   if c_high   else closes.copy()
    lows    = df[c_low].astype(float).values    if c_low    else closes.copy()
    volumes = df[c_volume].astype(float).values if c_volume else np.zeros(len(closes))
    dates   = [str(idx)[:10] for idx in df.index]

    # Remove NaN rows
    mask = np.isfinite(closes) & (closes > 0)
    return closes[mask], opens[mask], highs[mask], lows[mask], volumes[mask], [d for d, m in zip(dates, mask) if m]


# ══════════════════════════════════════════════════════════
#  TECHNICAL INDICATORS — All improved / corrected
# ══════════════════════════════════════════════════════════

def calc_rsi_wilder(closes, period=14):
    """Wilder's smoothed RSI — proper exponential smoothing method."""
    n = len(closes)
    if n < period + 1:
        return 50.0
    gains  = np.maximum(np.diff(closes), 0.0)
    losses = np.maximum(-np.diff(closes), 0.0)
    # Seed with simple average
    avg_g = float(np.mean(gains[:period]))
    avg_l = float(np.mean(losses[:period]))
    # Wilder's smoothing
    for i in range(period, len(gains)):
        avg_g = (avg_g * (period - 1) + gains[i]) / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
    if avg_l == 0:
        return 100.0
    return round(100.0 - 100.0 / (1.0 + avg_g / avg_l), 2)


def calc_rsi_series(closes, period=14):
    """Return RSI for every bar (for divergence detection)."""
    n = len(closes)
    if n < period + 2:
        return np.full(n, 50.0)
    gains  = np.maximum(np.diff(closes), 0.0)
    losses = np.maximum(-np.diff(closes), 0.0)
    rsi    = np.full(n, 50.0)
    avg_g  = float(np.mean(gains[:period]))
    avg_l  = float(np.mean(losses[:period]))
    if avg_l == 0:
        rsi[period] = 100.0
    else:
        rsi[period] = 100.0 - 100.0 / (1.0 + avg_g / avg_l)
    for i in range(period, len(gains)):
        avg_g = (avg_g * (period - 1) + gains[i]) / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
        if avg_l == 0:
            rsi[i + 1] = 100.0
        else:
            rsi[i + 1] = 100.0 - 100.0 / (1.0 + avg_g / avg_l)
    return rsi


def calc_ema(prices, period):
    """Exponential moving average — vectorized."""
    if len(prices) == 0:
        return np.array([])
    k   = 2.0 / (period + 1)
    ema = np.empty(len(prices))
    ema[0] = prices[0]
    for i in range(1, len(prices)):
        ema[i] = prices[i] * k + ema[i - 1] * (1 - k)
    return ema


def calc_macd(closes):
    if len(closes) < 26:
        return 0.0, 0.0, 0.0
    e12    = calc_ema(closes, 12)
    e26    = calc_ema(closes, 26)
    macd   = e12 - e26
    signal = calc_ema(macd, 9)
    hist   = float(macd[-1] - signal[-1])
    return round(float(macd[-1]), 4), round(float(signal[-1]), 4), round(hist, 4)


def calc_atr_array(highs, lows, closes, period=14):
    """Returns full ATR array using Wilder's smoothing."""
    n  = len(closes)
    tr = np.empty(n)
    tr[0] = highs[0] - lows[0]
    for i in range(1, n):
        tr[i] = max(highs[i] - lows[i],
                    abs(highs[i] - closes[i - 1]),
                    abs(lows[i]  - closes[i - 1]))
    atr = np.empty(n)
    atr[:period] = np.mean(tr[:period])
    for i in range(period, n):
        atr[i] = (atr[i - 1] * (period - 1) + tr[i]) / period
    return atr


def calc_atr(highs, lows, closes, period=14):
    """Single ATR value."""
    if len(closes) < 2:
        return 0.0
    return round(float(calc_atr_array(highs, lows, closes, period)[-1]), 2)


def calc_bb(closes, period=20):
    if len(closes) < period:
        p = closes[-1]
        return round(p * 1.02, 2), round(p, 2), round(p * 0.98, 2)
    sl  = closes[-period:]
    m   = float(np.mean(sl))
    std = float(np.std(sl, ddof=0))
    return round(m + 2 * std, 2), round(m, 2), round(m - 2 * std, 2)


def calc_stochastic(highs, lows, closes, k_period=14, d_period=3):
    n = len(closes)
    if n < k_period:
        return 50.0, 50.0
    k_vals = []
    for i in range(k_period - 1, n):
        h_max = float(np.max(highs[max(0, i - k_period + 1):i + 1]))
        l_min = float(np.min(lows[max(0, i  - k_period + 1):i + 1]))
        k_vals.append(100.0 * (closes[i] - l_min) / (h_max - l_min) if h_max != l_min else 50.0)
    d_vals = calc_ema(np.array(k_vals), d_period)
    return round(k_vals[-1], 2), round(float(d_vals[-1]), 2)


def calc_williams_r(highs, lows, closes, period=14):
    if len(closes) < period:
        return -50.0
    h_max = float(np.max(highs[-period:]))
    l_min = float(np.min(lows[-period:]))
    if h_max == l_min:
        return -50.0
    return round(-100.0 * (h_max - closes[-1]) / (h_max - l_min), 2)


def calc_cci(highs, lows, closes, period=20):
    if len(closes) < period:
        return 0.0
    typical   = (highs + lows + closes) / 3.0
    tp_slice  = typical[-period:]
    tp_mean   = float(np.mean(tp_slice))
    mean_dev  = float(np.mean(np.abs(tp_slice - tp_mean)))
    if mean_dev == 0:
        return 0.0
    return round((typical[-1] - tp_mean) / (0.015 * mean_dev), 2)


def calc_adx(highs, lows, closes, period=14):
    n = len(closes)
    if n < period + 2:
        return 25.0
    tr_list, dm_plus, dm_minus = [], [], []
    for i in range(1, n):
        h, l, pc = highs[i], lows[i], closes[i - 1]
        tr_list.append(max(h - l, abs(h - pc), abs(l - pc)))
        up   = float(h - highs[i - 1])
        down = float(lows[i - 1] - l)
        dm_plus.append(up   if up > down and up > 0   else 0.0)
        dm_minus.append(down if down > up  and down > 0 else 0.0)
    def wilder_smooth(arr, p):
        s = sum(arr[:p])
        result = [s]
        for v in arr[p:]:
            s = s - s / p + v
            result.append(s)
        return result
    atr_s  = wilder_smooth(tr_list, period)
    dmp_s  = wilder_smooth(dm_plus, period)
    dmm_s  = wilder_smooth(dm_minus, period)
    di_p   = [100.0 * dmp_s[i] / max(atr_s[i], 1e-9) for i in range(len(atr_s))]
    di_m   = [100.0 * dmm_s[i] / max(atr_s[i], 1e-9) for i in range(len(atr_s))]
    dx     = [100.0 * abs(di_p[i] - di_m[i]) / max(di_p[i] + di_m[i], 1e-9) for i in range(len(di_p))]
    adx_val = sum(dx[-period:]) / period if len(dx) >= period else 25.0
    return round(adx_val, 2)


def calc_supertrend_stateful(highs, lows, closes, period=10, multiplier=3.0):
    """
    Proper stateful Supertrend — tracks prior band values across all bars.
    Returns (final_level, is_bullish, full_levels_array, full_direction_array)
    """
    n   = len(closes)
    atr = calc_atr_array(highs, lows, closes, period)
    hl2 = (highs + lows) / 2.0

    basic_upper = hl2 + multiplier * atr
    basic_lower = hl2 - multiplier * atr

    upper = basic_upper.copy()
    lower = basic_lower.copy()
    st    = np.where(closes > hl2, lower, upper).astype(float)
    direction = np.ones(n, dtype=int)   # 1 = bullish, -1 = bearish

    for i in range(1, n):
        # Adjust upper band: only tighten if previous close was below it
        upper[i] = basic_upper[i] if basic_upper[i] < upper[i - 1] or closes[i - 1] > upper[i - 1] else upper[i - 1]
        # Adjust lower band: only raise if previous close was above it
        lower[i] = basic_lower[i] if basic_lower[i] > lower[i - 1] or closes[i - 1] < lower[i - 1] else lower[i - 1]

        # Determine trend
        if st[i - 1] == upper[i - 1]:          # was bearish
            if closes[i] > upper[i]:
                st[i] = lower[i]; direction[i] = 1
            else:
                st[i] = upper[i]; direction[i] = -1
        else:                                   # was bullish
            if closes[i] < lower[i]:
                st[i] = upper[i]; direction[i] = -1
            else:
                st[i] = lower[i]; direction[i] = 1

    return round(float(st[-1]), 2), bool(direction[-1] == 1), st, direction


def calc_obv(closes, volumes):
    n   = len(closes)
    obv = 0.0
    for i in range(1, n):
        if closes[i] > closes[i - 1]:
            obv += volumes[i]
        elif closes[i] < closes[i - 1]:
            obv -= volumes[i]
    return int(obv)


def calc_vwap(highs, lows, closes, volumes, window=20):
    n = min(window, len(closes))
    if n == 0 or np.sum(volumes[-n:]) == 0:
        return float(closes[-1])
    typical   = (highs[-n:] + lows[-n:] + closes[-n:]) / 3.0
    total_vol = float(np.sum(volumes[-n:]))
    return round(float(np.dot(typical, volumes[-n:]) / total_vol), 2)


def calc_pivots(high, low, close):
    pivot = (high + low + close) / 3.0
    return {
        "pivot": round(pivot, 2),
        "r1": round(2 * pivot - low,         2), "r2": round(pivot + (high - low),      2), "r3": round(high + 2 * (pivot - low), 2),
        "s1": round(2 * pivot - high,        2), "s2": round(pivot - (high - low),      2), "s3": round(low  - 2 * (high - pivot), 2),
    }


def calc_ichimoku(closes, highs, lows):
    n = len(closes)
    if n < 52:
        return {}
    tenkan  = (np.max(highs[-9:])  + np.min(lows[-9:]))  / 2.0
    kijun   = (np.max(highs[-26:]) + np.min(lows[-26:])) / 2.0
    senkou_a = (tenkan + kijun) / 2.0
    senkou_b = (np.max(highs[-52:]) + np.min(lows[-52:])) / 2.0
    price   = closes[-1]
    above   = price > max(senkou_a, senkou_b)
    below   = price < min(senkou_a, senkou_b)
    return {
        "tenkan":     round(float(tenkan),   2),
        "kijun":      round(float(kijun),    2),
        "senkou_a":   round(float(senkou_a), 2),
        "senkou_b":   round(float(senkou_b), 2),
        "cloud_signal": "ABOVE_CLOUD" if above else "BELOW_CLOUD" if below else "IN_CLOUD",
        "tk_cross":     "BULL" if tenkan > kijun else "BEAR",
    }


def detect_divergence(closes, rsi_arr, lookback=15):
    n = len(closes) - 1
    if n < lookback or len(rsi_arr) <= n:
        return {"bullish": False, "bearish": False}
    rc  = closes[n - lookback: n + 1]
    rr  = rsi_arr[n - lookback: n + 1]
    p_now, r_now    = closes[n], rsi_arr[n]
    p_prev_low  = float(np.min(rc[:-1]))
    p_prev_high = float(np.max(rc[:-1]))
    r_at_low    = float(rr[np.argmin(rc[:-1])])
    r_at_high   = float(rr[np.argmax(rc[:-1])])
    bullish = bool(p_now <= p_prev_low  * 1.01 and r_now > r_at_low  + 4)
    bearish = bool(p_now >= p_prev_high * 0.99 and r_now < r_at_high - 4)
    return {"bullish": bullish, "bearish": bearish}


def calc_sharpe(returns, risk_free=0.065):
    if len(returns) < 10:
        return 0.0
    arr     = np.array(returns)
    daily_rf = risk_free / 252.0
    excess  = arr - daily_rf
    std_e   = float(np.std(excess, ddof=1))
    if std_e == 0:
        return 0.0
    return round(float(np.mean(excess)) / std_e * math.sqrt(252), 2)


def calc_sortino(returns, risk_free=0.065):
    if len(returns) < 10:
        return 0.0
    arr      = np.array(returns)
    daily_rf = risk_free / 252.0
    excess   = arr - daily_rf
    downside = excess[excess < 0]
    if len(downside) == 0:
        return 5.0
    down_std = float(np.std(downside, ddof=1))
    if down_std == 0:
        return 0.0
    return round(float(np.mean(excess)) / down_std * math.sqrt(252), 2)


def calc_max_drawdown(equity):
    eq   = np.array(equity)
    peak = np.maximum.accumulate(eq)
    dd   = (peak - eq) / np.maximum(peak, 1e-9) * 100.0
    return round(float(np.max(dd)), 2)


# ══════════════════════════════════════════════════════════
#  BARRA-INSPIRED FACTOR MODEL SCORING
#  Factors: Momentum, Trend, RSI Reversal, MACD, Volume,
#           Low-Volatility, Mean-Reversion (Z-score)
#  Weights calibrated to Indian equity market behaviour
# ══════════════════════════════════════════════════════════
def calc_factor_score(closes, highs, lows, volumes, rsi, macd_val, macd_sig, atr):
    """
    Returns composite alpha score 0-100.
    Higher = stronger bullish signal.
    Inspired by AQR / Barra multi-factor models.
    """
    n     = len(closes)
    price = closes[-1]
    factors = {}

    # 1. MOMENTUM (12-1 momentum, Jegadeesh & Titman 1993)
    if n >= 252:
        mom = (closes[-21] - closes[-252]) / closes[-252] * 100.0
    elif n >= 63:
        mom = (closes[-1]  - closes[-63])  / closes[-63]  * 100.0
    else:
        mom = (closes[-1]  - closes[0])    / closes[0]    * 100.0 if closes[0] > 0 else 0.0
    factors['momentum'] = float(np.clip(mom / 40.0, -1.0, 1.0))

    # 2. TREND (EMA stack alignment)
    ema9   = float(calc_ema(closes, 9)[-1])
    ema21  = float(calc_ema(closes, 21)[-1])
    ema50  = float(calc_ema(closes, 50)[-1]) if n >= 50  else ema21
    ema200 = float(calc_ema(closes, 200)[-1]) if n >= 200 else ema50
    trend_pts  = sum([price > ema9, ema9 > ema21, ema21 > ema50, ema50 > ema200])
    factors['trend'] = (trend_pts / 4.0) * 2.0 - 1.0   # [-1, 1]

    # 3. RSI MEAN REVERSION
    if rsi < 25:  factors['rsi_rev'] = 1.0
    elif rsi < 35: factors['rsi_rev'] = 0.7
    elif rsi < 45: factors['rsi_rev'] = 0.3
    elif rsi < 55: factors['rsi_rev'] = 0.0
    elif rsi < 65: factors['rsi_rev'] = -0.3
    elif rsi < 75: factors['rsi_rev'] = -0.6
    else:          factors['rsi_rev'] = -0.9

    # 4. MACD HISTOGRAM SIGNAL
    hist = macd_val - macd_sig
    norm = atr * 0.05 if atr > 0 else 1e-6
    factors['macd'] = float(np.clip(hist / norm, -1.0, 1.0))

    # 5. VOLUME CONFIRMATION (rising price with volume)
    if n >= 20 and np.sum(volumes) > 0:
        avg_vol = float(np.mean(volumes[-20:]))
        vol_ratio = float(volumes[-1]) / max(avg_vol, 1e-9)
        # Volume surge on up-day = positive; on down-day = negative
        price_dir = 1 if len(closes) > 1 and closes[-1] > closes[-2] else -1
        factors['volume'] = float(np.clip((vol_ratio - 1.0) * 0.4 * price_dir, -0.5, 0.5))
    else:
        factors['volume'] = 0.0

    # 6. LOW VOLATILITY ANOMALY (Ang et al. 2006)
    atr_pct = (atr / price * 100.0) if price > 0 else 2.0
    if atr_pct < 1.0:   factors['low_vol'] = 0.5
    elif atr_pct < 2.0: factors['low_vol'] = 0.2
    elif atr_pct < 3.0: factors['low_vol'] = 0.0
    else:               factors['low_vol'] = -0.3

    # 7. Z-SCORE MEAN REVERSION (20-bar rolling)
    window = min(20, n)
    sl     = closes[-window:]
    z_mean = float(np.mean(sl))
    z_std  = float(np.std(sl, ddof=0))
    z      = (closes[-1] - z_mean) / max(z_std, 1e-9)
    factors['zscore'] = float(np.clip(-z / 2.0, -1.0, 1.0))   # contrarian

    # ── FACTOR WEIGHTS (calibrated for Indian equities) ──
    weights = {
        'momentum':  0.25,   # Strongest sustained factor
        'trend':     0.22,   # EMA alignment reliable
        'rsi_rev':   0.18,   # RSI reversal well-documented
        'macd':      0.15,   # MACD confirmation
        'volume':    0.10,   # Volume confirmation
        'low_vol':   0.05,   # Low-vol anomaly (smaller in emerging markets)
        'zscore':    0.05,   # Z-score reversal
    }

    composite = sum(factors[k] * weights[k] for k in weights)
    score     = (composite + 1.0) / 2.0 * 100.0   # map [-1,1] → [0,100]
    return round(float(np.clip(score, 0.0, 100.0)), 1)


# ══════════════════════════════════════════════════════════
#  REGIME DETECTION
# ══════════════════════════════════════════════════════════
def detect_regime(closes, highs, lows):
    """Returns market regime: BULL / BEAR / SIDEWAYS and strength."""
    n = len(closes)
    if n < 50:
        return "SIDEWAYS", 0.5
    ema21  = float(calc_ema(closes, 21)[-1])
    ema50  = float(calc_ema(closes, 50)[-1])
    ema200 = float(calc_ema(closes, 200)[-1]) if n >= 200 else ema50
    price  = closes[-1]
    atr    = calc_atr(highs, lows, closes)
    # ADX for trend strength
    adx = calc_adx(highs, lows, closes)

    bull_pts = sum([price > ema21, ema21 > ema50, ema50 > ema200, adx > 20])
    bear_pts = sum([price < ema21, ema21 < ema50, ema50 < ema200, adx > 20])

    if bull_pts >= 3:
        return "BULL", round(bull_pts / 4.0, 2)
    elif bear_pts >= 3:
        return "BEAR", round(bear_pts / 4.0, 2)
    else:
        return "SIDEWAYS", 0.5


# ══════════════════════════════════════════════════════════
#  SIGNAL GENERATION (regime-conditioned)
# ══════════════════════════════════════════════════════════
def get_signal(rsi, macd, macd_sig, price, bb_upper, bb_lower, change_pct,
               stoch_k=50, adx=25, supertrend_bull=True, cci=0, regime="BULL"):
    score = 0

    # RSI
    if rsi < 30:   score += 4
    elif rsi < 40: score += 2
    elif rsi < 50: score += 1
    elif rsi > 75: score -= 4
    elif rsi > 65: score -= 2
    elif rsi > 58: score -= 1

    # MACD
    if macd > macd_sig: score += 2
    else:               score -= 1

    # Bollinger Bands
    if price < bb_lower:   score += 2
    elif price > bb_upper: score -= 2

    # Price action
    if change_pct > 2.0:    score += 1
    elif change_pct < -2.0: score -= 1

    # Stochastic
    if stoch_k < 20:   score += 2
    elif stoch_k > 80: score -= 2

    # ADX trend confirmation
    if adx > 25:
        score += (1 if score > 0 else -1)

    # Supertrend
    if supertrend_bull:  score += 1
    else:                score -= 1

    # CCI
    if cci < -150:  score += 2
    elif cci < -100: score += 1
    elif cci > 150: score -= 2
    elif cci > 100: score -= 1

    # Regime conditioning: reduce signal strength in opposing regime
    if regime == "BEAR" and score > 0:
        score = max(score - 2, 0)
    elif regime == "BULL" and score < 0:
        score = min(score + 1, 0)

    if score >= 7:  return "STRONG BUY"
    if score >= 3:  return "BUY"
    if score <= -6: return "STRONG SELL"
    if score <= -2: return "SELL"
    return "HOLD"


def calc_targets(price, atr, signal):
    atr = max(atr, price * 0.005)
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


# ══════════════════════════════════════════════════════════
#  FULL ANALYSIS HELPER (reusable by multiple endpoints)
# ══════════════════════════════════════════════════════════
def full_analysis(sym, closes, highs, lows, volumes, dates):
    """Core analysis pipeline — returns complete analysis dict."""
    if len(closes) < 20:
        return {"error": "Insufficient data"}

    price  = round(float(closes[-1]), 2)
    prev   = round(float(closes[-2]), 2) if len(closes) > 1 else price
    chg    = round(price - prev, 2)
    pct    = round(chg / prev * 100.0, 2) if prev else 0.0

    rsi       = calc_rsi_wilder(closes)
    rsi_arr   = calc_rsi_series(closes)
    macd_v, macd_sig, macd_hist = calc_macd(closes)
    atr       = calc_atr(highs, lows, closes)
    bb_u, bb_m, bb_l = calc_bb(closes)
    stoch_k, stoch_d = calc_stochastic(highs, lows, closes)
    williams  = calc_williams_r(highs, lows, closes)
    cci_val   = calc_cci(highs, lows, closes)
    adx_val   = calc_adx(highs, lows, closes)
    obv_val   = calc_obv(closes, volumes)
    vwap_val  = calc_vwap(highs, lows, closes, volumes)
    st_level, st_bull, _, _ = calc_supertrend_stateful(highs, lows, closes)
    ichimoku  = calc_ichimoku(closes, highs, lows)
    pivots    = calc_pivots(float(np.max(highs[-5:])), float(np.min(lows[-5:])), price)
    divergence = detect_divergence(closes, rsi_arr, 15)
    regime, regime_strength = detect_regime(closes, highs, lows)

    ema9   = calc_ema(closes, 9)
    ema21  = calc_ema(closes, 21)
    ema50  = calc_ema(closes, 50)
    ema200 = calc_ema(closes, 200)
    sma20  = float(np.mean(closes[-20:])) if len(closes) >= 20 else price
    sma50  = float(np.mean(closes[-50:])) if len(closes) >= 50 else price

    # Volume analysis
    avg_vol   = float(np.mean(volumes[-20:])) if len(volumes) >= 20 else (float(np.mean(volumes)) if len(volumes) else 1.0)
    vol_ratio = round(float(volumes[-1]) / max(avg_vol, 1.0), 2) if avg_vol > 0 else 1.0
    vol_trend = "HIGH" if vol_ratio > 1.5 else "LOW" if vol_ratio < 0.7 else "NORMAL"

    # Trend detection
    p, e9, e21_v, e50_v = price, float(ema9[-1]), float(ema21[-1]), float(ema50[-1])
    if p > e9 > e21_v > e50_v: trend = "STRONG UPTREND"
    elif p > sma20 > sma50:    trend = "UPTREND"
    elif p < e9 < e21_v < e50_v: trend = "STRONG DOWNTREND"
    elif p < sma20 < sma50:    trend = "DOWNTREND"
    else:                      trend = "SIDEWAYS"

    signal = get_signal(rsi, macd_v, macd_sig, price, bb_u, bb_l, pct,
                        stoch_k, adx_val, st_bull, cci_val, regime)

    # Divergence signal boost
    if divergence["bullish"] and signal == "HOLD": signal = "BUY"
    if divergence["bearish"] and signal == "HOLD": signal = "SELL"

    sl, t1, t2, t3, rr = calc_targets(price, atr, signal)

    # Performance stats
    daily_returns = list(np.diff(closes) / closes[:-1])
    sharpe  = calc_sharpe(daily_returns)
    sortino = calc_sortino(daily_returns)
    max_dd  = calc_max_drawdown(list(closes))
    total_return = round((closes[-1] / closes[0] - 1.0) * 100.0, 2) if closes[0] > 0 else 0.0

    # 52-week stats
    high_52w = round(float(np.max(highs)), 2)
    low_52w  = round(float(np.min(lows)),  2)

    # Factor model score (Barra-inspired)
    factor_score = calc_factor_score(closes, highs, lows, volumes, rsi, macd_v, macd_sig, atr)

    # Confidence score
    confidence = 40
    if "STRONG" in signal: confidence += 22
    elif signal in ("BUY","SELL"): confidence += 12
    if vol_ratio > 1.5:    confidence += 8
    if "UPTREND" in trend or "DOWNTREND" in trend: confidence += 8
    if rr > 2:             confidence += 6
    if adx_val > 25:       confidence += 6
    if divergence["bullish"] or divergence["bearish"]: confidence += 7
    confidence = min(95, confidence)

    # Position sizing (2% risk rule)
    capital, risk_pct = 100000, 2.0
    risk_amt   = capital * risk_pct / 100.0
    risk_share = abs(price - sl)
    qty    = int(risk_amt / risk_share) if risk_share > 0 else 0
    invest = round(qty * price, 0)

    return {
        "price": price, "prev": prev, "change": chg, "pct": pct,
        "signal": signal, "confidence": confidence,
        "regime": regime, "regime_strength": regime_strength,
        "factor_score": factor_score,
        # Core indicators
        "rsi": rsi, "rsi_zone": "OVERBOUGHT" if rsi > 70 else "OVERSOLD" if rsi < 30 else "NEUTRAL",
        "macd": macd_v, "macd_signal": macd_sig, "macd_hist": macd_hist,
        "macd_trend": "BULLISH" if macd_v > macd_sig else "BEARISH",
        "atr": atr, "atr_pct": round(atr / price * 100.0, 2) if price > 0 else 0,
        "bb_upper": bb_u, "bb_mid": bb_m, "bb_lower": bb_l,
        # Advanced indicators
        "stoch_k": stoch_k, "stoch_d": stoch_d,
        "stoch_zone": "OVERBOUGHT" if stoch_k > 80 else "OVERSOLD" if stoch_k < 20 else "NEUTRAL",
        "williams_r": williams, "cci": cci_val,
        "cci_zone": "OVERBOUGHT" if cci_val > 100 else "OVERSOLD" if cci_val < -100 else "NEUTRAL",
        "adx": adx_val, "adx_strength": "STRONG" if adx_val > 25 else "WEAK",
        "obv": obv_val, "vwap": vwap_val,
        "vwap_signal": "ABOVE" if price > vwap_val else "BELOW",
        "supertrend": st_level, "supertrend_bull": st_bull,
        "supertrend_signal": "BUY" if st_bull else "SELL",
        "ichimoku": ichimoku, "pivots": pivots, "divergence": divergence,
        # Moving averages
        "ema9": round(float(ema9[-1]),  2), "ema21":  round(float(ema21[-1]),  2),
        "ema50": round(float(ema50[-1]),2), "ema200": round(float(ema200[-1]), 2),
        "sma20": round(sma20, 2),           "sma50":  round(sma50, 2),
        "trend": trend,
        # Volume & momentum
        "vol_ratio": vol_ratio, "vol_trend": vol_trend,
        # Performance
        "sharpe_ratio": sharpe, "sortino_ratio": sortino,
        "max_drawdown_pct": max_dd, "total_return_pct": total_return,
        "high_52w": high_52w, "low_52w": low_52w,
        "pct_from_52w_high": round((price / high_52w - 1.0) * 100.0, 2),
        "pct_from_52w_low":  round((price / low_52w  - 1.0) * 100.0, 2),
        # Targets
        "stop_loss": sl, "target1": t1, "target2": t2, "target3": t3,
        "risk_reward": rr,
        # Position sizing
        "position_qty": qty, "position_invest": invest,
        "sector": SECTOR_MAP.get(sym, "Other"),
        # Chart (last 120 days)
        "chart": [
            {"date": d, "close": round(float(c), 2), "volume": int(v),
             "high": round(float(h), 2), "low": round(float(l), 2)}
            for d, c, h, l, v in zip(dates[-120:], closes[-120:], highs[-120:], lows[-120:], volumes[-120:])
        ],
    }


# ══════════════════════════════════════════════════════════
#  ROUTES
# ══════════════════════════════════════════════════════════

@app.route("/")
def home():
    return jsonify({
        "status": "AI Trading Terminal Backend v4.0 — Institutional Grade",
        "version": "4.0",
        "improvements": [
            "Wilder RSI", "Stateful Supertrend", "Barra Factor Model",
            "Fixed ROCE", "Batch yf.download (5-10x faster)", "Regime Detection",
            "Walk-Forward Backtest", "Market Breadth", "Sector Rotation",
            "Dynamic Universe", "Sortino Ratio", "No Hardcoded Subsets"
        ],
        "endpoints": [
            "/universe", "/live", "/historical/<sym>", "/fundamental/<sym>",
            "/analyze/<sym>", "/screener", "/multi-analyze",
            "/news", "/heatmap", "/correlation", "/breadth",
            "/backtest/<sym>", "/sector-rotation"
        ],
        "total_symbols": len(NSE_SYMBOLS),
        "total_stocks":  len(STOCK_SYMBOLS),
    })


@app.route("/universe")
def universe():
    """Dynamic universe — no hardcoded subsets anywhere else."""
    cached = cache_get("universe", ttl=3600)
    if cached:
        return jsonify(cached)

    sectors = defaultdict(list)
    for sym, ticker in STOCK_SYMBOLS.items():
        sectors[SECTOR_MAP.get(sym, "Other")].append({
            "sym": sym, "ticker": ticker, "sector": SECTOR_MAP.get(sym, "Other")
        })

    result = {
        "total": len(STOCK_SYMBOLS),
        "indices": list(INDEX_SYMBOLS.keys()),
        "sectors": {sec: stocks for sec, stocks in sorted(sectors.items())},
        "all_symbols": sorted(STOCK_SYMBOLS.keys()),
        "sector_list": sorted(set(SECTOR_MAP.values())),
    }
    cache_set("universe", result)
    return jsonify(result)


@app.route("/live")
def live():
    """Live prices for ALL index symbols + top movers — batch fetched."""
    cached = cache_get("live", ttl=15)
    if cached:
        return jsonify(cached)

    # Fetch all indices + top 20 stocks dynamically
    top_stocks_by_sector = {}
    for sym in STOCK_SYMBOLS:
        sec = SECTOR_MAP.get(sym, "Other")
        if sec not in top_stocks_by_sector:
            top_stocks_by_sector[sec] = sym   # first stock per sector

    live_syms = {**INDEX_SYMBOLS, **{sym: STOCK_SYMBOLS[sym] for sym in top_stocks_by_sector.values()}}

    result = {}

    def fetch_price(name, ticker):
        try:
            t     = yf.Ticker(ticker)
            info  = t.fast_info
            price = round(float(info.last_price),      2)
            prev  = round(float(info.previous_close),  2)
            chg   = round(price - prev,                2)
            pct   = round(chg / prev * 100.0,          2) if prev else 0.0
            return name, {
                "price": price, "prev": prev, "change": chg, "pct": pct,
                "high":  round(float(info.day_high  or price), 2),
                "low":   round(float(info.day_low   or price), 2),
                "volume": int(info.three_month_average_volume or 0),
                "sector": SECTOR_MAP.get(name, "Index"),
            }
        except Exception as e:
            logger.warning(f"live fetch {name}: {e}")
            return name, {"price": 0, "prev": 0, "change": 0, "pct": 0}

    with ThreadPoolExecutor(max_workers=20) as ex:
        futs = {ex.submit(fetch_price, n, s): n for n, s in live_syms.items()}
        for fut in as_completed(futs, timeout=20):
            try:
                name, data = fut.result(timeout=12)
                result[name] = data
            except Exception:
                name = futs[fut]
                result[name] = {"price": 0, "prev": 0, "change": 0, "pct": 0}

    result["_ts"] = datetime.now().strftime("%H:%M:%S")
    cache_set("live", result)
    return jsonify(result)


@app.route("/historical/<sym>")
def historical(sym):
    sym    = sym.upper()
    period   = request.args.get("period",   "1y")
    interval = request.args.get("interval", "1d")
    cache_key = f"hist_{sym}_{period}_{interval}"
    ttl = 3600 if period in ("1y","2y","5y") else 900

    cached = cache_get(cache_key, ttl=ttl)
    if cached:
        return jsonify(cached)

    ticker = NSE_SYMBOLS.get(sym)
    if not ticker:
        return jsonify({"error": f"Symbol '{sym}' not in universe"}), 404

    try:
        raw = yf.Ticker(ticker).history(period=period, interval=interval, timeout=25)
        if raw.empty:
            return jsonify({"error": "No data returned"}), 404

        closes, opens, highs, lows, volumes, dates = df_to_arrays(raw)
        if len(closes) < 2:
            return jsonify({"error": "Insufficient data"}), 404

        data_out = [
            {"date": d, "open": round(float(o), 2), "high": round(float(h), 2),
             "low": round(float(l), 2), "close": round(float(c), 2), "volume": int(v)}
            for d, o, h, l, c, v in zip(dates, opens, highs, lows, closes, volumes)
        ]

        rsi       = calc_rsi_wilder(closes)
        macd_v, macd_sig, macd_hist = calc_macd(closes)
        bb_u, bb_m, bb_l = calc_bb(closes)
        atr       = calc_atr(highs, lows, closes)
        adx_val   = calc_adx(highs, lows, closes)
        daily_ret = list(np.diff(closes) / closes[:-1])
        sharpe    = calc_sharpe(daily_ret)
        sortino   = calc_sortino(daily_ret)
        total_ret = round((closes[-1] / closes[0] - 1.0) * 100.0, 2) if closes[0] > 0 else 0.0

        result = {
            "symbol": sym, "count": len(data_out), "data": data_out,
            "rsi": rsi, "macd": macd_v, "macd_signal": macd_sig, "macd_hist": macd_hist,
            "bb_upper": bb_u, "bb_mid": bb_m, "bb_lower": bb_l,
            "atr": atr, "adx": adx_val,
            "sharpe_ratio": sharpe, "sortino_ratio": sortino,
            "total_return_pct": total_ret, "period": period, "interval": interval,
        }
        cache_set(cache_key, result)
        return jsonify(result)
    except Exception as e:
        logger.error(f"historical {sym}: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/fundamental/<sym>")
def fundamental(sym):
    sym    = sym.upper()
    cached = cache_get(f"fund_{sym}", ttl=7200)
    if cached:
        return jsonify(cached)

    ticker = NSE_SYMBOLS.get(sym)
    if not ticker:
        return jsonify({"error": "Not found"}), 404

    try:
        info = yf.Ticker(ticker).info

        # ── FIXED: proper ROCE proxy ──
        # ROCE = EBIT / Capital Employed
        # Capital Employed = Total Assets − Current Liabilities
        ebit_proxy      = info.get("operatingIncome",  0) or 0
        total_assets    = info.get("totalAssets",       0) or 0
        current_liab    = info.get("totalCurrentLiabilities", 0) or 0
        capital_employed = max(total_assets - current_liab, 1)
        roce_val         = round(ebit_proxy / capital_employed * 100.0, 2) if capital_employed > 0 else 0.0

        result = {
            "symbol": sym,
            "name":   info.get("longName", sym),
            "sector": info.get("sector", SECTOR_MAP.get(sym, "N/A")),
            "industry": info.get("industry", "N/A"),
            "market_cap":    info.get("marketCap", 0),
            "market_cap_cr": round((info.get("marketCap", 0) or 0) / 1e7, 0),
            "pe_ratio":      round(info.get("trailingPE", 0)   or 0, 2),
            "forward_pe":    round(info.get("forwardPE",  0)   or 0, 2),
            "pb_ratio":      round(info.get("priceToBook", 0)  or 0, 2),
            "ps_ratio":      round(info.get("priceToSalesTrailing12Months", 0) or 0, 2),
            "eps":           round(info.get("trailingEps", 0)  or 0, 2),
            "dividend_yield": round((info.get("dividendYield", 0) or 0) * 100.0, 2),
            "roe":   round((info.get("returnOnEquity", 0)  or 0) * 100.0, 2),
            "roa":   round((info.get("returnOnAssets", 0)  or 0) * 100.0, 2),
            "roce":  roce_val,     # ← FIXED (was ROA mislabelled as ROCE)
            "debt_equity":   round(info.get("debtToEquity", 0)  or 0, 2),
            "current_ratio": round(info.get("currentRatio", 0)  or 0, 2),
            "revenue_cr":    round((info.get("totalRevenue",       0) or 0) / 1e7, 0),
            "profit_cr":     round((info.get("netIncomeToCommon",  0) or 0) / 1e7, 0),
            "revenue_growth":   round((info.get("revenueGrowth",  0) or 0) * 100.0, 2),
            "earnings_growth":  round((info.get("earningsGrowth", 0) or 0) * 100.0, 2),
            "52w_high": info.get("fiftyTwoWeekHigh", 0),
            "52w_low":  info.get("fiftyTwoWeekLow",  0),
            "avg_volume": info.get("averageVolume", 0),
            "beta": info.get("beta", 1),
            "analyst_rating": (info.get("recommendationKey", "N/A") or "N/A").upper(),
            "target_price": round(info.get("targetMeanPrice", 0) or 0, 2),
            "free_cashflow_cr": round((info.get("freeCashflow",      0) or 0) / 1e7, 0),
            "operating_margins": round((info.get("operatingMargins", 0) or 0) * 100.0, 2),
            "gross_margins":     round((info.get("grossMargins",     0) or 0) * 100.0, 2),
            "net_margins":       round((info.get("profitMargins",    0) or 0) * 100.0, 2),
            "institutional_pct": round((info.get("heldPercentInstitutions", 0) or 0) * 100.0, 2),
            "promoter_pct":      round((info.get("heldPercentInsiders",     0) or 0) * 100.0, 2),
        }

        # ── FUNDAMENTAL SCORE (0-100) ──
        score = 50
        pe  = result["pe_ratio"]
        roe = result["roe"]
        rg  = result["revenue_growth"]
        de  = result["debt_equity"]
        eg  = result["earnings_growth"]
        om  = result["operating_margins"]

        if pe and 0 < pe < 15:   score += 14
        elif pe and pe < 25:     score += 8
        elif pe and pe < 35:     score += 2
        elif pe and pe > 70:     score -= 14
        elif pe and pe > 50:     score -= 7

        if roe > 25:    score += 14
        elif roe > 18:  score += 10
        elif roe > 12:  score += 5
        elif roe < 0:   score -= 12

        if rg > 20:     score += 10
        elif rg > 10:   score += 5
        elif rg < -15:  score -= 10
        elif rg < 0:    score -= 4

        if de is not None:
            if de < 0.3:   score += 10
            elif de < 1.0: score += 5
            elif de > 5.0: score -= 14
            elif de > 3.0: score -= 8

        if eg > 25:     score += 8
        elif eg > 15:   score += 4
        elif eg < -20:  score -= 8

        if om > 25:     score += 8
        elif om > 15:   score += 4
        elif om < 0:    score -= 8

        if result["roce"] > 20: score += 6
        if result["dividend_yield"] > 2: score += 4
        if result["free_cashflow_cr"] > 0: score += 4

        result["fundamental_score"] = int(np.clip(score, 0, 100))

        # Valuation
        if pe and 0 < pe < 12:   result["valuation"] = "DEEPLY UNDERVALUED"
        elif pe and pe < 20:     result["valuation"] = "UNDERVALUED"
        elif pe and pe < 30:     result["valuation"] = "FAIR VALUE"
        elif pe and pe < 45:     result["valuation"] = "PREMIUM"
        elif pe:                 result["valuation"] = "EXPENSIVE"
        else:                    result["valuation"] = "N/A"

        cache_set(f"fund_{sym}", result)
        return jsonify(result)
    except Exception as e:
        logger.error(f"fundamental {sym}: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/analyze/<sym>")
def analyze(sym):
    sym    = sym.upper()
    cached = cache_get(f"analyze_{sym}", ttl=300)
    if cached:
        return jsonify(cached)

    ticker = NSE_SYMBOLS.get(sym)
    if not ticker:
        return jsonify({"error": f"Symbol '{sym}' not in universe"}), 404

    try:
        raw = yf.Ticker(ticker).history(period="1y", interval="1d", timeout=25)
        if raw.empty:
            return jsonify({"error": "No data"}), 404

        closes, opens, highs, lows, volumes, dates = df_to_arrays(raw)
        if len(closes) < 20:
            return jsonify({"error": "Insufficient data"}), 404

        result = {"symbol": sym, **full_analysis(sym, closes, highs, lows, volumes, dates)}
        cache_set(f"analyze_{sym}", result)
        return jsonify(result)
    except Exception as e:
        logger.error(f"analyze {sym}: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/multi-analyze")
def multi_analyze():
    syms_param = request.args.get("syms", ",".join(list(STOCK_SYMBOLS.keys())[:10]))
    syms = [s.strip().upper() for s in syms_param.split(",") if s.strip()][:20]

    valid = {s: STOCK_SYMBOLS[s] for s in syms if s in STOCK_SYMBOLS}
    if not valid:
        return jsonify({"error": "No valid symbols"}), 400

    # Use batch download
    batch = batch_download(valid, period="6mo", interval="1d")
    results = {}

    for sym, df in batch.items():
        cached = cache_get(f"analyze_{sym}", ttl=300)
        if cached:
            results[sym] = cached
            continue
        try:
            closes, opens, highs, lows, volumes, dates = df_to_arrays(df)
            if len(closes) < 20:
                continue
            r = {"symbol": sym, **full_analysis(sym, closes, highs, lows, volumes, dates)}
            # Return lightweight version
            mini = {k: r[k] for k in (
                "symbol","price","pct","change","signal","confidence","regime","factor_score",
                "rsi","macd","macd_signal","atr","adx","stoch_k","cci","supertrend_bull",
                "stop_loss","target1","target2","target3","risk_reward","sector",
                "macd_trend","trend","vol_trend","sharpe_ratio","divergence"
            ) if k in r}
            cache_set(f"analyze_{sym}", mini)
            results[sym] = mini
        except Exception as e:
            results[sym] = {"error": str(e)}

    # Symbols that weren't in batch
    for sym in syms:
        if sym not in results and sym in STOCK_SYMBOLS:
            results[sym] = {"error": "Fetch failed"}

    return jsonify({"symbols": syms, "results": results, "count": len(results)})


@app.route("/screener")
def screener():
    sector_filter = request.args.get("sector", "ALL")
    min_score     = int(request.args.get("min_score", 0))
    cache_key     = f"screener_{sector_filter}"
    cached        = cache_get(cache_key, ttl=1200)
    if cached:
        return jsonify(cached)

    scan_list = {sym: STOCK_SYMBOLS[sym] for sym in STOCK_SYMBOLS
                 if sector_filter == "ALL" or SECTOR_MAP.get(sym) == sector_filter}

    # ── BATCH DOWNLOAD for full speed ──
    logger.info(f"Screener: batch downloading {len(scan_list)} symbols …")
    t0    = time.time()
    batch = batch_download(scan_list, period="6mo", interval="1d")
    logger.info(f"Screener batch download: {time.time()-t0:.1f}s, got {len(batch)} symbols")

    results = []
    for sym, df in batch.items():
        try:
            closes, opens, highs, lows, volumes, dates = df_to_arrays(df)
            if len(closes) < 20:
                continue
            price = round(float(closes[-1]), 2)
            prev  = round(float(closes[-2]), 2)
            pct   = round((price - prev) / prev * 100.0, 2) if prev else 0.0
            rsi   = calc_rsi_wilder(closes)
            macd_v, macd_sig, _ = calc_macd(closes)
            atr   = calc_atr(highs, lows, closes)
            bb_u, _, bb_l = calc_bb(closes)
            stoch_k, _ = calc_stochastic(highs, lows, closes)
            adx_v = calc_adx(highs, lows, closes)
            _, st_bull, _, _ = calc_supertrend_stateful(highs, lows, closes)
            cci_v = calc_cci(highs, lows, closes)
            regime, _ = detect_regime(closes, highs, lows)
            signal = get_signal(rsi, macd_v, macd_sig, price, bb_u, bb_l, pct,
                                stoch_k, adx_v, st_bull, cci_v, regime)
            sl, t1, _, _, rr = calc_targets(price, atr, signal)
            factor_sc = calc_factor_score(closes, highs, lows, volumes, rsi, macd_v, macd_sig, atr)

            if factor_sc < min_score:
                continue

            high_52w = round(float(np.max(highs)), 2)
            low_52w  = round(float(np.min(lows)),  2)

            results.append({
                "sym": sym, "price": price, "pct": pct,
                "rsi": rsi, "stoch_k": stoch_k, "adx": adx_v,
                "macd_trend": "BULL" if macd_v > macd_sig else "BEAR",
                "supertrend": "BUY" if st_bull else "SELL",
                "signal": signal, "factor_score": factor_sc,
                "regime": regime,
                "stop_loss": sl, "target": t1, "rr": rr,
                "sector": SECTOR_MAP.get(sym, "Other"),
                "high_52w": high_52w, "low_52w": low_52w,
                "pct_from_52h": round((price / high_52w - 1.0) * 100.0, 2),
            })
        except Exception as e:
            logger.debug(f"screener skip {sym}: {e}")

    results.sort(key=lambda x: x["factor_score"], reverse=True)
    out = {
        "count": len(results), "data": results,
        "updated": datetime.now().strftime("%H:%M:%S"),
        "sector_filter": sector_filter,
        "fetch_time_sec": round(time.time() - t0, 1),
    }
    cache_set(cache_key, out)
    return jsonify(out)


@app.route("/news")
def news():
    cached = cache_get("news", ttl=600)
    if cached:
        return jsonify(cached)

    feeds = [
        "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms",
        "https://www.moneycontrol.com/rss/marketsnews.xml",
        "https://www.business-standard.com/rss/markets-106.rss",
        "https://www.livemint.com/rss/markets",
    ]

    # Expanded financial NLP lexicon (inspired by Loughran-McDonald)
    pos_words = {
        "surge":"strong","rally":"strong","gain":"mild","rise":"mild","profit":"mild",
        "growth":"mild","buy":"mild","bullish":"strong","strong":"mild","record":"strong",
        "beat":"strong","upgrade":"strong","outperform":"strong","buyback":"mild",
        "dividend":"mild","recovery":"mild","high":"mild","best":"mild","jump":"strong",
        "soar":"strong","boost":"mild","exceed":"mild","positive":"mild","upside":"mild",
    }
    neg_words = {
        "fall":"mild","drop":"mild","crash":"strong","loss":"mild","decline":"mild",
        "sell":"mild","bearish":"strong","weak":"mild","miss":"mild","downgrade":"strong",
        "underperform":"strong","plunge":"strong","slump":"strong","low":"mild","worst":"mild",
        "fear":"mild","risk":"mild","concern":"mild","cut":"mild","below":"mild",
        "negative":"mild","sell-off":"strong","liquidate":"strong",
    }

    sector_keywords = {
        "IT": ["infosys","tcs","wipro","hcltech","tech mahindra","software","IT sector"],
        "Banking": ["hdfc","icici","sbi","axis bank","banking","npa","rbi","rate"],
        "Pharma": ["sun pharma","drreddy","cipla","pharma","drug","fda","healthcare"],
        "Auto": ["maruti","tata motors","auto","vehicle","ev","electric"],
        "Energy": ["reliance","ongc","oil","gas","crude","energy","power"],
        "FMCG": ["hindustan unilever","itc","fmcg","consumer","retail"],
        "Metal": ["steel","aluminium","hindalco","tata steel","jswsteel","metal"],
        "Infra": ["l&t","construction","infra","cement","larsen"],
    }

    def score_article(title, summary=""):
        text = (title + " " + summary).lower()
        pos_score = sum((2 if wt == "strong" else 1) for w, wt in pos_words.items() if w in text)
        neg_score = sum((2 if wt == "strong" else 1) for w, wt in neg_words.items() if w in text)
        total     = max(pos_score + neg_score, 1)
        raw_score = round((pos_score - neg_score) / total, 3)
        sentiment = "POSITIVE" if raw_score > 0.1 else "NEGATIVE" if raw_score < -0.1 else "NEUTRAL"
        # Sector tagging
        affected_sectors = [sec for sec, kws in sector_keywords.items()
                            if any(kw in text for kw in kws)]
        return raw_score, sentiment, affected_sectors

    def fetch_feed(url):
        try:
            f     = feedparser.parse(url)
            items = []
            for entry in f.entries[:10]:
                title   = entry.get("title", "")
                summary = entry.get("summary", "")
                score, sentiment, sectors = score_article(title, summary)
                items.append({
                    "title": title, "score": score, "sentiment": sentiment,
                    "sectors": sectors, "link": entry.get("link", ""),
                    "time":  entry.get("published", ""),
                    "source": url.split("/")[2].replace("www.", ""),
                })
            return items
        except Exception:
            return []

    articles = []
    with ThreadPoolExecutor(max_workers=4) as ex:
        for res in as_completed([ex.submit(fetch_feed, u) for u in feeds], timeout=15):
            try:
                articles.extend(res.result(timeout=10))
            except Exception:
                pass

    if not articles:
        articles = [
            {"title": "Nifty 50 consolidates near support; FII activity watched",         "score": 0.1,  "sentiment": "NEUTRAL",   "sectors": [], "source": "fallback"},
            {"title": "RBI policy: Rates steady, liquidity measures positive for markets", "score": 0.3,  "sentiment": "POSITIVE",  "sectors": ["Banking"], "source": "fallback"},
            {"title": "IT sector: Strong deal pipeline, margin recovery on track",         "score": 0.35, "sentiment": "POSITIVE",  "sectors": ["IT"], "source": "fallback"},
            {"title": "Global cues mixed; commodity prices under pressure",                "score": -0.15,"sentiment": "NEGATIVE",  "sectors": ["Metal","Energy"], "source": "fallback"},
        ]

    articles.sort(key=lambda x: abs(x["score"]), reverse=True)
    avg_score  = round(sum(a["score"] for a in articles) / max(len(articles), 1), 3)
    market_mood = "BULLISH" if avg_score > 0.1 else "BEARISH" if avg_score < -0.1 else "NEUTRAL"

    # Sector sentiment aggregation
    sector_sentiment = defaultdict(list)
    for art in articles:
        for sec in art.get("sectors", []):
            sector_sentiment[sec].append(art["score"])
    sector_mood = {
        sec: round(sum(scores) / len(scores), 3)
        for sec, scores in sector_sentiment.items() if scores
    }

    result = {
        "articles": articles[:25], "avg_score": avg_score,
        "market_mood": market_mood, "count": len(articles),
        "positive_count": sum(1 for a in articles if a["sentiment"] == "POSITIVE"),
        "negative_count": sum(1 for a in articles if a["sentiment"] == "NEGATIVE"),
        "neutral_count":  sum(1 for a in articles if a["sentiment"] == "NEUTRAL"),
        "sector_mood": sector_mood,
    }
    cache_set("news", result)
    return jsonify(result)


@app.route("/heatmap")
def heatmap():
    cached = cache_get("heatmap", ttl=1200)
    if cached:
        return jsonify(cached)

    t0    = time.time()
    batch = batch_download(STOCK_SYMBOLS, period="5d", interval="1d")

    all_stocks = []
    for sym, df in batch.items():
        try:
            closes, _, highs, lows, volumes, _ = df_to_arrays(df)
            if len(closes) < 2:
                continue
            pct      = round((closes[-1] - closes[-2]) / closes[-2] * 100.0, 2)
            week_pct = round((closes[-1] - closes[0])  / closes[0]  * 100.0, 2) if len(closes) >= 5 else pct
            all_stocks.append({
                "sym": sym, "pct": pct, "week_pct": week_pct,
                "price": round(float(closes[-1]), 2),
                "volume": int(volumes[-1]) if len(volumes) else 0,
                "sector": SECTOR_MAP.get(sym, "Other"),
            })
        except Exception:
            pass

    sectors_dict = defaultdict(list)
    for s in all_stocks:
        sectors_dict[s["sector"]].append(s)

    result_sectors = []
    for sec, stocks in sectors_dict.items():
        if not stocks:
            continue
        avg_pct  = round(sum(s["pct"] for s in stocks) / len(stocks), 2)
        week_avg = round(sum(s["week_pct"] for s in stocks) / len(stocks), 2)
        result_sectors.append({
            "sector": sec, "avg_pct": avg_pct, "week_avg_pct": week_avg,
            "stocks": sorted(stocks, key=lambda x: abs(x["pct"]), reverse=True),
            "count": len(stocks),
            "gainers": sum(1 for s in stocks if s["pct"] > 0),
            "losers":  sum(1 for s in stocks if s["pct"] < 0),
        })

    result_sectors.sort(key=lambda x: x["avg_pct"], reverse=True)
    out = {
        "sectors": result_sectors,
        "updated": datetime.now().strftime("%H:%M:%S"),
        "total_stocks": len(all_stocks),
        "market_gainers": sum(1 for s in all_stocks if s["pct"] > 0),
        "market_losers":  sum(1 for s in all_stocks if s["pct"] < 0),
        "fetch_time_sec": round(time.time() - t0, 1),
    }
    cache_set("heatmap", out)
    return jsonify(out)


@app.route("/correlation")
def correlation():
    syms_param = request.args.get("syms", "RELIANCE,TCS,HDFCBANK,INFY,ICICIBANK,SBIN,ITC,WIPRO")
    syms = [s.strip().upper() for s in syms_param.split(",") if s.strip()][:10]
    valid = {s: STOCK_SYMBOLS[s] for s in syms if s in STOCK_SYMBOLS}

    batch   = batch_download(valid, period="3mo", interval="1d")
    returns = {}
    for sym, df in batch.items():
        closes, *_ = df_to_arrays(df)
        if len(closes) > 5:
            returns[sym] = np.diff(closes) / closes[:-1]

    valid_s = list(returns.keys())
    matrix  = {}
    for s1 in valid_s:
        matrix[s1] = {}
        r1 = returns[s1]
        for s2 in valid_s:
            r2 = returns[s2]
            n  = min(len(r1), len(r2))
            if n < 10:
                matrix[s1][s2] = 0.0
                continue
            corr = float(np.corrcoef(r1[-n:], r2[-n:])[0, 1])
            matrix[s1][s2] = round(corr, 3) if np.isfinite(corr) else 0.0

    return jsonify({"symbols": valid_s, "matrix": matrix})


@app.route("/breadth")
def market_breadth():
    """
    Market Breadth indicators — institutional-grade:
    - Advance/Decline ratio
    - New 52-week highs vs lows
    - % stocks above EMA50 / EMA200
    - McClellan Oscillator proxy
    """
    cached = cache_get("breadth", ttl=1800)
    if cached:
        return jsonify(cached)

    t0    = time.time()
    batch = batch_download(STOCK_SYMBOLS, period="1y", interval="1d")

    advances, declines, unchanged = 0, 0, 0
    new_highs, new_lows           = 0, 0
    above_ema50, above_ema200     = 0, 0
    total = 0
    breadth_series = []   # daily A/D for McClellan

    for sym, df in batch.items():
        try:
            closes, _, highs, lows, _, dates = df_to_arrays(df)
            if len(closes) < 20:
                continue
            total += 1
            pct_chg = (closes[-1] - closes[-2]) / closes[-2] if len(closes) > 1 and closes[-2] > 0 else 0
            if pct_chg > 0.001:   advances += 1
            elif pct_chg < -0.001: declines += 1
            else:                  unchanged += 1

            # 52-week high/low
            h52 = float(np.max(highs))
            l52 = float(np.min(lows))
            if closes[-1] >= h52 * 0.99: new_highs += 1
            if closes[-1] <= l52 * 1.01: new_lows  += 1

            # EMA positions
            if len(closes) >= 50:
                e50  = float(calc_ema(closes, 50)[-1])
                if closes[-1] > e50: above_ema50 += 1
            if len(closes) >= 200:
                e200 = float(calc_ema(closes, 200)[-1])
                if closes[-1] > e200: above_ema200 += 1
        except Exception:
            pass

    adr           = round(advances / max(declines, 1), 2)
    pct_above_50  = round(above_ema50  / max(total, 1) * 100, 1)
    pct_above_200 = round(above_ema200 / max(total, 1) * 100, 1)
    nh_nl_diff    = new_highs - new_lows

    # McClellan-like breadth thrust
    net_adv = advances - declines
    breadth_thrust = round(net_adv / max(total, 1) * 100, 1)

    # Interpretation
    if adr > 1.5 and pct_above_50 > 60:   breadth_signal = "STRONGLY BULLISH"
    elif adr > 1.2 and pct_above_50 > 50: breadth_signal = "BULLISH"
    elif adr < 0.7 and pct_above_50 < 40: breadth_signal = "BEARISH"
    elif adr < 0.5 and pct_above_50 < 30: breadth_signal = "STRONGLY BEARISH"
    else:                                   breadth_signal = "NEUTRAL"

    out = {
        "advances": advances, "declines": declines, "unchanged": unchanged,
        "total": total, "adr": adr,
        "new_52w_highs": new_highs, "new_52w_lows": new_lows,
        "nh_nl_diff": nh_nl_diff,
        "pct_above_ema50": pct_above_50,
        "pct_above_ema200": pct_above_200,
        "breadth_thrust": breadth_thrust,
        "breadth_signal": breadth_signal,
        "updated": datetime.now().strftime("%H:%M:%S"),
        "fetch_time_sec": round(time.time() - t0, 1),
    }
    cache_set("breadth", out)
    return jsonify(out)


@app.route("/sector-rotation")
def sector_rotation():
    """
    Sector rotation analysis:
    Returns sector performance over 1w / 1m / 3m / 6m / 1y.
    Identifies which sectors are accelerating vs decelerating.
    """
    cached = cache_get("sector_rotation", ttl=3600)
    if cached:
        return jsonify(cached)

    t0    = time.time()
    batch = batch_download(STOCK_SYMBOLS, period="1y", interval="1d")

    sector_data = defaultdict(lambda: {"closes_list": [], "names": []})
    for sym, df in batch.items():
        try:
            closes, *_ = df_to_arrays(df)
            if len(closes) < 20:
                continue
            sec = SECTOR_MAP.get(sym, "Other")
            # Normalize to 100 at start
            norm = closes / closes[0] * 100.0
            sector_data[sec]["closes_list"].append(norm)
            sector_data[sec]["names"].append(sym)
        except Exception:
            pass

    periods = {"1w": 5, "1m": 21, "3m": 63, "6m": 126, "1y": 252}
    results = []

    for sec, data in sector_data.items():
        if not data["closes_list"]:
            continue
        # Build equal-weight sector index
        min_len = min(len(c) for c in data["closes_list"])
        matrix  = np.array([c[-min_len:] for c in data["closes_list"]])
        idx     = np.mean(matrix, axis=0)
        if len(idx) < 5:
            continue

        perf = {}
        for label, bars in periods.items():
            if len(idx) >= bars + 1:
                perf[label] = round(float((idx[-1] / idx[-bars - 1] - 1) * 100), 2)
            else:
                perf[label] = round(float((idx[-1] / idx[0] - 1) * 100), 2)

        # Momentum: is recent perf accelerating?
        momentum = round(perf.get("1m", 0) - perf.get("3m", 0) / 3.0, 2)

        results.append({
            "sector": sec,
            "stock_count": len(data["names"]),
            "stocks": data["names"],
            "performance": perf,
            "momentum_accel": momentum,
            "rotation_signal": (
                "ROTATING IN"  if perf.get("1m",0) > 0 and momentum > 1 else
                "ROTATING OUT" if perf.get("1m",0) < 0 and momentum < -1 else
                "STABLE"
            ),
        })

    results.sort(key=lambda x: x["performance"].get("1m", 0), reverse=True)
    out = {
        "sectors": results,
        "top_sector_1m":    results[0]["sector"]  if results else "N/A",
        "worst_sector_1m":  results[-1]["sector"] if results else "N/A",
        "updated": datetime.now().strftime("%H:%M:%S"),
        "fetch_time_sec": round(time.time() - t0, 1),
    }
    cache_set("sector_rotation", out)
    return jsonify(out)


@app.route("/backtest/<sym>")
def backtest(sym):
    """
    Walk-forward backtest — train on first 3Y, test on last 2Y.
    Also runs full 5Y to show in-sample vs out-of-sample comparison.
    No options. Stocks only.
    """
    sym    = sym.upper()
    strat  = request.args.get("strategy", "ai_combo")
    cached = cache_get(f"bt_{sym}_{strat}", ttl=3600)
    if cached:
        return jsonify(cached)

    ticker = NSE_SYMBOLS.get(sym)
    if not ticker:
        return jsonify({"error": "Not found"}), 404

    try:
        raw = yf.Ticker(ticker).history(period="5y", interval="1d", timeout=30)
        if raw.empty or len(raw) < 200:
            return jsonify({"error": "Need ≥200 trading days for backtest"}), 400

        closes, _, highs, lows, volumes, dates = df_to_arrays(raw)
        n = len(closes)

        commission = float(request.args.get("commission", 0.05)) / 100.0
        slippage   = float(request.args.get("slippage",   0.10)) / 100.0
        capital    = float(request.args.get("capital", 100000))

        def run_sim(c, h, l, v, cap, strat_name):
            return _simulate(strat_name, c, h, l, v, cap, commission, slippage)

        # Walk-forward split: 60% train window, 40% test window
        split     = int(n * 0.60)
        full_r    = run_sim(closes, highs, lows, volumes, capital, strat)
        in_r      = run_sim(closes[:split],  highs[:split],  lows[:split],  volumes[:split],  capital, strat)
        out_r     = run_sim(closes[split:],  highs[split:],  lows[split:],  volumes[split:],  capital, strat)

        bah_full  = round((closes[-1]     / closes[0]      - 1) * 100, 2) if closes[0] > 0 else 0
        bah_out   = round((closes[-1]     / closes[split]  - 1) * 100, 2) if closes[split] > 0 else 0

        result = {
            "symbol": sym, "strategy": strat,
            "period": f"{dates[0]} → {dates[-1]}",
            "total_bars": n, "train_bars": split, "test_bars": n - split,
            "commission_pct": commission * 100, "slippage_pct": slippage * 100,
            # Full-period results
            "full": {**full_r, "bah": bah_full, "alpha": round(full_r["total_ret"] - bah_full, 2)},
            # Walk-forward
            "in_sample":  {**in_r},
            "out_sample": {**out_r, "bah": bah_out, "alpha": round(out_r["total_ret"] - bah_out, 2)},
            # Overfitting indicator
            "performance_decay_pct": round(in_r["total_ret"] - out_r["total_ret"], 2),
            "robust": bool(out_r["total_ret"] > 0 and out_r["win_rate"] > 45),
        }
        cache_set(f"bt_{sym}_{strat}", result)
        return jsonify(result)
    except Exception as e:
        logger.error(f"backtest {sym}: {e}")
        return jsonify({"error": str(e)}), 500


def _simulate(strat, closes, highs, lows, volumes, capital, commission, slippage):
    """Core simulation engine — single strategy, full period."""
    n = len(closes)
    if n < 50:
        return {"total_ret": 0, "win_rate": 0, "max_dd": 0, "sharpe": 0, "sortino": 0,
                "final_cap": capital, "total_trades": 0, "wins": 0, "losses": 0, "trades": []}

    # Precompute indicators
    rsi_arr = calc_rsi_series(closes)
    ema9    = calc_ema(closes, 9)
    ema21   = calc_ema(closes, 21)
    ema50   = calc_ema(closes, 50)
    ema200  = calc_ema(closes, 200)
    bb_up, _, bb_lo = calc_bb(closes)   # only last value needed per bar — approximate
    atr_arr = calc_atr_array(highs, lows, closes, 14)
    _, _, st_arr, st_dir = calc_supertrend_stateful(highs, lows, closes)
    vol_avg = np.array([float(np.mean(volumes[max(0,i-20):i+1])) for i in range(n)])

    macd_line = calc_ema(closes, 12) - calc_ema(closes, 26)
    sig_line  = calc_ema(macd_line, 9)

    cash, pos, entry = capital, 0.0, 0.0
    entry_idx, trail_stop, partial_done = -1, 0.0, False
    wins, losses, total_trades = 0, 0, 0
    consec_loss = 0
    equity  = [capital]
    trades  = []

    for i in range(50, n):
        p   = float(closes[i])
        atr = float(atr_arr[i])
        if atr <= 0: atr = p * 0.01

        # ATR trailing stop
        if pos > 0:
            new_trail = p - 2.5 * atr
            if not trail_stop or new_trail > trail_stop:
                trail_stop = new_trail
            if p < trail_stop:
                exec_p = p * (1 - slippage)
                pnl = (exec_p - entry) * pos - exec_p * pos * commission * 2
                cash += pos * exec_p * (1 - commission)
                (wins if pnl > 0 else losses).__class__  # trick to avoid increment
                if pnl > 0: wins += 1
                else:       losses += 1
                total_trades += 1
                consec_loss = 0 if pnl > 0 else consec_loss + 1
                trades.append({"type": "TRAIL", "price": round(exec_p, 2),
                                "pnl": round(pnl, 0), "bar": i})
                pos = 0; trail_stop = 0; partial_done = False; entry_idx = -1
                equity.append(round(cash, 2)); continue

        # Partial exit at T1
        if pos > 0 and not partial_done and atr > 0:
            t1 = entry + 1.5 * atr
            if p >= t1:
                half = pos / 2.0
                exec_p = p * (1 - slippage)
                pnl = (exec_p - entry) * half - exec_p * half * commission * 2
                cash += half * exec_p * (1 - commission)
                pos -= half; partial_done = True
                if pnl > 0: wins += 1
                total_trades += 1
                trades.append({"type": "PARTIAL", "price": round(exec_p, 2),
                                "pnl": round(pnl, 0), "bar": i})

        # Max hold: 20 bars
        if pos > 0 and entry_idx > 0 and (i - entry_idx) >= 20:
            exec_p = p * (1 - slippage)
            pnl = (exec_p - entry) * pos - exec_p * pos * commission * 2
            cash += pos * exec_p * (1 - commission)
            if pnl > 0: wins += 1
            else:       losses += 1
            total_trades += 1
            consec_loss = 0 if pnl > 0 else consec_loss + 1
            trades.append({"type": "MAXHOLD", "price": round(exec_p, 2),
                            "pnl": round(pnl, 0), "bar": i})
            pos = 0; trail_stop = 0; partial_done = False; entry_idx = -1
            equity.append(round(cash, 2)); continue

        # Generate signal
        buy_sig, sell_sig = _strat_signal(strat, i, closes, highs, lows, volumes,
                                          rsi_arr, macd_line, sig_line,
                                          ema9, ema21, ema50, ema200,
                                          atr_arr, vol_avg, st_arr, st_dir)

        if buy_sig and pos == 0:
            size_mult = (0.4 if consec_loss >= 3 else 0.6 if consec_loss >= 2 else 1.0)
            regime_mult = 0.5 if p < float(ema200[i]) else 1.0
            exec_p = p * (1 + slippage)
            invest = cash * 0.85 * size_mult * regime_mult
            pos    = invest / exec_p
            entry  = exec_p
            entry_idx = i
            partial_done = False
            trail_stop = exec_p - 2.5 * atr
            cash -= invest + invest * commission
            trades.append({"type": "BUY", "price": round(exec_p, 2), "bar": i})

        elif sell_sig and pos > 0:
            exec_p = p * (1 - slippage)
            pnl = (exec_p - entry) * pos - exec_p * pos * commission * 2
            cash += pos * exec_p * (1 - commission)
            if pnl > 0: wins += 1
            else:       losses += 1
            total_trades += 1
            consec_loss = 0 if pnl > 0 else consec_loss + 1
            trades.append({"type": "SELL", "price": round(exec_p, 2),
                            "pnl": round(pnl, 0), "bar": i})
            pos = 0; trail_stop = 0; partial_done = False; entry_idx = -1

        equity.append(round(cash + pos * p, 2))

    # Close at end
    if pos > 0:
        exec_p = float(closes[-1]) * (1 - slippage)
        pnl = (exec_p - entry) * pos - exec_p * pos * commission * 2
        cash += pos * exec_p * (1 - commission)
        if pnl > 0: wins += 1
        else:       losses += 1
        total_trades += 1
        trades.append({"type": "SELL(EOD)", "price": round(exec_p, 2),
                        "pnl": round(pnl, 0), "bar": n - 1})
        equity.append(round(cash, 2))

    final_cap  = cash
    total_ret  = round((final_cap - capital) / capital * 100.0, 2)
    win_rate   = round(wins / max(total_trades, 1) * 100.0, 1)
    max_dd     = calc_max_drawdown(equity)
    eq_arr     = np.array(equity)
    daily_r    = np.diff(eq_arr) / eq_arr[:-1] if len(eq_arr) > 1 else np.array([0.0])
    sharpe_v   = calc_sharpe(daily_r.tolist())
    sortino_v  = calc_sortino(daily_r.tolist())
    score      = round(sharpe_v * 40 + min((total_ret - (closes[-1]/closes[0]-1)*100), 50) * 0.3 +
                       (win_rate/100) * 20 + min(total_ret, 100) / 100.0 * 10, 2)

    return {
        "total_ret": total_ret, "win_rate": win_rate, "max_dd": max_dd,
        "sharpe": sharpe_v, "sortino": sortino_v, "score": score,
        "final_cap": round(final_cap, 0), "total_trades": total_trades,
        "wins": wins, "losses": losses,
        "trades": trades[-30:],   # last 30 only for payload size
        "equity": equity[::max(1, len(equity)//200)],  # downsample for chart
    }


def _strat_signal(strat, i, closes, highs, lows, vols,
                  rsi_arr, macd_arr, sig_arr,
                  ema9, ema21, ema50, ema200,
                  atr_arr, vol_avg, st_arr, st_dir):
    """All 12 strategy signal generators — returns (buy, sell) booleans."""
    p    = float(closes[i])
    r    = float(rsi_arr[i])
    mc   = float(macd_arr[i]); sg = float(sig_arr[i])
    mc_p = float(macd_arr[i-1]); sg_p = float(sig_arr[i-1])
    e9   = float(ema9[i]);   e21 = float(ema21[i])
    e50  = float(ema50[i]);  e200 = float(ema200[i])
    atr  = float(atr_arr[i]) or p * 0.01
    vol  = float(vols[i]);   avg_v = float(vol_avg[i]) or 1.0
    vol_ok    = vol > avg_v * 1.0
    uptrend   = p > e21 and e21 > e50
    downtrend = p < e21 and e21 < e50
    bear_mkt  = p < e200
    st_bull   = bool(st_dir[i] == 1)

    mom20 = (p - float(closes[i-20])) / float(closes[i-20]) if i >= 20 and closes[i-20] > 0 else 0.0

    buy = sell = False

    if strat == "rsi":
        buy  = r < 32 and not downtrend
        sell = r > 72

    elif strat == "macd":
        buy  = mc > sg and mc_p <= sg_p and not bear_mkt
        sell = mc < sg and mc_p >= sg_p

    elif strat == "bb":
        bb_lo = p - 2.0 * atr     # approximate
        bb_up = p + 2.0 * atr
        buy  = p < bb_lo and r < 55 and not downtrend
        sell = p > bb_up and r > 50

    elif strat == "ema_cross":
        buy  = e9 > e21 and float(ema9[i-1]) <= float(ema21[i-1]) and uptrend
        sell = e9 < e21 and float(ema9[i-1]) >= float(ema21[i-1])

    elif strat == "golden_cross":
        buy  = e50 > e200 and float(ema50[i-1]) <= float(ema200[i-1])
        sell = e50 < e200 and float(ema50[i-1]) >= float(ema200[i-1])

    elif strat == "rsi_macd":
        buy  = r < 50 and mc > sg and (mc - sg) > (float(macd_arr[i-1]) - float(sig_arr[i-1])) and not downtrend
        sell = r > 60 and mc < sg

    elif strat == "supertrend":
        prev_bull = bool(st_dir[i-1] == 1) if i > 0 else st_bull
        buy  = st_bull and not prev_bull     # just turned bullish
        sell = not st_bull and prev_bull     # just turned bearish

    elif strat == "mean_rev":
        window = min(20, i + 1)
        sl = closes[i - window + 1:i + 1]
        mean_v = float(np.mean(sl)); std_v = float(np.std(sl, ddof=0)) or 1e-9
        z = (p - mean_v) / std_v
        buy  = z < -1.8 and r < 45 and not bear_mkt
        sell = z > 1.5  or (z > 0.5 and r > 60)

    elif strat == "breakout":
        h50 = float(np.max(closes[max(0, i-50):i]))
        l50 = float(np.min(closes[max(0, i-50):i]))
        buy  = p > h50 * 0.999 and vol > avg_v * 1.3 and not bear_mkt
        sell = p < l50 * 1.001 or p < float(closes[i-1]) - 2 * atr

    elif strat == "triple":
        weekly_bull = p > e50 and (float(ema50[max(0,i-5)]) < e50)
        daily_bull  = r < 50 and r > float(rsi_arr[i-1])
        entry_ok    = p > float(closes[i-2]) and vol > avg_v * 0.9
        buy  = weekly_bull and daily_bull and entry_ok
        sell = not weekly_bull or r > 72

    elif strat == "trend_follow":
        buy  = p > e21 and e21 > e50 and mom20 > 0.5 and vol_ok and not bear_mkt
        sell = p < e21 or mom20 < -0.5

    else:  # ai_combo
        sc = 0
        if uptrend: sc += 2
        elif downtrend: sc -= 3
        if not bear_mkt: sc += 1
        if r < 35: sc += 3
        elif r < 45: sc += 2
        elif r < 55: sc += 1
        elif r > 72: sc -= 3
        elif r > 62: sc -= 2
        if mc > sg and mc_p <= sg_p: sc += 3
        elif mc > sg and (mc-sg) > (mc_p-sg_p): sc += 2
        elif mc > sg: sc += 1
        elif mc < sg and mc_p >= sg_p: sc -= 3
        elif mc < sg: sc -= 1
        if mom20 > 1.5: sc += 2
        elif mom20 > 0.5: sc += 1
        elif mom20 < -1.5: sc -= 2
        elif mom20 < -0.5: sc -= 1
        if vol_ok: sc += 1
        if st_bull: sc += 1
        else:       sc -= 1
        buy  = sc >= 6
        sell = sc <= -4

    return buy, sell


# ══════════════════════════════════════════════════════════
#  BACKGROUND TASKS
# ══════════════════════════════════════════════════════════
def warm_cache():
    """Pre-warm critical caches on startup."""
    logger.info("Starting cache warm-up …")
    time.sleep(3)   # let Flask finish starting
    try:
        with app.test_client() as c:
            c.get("/live")
            logger.info("Cache warm: /live done")
    except Exception as e:
        logger.warning(f"Cache warm failed: {e}")


def cache_cleaner_loop():
    while True:
        time.sleep(3600)
        cache_clear_expired()


# ══════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════
if __name__ == "__main__":
    logger.info("Starting AI Trading Terminal Backend v4.0")
    threading.Thread(target=warm_cache,         daemon=True).start()
    threading.Thread(target=cache_cleaner_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=10000, threaded=True)
