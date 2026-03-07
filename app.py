from flask import Flask, jsonify
from flask_cors import CORS
import yfinance as yf

app = Flask(__name__)
CORS(app)

SYMBOLS = {
    "NIFTY":    "^NSEI",
    "NIFTY50":  "^NSEI",
    "NIFTY": "NIFTYBEES.NS"
    "SENSEX":   "^BSESN",
    "RELIANCE": "RELIANCE.NS",
    "TCS":      "TCS.NS",
    "INFY":     "INFY.NS",
    "HDFCBANK": "HDFCBANK.NS",
    "WIPRO":    "WIPRO.NS",
    "ITC":      "ITC.NS",
    "AIRTEL":   "BHARTIARTL.NS",
    "BAJFIN":   "BAJFINANCE.NS",
    "ICICI":    "ICICIBANK.NS",
    "ADANI":    "ADANIENT.NS",
}

@app.route("/")
def home():
    return "Free yfinance Backend Running!"

@app.route("/live")
def live():
    result = {}
    for name, sym in SYMBOLS.items():
        try:
            ticker = yf.Ticker(sym)
            price  = round(float(ticker.fast_info.last_price), 2)
            result[name] = price
        except Exception:
            result[name] = 0
    return __import__('flask').jsonify(result)

@app.route("/historical/<sym>")
def historical(sym):
    symbol = SYMBOLS.get(sym.upper())
    if not symbol:
        return __import__('flask').jsonify({"error": "Symbol not found"}), 404
    ticker = yf.Ticker(symbol)
    hist   = ticker.history(period="2y", interval="1d")
    prices = [{"date": str(idx)[:10], "close": round(float(row["Close"]), 2)}
              for idx, row in hist.iterrows()]
    return __import__('flask').jsonify({"symbol": sym.upper(), "count": len(prices), "data": prices})

@app.route("/indicators/<sym>")
def indicators(sym):
    symbol = SYMBOLS.get(sym.upper())
    if not symbol:
        return __import__('flask').jsonify({"error": "Symbol not found"}), 404
    ticker = yf.Ticker(symbol)
    hist   = ticker.history(period="3mo")
    closes = list(hist["Close"])
    period = 14
    gains  = [max(closes[i]-closes[i-1], 0) for i in range(1, len(closes))]
    losses = [max(closes[i-1]-closes[i], 0) for i in range(1, len(closes))]
    rs     = (sum(gains[-period:])/period) / max(sum(losses[-period:])/period, 0.001)
    rsi    = round(100 - 100/(1+rs), 2)
    def ema(p, n):
        k=2/(n+1); e=[p[0]]
        for x in p[1:]: e.append(x*k+e[-1]*(1-k))
        return e
    e12  = ema(closes, 12)
    e26  = ema(closes, 26)
    macd = round(e12[-1]-e26[-1], 2)
    sig  = round(ema([e12[i]-e26[i] for i in range(len(closes))], 9)[-1], 2)
    return __import__('flask').jsonify({
        "symbol": sym.upper(), "close": round(closes[-1], 2),
        "rsi": rsi, "rsi_signal": "OVERBOUGHT" if rsi>70 else "OVERSOLD" if rsi<30 else "NEUTRAL",
        "macd": macd, "signal": sig, "macd_trend": "BULLISH" if macd>sig else "BEARISH"
    })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
