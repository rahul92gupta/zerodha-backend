from flask import Flask, jsonify, request
from flask_cors import CORS
from kiteconnect import KiteConnect
from datetime import date, timedelta
import os

app = Flask(__name__)
CORS(app)

API_KEY    = os.environ.get("ZERODHA_API_KEY", "")
API_SECRET = os.environ.get("ZERODHA_SECRET", "")
kite = KiteConnect(api_key=API_KEY)

ACCESS_TOKEN = ""

TOKENS = {
    "NIFTY":    256265,
    "SENSEX":   265,
    "RELIANCE": 738561,
    "TCS":      2953217,
    "INFY":     408065,
    "HDFCBANK": 341249,
    "WIPRO":    969473,
    "ITC":      424961,
}

@app.route("/")
def home():
    return jsonify({"status": "running"})

@app.route("/login")
def login():
    return jsonify({"url": kite.login_url()})

@app.route("/callback")
def callback():
    global ACCESS_TOKEN
    req_token = request.args.get("request_token")
    session = kite.generate_session(req_token, API_SECRET)
    ACCESS_TOKEN = session["access_token"]
    kite.set_access_token(ACCESS_TOKEN)
    return "<h2>Login Successful! Close this tab.</h2>"

@app.route("/live")
def live():
    if not ACCESS_TOKEN:
        return jsonify({"error": "Login first at /login"}), 401
    kite.set_access_token(ACCESS_TOKEN)
    data = kite.ltp([
        "NSE:NIFTY 50", "BSE:SENSEX",
        "NSE:RELIANCE", "NSE:TCS",
        "NSE:INFY", "NSE:HDFCBANK",
        "NSE:WIPRO", "NSE:ITC"
    ])
    result = {}
    for key, val in data.items():
        result[key.split(":")[1]] = val["last_price"]
    return jsonify(result)

@app.route("/historical/<sym>")
def historical(sym):
    if not ACCESS_TOKEN:
        return jsonify({"error": "Login first"}), 401
    kite.set_access_token(ACCESS_TOKEN)
    token = TOKENS.get(sym.upper())
    if not token:
        return jsonify({"error": "Symbol not found"}), 404
    hist = kite.historical_data(
        instrument_token=token,
        from_date=date.today() - timedelta(days=730),
        to_date=date.today(),
        interval="day"
    )
    prices = [{"date": str(d["date"])[:10], "close": d["close"]} for d in hist]
    return jsonify({"symbol": sym.upper(), "data": prices})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
```

---

**File 2 — `requirements.txt`** (Add file → Create new file → name it `requirements.txt`)
```
flask
flask-cors
kiteconnect
gunicorn
```

---

## STEP 2 — Deploy on Render.com

1. Go to `render.com` → Sign up free with Google
2. Click **"New +"** → **"Web Service"**
3. Connect your **GitHub account**
4. Select `zerodha-backend` repo
5. Fill these fields:
```
Name:          zerodha-backend
Runtime:       Python 3
Build Command: pip install -r requirements.txt
Start Command: gunicorn app:app
```
6. Scroll down → **Environment Variables** → Add:
```
Key:   ZERODHA_API_KEY
Value: your_new_api_key

Key:   ZERODHA_SECRET  
Value: your_new_api_secret
```
7. Click **"Create Web Service"**
8. Wait 3 minutes → You get URL like:
```
https://zerodha-backend.onrender.com
```

---

## STEP 3 — Set Callback URL in Zerodha

1. Go to `kite.zerodha.com` → Console → Your App
2. Change **Redirect URL** to:
```
https://zerodha-backend.onrender.com/callback
```
3. Save

---

## STEP 4 — Login Once Daily

Open this in browser:
```
https://zerodha-backend.onrender.com/login
