import requests
import pandas as pd
import numpy as np
import streamlit as st

try:
    import yfinance as yf
except Exception:
    yf = None

# -----------------------------
# SETUP
# -----------------------------
st.set_page_config(page_title="Yahoo Stock Scoring Dashboard + Scanner", layout="wide")

# -----------------------------
# LOGIN
# -----------------------------
def login_screen():
    st.markdown(
        """
        <div style="text-align:center; padding: 2rem 0 1rem 0;">
            <h1>Stock Scoring Dashboard</h1>
            <p style="font-size:18px; color: gray;">Private access only</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    login_box = st.container(border=True)

    with login_box:
        st.subheader("Login")
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        login_clicked = st.button("Sign in", use_container_width=True)

        correct_username = st.secrets.get("APP_USERNAME", "admin")
        correct_password = st.secrets.get("APP_PASSWORD", "")

        if login_clicked:
            if username == correct_username and password == correct_password and correct_password:
                st.session_state["authenticated"] = True
                st.session_state["username"] = username
                st.rerun()
            else:
                st.error("Incorrect username or password.")

    st.caption("Set APP_USERNAME and APP_PASSWORD in Streamlit Secrets.")


if "authenticated" not in st.session_state:
    st.session_state["authenticated"] = False

if not st.session_state["authenticated"]:
    login_screen()
    st.stop()


with st.sidebar:
    st.success(f"Logged in as {st.session_state.get('username', 'user')}")
    if st.button("Log out"):
        st.session_state["authenticated"] = False
        st.session_state.pop("username", None)
        st.rerun()



# -----------------------------
# HELPERS
# -----------------------------
def safe_float(x, default=np.nan):
    try:
        if x is None or x == "":
            return default
        if isinstance(x, str):
            x = x.replace("%", "").replace(",", "").strip()
        return float(x)
    except Exception:
        return default


def calculate_rsi(close, period=14):
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.rolling(period).mean()
    avg_loss = loss.rolling(period).mean()

    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def score_range(value, rules):
    for condition, points in rules:
        try:
            if condition(value):
                return points
        except Exception:
            pass
    return 0


def rating(score):
    if score >= 85:
        return "STRONG SETUP"
    if score >= 70:
        return "GOOD SETUP"
    if score >= 55:
        return "WATCHLIST / WAIT"
    return "AVOID / WEAK SETUP"


def generate_trade_alert(values, entry_price=np.nan):
    score = safe_float(values.get("Score"))
    current_price = safe_float(values.get("Current Price"))
    rsi = safe_float(values.get("RSI"))
    change_5d = safe_float(values.get("5D Price Change %"))
    dist_50 = safe_float(values.get("Distance from 50D MA %"))
    dist_200 = safe_float(values.get("Distance from 200D MA %"))
    entry_price = safe_float(entry_price)

    gain_loss_pct = np.nan
    if not np.isnan(entry_price) and entry_price > 0 and not np.isnan(current_price):
        gain_loss_pct = ((current_price / entry_price) - 1) * 100

    sell_reasons = []
    trim_reasons = []
    buy_reasons = []

    if not np.isnan(score) and score < 55:
        sell_reasons.append("Score dropped below 55")
    if not np.isnan(dist_200) and dist_200 < 0:
        sell_reasons.append("Price is below the 200D moving average")
    if not np.isnan(gain_loss_pct) and gain_loss_pct <= -7:
        sell_reasons.append("Position is down 7% or more from entry")
    if not np.isnan(dist_50) and dist_50 < -10:
        sell_reasons.append("Price is more than 10% below the 50D moving average")

    if not np.isnan(gain_loss_pct) and gain_loss_pct >= 10:
        trim_reasons.append("Position is up 10% or more from entry")
    if not np.isnan(rsi) and rsi >= 75:
        trim_reasons.append("RSI is 75 or higher")
    if not np.isnan(change_5d) and change_5d > 8:
        trim_reasons.append("5-day price change is above +8%")

    if not np.isnan(score) and score >= 85:
        buy_reasons.append("Score is 85 or higher")
    elif not np.isnan(score) and score >= 70:
        buy_reasons.append("Score is 70 or higher")

    if sell_reasons:
        return "SELL", "Exit / avoid new entry", sell_reasons, gain_loss_pct
    if trim_reasons:
        return "TRIM", "Take partial profit or tighten stop", trim_reasons, gain_loss_pct
    if buy_reasons:
        return "BUY", "Setup is acceptable for entry/watchlist", buy_reasons, gain_loss_pct
    return "WATCH", "No clean buy or exit signal", ["Setup is not strong enough for BUY, but no major SELL trigger"], gain_loss_pct


# -----------------------------
# YAHOO DATA
# -----------------------------
@st.cache_data(ttl=900)
def get_yahoo_chart_history(symbol):
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    params = {
        "range": "1y",
        "interval": "1d",
        "includePrePost": "false",
        "events": "div,splits",
    }
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        response = requests.get(url, params=params, headers=headers, timeout=20)

        if response.status_code != 200:
            return {}, pd.DataFrame(), f"Yahoo chart HTTP {response.status_code}"

        data = response.json()
        result = data.get("chart", {}).get("result", [])

        if not result:
            error = data.get("chart", {}).get("error")
            return {}, pd.DataFrame(), f"Yahoo chart no result: {error}"

        result = result[0]
        timestamps = result.get("timestamp", [])
        quote = result.get("indicators", {}).get("quote", [{}])[0]
        close = quote.get("close", [])

        meta = result.get("meta", {}) or {}

        if not timestamps or not close:
            return meta, pd.DataFrame(), "Yahoo chart missing timestamps/close"

        df = pd.DataFrame({
            "date": pd.to_datetime(timestamps, unit="s"),
            "close": close,
        })

        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        df = df.dropna(subset=["close"]).sort_values("date")

        return meta, df, ""

    except Exception as e:
        return {}, pd.DataFrame(), f"Yahoo chart failed: {e}"


@st.cache_data(ttl=3600)
def get_yfinance_info(symbol):
    if yf is None:
        return {}

    try:
        ticker = yf.Ticker(symbol)
        return ticker.get_info() or {}
    except Exception:
        return {}


# -----------------------------
# SCORING MODEL — 100 POINTS
# -----------------------------
def build_metrics(symbol, tsp_yes=False, sr_yes=False, put_call_yes=False, gamma_yes=False):
    debug = []

    yahoo_meta, prices, yahoo_error = get_yahoo_chart_history(symbol)

    if not prices.empty:
        debug.append("Yahoo direct chart history loaded")
    else:
        debug.append(f"Yahoo direct chart failed: {yahoo_error}")

    yf_info = get_yfinance_info(symbol)

    if yf_info:
        debug.append("yfinance company/fundamental info loaded")
    else:
        debug.append("yfinance company/fundamental info not loaded")

    current_price = safe_float(yahoo_meta.get("regularMarketPrice"))

    if np.isnan(current_price):
        current_price = safe_float(yf_info.get("currentPrice"), safe_float(yf_info.get("regularMarketPrice")))

    if np.isnan(current_price) and not prices.empty:
        current_price = safe_float(prices["close"].iloc[-1])

    if not prices.empty:
        prices = prices.copy()
        prices["rsi"] = calculate_rsi(prices["close"])
        prices["sma50"] = prices["close"].rolling(50).mean()
        prices["sma200"] = prices["close"].rolling(200).mean()

        last = prices.iloc[-1]
        close = safe_float(last["close"])

        change_5d = (close / prices["close"].iloc[-6] - 1) * 100 if len(prices) > 6 else np.nan
        change_1m = (close / prices["close"].iloc[-22] - 1) * 100 if len(prices) > 22 else np.nan
        rsi = safe_float(last["rsi"])
        dist_50 = (close / last["sma50"] - 1) * 100 if not np.isnan(safe_float(last["sma50"])) else np.nan
        dist_200 = (close / last["sma200"] - 1) * 100 if not np.isnan(safe_float(last["sma200"])) else np.nan
    else:
        change_5d = change_1m = rsi = dist_50 = dist_200 = np.nan

    ttm_pe = safe_float(yf_info.get("trailingPE"))
    forward_pe = safe_float(yf_info.get("forwardPE"))
    peg = safe_float(yf_info.get("pegRatio"))

    eps_growth = safe_float(yf_info.get("earningsQuarterlyGrowth")) * 100
    revenue_growth = safe_float(yf_info.get("revenueGrowth")) * 100

    avg_target = safe_float(yf_info.get("targetMeanPrice"))
    analyst_upside = (
        ((avg_target / current_price) - 1) * 100
        if current_price and avg_target and not np.isnan(avg_target)
        else np.nan
    )

    forward_pe_less_than_ttm = (
        bool(forward_pe < ttm_pe)
        if not np.isnan(forward_pe) and not np.isnan(ttm_pe)
        else False
    )

    peg_under_1_2 = bool(peg < 1.2) if not np.isnan(peg) else False

    values = {
        "Ticker": symbol,
        "Current Price": current_price,
        "TTM P/E": ttm_pe,
        "Forward P/E": forward_pe,
        "Forward P/E < TTM P/E": forward_pe_less_than_ttm,
        "PEG": peg,
        "PEG < 1.2": peg_under_1_2,
        "EPS Growth %": eps_growth,
        "Revenue Growth %": revenue_growth,
        "5D Price Change %": change_5d,
        "1M Price Change %": change_1m,
        "RSI": rsi,
        "Distance from 50D MA %": dist_50,
        "Distance from 200D MA %": dist_200,
        "Analyst Target Upside %": analyst_upside,
        "TSP": tsp_yes,
        "SR": sr_yes,
        "Put/Call Ratio > 1.1": put_call_yes,
        "Gamma Exposure Positive": gamma_yes,
        "Data Source": "Yahoo only",
    }

    score = 0
    details = []

    # FUNDAMENTALS — 30 POINTS
    pts = 12 if eps_growth > 15 else 6 if eps_growth > 0 else 0
    score += pts
    details.append(("EPS growth", pts, eps_growth, "Max 12"))

    pts = 10 if revenue_growth > 10 else 5 if revenue_growth > 0 else 0
    score += pts
    details.append(("Revenue growth", pts, revenue_growth, "Max 10"))

    pts = 8 if analyst_upside > 10 else 4 if analyst_upside > 0 else 0
    score += pts
    details.append(("Analyst target upside", pts, analyst_upside, "Max 8"))

    # VALUATION — 25 POINTS
    pts = 8 if forward_pe_less_than_ttm else 0
    score += pts
    details.append(("Forward P/E < TTM P/E", pts, forward_pe_less_than_ttm, "Max 8"))

    pts = 10 if peg_under_1_2 else 5 if not np.isnan(peg) and peg < 1.8 else 0
    score += pts
    details.append(("PEG", pts, peg, "Max 10"))

    pts = 7 if not np.isnan(ttm_pe) and 0 < ttm_pe < 35 else 3 if not np.isnan(ttm_pe) and 35 <= ttm_pe < 60 else 0
    score += pts
    details.append(("P/E ratio reasonableness", pts, ttm_pe, "Max 7"))

    # MOMENTUM / TECHNICALS — 25 POINTS
    pts = score_range(change_5d, [
        (lambda x: -8 <= x <= -3, 5),
        (lambda x: -3 < x <= 3, 3),
        (lambda x: x > 8, -2),
    ])
    score += pts
    details.append(("5-day price change", pts, change_5d, "Max 5"))

    pts = 5 if not np.isnan(change_1m) and change_1m > 0 else 3 if not np.isnan(change_1m) and -5 <= change_1m <= 0 else 0
    score += pts
    details.append(("1-month price change", pts, change_1m, "Max 5"))

    pts = score_range(rsi, [
        (lambda x: 30 <= x <= 45, 6),
        (lambda x: 45 < x <= 60, 4),
        (lambda x: x < 30, 3),
        (lambda x: x > 70, -4),
    ])
    score += pts
    details.append(("RSI", pts, rsi, "Max 6"))

    pts = 5 if not np.isnan(dist_50) and -5 <= dist_50 <= 2 else 3 if not np.isnan(dist_50) and -10 <= dist_50 < -5 else 0
    score += pts
    details.append(("Distance from 50D MA", pts, dist_50, "Max 5"))

    pts = 4 if not np.isnan(dist_200) and dist_200 > 0 else -6 if not np.isnan(dist_200) and dist_200 < 0 else 0
    score += pts
    details.append(("Distance from 200D MA", pts, dist_200, "Max 4"))

    # MANUAL CONFIRMATION SIGNALS — 20 POINTS
    pts = 5 if tsp_yes else 0
    score += pts
    details.append(("TSP", pts, tsp_yes, "Max 5"))

    pts = 5 if sr_yes else 0
    score += pts
    details.append(("SR", pts, sr_yes, "Max 5"))

    pts = 5 if put_call_yes else 0
    score += pts
    details.append(("Put/Call Ratio > 1.1", pts, put_call_yes, "Max 5"))

    pts = 5 if gamma_yes else 0
    score += pts
    details.append(("Gamma Exposure Positive", pts, gamma_yes, "Max 5"))

    score = max(0, min(100, score))

    detail_df = pd.DataFrame(details, columns=["Factor", "Points", "Value", "Max Points"])
    values["Score"] = score
    values["Rating"] = rating(score)

    alert, action, reasons, _ = generate_trade_alert(values)
    values["Alert"] = alert
    values["Suggested Action"] = action
    values["Alert Reasons"] = "; ".join(reasons)

    return values, score, detail_df, prices, debug


def format_scanner_df(df):
    columns = [
        "Ticker", "Score", "Rating", "Alert", "Suggested Action", "Current Price", "RSI",
        "5D Price Change %", "1M Price Change %", "Distance from 50D MA %",
        "Distance from 200D MA %", "TTM P/E", "Forward P/E", "PEG",
        "EPS Growth %", "Revenue Growth %", "Analyst Target Upside %",
        "Forward P/E < TTM P/E", "PEG < 1.2",
    ]
    available = [c for c in columns if c in df.columns]
    return df[available]


# -----------------------------
# UI
# -----------------------------
st.title("Yahoo Stock Scoring Dashboard + Scanner")
st.caption("Educational stock scoring tool only. Not financial advice.")

mode = st.sidebar.radio("Mode", ["Single Stock Analyzer", "Stock Scanner"])

st.sidebar.divider()
st.sidebar.caption("Data Source")
st.sidebar.write("Yahoo direct chart: ✅ Built-in")
st.sidebar.write("yfinance info backup:", "✅ Available" if yf is not None else "❌ Missing yfinance")
st.sidebar.caption("No API key or Streamlit Secrets needed.")


if mode == "Single Stock Analyzer":
    with st.sidebar:
        st.header("Input")
        symbol = st.text_input("Ticker", value="META").upper().strip()

        st.subheader("Manual Confirmation Signals")
        tsp_yes = st.checkbox("TSP = Yes", value=False)
        sr_yes = st.checkbox("SR = Yes", value=False)
        put_call_yes = st.checkbox("Put/Call Ratio > 1.1 = Yes", value=False)
        gamma_yes = st.checkbox("Gamma Exposure Positive = Yes", value=False)

        st.subheader("Position / Exit Inputs")
        entry_price = st.number_input(
            "Entry price (optional, use 0 if not in position)",
            min_value=0.0,
            value=0.0,
            step=0.01,
        )

        run = st.button("Analyze")

    if run and symbol:
        with st.spinner(f"Pulling Yahoo data for {symbol}..."):
            values, total_score, detail_df, prices, debug = build_metrics(
                symbol,
                tsp_yes,
                sr_yes,
                put_call_yes,
                gamma_yes
            )

        alert, action, reasons, gain_loss_pct = generate_trade_alert(values, entry_price)

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Ticker", symbol)
        col2.metric("Score / 100", f"{total_score:.0f}")
        col3.metric("Rating", rating(total_score))
        col4.metric("Alert", alert)

        st.subheader("Auto Trade Alert")
        if alert == "BUY":
            st.success(f"BUY — {action}")
        elif alert == "TRIM":
            st.warning(f"TRIM — {action}")
        elif alert == "SELL":
            st.error(f"SELL — {action}")
        else:
            st.info(f"WATCH — {action}")

        if not np.isnan(gain_loss_pct):
            st.metric("Gain/Loss From Entry", f"{gain_loss_pct:.2f}%")

        with st.expander("Why this alert fired"):
            for reason in reasons:
                st.write("-", reason)

        if np.isnan(safe_float(values["Current Price"])):
            st.error("No price data loaded. Try AAPL, META, MSFT, or SCHW. If still blank, Streamlit may be blocking Yahoo calls.")
            with st.expander("Debug data source status"):
                for item in debug:
                    st.write("-", item)
        else:
            st.success("Yahoo price and technical indicators loaded.")

        st.subheader("Key Metrics")
        metric_df = pd.DataFrame([values]).T.reset_index()
        metric_df.columns = ["Metric", "Value"]
        st.dataframe(metric_df, use_container_width=True)

        st.subheader("Score Breakdown")
        st.dataframe(detail_df, use_container_width=True)

        if not prices.empty:
            st.subheader("Price Chart")
            chart_df = prices.tail(250).set_index("date")[["close"]]
            st.line_chart(chart_df)

        st.subheader("Decision Logic")
        if total_score >= 85:
            st.success("Strong setup. Fundamentals, valuation, technicals, and confirmation signals are aligned.")
        elif total_score >= 70:
            st.info("Good setup. Consider waiting for clean entry confirmation if some confirmations are missing.")
        elif total_score >= 55:
            st.warning("Watchlist only. Some factors are positive, but the full setup is not strong yet.")
        else:
            st.error("Weak setup. Avoid unless there is a separate catalyst or reversal confirmation.")
    else:
        st.info("Enter a ticker and click Analyze.")


else:
    with st.sidebar:
        st.header("Scanner Input")
        tickers_text = st.text_area(
            "Tickers separated by commas",
            value="AAPL, META, MSFT, NVDA, SCHW, PLTR, TSLA, AMD, AMZN, GOOGL",
            height=150
        )

        min_score = st.slider("Minimum score to display", 0, 100, 55)
        exclude_overbought = st.checkbox("Exclude RSI > 75", value=True)
        require_above_200 = st.checkbox("Require price above 200D MA", value=False)

        st.caption("Scanner uses automatic score only; manual confirmations default to No.")
        run_scanner = st.button("Run Scanner")

    if run_scanner:
        raw_tickers = [t.strip().upper() for t in tickers_text.replace("\n", ",").split(",")]
        tickers = sorted(list(dict.fromkeys([t for t in raw_tickers if t])))

        results = []
        errors = []

        progress = st.progress(0)
        status = st.empty()

        for i, ticker in enumerate(tickers):
            status.write(f"Scanning {ticker} ({i + 1}/{len(tickers)})...")
            try:
                values, score, _, _, debug = build_metrics(
                    ticker,
                    tsp_yes=False,
                    sr_yes=False,
                    put_call_yes=False,
                    gamma_yes=False
                )

                if np.isnan(safe_float(values["Current Price"])):
                    errors.append(f"{ticker}: no price data")
                else:
                    results.append(values)

            except Exception as e:
                errors.append(f"{ticker}: {e}")

            progress.progress((i + 1) / max(len(tickers), 1))

        status.empty()

        if results:
            df = pd.DataFrame(results)

            df = df[df["Score"] >= min_score]

            if exclude_overbought and "RSI" in df.columns:
                df = df[(df["RSI"].isna()) | (df["RSI"] <= 75)]

            if require_above_200 and "Distance from 200D MA %" in df.columns:
                df = df[(df["Distance from 200D MA %"].isna()) | (df["Distance from 200D MA %"] > 0)]

            df = df.sort_values("Score", ascending=False)

            st.subheader("Scanner Results")
            st.caption("Manual confirmation signals are not included in scanner mode.")
            st.dataframe(format_scanner_df(df), use_container_width=True)

            csv = df.to_csv(index=False).encode("utf-8")
            st.download_button(
                label="Download scanner results as CSV",
                data=csv,
                file_name="stock_scanner_results.csv",
                mime="text/csv"
            )

            st.subheader("Top Setups")
            top = df.head(5)
            if not top.empty:
                for _, row in top.iterrows():
                    st.write(f"**{row['Ticker']}** — Score: **{row['Score']:.0f}**, Rating: {row['Rating']}")
            else:
                st.warning("No stocks passed your filters.")

        else:
            st.error("No scanner results loaded.")

        if errors:
            with st.expander("Errors / skipped tickers"):
                for err in errors:
                    st.write("-", err)
    else:
        st.info("Enter a watchlist and click Run Scanner.")
