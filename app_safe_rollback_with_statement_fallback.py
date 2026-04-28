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

def first_valid(*values):
    for value in values:
        v = safe_float(value)
        if is_num(v):
            return v
    return np.nan


def raw_value(obj):
    """Extract Yahoo quoteSummary values that may look like {'raw': 123, 'fmt': '123'}."""
    try:
        if isinstance(obj, dict):
            if "raw" in obj:
                return obj.get("raw")
            if "fmt" in obj:
                return obj.get("fmt")
        return obj
    except Exception:
        return np.nan


def nested_raw(data, *path):
    """Safely extract a nested Yahoo quoteSummary value."""
    try:
        cur = data
        for key in path:
            cur = cur.get(key, {})
        return raw_value(cur)
    except Exception:
        return np.nan


def display_value(x):
    """Make Streamlit show missing values clearly instead of blank cells."""
    if isinstance(x, str):
        return x
    try:
        if np.isnan(float(x)):
            return "N/A"
    except Exception:
        pass
    return x



def is_num(x):
    try:
        return not np.isnan(float(x))
    except Exception:
        return False


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
    squeeze_score = safe_float(values.get("Squeeze Score"))
    squeeze_setup = str(values.get("Squeeze Setup", "NO"))
    entry_price = safe_float(entry_price)

    gain_loss_pct = np.nan
    if is_num(entry_price) and entry_price > 0 and is_num(current_price):
        gain_loss_pct = ((current_price / entry_price) - 1) * 100

    sell_reasons = []
    trim_reasons = []
    buy_reasons = []

    if is_num(score) and score < 55:
        sell_reasons.append("Score dropped below 55")
    if is_num(dist_200) and dist_200 < 0:
        sell_reasons.append("Price is below the 200D moving average")
    if is_num(gain_loss_pct) and gain_loss_pct <= -7:
        sell_reasons.append("Position is down 7% or more from entry")
    if is_num(dist_50) and dist_50 < -10:
        sell_reasons.append("Price is more than 10% below the 50D moving average")

    if is_num(gain_loss_pct) and gain_loss_pct >= 10:
        trim_reasons.append("Position is up 10% or more from entry")
    if is_num(rsi) and rsi >= 75:
        trim_reasons.append("RSI is 75 or higher")
    if is_num(change_5d) and change_5d > 8:
        trim_reasons.append("5-day price change is above +8%")

    if is_num(score) and score >= 85:
        buy_reasons.append("Score is 85 or higher")
    elif is_num(score) and score >= 70:
        buy_reasons.append("Score is 70 or higher")

    if squeeze_setup == "YES" and is_num(squeeze_score):
        buy_reasons.append(f"Squeeze setup detected, squeeze score {squeeze_score:.0f}/10")

    if sell_reasons:
        return "SELL", "Exit / avoid new entry", sell_reasons, gain_loss_pct
    if trim_reasons:
        if squeeze_setup == "YES":
            trim_reasons.append("Squeeze setup may be getting extended")
        return "TRIM", "Take partial profit or tighten stop", trim_reasons, gain_loss_pct
    if buy_reasons:
        if squeeze_setup == "YES" and is_num(score) and score >= 70:
            return "BUY (SQUEEZE)", "High score plus squeeze setup", buy_reasons, gain_loss_pct
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


@st.cache_data(ttl=3600)
def get_yahoo_quote_summary(symbol):
    """
    Direct Yahoo fallback for fundamentals.
    This helps when yfinance.get_info() returns blanks on Streamlit Cloud.
    """
    modules = ",".join([
        "defaultKeyStatistics",
        "financialData",
        "summaryDetail",
        "price",
    ])
    url = f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{symbol}"
    params = {"modules": modules}
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        response = requests.get(url, params=params, headers=headers, timeout=20)
        if response.status_code != 200:
            return {}, f"Yahoo quoteSummary HTTP {response.status_code}"

        data = response.json()
        result = data.get("quoteSummary", {}).get("result", [])
        if not result:
            error = data.get("quoteSummary", {}).get("error")
            return {}, f"Yahoo quoteSummary no result: {error}"

        return result[0] or {}, ""
    except Exception as e:
        return {}, f"Yahoo quoteSummary failed: {e}"


@st.cache_data(ttl=3600)
def get_cashflow_data(symbol):
    if yf is None:
        return pd.DataFrame(), pd.DataFrame()

    try:
        ticker = yf.Ticker(symbol)
        annual_cf = ticker.cashflow
        quarterly_cf = ticker.quarterly_cashflow

        if annual_cf is None:
            annual_cf = pd.DataFrame()
        if quarterly_cf is None:
            quarterly_cf = pd.DataFrame()

        return annual_cf, quarterly_cf
    except Exception:
        return pd.DataFrame(), pd.DataFrame()


def extract_operating_cash_flow_from_df(cashflow):
    if cashflow is None or cashflow.empty:
        return np.nan

    possible_rows = [
        "Operating Cash Flow",
        "Total Cash From Operating Activities",
        "Cash Flow From Continuing Operating Activities",
    ]

    for row in possible_rows:
        if row in cashflow.index:
            vals = pd.to_numeric(cashflow.loc[row], errors="coerce").dropna()
            if len(vals) > 0:
                return safe_float(vals.iloc[0])

    return np.nan


def get_operating_cash_flow(symbol):
    annual_cf, quarterly_cf = get_cashflow_data(symbol)

    # Prefer annual cash flow for runway stability, fallback to latest quarter annualized.
    annual_ocf = extract_operating_cash_flow_from_df(annual_cf)
    if is_num(annual_ocf):
        return annual_ocf

    quarterly_ocf = extract_operating_cash_flow_from_df(quarterly_cf)
    if is_num(quarterly_ocf):
        return quarterly_ocf * 4

    return np.nan


@st.cache_data(ttl=3600)
def get_statement_fallbacks(symbol, current_price=np.nan):
    """
    Backup fundamentals from yfinance financial statements.
    This does not rely on stock.info, so it can still work when Yahoo's
    statistics/info endpoint returns blanks.
    """
    result = {}

    if yf is None:
        return result, "yfinance not available"

    try:
        ticker = yf.Ticker(symbol)

        # Quarterly income statement fallback
        q_income = pd.DataFrame()
        try:
            q_income = ticker.quarterly_income_stmt
        except Exception:
            pass

        # Balance sheet fallback
        q_balance = pd.DataFrame()
        try:
            q_balance = ticker.quarterly_balance_sheet
        except Exception:
            pass

        debug_bits = []

        if q_income is not None and not q_income.empty:
            debug_bits.append("quarterly income statement loaded")

            def row_latest(row_names, col_offset=0):
                for row in row_names:
                    if row in q_income.index and q_income.shape[1] > col_offset:
                        vals = pd.to_numeric(q_income.loc[row], errors="coerce")
                        vals = vals.dropna()
                        if len(vals) > col_offset:
                            return safe_float(vals.iloc[col_offset])
                return np.nan

            latest_rev = row_latest(["Total Revenue", "Operating Revenue"], 0)
            year_ago_rev = row_latest(["Total Revenue", "Operating Revenue"], 4)

            if is_num(latest_rev) and is_num(year_ago_rev) and year_ago_rev != 0:
                result["revenue_growth"] = ((latest_rev / year_ago_rev) - 1) * 100

            latest_eps = row_latest(["Diluted EPS", "Basic EPS"], 0)
            year_ago_eps = row_latest(["Diluted EPS", "Basic EPS"], 4)

            if is_num(latest_eps) and is_num(year_ago_eps) and year_ago_eps != 0:
                result["eps_growth"] = ((latest_eps / year_ago_eps) - 1) * 100

            # TTM EPS fallback
            ttm_eps = np.nan
            for row in ["Diluted EPS", "Basic EPS"]:
                if row in q_income.index:
                    vals = pd.to_numeric(q_income.loc[row], errors="coerce").dropna()
                    if len(vals) >= 4:
                        ttm_eps = vals.iloc[:4].sum()
                        break

            if is_num(current_price) and is_num(ttm_eps) and ttm_eps > 0:
                result["ttm_pe"] = current_price / ttm_eps

        else:
            debug_bits.append("quarterly income statement not loaded")

        if q_balance is not None and not q_balance.empty:
            debug_bits.append("quarterly balance sheet loaded")

            for row in [
                "Cash And Cash Equivalents",
                "Cash Cash Equivalents And Short Term Investments",
                "Cash Financial",
                "Cash Equivalents",
            ]:
                if row in q_balance.index:
                    vals = pd.to_numeric(q_balance.loc[row], errors="coerce").dropna()
                    if len(vals) > 0:
                        result["total_cash"] = safe_float(vals.iloc[0])
                        break
        else:
            debug_bits.append("quarterly balance sheet not loaded")

        return result, "; ".join(debug_bits)

    except Exception as e:
        return result, f"statement fallback failed: {e}"



# -----------------------------
# OPTIONAL FMP FALLBACK
# -----------------------------
def get_fmp_key():
    try:
        return st.secrets.get("FMP_API_KEY", "")
    except Exception:
        return ""


@st.cache_data(ttl=3600)
def fmp_request(endpoint, symbol, extra_params=None):
    api_key = get_fmp_key()
    if not api_key:
        return None, "No FMP_API_KEY in Streamlit Secrets"

    url = f"https://financialmodelingprep.com/api/v3/{endpoint}/{symbol}"
    params = {"apikey": api_key}
    if extra_params:
        params.update(extra_params)

    try:
        r = requests.get(url, params=params, timeout=20)
        if r.status_code != 200:
            return None, f"FMP HTTP {r.status_code}: {r.text[:160]}"
        data = r.json()
        if not data:
            return None, "FMP returned empty data"
        return data, ""
    except Exception as e:
        return None, f"FMP failed: {e}"


@st.cache_data(ttl=3600)
def get_fmp_fundamentals(symbol):
    out = {}
    errors = []

    quote, err = fmp_request("quote", symbol)
    if isinstance(quote, list) and quote:
        q = quote[0]
        out["price"] = safe_float(q.get("price"))
        out["ttm_pe"] = safe_float(q.get("pe"))
        out["eps"] = safe_float(q.get("eps"))
    else:
        errors.append(f"quote: {err}")

    ratios, err = fmp_request("ratios-ttm", symbol)
    if isinstance(ratios, list) and ratios:
        r = ratios[0]
        out["ttm_pe"] = first_valid(out.get("ttm_pe"), r.get("priceEarningsRatioTTM"), r.get("peRatioTTM"))
        out["peg"] = first_valid(r.get("priceEarningsToGrowthRatioTTM"), r.get("pegRatioTTM"))
    else:
        errors.append(f"ratios: {err}")

    income, err = fmp_request("income-statement", symbol, {"period": "quarter", "limit": 8})
    if isinstance(income, list) and len(income) >= 5:
        latest = income[0]
        year_ago = income[4]
        rev_latest = safe_float(latest.get("revenue"))
        rev_year_ago = safe_float(year_ago.get("revenue"))
        if is_num(rev_latest) and is_num(rev_year_ago) and rev_year_ago != 0:
            out["revenue_growth"] = ((rev_latest / rev_year_ago) - 1) * 100

        eps_latest = first_valid(latest.get("eps"), latest.get("epsdiluted"))
        eps_year_ago = first_valid(year_ago.get("eps"), year_ago.get("epsdiluted"))
        if is_num(eps_latest) and is_num(eps_year_ago) and eps_year_ago != 0:
            out["eps_growth"] = ((eps_latest / eps_year_ago) - 1) * 100
    else:
        errors.append(f"income: {err}")

    balance, err = fmp_request("balance-sheet-statement", symbol, {"period": "quarter", "limit": 1})
    if isinstance(balance, list) and balance:
        b = balance[0]
        out["total_cash"] = first_valid(
            b.get("cashAndCashEquivalents"),
            b.get("cashAndShortTermInvestments"),
            b.get("cashAndCashEquivalentsShortTermInvestments"),
        )
    else:
        errors.append(f"balance: {err}")

    cashflow, err = fmp_request("cash-flow-statement", symbol, {"period": "quarter", "limit": 4})
    if isinstance(cashflow, list) and cashflow:
        ocfs = []
        for row in cashflow[:4]:
            ocf = first_valid(row.get("netCashProvidedByOperatingActivities"), row.get("operatingCashFlow"))
            if is_num(ocf):
                ocfs.append(ocf)
        if ocfs:
            out["operating_cf"] = sum(ocfs)
    else:
        errors.append(f"cashflow: {err}")

    estimates, err = fmp_request("analyst-estimates", symbol, {"period": "annual", "limit": 2})
    if isinstance(estimates, list) and estimates:
        e = estimates[0]
        est_eps = first_valid(e.get("estimatedEpsAvg"), e.get("estimatedEpsHigh"), e.get("estimatedEpsLow"))
        price = first_valid(out.get("price"))
        if is_num(price) and is_num(est_eps) and est_eps > 0:
            out["forward_pe"] = price / est_eps
    else:
        errors.append(f"estimates: {err}")

    # FMP target price endpoint; this may not be available on every plan.
    target, err = fmp_request("price-target", symbol)
    if isinstance(target, list) and target:
        t = target[0]
        out["target_mean_price"] = first_valid(t.get("priceTargetAverage"), t.get("targetPrice"))
    else:
        errors.append(f"target: {err}")

    return out, " | ".join(errors)


# -----------------------------
# SCORING MODEL — 100 POINTS
# Pure metric model: no manual confirmations
# -----------------------------
def build_metrics(symbol):
    debug = []

    yahoo_meta, prices, yahoo_error = get_yahoo_chart_history(symbol)

    if not prices.empty:
        debug.append("Yahoo direct chart history loaded")
    else:
        debug.append(f"Yahoo direct chart failed: {yahoo_error}")

    yf_info = get_yfinance_info(symbol)
    yahoo_summary, yahoo_summary_error = get_yahoo_quote_summary(symbol)
    fmp_info, fmp_error = get_fmp_fundamentals(symbol)

    if yf_info:
        debug.append("yfinance company/fundamental info loaded")
    else:
        debug.append("yfinance company/fundamental info not loaded")

    if yahoo_summary:
        debug.append("Yahoo quoteSummary fallback loaded")
    else:
        debug.append(f"Yahoo quoteSummary fallback not loaded: {yahoo_summary_error}")

    if fmp_info:
        debug.append("FMP fallback loaded")
    else:
        debug.append(f"FMP fallback not loaded: {fmp_error}")

    current_price = safe_float(yahoo_meta.get("regularMarketPrice"))

    if not is_num(current_price):
        current_price = first_valid(
            yf_info.get("currentPrice"),
            yf_info.get("regularMarketPrice"),
            yf_info.get("fast_last_price"),
            yf_info.get("fast_regular_market_price"),
            nested_raw(yahoo_summary, "price", "regularMarketPrice"),
            fmp_info.get("price"),
        )

    if not is_num(current_price) and not prices.empty:
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
        dist_50 = (close / last["sma50"] - 1) * 100 if is_num(safe_float(last["sma50"])) else np.nan
        dist_200 = (close / last["sma200"] - 1) * 100 if is_num(safe_float(last["sma200"])) else np.nan
    else:
        change_5d = change_1m = rsi = dist_50 = dist_200 = np.nan

    statement_info, statement_debug = get_statement_fallbacks(symbol, current_price)
    debug.append(f"Financial statement fallback: {statement_debug}")

    # Fundamentals: try yfinance first, then direct Yahoo quoteSummary fallback, then statements/FMP.
    ttm_pe = first_valid(
        yf_info.get("trailingPE"),
        yf_info.get("trailingPe"),
        nested_raw(yahoo_summary, "summaryDetail", "trailingPE"),
        nested_raw(yahoo_summary, "defaultKeyStatistics", "trailingPE"),
        statement_info.get("ttm_pe"),
        fmp_info.get("ttm_pe"),
    )

    forward_pe = first_valid(
        yf_info.get("forwardPE"),
        yf_info.get("forwardPe"),
        nested_raw(yahoo_summary, "defaultKeyStatistics", "forwardPE"),
        nested_raw(yahoo_summary, "summaryDetail", "forwardPE"),
        fmp_info.get("forward_pe"),
    )

    peg = first_valid(
        yf_info.get("pegRatio"),
        yf_info.get("trailingPegRatio"),
        nested_raw(yahoo_summary, "defaultKeyStatistics", "pegRatio"),
        nested_raw(yahoo_summary, "defaultKeyStatistics", "trailingPegRatio"),
        fmp_info.get("peg"),
    )

    eps_growth_raw = first_valid(
        yf_info.get("earningsQuarterlyGrowth"),
        yf_info.get("earningsGrowth"),
        nested_raw(yahoo_summary, "financialData", "earningsGrowth"),
    )
    eps_growth = eps_growth_raw * 100 if is_num(eps_growth_raw) else first_valid(
        statement_info.get("eps_growth"),
        fmp_info.get("eps_growth"),
    )

    # Yahoo/yfinance revenueGrowth is most recent quarter YoY growth.
    revenue_growth_raw = first_valid(
        yf_info.get("revenueGrowth"),
        yf_info.get("quarterlyRevenueGrowth"),
        nested_raw(yahoo_summary, "financialData", "revenueGrowth"),
    )
    revenue_growth = revenue_growth_raw * 100 if is_num(revenue_growth_raw) else first_valid(
        statement_info.get("revenue_growth"),
        fmp_info.get("revenue_growth"),
    )

    avg_target = first_valid(
        yf_info.get("targetMeanPrice"),
        yf_info.get("targetMedianPrice"),
        nested_raw(yahoo_summary, "financialData", "targetMeanPrice"),
        nested_raw(yahoo_summary, "financialData", "targetMedianPrice"),
        fmp_info.get("target_mean_price"),
    )

    analyst_upside = (
        ((avg_target / current_price) - 1) * 100
        if is_num(current_price) and current_price > 0 and is_num(avg_target) and avg_target > 0
        else np.nan
    )

    forward_pe_less_than_ttm = (
        bool(forward_pe < ttm_pe)
        if is_num(forward_pe) and is_num(ttm_pe)
        else False
    )

    peg_under_1_2 = bool(peg < 1.2) if is_num(peg) else False

    short_float_raw = first_valid(
        yf_info.get("shortPercentOfFloat"),
        nested_raw(yahoo_summary, "defaultKeyStatistics", "shortPercentOfFloat"),
    )
    short_float = short_float_raw * 100 if is_num(short_float_raw) else np.nan

    total_cash = first_valid(
        yf_info.get("totalCash"),
        nested_raw(yahoo_summary, "financialData", "totalCash"),
        statement_info.get("total_cash"),
        fmp_info.get("total_cash"),
    )
    operating_cf = first_valid(
        get_operating_cash_flow(symbol),
        fmp_info.get("operating_cf"),
    )

    cash_runway_months = np.nan
    if is_num(total_cash) and total_cash > 0 and is_num(operating_cf) and operating_cf < 0:
        monthly_burn = abs(operating_cf) / 12
        cash_runway_months = total_cash / monthly_burn if monthly_burn > 0 else np.nan
    elif is_num(total_cash) and total_cash > 0 and is_num(operating_cf) and operating_cf >= 0:
        # Positive operating cash flow means no burn-rate runway issue.
        cash_runway_months = 999

    # Squeeze score is separate from the main 100-point score.
    squeeze_score = 0
    if is_num(short_float):
        if short_float > 15:
            squeeze_score += 4
        elif short_float > 7:
            squeeze_score += 2

    if is_num(change_5d) and change_5d > 0:
        squeeze_score += 2

    if is_num(rsi) and 40 <= rsi <= 70:
        squeeze_score += 2

    if is_num(dist_50) and -5 <= dist_50 <= 2:
        squeeze_score += 2

    squeeze_setup = "YES" if squeeze_score >= 8 else "NO"

    values = {
        "Ticker": symbol,
        "Current Price": current_price,
        "TTM P/E": ttm_pe,
        "Forward P/E": forward_pe,
        "Forward P/E < TTM P/E": forward_pe_less_than_ttm,
        "PEG": peg,
        "PEG < 1.2": peg_under_1_2,
        "EPS Growth %": eps_growth,
        "Quarterly Revenue Growth %": revenue_growth,
        "Revenue Growth %": revenue_growth,
        "5D Price Change %": change_5d,
        "1M Price Change %": change_1m,
        "RSI": rsi,
        "Distance from 50D MA %": dist_50,
        "Distance from 200D MA %": dist_200,
        "Analyst Target Upside %": analyst_upside,
        "Short % of Float": short_float,
        "Total Cash": total_cash,
        "Operating Cash Flow": operating_cf,
        "Cash Runway Months": cash_runway_months,
        "Squeeze Score": squeeze_score,
        "Squeeze Setup": squeeze_setup,
        "Fundamental Data Status": "Loaded" if any(is_num(x) for x in [ttm_pe, forward_pe, peg, eps_growth, revenue_growth, avg_target, short_float, total_cash]) else "Limited / unavailable from Yahoo",
        "Data Source": "Yahoo direct chart + yfinance + Yahoo quoteSummary + financial statements + optional FMP fallback",
    }

    score = 0
    details = []

    # FUNDAMENTALS — 40 POINTS
    pts = 10 if is_num(eps_growth) and eps_growth > 15 else 5 if is_num(eps_growth) and eps_growth > 0 else 0
    score += pts
    details.append(("EPS growth", pts, eps_growth, "Max 10"))

    pts = (
        15 if is_num(revenue_growth) and revenue_growth > 20 else
        10 if is_num(revenue_growth) and revenue_growth > 10 else
        5 if is_num(revenue_growth) and revenue_growth > 0 else
        0
    )
    score += pts
    details.append(("Quarterly revenue growth", pts, revenue_growth, "Max 15"))

    pts = 7 if is_num(analyst_upside) and analyst_upside > 10 else 3 if is_num(analyst_upside) and analyst_upside > 0 else 0
    score += pts
    details.append(("Analyst target upside", pts, analyst_upside, "Max 7"))

    pts = (
        8 if is_num(cash_runway_months) and cash_runway_months > 24 else
        5 if is_num(cash_runway_months) and cash_runway_months > 12 else
        2 if is_num(cash_runway_months) and cash_runway_months > 6 else
        0
    )
    score += pts
    runway_display = "Positive OCF / no burn issue" if cash_runway_months == 999 else cash_runway_months
    details.append(("Cash runway", pts, runway_display, "Max 8"))

    # VALUATION — 20 POINTS
    pts = 6 if forward_pe_less_than_ttm else 0
    score += pts
    details.append(("Forward P/E < TTM P/E", pts, forward_pe_less_than_ttm, "Max 6"))

    pts = 8 if peg_under_1_2 else 4 if is_num(peg) and peg < 1.8 else 0
    score += pts
    details.append(("PEG", pts, peg, "Max 8"))

    pts = 6 if is_num(ttm_pe) and 0 < ttm_pe < 35 else 3 if is_num(ttm_pe) and 35 <= ttm_pe < 60 else 0
    score += pts
    details.append(("P/E ratio reasonableness", pts, ttm_pe, "Max 6"))

    # MOMENTUM / TECHNICALS — 25 POINTS
    pts = score_range(change_5d, [
        (lambda x: -8 <= x <= -3, 5),
        (lambda x: -3 < x <= 3, 3),
        (lambda x: x > 8, -2),
    ])
    score += pts
    details.append(("5-day price change", pts, change_5d, "Max 5"))

    pts = 5 if is_num(change_1m) and change_1m > 0 else 3 if is_num(change_1m) and -5 <= change_1m <= 0 else 0
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

    pts = 5 if is_num(dist_50) and -5 <= dist_50 <= 2 else 3 if is_num(dist_50) and -10 <= dist_50 < -5 else 0
    score += pts
    details.append(("Distance from 50D MA", pts, dist_50, "Max 5"))

    pts = 4 if is_num(dist_200) and dist_200 > 0 else -6 if is_num(dist_200) and dist_200 < 0 else 0
    score += pts
    details.append(("Distance from 200D MA", pts, dist_200, "Max 4"))

    # SENTIMENT / POSITIONING — 15 POINTS
    pts = 15 if is_num(short_float) and short_float > 20 else 10 if is_num(short_float) and short_float > 10 else 5 if is_num(short_float) and short_float > 5 else 0
    score += pts
    details.append(("Short % of Float", pts, short_float, "Max 15"))

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
        "Ticker", "Score", "Rating", "Alert", "Suggested Action", "Squeeze Score", "Squeeze Setup",
        "Current Price", "RSI", "5D Price Change %", "1M Price Change %",
        "Distance from 50D MA %", "Distance from 200D MA %",
        "TTM P/E", "Forward P/E", "Forward P/E < TTM P/E", "PEG", "PEG < 1.2",
        "EPS Growth %", "Quarterly Revenue Growth %", "Analyst Target Upside %",
        "Short % of Float", "Cash Runway Months",
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
st.sidebar.caption("Pure metric model: no manual confirmation checkboxes.")
st.sidebar.caption("Optional: add FMP_API_KEY to Streamlit Secrets for more reliable fundamentals.")


if mode == "Single Stock Analyzer":
    with st.sidebar:
        st.header("Input")
        symbol = st.text_input("Ticker", value="META").upper().strip()

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
            values, total_score, detail_df, prices, debug = build_metrics(symbol)

        alert, action, reasons, gain_loss_pct = generate_trade_alert(values, entry_price)

        col1, col2, col3, col4, col5 = st.columns(5)
        col1.metric("Ticker", symbol)
        col2.metric("Score / 100", f"{total_score:.0f}")
        col3.metric("Rating", rating(total_score))
        col4.metric("Alert", alert)
        col5.metric("Squeeze", f"{values.get('Squeeze Score', 0)}/10")

        st.subheader("Auto Trade Alert")
        if "BUY" in alert:
            st.success(f"{alert} — {action}")
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

        if values.get("Squeeze Setup") == "YES":
            st.warning("🔥 Squeeze Setup Detected: high short-interest fuel plus constructive momentum.")
        else:
            st.info("No strong squeeze setup detected.")

        if not is_num(safe_float(values["Current Price"])):
            st.error("No price data loaded. Try AAPL, META, MSFT, or SCHW. If still blank, Streamlit may be blocking Yahoo calls.")
            with st.expander("Debug data source status"):
                for item in debug:
                    st.write("-", item)
        else:
            st.success("Yahoo price and technical indicators loaded.")

        with st.expander("Data source diagnostics"):
            for item in debug:
                st.write("-", item)

        st.subheader("Key Metrics")
        metric_df = pd.DataFrame([values]).T.reset_index()
        metric_df.columns = ["Metric", "Value"]
        metric_df["Value"] = metric_df["Value"].apply(display_value)
        st.dataframe(metric_df, use_container_width=True)

        st.subheader("Score Breakdown")
        detail_display_df = detail_df.copy()
        detail_display_df["Value"] = detail_display_df["Value"].apply(display_value)
        st.dataframe(detail_display_df, use_container_width=True)

        if not prices.empty:
            st.subheader("Price Chart")
            chart_df = prices.tail(250).set_index("date")[["close"]]
            st.line_chart(chart_df)

        st.subheader("Decision Logic")
        if total_score >= 85:
            st.success("Strong setup. Fundamentals, valuation, technicals, and positioning are aligned.")
        elif total_score >= 70:
            st.info("Good setup. Consider waiting for clean entry confirmation if price is extended.")
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
        squeeze_only = st.checkbox("Show squeeze setups only", value=False)

        st.caption("Scanner uses automatic score only. Manual confirmations were removed.")
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
                values, score, _, _, debug = build_metrics(ticker)

                if not is_num(safe_float(values["Current Price"])):
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

            if squeeze_only and "Squeeze Setup" in df.columns:
                df = df[df["Squeeze Setup"] == "YES"]

            df = df.sort_values(["Score", "Squeeze Score"], ascending=False)

            st.subheader("Scanner Results")
            st.caption("Pure metric score. Squeeze Score is a separate 0–10 overlay.")
            scanner_display_df = format_scanner_df(df).copy()
            for col in scanner_display_df.columns:
                scanner_display_df[col] = scanner_display_df[col].apply(display_value)
            st.dataframe(scanner_display_df, use_container_width=True)

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
                    squeeze = f", Squeeze: {row.get('Squeeze Score', 0)}/10" if "Squeeze Score" in row else ""
                    st.write(f"**{row['Ticker']}** — Score: **{row['Score']:.0f}**, Rating: {row['Rating']}, Alert: {row.get('Alert', 'N/A')}{squeeze}")
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
