
# UPDATED APP WITH PURE METRIC MODEL + SQUEEZE SCORE

# NOTE: This is a simplified patch version focusing on scoring + new metrics.
# Replace your build_metrics function with this version.

def build_metrics(symbol):
    import numpy as np

    debug = []

    yahoo_meta, prices, yahoo_error = get_yahoo_chart_history(symbol)
    yf_info = get_yfinance_info(symbol)

    current_price = safe_float(yahoo_meta.get("regularMarketPrice"))
    if np.isnan(current_price):
        current_price = safe_float(yf_info.get("currentPrice"))

    # TECHNICALS
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
        dist_50 = (close / last["sma50"] - 1) * 100
        dist_200 = (close / last["sma200"] - 1) * 100
    else:
        change_5d = change_1m = rsi = dist_50 = dist_200 = np.nan

    # FUNDAMENTALS
    ttm_pe = safe_float(yf_info.get("trailingPE"))
    forward_pe = safe_float(yf_info.get("forwardPE"))
    peg = safe_float(yf_info.get("pegRatio"))

    eps_growth = safe_float(yf_info.get("earningsQuarterlyGrowth")) * 100
    revenue_growth = safe_float(yf_info.get("revenueGrowth")) * 100

    avg_target = safe_float(yf_info.get("targetMeanPrice"))
    analyst_upside = ((avg_target / current_price) - 1) * 100 if current_price and avg_target else np.nan

    # NEW METRICS
    short_float = safe_float(yf_info.get("shortPercentOfFloat")) * 100
    total_cash = safe_float(yf_info.get("totalCash"))

    try:
        ticker = yf.Ticker(symbol)
        cashflow = ticker.cashflow
        operating_cf = safe_float(cashflow.loc["Operating Cash Flow"][0])
    except:
        operating_cf = np.nan

    cash_runway = np.nan
    if total_cash and operating_cf < 0:
        burn = abs(operating_cf) / 12
        cash_runway = total_cash / burn

    # SQUEEZE SCORE
    squeeze_score = 0
    if short_float > 15: squeeze_score += 4
    elif short_float > 7: squeeze_score += 2
    if change_5d > 0: squeeze_score += 2
    if 40 <= rsi <= 70: squeeze_score += 2
    if -5 <= dist_50 <= 2: squeeze_score += 2

    squeeze_tag = "YES" if squeeze_score >= 8 else "NO"

    # SCORING (100)
    score = 0

    # Fundamentals 40
    score += 10 if eps_growth > 15 else 5 if eps_growth > 0 else 0
    score += 15 if revenue_growth > 20 else 10 if revenue_growth > 10 else 5 if revenue_growth > 0 else 0
    score += 7 if analyst_upside > 10 else 3 if analyst_upside > 0 else 0
    score += 8 if cash_runway > 24 else 5 if cash_runway > 12 else 2 if cash_runway > 6 else 0

    # Valuation 20
    score += 6 if forward_pe < ttm_pe else 0
    score += 8 if peg < 1.2 else 4 if peg < 1.8 else 0
    score += 6 if 0 < ttm_pe < 35 else 3 if ttm_pe < 60 else 0

    # Technicals 25
    score += 5 if -8 <= change_5d <= -3 else 3 if -3 < change_5d <= 3 else -2 if change_5d > 8 else 0
    score += 5 if change_1m > 0 else 3 if -5 <= change_1m <= 0 else 0
    score += 6 if 30 <= rsi <= 45 else 4 if 45 < rsi <= 60 else -4 if rsi > 70 else 0
    score += 5 if -5 <= dist_50 <= 2 else 3 if -10 <= dist_50 < -5 else 0
    score += 4 if dist_200 > 0 else -6 if dist_200 < 0 else 0

    # Sentiment 15
    score += 7 if short_float > 15 else 4 if short_float > 7 else 0

    score = max(0, min(100, score))

    return {
        "Ticker": symbol,
        "Score": score,
        "Squeeze Score": squeeze_score,
        "Squeeze Setup": squeeze_tag,
        "Short % Float": short_float,
        "Cash Runway": cash_runway,
        "EPS Growth": eps_growth,
        "Revenue Growth": revenue_growth,
        "RSI": rsi,
    }
