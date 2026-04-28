
# --- UPDATED PATCH INCLUDED ---
# Added Yahoo /v7/finance/quote fallback for missing fundamentals

@st.cache_data(ttl=3600)
def get_yahoo_quote(symbol):
    url = "https://query1.finance.yahoo.com/v7/finance/quote"
    params = {"symbols": symbol}
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        response = requests.get(url, params=params, headers=headers, timeout=20)
        if response.status_code != 200:
            return {}, f"Yahoo quote HTTP {response.status_code}"

        data = response.json()
        result = data.get("quoteResponse", {}).get("result", [])
        if not result:
            return {}, "Yahoo quote no result"

        return result[0] or {}, ""
    except Exception as e:
        return {}, f"Yahoo quote failed: {e}"
