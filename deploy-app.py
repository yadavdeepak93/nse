import streamlit as st
import pandas as pd
import requests
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ============================================================
# PAGE CONFIGURATION
# ============================================================
st.set_page_config(
    page_title="NSE Options PnL Scanner",
    page_icon="📈",
    layout="wide"
)

# ============================================================
# SETTINGS & DATA CONSTANTS
# ============================================================
STRIKES_EACH_SIDE = 5
MAX_WORKERS = 1  # Safely restricted to 3 parallel threads

STOCKS = [
    "360ONE", "ABB", "ABCAPITAL", "ADANIENSOL", "ADANIENT", "ADANIGREEN", "ADANIPORTS", "ADANIPOWER", "ALKEM", "AMBER", 
    "AMBUJACEM", "ANGELONE", "APLAPOLLO", "APOLLOHOSP", "ASHOKLEY", "ASIANPAINT", "ASTRAL", "AUBANK", "AUROPHARMA", "AXISBANK", 
    "BAJAJ-AUTO", "BAJAJFINSV", "BAJAJHLDNG", "BAJFINANCE", "BANDHANBNK", "BANKBARODA", "BANKINDIA", "BDL", "BEL", "BHARATFORG", 
    "BHARTIARTL", "BHEL", "BIOCON", "BLUESTARCO", "BOSCHLTD", "BPCL", "BRITANNIA", "BSE", "CAMS", "CANBK", 
    "CDSL", "CGPOWER", "CHOLAFIN", "CIPLA", "COALINDIA", "COCHINSHIP", "COFORGE", "COLPAL", "CONCOR", "CROMPTON", 
    "CUMMINSIND", "DABUR", "DALBHARAT", "DELHIVERY", "DIVISLAB", "DIXON", "DLF", "DMART", "DRREDDY", "EICHERMOT", 
    "ETERNAL", "FEDERALBNK", "FORCEMOT", "FORTIS", "GAIL", "GLENMARK", "GMRAIRPORT", "GODFRYPHLP", "GODREJCP", "GODREJPROP", 
    "GRASIM", "GVT&D", "HAL", "HAVELLS", "HCLTECH", "HDFCAMC", "HDFCBANK", "HDFCLIFE", "HEROMOTOCO", "HINDALCO", 
    "HINDPETRO", "HINDUNILVR", "HINDZINC", "HYUNDAI", "ICICIBANK", "ICICIGI", "ICICIPRULI", "IDEA", "IDFCFIRSTB", "IEX", 
    "INDHOTEL", "INDIANB", "INDIGO", "INDUSINDBK", "INDUSTOWER", "INFY", "INOXWIND", "IOC", "IREDA", "IRFC", 
    "ITC", "JINDALSTEL", "JIOFIN", "JSWENERGY", "JSWSTEEL", "JUBLFOOD", "KALYANKJIL", "KAYNES", "KEI", "KFINTECH", 
    "KOTAKBANK", "KPITTECH", "LAURUSLABS", "LICHSGFIN", "LICI", "LODHA", "LT", "LTF", "LTM", "LUPIN", 
    "M&M", "MANAPPURAM", "MANKIND", "MARICO", "MARUTI", "MAXHEALTH", "MAZDOCK", "MCX", "MFSL", "MOTHERSON", 
    "MOTILALOFS", "MPHASIS", "MUTHOOTFIN", "NAM-INDIA", "NATIONALUM", "NAUKRI", "NBCC", "NESTLEIND", "NHPC", "NMDC", 
    "NTPC", "NYKAA", "OBEROIRLTY", "OFSS", "OIL", "ONGC", "PAGEIND", "PATANJALI", "PAYTM", "PERSISTENT", 
    "PETRONET", "PFC", "PGEL", "PHOENIXLTD", "PIDILITIND", "PIIND", "PNB", "PNBHOUSING", "POLICYBZR", "POLYCAB", 
    "POWERGRID", "POWERINDIA", "PREMIERENE", "PRESTIGE", "RADICO", "RBLBANK", "RECLTD", "RELIANCE", "RVNL", "SAIL", 
    "SBICARD", "SBILIFE", "SBIN", "SHREECEM", "SHRIRAMFIN", "SIEMENS", "SOLARINDS", "SONACOMS", "SRF", "SUNPHARMA", 
    "SUPREMEIND", "SUZLON", "SWIGGY", "TATACONSUM", "TATAELXSI", "TATAPOWER", "TATASTEEL", "TCS", "TECHM", "TIINDIA", 
    "TITAN", "TMPV", "TORNTPHARM", "TRENT", "TVSMOTOR", "ULTRACEMCO", "UNIONBANK", "UNITDSPR", "UNOMINDA", "UPL", 
    "VBL", "VEDL", "VMM", "VOLTAS", "WAAREEENER", "WIPRO", "YESBANK", "ZYDUSLIFE"
]

LOT_SIZES = {
    "360ONE": 500, "ABB": 125, "ABCAPITAL": 3100, "ADANIENSOL": 675, "ADANIENT": 309, "ADANIGREEN": 600, "ADANIPORTS": 475, "ADANIPOWER": 3550, "ALKEM": 125, "AMBER": 100,
    "AMBUJACEM": 1200, "ANGELONE": 2500, "APLAPOLLO": 350, "APOLLOHOSP": 125, "ASHOKLEY": 5000, "ASIANPAINT": 250, "ASTRAL": 425, "AUBANK": 1000, "AUROPHARMA": 550, "AXISBANK": 625,
    "BAJAJ-AUTO": 75, "BAJAJFINSV": 300, "BAJAJHLDNG": 75, "BAJFINANCE": 750, "BANDHANBNK": 3600, "BANKBARODA": 2925, "BANKINDIA": 5200, "BDL": 425, "BEL": 1425, "BHARATFORG": 500,
    "BHARTIARTL": 475, "BHEL": 2625, "BIOCON": 2500, "BLUESTARCO": 325, "BOSCHLTD": 25, "BPCL": 1975, "BRITANNIA": 125, "BSE": 200,
    "CAMS": 825, "CANBK": 6750, "CDSL": 475, "CGPOWER": 850, "CHOLAFIN": 625, "CIPLA": 425, "COALINDIA": 1350, "COCHINSHIP": 400, "COFORGE": 475, "COLPAL": 275, "CONCOR": 1250, "CROMPTON": 2150, "CUMMINSIND": 200,
    "DABUR": 1250, "DALBHARAT": 325, "DELHIVERY": 2075, "DIVISLAB": 100, "DIXON": 50, "DLF": 950, "DMART": 150, "DRREDDY": 625,
    "EICHERMOT": 100, "ETERNAL": 2425, "FEDERALBNK": 2500, "FORCEMOT": 25, "FORTIS": 775,
    "GAIL": 3550, "GLENMARK": 375, "GMRAIRPORT": 6975, "GODFRYPHLP": 275, "GODREJCP": 500, "GODREJPROP": 325, "GRASIM": 250, "GVT&D": 125,
    "HAL": 150, "HAVELLS": 500, "HCLTECH": 400, "HDFCAMC": 300, "HDFCBANK": 650, "HDFCLIFE": 1100, "HEROMOTOCO": 150, "HINDALCO": 700, "HINDPETRO": 2025, "HINDUNILVR": 300, "HINDZINC": 1225, "HYUNDAI": 275,
    "ICICIBANK": 700, "ICICIGI": 325, "ICICIPRULI": 925, "IDEA": 71475, "IDFCFIRSTB": 9275, "IEX": 4350, "INDHOTEL": 1000, "INDIANB": 1000, "INDIGO": 150, "INDUSINDBK": 700, "INDUSTOWER": 1700, "INFY": 400, "INOXWIND": 6400, "IOC": 4875, "IREDA": 4525, "IRFC": 5425, "ITC": 1725,
    "JINDALSTEL": 625, "JIOFIN": 2350, "JSWENERGY": 1075, "JSWSTEEL": 675, "JUBLFOOD": 1250,
    "KALYANKJIL": 1350, "KAYNES": 150, "KEI": 175, "KFINTECH": 575, "KOTAKBANK": 2000, "KPITTECH": 775,
    "LAURUSLABS": 850, "LICHSGFIN": 1000, "LICI": 1400, "LODHA": 625, "LT": 175, "LTF": 2250, "LTM": 150, "LUPIN": 425,
    "M&M": 200, "MANAPPURAM": 3000, "MANKIND": 250, "MARICO": 1200, "MARUTI": 50, "MAXHEALTH": 525, "MAZDOCK": 225, "MCX": 225, "MFSL": 400, "MOTHERSON": 6150, "MOTILALOFS": 775, "MPHASIS": 275, "MUTHOOTFIN": 275,
    "NAM-INDIA": 625, "NATIONALUM": 1875, "NAUKRI": 550, "NBCC": 6500, "NESTLEIND": 500, "NHPC": 6950, "NMDC": 6750, "NTPC": 1500, "NYKAA": 3125,
    "OBEROIRLTY": 350, "OFSS": 100, "OIL": 1400, "ONGC": 2250,
    "PAGEIND": 20, "PATANJALI": 1075, "PAYTM": 725, "PERSISTENT": 125, "PETRONET": 1900, "PFC": 1300, "PGEL": 950, "PHOENIXLTD": 350, "PIDILITIND": 500, "PIIND": 175, "PNB": 8000, "PNBHOUSING": 650, "POLICYBZR": 350, "POLYCAB": 125, "POWERGRID": 1900, "POWERINDIA": 25, "PREMIERENE": 650, "PRESTIGE": 450,
    "RADICO": 150, "RBLBANK": 3175, "RECLTD": 1575, "RELIANCE": 500, "RVNL": 1925,
    "SAIL": 4700, "SBICARD": 800, "SBILIFE": 375, "SBIN": 750, "SHREECEM": 25, "SHRIRAMFIN": 825, "SIEMENS": 175, "SOLARINDS": 50, "SONACOMS": 1225, "SRF": 200, "SUNPHARMA": 350, "SUPREMEIND": 175, "SUZLON": 12700, "SWIGGY": 1825,
    "TATACONSUM": 550, "TATAELXSI": 125, "TATAPOWER": 1450, "TATASTEEL": 2750, "TCS": 225, "TECHM": 600, "TIINDIA": 200, "TITAN": 175, "TMPV": 1600, "TORNTPHARM": 125, "TRENT": 225, "TVSMOTOR": 175,
    "ULTRACEMCO": 50, "UNIONBANK": 4425, "UNITDSPR": 400, "UNOMINDA": 550, "UPL": 1355,
    "VBL": 1275, "VEDL": 1150, "VMM": 4850, "VOLTAS": 375,
    "WAAREEENER": 175, "WIPRO": 3000, "YESBANK": 31100, "ZYDUSLIFE": 900
}

# ============================================================
# SCRAPING & PROCESSING LOGIC
# ============================================================
def create_nse_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.nseindia.com/option-chain",
        "X-Requested-With": "XMLHttpRequest",
    })
    try:
        s.get("https://www.nseindia.com", timeout=10)
        s.get("https://www.nseindia.com/option-chain", timeout=10)
    except Exception:
        pass
    return s

def fetch_single_stock(symbol, timestamp):
    session = create_nse_session()
    rows = []
    max_retries = 3
    
    for attempt in range(max_retries):
        try:
            res = session.get("https://www.nseindia.com/api/option-chain-contract-info", params={"symbol": symbol}, timeout=8)
            if res.status_code != 200:
                time.sleep(1)
                continue
                
            expiries = res.json().get("expiryDates", [])
            if not expiries:
                return rows
            expiry = expiries[1]
            
            lot_size = LOT_SIZES.get(symbol, 1)
            
            chain_res = session.get("https://www.nseindia.com/api/option-chain-v3", params={"type": "Equity", "symbol": symbol, "expiry": expiry}, timeout=8)
            if chain_res.status_code != 200:
                time.sleep(1)
                continue
                
            records = chain_res.json().get("records", {})
            option_data = records.get("data", [])
            if not option_data:
                return rows
                
            underlying = records.get("underlyingValue")
            if underlying is None:
                for item in option_data:
                    ce, pe = item.get("CE", {}), item.get("PE", {})
                    underlying = ce.get("underlyingValue") or pe.get("underlyingValue")
                    if underlying is not None:
                        break
            if underlying is None:
                return rows
            underlying = float(underlying)
            
            strikes = sorted(set(float(x["strikePrice"]) for x in option_data if x.get("strikePrice")))
            if not strikes:
                return rows
                
            atm = min(strikes, key=lambda x: abs(x - underlying))
            atm_index = strikes.index(atm)
            start = max(0, atm_index - STRIKES_EACH_SIDE)
            end = min(len(strikes), atm_index + STRIKES_EACH_SIDE + 1)
            selected_strikes = set(strikes[start:end])
            
            for item in option_data:
                strike = item.get("strikePrice")
                if strike is None:
                    continue
                strike = float(strike)
                if strike not in selected_strikes:
                    continue
                
                ce = item.get("CE", {})
                pe = item.get("PE", {})
                
                rows.append({
                    "Timestamp": timestamp,
                    "Symbol": symbol,
                    "Expiry": expiry,
                    "Underlying": underlying,
                    "ATM": atm,
                    "Strike": strike,
                    "LotSize": lot_size,
                    "CE_LTP": ce.get("lastPrice", 0.0) or 0.0,
                    "PE_LTP": pe.get("lastPrice", 0.0) or 0.0,
                })
            break # Success, break retry loop
        except Exception:
            time.sleep(1)  # Backoff before retry
            
    # Enforce the 1-second delay per thread execution call to ensure fail-safe stability against rate limits
    time.sleep(1.0)
    return rows

def fetch_data():
    all_rows = []
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    total_stocks = len(STOCKS)
    completed = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_symbol = {executor.submit(fetch_single_stock, symbol, timestamp): symbol for symbol in STOCKS}
        
        for future in as_completed(future_to_symbol):
            completed += 1
            progress_bar.progress(completed / total_stocks)
            status_text.text(f"Processed [{completed}/{total_stocks}] stocks...")
            
            try:
                res_rows = future.result()
                if res_rows:
                    all_rows.extend(res_rows)
            except Exception:
                pass

    progress_bar.empty()
    status_text.empty()
    
    if not all_rows:
        return pd.DataFrame()
        
    return pd.DataFrame(all_rows)

def process_options_dataframe(df):
    if df.empty:
        return df
        
    filtered_df = df[
        (df['ATM'] == df['Strike']) & 
        (df['CE_LTP'] != 0) & 
        (df['PE_LTP'] != 0)
    ].copy()
    
    if filtered_df.empty:
        return filtered_df

    filtered_df['CE_PE_Diff'] = filtered_df['CE_LTP'] - filtered_df['PE_LTP']
    filtered_df['Total_Diff_Value'] = filtered_df['LotSize'] * filtered_df['CE_PE_Diff']
    filtered_df['Gross_Final_Pnl'] = ((filtered_df['Strike'] + filtered_df['CE_LTP'] - filtered_df['PE_LTP'] - filtered_df['Underlying']) * filtered_df['LotSize'])
    filtered_df['Total_Investment'] = filtered_df['Underlying'] * filtered_df['LotSize']
    filtered_df['Taxes_And_Brokerage'] = 1500.0
    filtered_df['Net_Final_Pnl'] = filtered_df['Gross_Final_Pnl'] - filtered_df['Taxes_And_Brokerage']
    filtered_df['ROI_Percentage'] = ((filtered_df['Net_Final_Pnl'] / filtered_df['Total_Investment']) * 100).round(2)
    filtered_df['Annualized_ROI'] = (filtered_df['ROI_Percentage'] * 12).round(2)
    
    return filtered_df.sort_values(by='Net_Final_Pnl', ascending=False)

# ============================================================
# STREAMLIT USER INTERFACE
# ============================================================
st.title("📊 NSE F&O Options Scanner & PnL Analyzer")
st.markdown("This fail-safe tool fetches live option chains using 3 parallel workers with built-in retry mechanisms and rate control.")

if st.button("🚀 Run Live Option Scan & Calculate PnL", type="primary"):
    with st.spinner("Fetching data safely in parallel and compiling results..."):
        raw_df = fetch_data()
        
    if raw_df.empty:
        st.error("Could not retrieve data from NSE. Please try again later.")
    else:
        processed_df = process_options_dataframe(raw_df)
        
        if processed_df.empty:
            st.warning("Data fetched, but no rows matched the ATM and non-zero LTP criteria.")
        else:
            st.success(f"Scan complete! Found {len(processed_df)} high-priority opportunities.")
            st.session_state['results'] = processed_df

if 'results' in st.session_state:
    res_df = st.session_state['results']
    
    st.subheader("📋 Processed Options Matrix")
    st.caption("💡 Tap column headers in the table below to sort. Swipe horizontally on mobile to view all columns.")
    
    def color_pnl(val):
        color = 'green' if val > 0 else 'red'
        return f'color: {color}; font-weight: bold;'

    styled_df = res_df.style.map(
        color_pnl, subset=['Net_Final_Pnl', 'Gross_Final_Pnl']
    ).format({
        'Underlying': '{:.2f}',
        'ATM': '{:.2f}',
        'Strike': '{:.2f}',
        'CE_LTP': '{:.2f}',
        'PE_LTP': '{:.2f}',
        'CE_PE_Diff': '{:.2f}',
        'Total_Diff_Value': '{:,.2f}',
        'Gross_Final_Pnl': '{:,.2f}',
        'Total_Investment': '{:,.2f}',
        'Taxes_And_Brokerage': '{:,.2f}',
        'Net_Final_Pnl': '{:,.2f}',
        'ROI_Percentage': '{:.2f}%',
        'Annualized_ROI': '{:.2f}%'
    })

    st.dataframe(styled_df, use_container_width=True, height=500)
    
    csv_data = res_df.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="📥 Download Final Results as CSV",
        data=csv_data,
        file_name=f"final_processed_options_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
        mime="text/csv",
        use_container_width=True
    )
