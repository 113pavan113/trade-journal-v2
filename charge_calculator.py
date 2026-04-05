"""
Indian Market Charge Calculator
Calculates exact brokerage, STT, exchange charges, SEBI fees, GST, and stamp duty
for Fyers trades across Equity, Futures, and Options segments.
"""

# ----- Fyers Charge Schedule -----
# Brokerage:  ₹20 per executed order (flat, F&O)
# STT (pre Apr 1 2026):
#             Options - 0.10% on sell side  [Budget 2024, effective Oct 1 2024]
#             Futures - 0.02% on sell side  [Budget 2024, effective Oct 1 2024]
#             Equity Intraday - 0.025% on sell side
# STT (from Apr 1 2026):
#             Options - 0.15% on sell side  [Budget 2025, effective Apr 1 2026]
#             Futures - 0.05% on sell side  [Budget 2025, effective Apr 1 2026]
#             Equity Intraday - 0.025% on sell side  [unchanged]
# Exchange Txn: NSE Options  - ₹35.03/lakh + NSCCL ₹9/lakh    = 0.04403%
#               NSE Futures  - ₹1.73/lakh  + NSCCL ₹0.5/lakh  = 0.00223%
# SEBI:         ₹10 per crore  (0.000001)
# IPFT:         ₹10 per crore  (0.000005)
# GST:          18% on (brokerage + exchange txn + SEBI + IPFT)
# Stamp Duty:   0.003% on buy side

_STT_HIKE_DATE = "2026-04-01"  # Budget 2025 — new rates from Apr 1 2026


def _stt_rates(trade_date=None):
    """
    Return STT rates for the given trade date.
    trade_date: 'YYYY-MM-DD' string or None (defaults to current/new rates).
    """
    if trade_date and str(trade_date)[:10] < _STT_HIKE_DATE:
        return {"options": 0.001, "futures": 0.0002}   # pre-Apr-2026
    return {"options": 0.0015, "futures": 0.0005}       # from Apr 1 2026


def calculate_charges(turnover_buy, turnover_sell, segment="OPTIONS",
                      num_orders=2, trade_date=None):
    """
    Calculate all charges for a trade.

    Args:
        turnover_buy:  Total buy-side value (premium * qty for options)
        turnover_sell: Total sell-side value
        segment:       "OPTIONS", "FUTURES", or "EQUITY"
        num_orders:    Number of executed orders (default 2: one buy + one sell)
        trade_date:    'YYYY-MM-DD' string used to select correct STT rate

    Returns:
        dict with per-component breakdown and 'total_charges'
    """
    total_turnover = turnover_buy + turnover_sell
    num_orders = max(int(num_orders or 0), 1)
    stt_rates = _stt_rates(trade_date)

    if segment == "OPTIONS":
        brokerage    = 20 * num_orders
        stt          = turnover_sell * stt_rates["options"]
        exchange_txn = total_turnover * (0.0003503 + 0.00009)   # NSE + NSCCL
        sebi         = total_turnover * 0.000001
        ipft         = total_turnover * 0.000005
        gst          = (brokerage + exchange_txn + sebi + ipft) * 0.18
        stamp_duty   = turnover_buy * 0.00003

    elif segment == "FUTURES":
        brokerage    = min(20, total_turnover * 0.0003 / num_orders) * num_orders
        stt          = turnover_sell * stt_rates["futures"]
        exchange_txn = total_turnover * (0.0000173 + 0.000005)  # NSE + NSCCL
        sebi         = total_turnover * 0.000001
        ipft         = total_turnover * 0.000001
        gst          = (brokerage + exchange_txn + sebi + ipft) * 0.18
        stamp_duty   = turnover_buy * 0.00003

    else:  # EQUITY intraday
        brokerage    = min(20, total_turnover * 0.0003 / num_orders) * num_orders
        stt          = turnover_sell * 0.00025   # unchanged Apr 2026
        exchange_txn = total_turnover * 0.0000297
        sebi         = total_turnover * 0.000001
        ipft         = total_turnover * 0.000001
        gst          = (brokerage + exchange_txn + sebi + ipft) * 0.18
        stamp_duty   = turnover_buy * 0.00003

    total_charges = brokerage + stt + exchange_txn + sebi + ipft + gst + stamp_duty

    return {
        "brokerage":     round(brokerage, 2),
        "stt":           round(stt, 2),
        "exchange_txn":  round(exchange_txn, 2),
        "sebi_fees":     round(sebi, 2),
        "ipft":          round(ipft, 2),
        "gst":           round(gst, 2),
        "stamp_duty":    round(stamp_duty, 2),
        "total_charges": round(total_charges, 2),
    }


def detect_segment(symbol):
    """
    Detect segment from Fyers symbol.
    e.g. NSE:NIFTY2530622500CE → OPTIONS
         NSE:NIFTY25MARFUT      → FUTURES
         NSE:RELIANCE-EQ        → EQUITY
    """
    s = symbol.upper()
    if s.endswith("CE") or s.endswith("PE"):
        return "OPTIONS"
    if "FUT" in s:
        return "FUTURES"
    return "EQUITY"


def parse_instrument_details(symbol):
    """
    Parse Fyers symbol into components.
    Returns dict: underlying, strike, option_type, instrument_type
    """
    import re
    details = {"underlying": "", "strike": "", "option_type": "", "instrument_type": ""}
    clean   = symbol.split(":")[-1] if ":" in symbol else symbol
    segment = detect_segment(symbol)
    details["instrument_type"] = segment

    if segment == "OPTIONS":
        if clean.upper().endswith("CE"):
            details["option_type"] = "CE"
            rest = clean[:-2]
        elif clean.upper().endswith("PE"):
            details["option_type"] = "PE"
            rest = clean[:-2]
        else:
            rest = clean
        m = re.match(r'^([A-Za-z]+)(.*?)$', rest)
        if m:
            details["underlying"] = m.group(1).upper()
            date_and_strike = m.group(2)
            if len(date_and_strike) > 5:
                details["strike"] = date_and_strike[5:]

    elif segment == "FUTURES":
        m = re.match(r'([A-Za-z]+)', clean)
        if m:
            details["underlying"] = m.group(1).upper()

    else:
        details["underlying"] = clean.replace("-EQ", "").replace("-BE", "").upper()

    return details
