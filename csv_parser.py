"""
Fyers Tradebook CSV Parser
Reads a Fyers tradebook CSV, groups executions into logical trades
(spreads, synthetics, single legs), and returns structured trade dicts
ready for sheets_writer.

Supports:
- Synthetic Long / Short  (same-strike CE + PE)
- Credit Put Spread        (two PE legs, different strikes)
- Credit Call Spread       (two CE legs, different strikes)
- Single option legs       (lone CE or PE)
- Overnight / multi-day positions (entry and exit in separate days)
"""
import csv
import re
import io
from datetime import datetime
from collections import defaultdict

from charge_calculator import calculate_charges, parse_instrument_details

# ── Lot sizes ─────────────────────────────────────────────────────────────────
LOT_SIZES = {
    "NIFTY": 65,       # revised lot size effective Apr 2025
    "BANKNIFTY": 15,
    "FINNIFTY": 40,
    "MIDCPNIFTY": 120,
    "SENSEX": 10,
    "BANKEX": 15,
}

# Keep the old size too in case CSVs span the boundary
_NIFTY_OLD_LOT = 50   # pre-Nov-2024

# ── Public entry point ────────────────────────────────────────────────────────

def parse_fyers_csv(file_obj):
    """
    Parse a Fyers tradebook CSV file.

    Args:
        file_obj: a file-like object (open file or BytesIO / StringIO).
                  Streamlit uploads give BytesIO; local files give file handles.

    Returns:
        list[dict] — trade dicts sorted by entry_datetime, ready for sheets_writer.
    """
    executions = _read_csv(file_obj)
    if not executions:
        return []

    legs       = _aggregate_by_symbol(executions)
    groups     = _group_into_spreads(legs)
    trades     = []

    for group in groups:
        trade = _build_spread_trade(group) if len(group) >= 2 else _build_single_trade(group[0])
        if trade:
            trades.append(trade)

    trades.sort(key=lambda t: t.get("entry_datetime", datetime.min))
    return trades


# ── Step 1 — Read raw CSV rows ────────────────────────────────────────────────

def _read_csv(file_obj):
    """
    Fyers tradebook CSV layout:
        Rows 1-6  : metadata (Report Title, Date Range, Client, etc.)
        Row  7    : blank
        Row  8    : column headers
        Row  9+   : trade executions
    """
    # Normalise to text
    if isinstance(file_obj, (bytes, bytearray)):
        text = file_obj.decode("utf-8", errors="replace")
    elif hasattr(file_obj, "read"):
        raw = file_obj.read()
        text = raw.decode("utf-8-sig", errors="replace") if isinstance(raw, (bytes, bytearray)) else raw
    else:
        text = str(file_obj)

    lines  = text.splitlines()
    # Find the header row dynamically (look for "Name" or "Symbol" column)
    header_idx = None
    for i, line in enumerate(lines):
        first_col = line.split(",")[0].strip().strip('"').lower()
        if first_col in ("name", "symbol"):
            header_idx = i
            break
    if header_idx is None:
        return []
    data_lines = lines[header_idx:]
    reader     = csv.DictReader(data_lines)

    executions = []
    for row in reader:
        symbol = (row.get("Symbol") or row.get("Name") or "").strip()
        if not symbol:
            continue

        # Parse datetime: "25 Mar 2026, 02:21:43 PM"
        dt_raw = (row.get("Date & time") or "").strip().strip('"')
        dt     = _parse_dt(dt_raw)

        side   = (row.get("Side") or "").strip().upper()          # BUY / SELL
        qty    = _parse_int(row.get("Qty"))
        price  = _parse_float(row.get("Traded price"))
        value  = _parse_float(row.get("Total value"))

        if not symbol or qty == 0 or price == 0:
            continue

        # If Fyers omits Total value column, compute it
        if value == 0:
            value = round(price * qty, 2)

        executions.append({
            "symbol":   symbol,
            "datetime": dt,
            "date_str": dt.strftime("%Y-%m-%d"),
            "side":     side,   # "BUY" or "SELL"
            "qty":      qty,
            "price":    price,
            "value":    value,
        })

    return executions


def _parse_dt(s):
    for fmt in ("%d %b %Y, %I:%M:%S %p", "%d-%b-%Y %H:%M:%S",
                "%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    return datetime.min


def _parse_float(s):
    if s is None:
        return 0.0
    return float(str(s).replace(",", "").strip() or 0)


def _parse_int(s):
    if s is None:
        return 0
    try:
        return int(float(str(s).replace(",", "").strip()))
    except (ValueError, TypeError):
        return 0


# ── Step 2 — Aggregate executions per symbol ─────────────────────────────────

def _aggregate_by_symbol(executions):
    """
    For each unique symbol, collapse all executions into one leg dict.
    Handles partial fills and multi-day positions naturally.
    """
    buckets = defaultdict(lambda: {
        "buy_qty": 0, "buy_value": 0.0, "buy_execs": [],
        "sell_qty": 0, "sell_value": 0.0, "sell_execs": [],
    })

    for e in executions:
        b = buckets[e["symbol"]]
        if e["side"] == "BUY":
            b["buy_qty"]   += e["qty"]
            b["buy_value"] += e["value"]
            b["buy_execs"].append(e)
        else:
            b["sell_qty"]   += e["qty"]
            b["sell_value"] += e["value"]
            b["sell_execs"].append(e)

    legs = []
    for symbol, b in buckets.items():
        buy_qty   = b["buy_qty"]
        sell_qty  = b["sell_qty"]
        buy_val   = b["buy_value"]
        sell_val  = b["sell_value"]
        net_qty   = buy_qty - sell_qty   # positive = net long, negative = net short

        avg_buy  = buy_val  / buy_qty  if buy_qty  else 0.0
        avg_sell = sell_val / sell_qty if sell_qty else 0.0

        # Determine opening side from first execution
        all_execs      = sorted(b["buy_execs"] + b["sell_execs"], key=lambda e: e["datetime"])
        first_exec     = all_execs[0]  if all_execs else None
        last_exec      = all_execs[-1] if all_execs else None
        opening_side   = first_exec["side"] if first_exec else "BUY"  # "BUY" or "SELL"

        # Entry price = avg of OPENING executions
        # Exit  price = avg of CLOSING executions
        if opening_side == "BUY":
            entry_price = avg_buy
            exit_price  = avg_sell if sell_qty else 0.0
            entry_dt    = min((e["datetime"] for e in b["buy_execs"]),  default=datetime.min)
            exit_dt     = max((e["datetime"] for e in b["sell_execs"]), default=None) if sell_qty else None
        else:  # Opened SHORT
            entry_price = avg_sell
            exit_price  = avg_buy if buy_qty else 0.0
            entry_dt    = min((e["datetime"] for e in b["sell_execs"]), default=datetime.min)
            exit_dt     = max((e["datetime"] for e in b["buy_execs"]),  default=None) if buy_qty else None

        details = parse_instrument_details(symbol)

        legs.append({
            "symbol":       symbol,
            "underlying":   details.get("underlying", ""),
            "option_type":  details.get("option_type", ""),
            "strike":       details.get("strike", ""),
            "instrument_type": details.get("instrument_type", "OPTIONS"),
            "opening_side": opening_side,   # "BUY"(long) or "SELL"(short)
            "net_qty":      net_qty,        # 0 = closed, !=0 = open
            "buy_qty":      buy_qty,
            "sell_qty":     sell_qty,
            "buy_value":    buy_val,
            "sell_value":   sell_val,
            "avg_buy":      round(avg_buy,  2),
            "avg_sell":     round(avg_sell, 2),
            "entry_price":  round(entry_price, 2),
            "exit_price":   round(exit_price,  2),
            "entry_dt":     entry_dt,
            "exit_dt":      exit_dt,
            "is_open":      net_qty != 0,
        })

    return legs


# ── Step 3 — Group legs into spread candidates ────────────────────────────────

def _extract_underlying_expiry(symbol):
    """e.g. NIFTY26MAR23350CE → ('NIFTY', '26MAR')"""
    clean = symbol.split(":")[-1] if ":" in symbol else symbol
    m = re.match(r'([A-Za-z]+)(\d{2}[A-Za-z0-9]{2,3})', clean)
    if m:
        return m.group(1).upper(), m.group(2).upper()
    m2 = re.match(r'([A-Za-z]+)(\d{4,5})', clean)
    if m2:
        return m2.group(1).upper(), m2.group(2)
    return clean.upper(), ""


def _group_into_spreads(legs):
    """
    Group option legs by (underlying, expiry) and try to pair them.
    Returns list-of-lists: each inner list is one logical trade group.
    """
    option_legs = [l for l in legs if l["instrument_type"] == "OPTIONS"]
    other_legs  = [[l] for l in legs if l["instrument_type"] != "OPTIONS"]

    # Bucket by underlying + expiry
    buckets = defaultdict(list)
    for leg in option_legs:
        underlying, expiry = _extract_underlying_expiry(leg["symbol"])
        buckets[(underlying, expiry)].append(leg)

    groups = []
    for (underlying, expiry), bucket_legs in buckets.items():
        groups.extend(_pair_legs(bucket_legs))

    return groups + other_legs


def _pair_legs(legs):
    """
    Within a group sharing the same underlying+expiry, greedily pair legs:
    1. Synthetic: same strike, CE + PE
    2. Vertical spread: same type (CE-CE or PE-PE), different strikes, opposite sides
    3. Remainder: standalone singles
    """
    matched = set()
    groups  = []

    def _strike_int(leg):
        try:
            return int(leg["strike"])
        except (ValueError, TypeError):
            return 0

    # ── Pass 1: Synthetic (same strike, CE + PE) ──────────────────────────────
    for i in range(len(legs)):
        if i in matched:
            continue
        for j in range(i + 1, len(legs)):
            if j in matched:
                continue
            a, b = legs[i], legs[j]
            if (_strike_int(a) == _strike_int(b)
                    and {a["option_type"], b["option_type"]} == {"CE", "PE"}):
                groups.append([a, b])
                matched.update([i, j])
                break

    # ── Pass 2: Vertical spread (same type, different strikes, opposite sides) ─
    for i in range(len(legs)):
        if i in matched:
            continue
        for j in range(i + 1, len(legs)):
            if j in matched:
                continue
            a, b = legs[i], legs[j]
            if (a["option_type"] == b["option_type"]
                    and _strike_int(a) != _strike_int(b)
                    and a["opening_side"] != b["opening_side"]):
                groups.append([a, b])
                matched.update([i, j])
                break

    # ── Pass 3: Remaining singles ─────────────────────────────────────────────
    for i in range(len(legs)):
        if i not in matched:
            groups.append([legs[i]])

    return groups


# ── Step 4a — Build spread trade dict ────────────────────────────────────────

def _build_spread_trade(legs):
    """Process a 2-leg group into a single trade dict."""
    # Identify buy-leg (opened long) and sell-leg (opened short)
    buy_legs  = [l for l in legs if l["opening_side"] == "BUY"]
    sell_legs = [l for l in legs if l["opening_side"] == "SELL"]

    # If all legs ended up on the same side (can happen with closed synthetics),
    # determine roles from option type + strike for known spread types.
    if not buy_legs or not sell_legs:
        buy_legs, sell_legs = _infer_roles(legs)

    if not buy_legs or not sell_legs:
        return None

    first_sym  = legs[0]["symbol"]
    underlying, _ = _extract_underlying_expiry(first_sym)
    lot_size   = _get_lot_size(underlying, legs)

    # Spread type detection
    buy_types   = [l["option_type"] for l in buy_legs]
    sell_types  = [l["option_type"] for l in sell_legs]
    buy_strikes  = [_safe_strike(l) for l in buy_legs]
    sell_strikes = [_safe_strike(l) for l in sell_legs]
    all_strikes  = sorted(set(buy_strikes + sell_strikes))
    all_types    = list(set(buy_types + sell_types))

    spread_type, instrument_display = _classify_spread(
        underlying, buy_legs, sell_legs,
        buy_types, sell_types, buy_strikes, sell_strikes, all_strikes, all_types
    )

    # ── P&L (universal: sell_value - buy_value across ALL legs) ──────────────
    total_buy_val  = sum(l["buy_value"]  for l in legs)
    total_sell_val = sum(l["sell_value"] for l in legs)
    pl_rupees      = round(total_sell_val - total_buy_val, 2)

    # Total qty = qty of one side (they should match for closed positions)
    total_qty = max(l["buy_qty"] for l in legs) or max(l["sell_qty"] for l in legs)
    num_lots  = max(1, round(total_qty / lot_size)) if lot_size else 1

    pl_points = round(pl_rupees / total_qty, 2) if total_qty else 0

    # ── Entry / exit display prices ───────────────────────────────────────────
    sell_entry = sum(l["entry_price"] for l in sell_legs) / len(sell_legs)
    buy_entry  = sum(l["entry_price"] for l in buy_legs)  / len(buy_legs)
    entry_price = round(sell_entry - buy_entry, 2)  # net credit (positive) or debit (negative)

    is_open = any(l["is_open"] for l in legs)

    if not is_open:
        sell_exit = sum(l["exit_price"] for l in sell_legs) / len(sell_legs)
        buy_exit  = sum(l["exit_price"] for l in buy_legs)  / len(buy_legs)
        exit_price = round(sell_exit - buy_exit, 2)
    else:
        exit_price = 0.0   # Will show as blank / live for open positions

    # ── Dates & times ─────────────────────────────────────────────────────────
    entry_dt  = min(l["entry_dt"] for l in legs if l["entry_dt"])
    exit_dt   = max((l["exit_dt"] for l in legs if l["exit_dt"]), default=None)
    entry_date = entry_dt.strftime("%d/%m/%Y") if entry_dt else ""
    exit_date  = exit_dt.strftime("%d/%m/%Y")  if exit_dt and not is_open else ""

    # Use the entry date for STT rate selection
    trade_date_str = entry_dt.strftime("%Y-%m-%d") if entry_dt else None

    # ── Charges ───────────────────────────────────────────────────────────────
    num_orders = len(legs) * (1 if is_open else 2)
    charges    = calculate_charges(total_buy_val, total_sell_val, "OPTIONS",
                                   num_orders=num_orders, trade_date=trade_date_str)
    net_pl     = round(pl_rupees - charges["total_charges"], 2)

    # ── Duration ──────────────────────────────────────────────────────────────
    duration_display = ""
    if exit_dt and entry_dt and not is_open:
        duration_display = _fmt_duration(entry_dt, exit_dt)

    # ── Drawdown placeholder (recalculated by sheets_writer) ─────────────────
    return {
        "entry_date":      entry_date,
        "entry_datetime":  entry_dt,
        "segment":         "Index Options",
        "instrument":      instrument_display,
        "long_short":      spread_type,
        "status":          "OPEN" if is_open else "CLOSED",
        "lots":            num_lots,
        "lot_size":        lot_size,
        "entry_price":     entry_price,
        "exit_date":       exit_date,
        "exit_price":      exit_price,
        "pl_points":       pl_points,
        "actual_spot_points": "",
        "pl_rupees":       pl_rupees,
        "drawdown_pct":    0.0,
        "total_charges":   charges["total_charges"],
        "net_pl":          net_pl,
        "duration_display": duration_display,
        "is_carry_forward": False,
        # ── charge breakdown (for potential future display) ───────────────────
        "brokerage":    charges["brokerage"],
        "stt":          charges["stt"],
        "exchange_txn": charges["exchange_txn"],
        "sebi_fees":    charges["sebi_fees"],
        "gst":          charges["gst"],
        "stamp_duty":   charges["stamp_duty"],
    }


# ── Step 4b — Build single-leg trade dict ────────────────────────────────────

def _build_single_trade(leg):
    """Process a single (unpaired) option/futures/equity leg."""
    underlying, _ = _extract_underlying_expiry(leg["symbol"])
    lot_size  = _get_lot_size(underlying, [leg])
    total_qty = max(leg["buy_qty"], leg["sell_qty"])
    num_lots  = max(1, round(total_qty / lot_size)) if lot_size else 1

    total_buy_val  = leg["buy_value"]
    total_sell_val = leg["sell_value"]
    pl_rupees      = round(total_sell_val - total_buy_val, 2)
    pl_points      = round(pl_rupees / total_qty, 2) if total_qty else 0

    opt_type   = leg["option_type"]
    strike     = leg["strike"] or ""
    is_open    = leg["is_open"]

    if leg["opening_side"] == "BUY":
        long_short = f"Long {opt_type}" if opt_type else "Long"
    else:
        long_short = f"Short {opt_type}" if opt_type else "Short"

    instrument = f"{underlying} {strike}{opt_type}".strip() if opt_type else underlying

    entry_dt   = leg["entry_dt"]
    exit_dt    = leg["exit_dt"] if not is_open else None
    entry_date = entry_dt.strftime("%d/%m/%Y") if entry_dt else ""
    exit_date  = exit_dt.strftime("%d/%m/%Y")  if exit_dt else ""
    trade_date_str = entry_dt.strftime("%Y-%m-%d") if entry_dt else None

    num_orders = 1 if is_open else 2
    charges    = calculate_charges(total_buy_val, total_sell_val, "OPTIONS",
                                   num_orders=num_orders, trade_date=trade_date_str)
    net_pl     = round(pl_rupees - charges["total_charges"], 2)
    duration_display = _fmt_duration(entry_dt, exit_dt) if exit_dt and entry_dt else ""

    return {
        "entry_date":      entry_date,
        "entry_datetime":  entry_dt,
        "segment":         "Index Options",
        "instrument":      instrument,
        "long_short":      long_short,
        "status":          "OPEN" if is_open else "CLOSED",
        "lots":            num_lots,
        "lot_size":        lot_size,
        "entry_price":     leg["entry_price"],
        "exit_date":       exit_date,
        "exit_price":      leg["exit_price"] if not is_open else 0.0,
        "pl_points":       pl_points,
        "actual_spot_points": "",
        "pl_rupees":       pl_rupees,
        "drawdown_pct":    0.0,
        "total_charges":   charges["total_charges"],
        "net_pl":          net_pl,
        "duration_display": duration_display,
        "is_carry_forward": False,
        "brokerage":    charges["brokerage"],
        "stt":          charges["stt"],
        "exchange_txn": charges["exchange_txn"],
        "sebi_fees":    charges["sebi_fees"],
        "gst":          charges["gst"],
        "stamp_duty":   charges["stamp_duty"],
    }


# ── Helpers ───────────────────────────────────────────────────────────────────

def _safe_strike(leg):
    try:
        return int(leg["strike"])
    except (ValueError, TypeError):
        return 0


def _get_lot_size(underlying, legs):
    base = LOT_SIZES.get(underlying.upper(), 1)
    # Infer from actual qty if possible (handles lot-size changes)
    for leg in legs:
        qty = max(leg.get("buy_qty", 0), leg.get("sell_qty", 0))
        if qty and qty % base == 0:
            return base
        # Try old NIFTY lot
        if underlying == "NIFTY" and qty and qty % _NIFTY_OLD_LOT == 0:
            return _NIFTY_OLD_LOT
    return base


def _classify_spread(underlying, buy_legs, sell_legs,
                     buy_types, sell_types, buy_strikes, sell_strikes,
                     all_strikes, all_types):
    """Return (spread_type_label, instrument_display_string)."""
    type_str = "/".join(sorted(set(all_types))) if all_types else ""

    # Synthetic Long  = buy CE + sell PE (same strike)
    # Synthetic Short = sell CE + buy PE (same strike)
    if (len(buy_legs) == 1 and len(sell_legs) == 1
            and set(buy_types + sell_types) == {"CE", "PE"}
            and buy_strikes == sell_strikes):
        if buy_types[0] == "CE":
            spread_type = "Synthetic Long"
        else:
            spread_type = "Synthetic Short"
        strike_str = str(all_strikes[0])
        return spread_type, f"{underlying} {strike_str} {spread_type}"

    # Credit Put Spread  = short higher PE + long lower PE
    if set(all_types) == {"PE"}:
        if sell_strikes and buy_strikes and max(sell_strikes) > max(buy_strikes):
            spread_type = "Credit Put Spread"
        elif sell_strikes and buy_strikes:
            spread_type = "Debit Put Spread"
        else:
            spread_type = "Put Spread"
        strike_str = "/".join(str(s) for s in all_strikes)
        return spread_type, f"{underlying} {strike_str} PE"

    # Credit Call Spread = short lower CE + long higher CE
    if set(all_types) == {"CE"}:
        if sell_strikes and buy_strikes and max(sell_strikes) < max(buy_strikes):
            spread_type = "Credit Call Spread"
        elif sell_strikes and buy_strikes:
            spread_type = "Debit Call Spread"
        else:
            spread_type = "Call Spread"
        strike_str = "/".join(str(s) for s in all_strikes)
        return spread_type, f"{underlying} {strike_str} CE"

    # Fallback
    strike_str  = "/".join(str(s) for s in all_strikes)
    spread_type = "Spread"
    return spread_type, f"{underlying} {strike_str} {type_str}"


def _infer_roles(legs):
    """
    Fallback: when all legs have the same opening_side, infer buy/sell from
    option type + strike (same logic as V1 for closed synthetics).
    """
    if len(legs) != 2:
        return legs, []

    a, b = legs
    types = {a["option_type"], b["option_type"]}

    # Synthetic: same strike, CE + PE
    if types == {"CE", "PE"}:
        ce = a if a["option_type"] == "CE" else b
        pe = a if a["option_type"] == "PE" else b
        # Heuristic: whichever had higher sell-avg was likely the opened-short leg
        if ce["avg_sell"] >= pe["avg_sell"]:
            # CE was sold at higher price → Synthetic Short
            return [pe], [ce]   # buy=PE, sell=CE
        else:
            # PE was sold at higher price → Synthetic Long
            return [ce], [pe]   # buy=CE, sell=PE

    # Vertical spread: same type, different strikes
    if len(types) == 1:
        opt_type = types.pop()
        sorted_legs = sorted(legs, key=lambda l: _safe_strike(l))
        if opt_type == "PE":
            # Higher PE = more premium = likely the short (sell) leg
            return [sorted_legs[0]], [sorted_legs[1]]
        else:
            # Lower CE = more premium = likely the short (sell) leg
            return [sorted_legs[1]], [sorted_legs[0]]

    return legs, []


def _fmt_duration(start, end):
    if not start or not end:
        return ""
    diff = end - start
    total_mins = int(diff.total_seconds() / 60)
    if total_mins < 0:
        return ""
    days, remainder_mins = divmod(total_mins, 1440)  # 1440 mins in a day
    hours, mins = divmod(remainder_mins, 60)
    if days:
        return f"{days}d {hours}h {mins}m"
    if hours:
        return f"{hours}h {mins}m"
    return f"{mins}m"
