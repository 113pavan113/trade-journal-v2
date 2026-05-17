"""
Fyers Realized P&L CSV Parser

This parser treats the broker's realized P&L report as the accounting source of
truth. Unlike the tradebook parser, it does not try to infer open/close intent
from raw executions. It reads already-realized rows and converts them into the
same trade dict shape consumed by sheets_writer.
"""

import csv
import io
import re
from collections import defaultdict
from datetime import datetime

from csv_parser import LOT_SIZES
from charge_calculator import parse_instrument_details


def looks_like_realized_pnl_csv(file_obj):
    """Return True if the uploaded CSV appears to be a realized P&L report."""
    text = _read_text(file_obj)
    header = _find_header(text.splitlines())
    if not header:
        return False
    lowered = {_norm(h) for h in header}
    has_symbol = bool(_find_col(lowered, ["symbol", "name", "trading symbol", "scrip"]))
    has_pnl = bool(_find_col(lowered, ["realized pnl", "realised pnl", "net pnl", "net p&l", "gross pnl", "gross p&l"]))
    has_avg = bool(_find_col(lowered, ["buy avg", "buy average", "sell avg", "sell average"]))
    return has_symbol and has_pnl and has_avg


def parse_realized_pnl_csv(file_obj, group_option_pairs=True):
    """
    Parse a Fyers realized P&L CSV.

    Returns:
        (trades, source_ids)
        trades     — list[dict] ready for sheets_writer.
        source_ids — stable row/group identifiers for duplicate prevention.
    """
    text = _read_text(file_obj)
    lines = text.splitlines()
    date_range = _extract_date_range(lines)

    header_idx = None
    header = None
    for i in range(len(lines)):
        row = next(csv.reader([lines[i]]), [])
        normalized = {_norm(c) for c in row}
        if _find_col(normalized, ["symbol", "name", "trading symbol", "scrip"]) and _find_col(
            normalized, ["realized pnl", "realised pnl", "net pnl", "net p&l", "gross pnl", "gross p&l"]
        ):
            header_idx = i
            header = row
            break

    if header_idx is None or not header:
        return [], set()

    reader = csv.DictReader(lines[header_idx:])
    items = [_row_to_item(row, date_range) for row in reader]
    items = [item for item in items if item]

    if group_option_pairs:
        trades = _group_items(items)
    else:
        trades = [_item_to_trade([item]) for item in items]

    trades = [t for t in trades if t]
    trades.sort(key=lambda t: (_parse_sheet_date(t.get("exit_date", "")), t.get("instrument", "")))
    source_ids = {t["source_id"] for t in trades if t.get("source_id")}
    return trades, source_ids


def _read_text(file_obj):
    if isinstance(file_obj, (bytes, bytearray)):
        return file_obj.decode("utf-8-sig", errors="replace")
    if hasattr(file_obj, "getvalue"):
        raw = file_obj.getvalue()
    elif hasattr(file_obj, "read"):
        pos = None
        try:
            pos = file_obj.tell()
        except Exception:
            pass
        raw = file_obj.read()
        if pos is not None:
            try:
                file_obj.seek(pos)
            except Exception:
                pass
    else:
        raw = str(file_obj)
    return raw.decode("utf-8-sig", errors="replace") if isinstance(raw, (bytes, bytearray)) else str(raw)


def _find_header(lines):
    for line in lines:
        row = next(csv.reader([line]), [])
        normalized = {_norm(c) for c in row}
        if _find_col(normalized, ["symbol", "name", "trading symbol", "scrip"]):
            return row
    return []


def _norm(s):
    return re.sub(r"[^a-z0-9]+", " ", str(s).strip().lower()).strip()


def _find_col(headers, candidates):
    normalized_candidates = [_norm(c) for c in candidates]
    for candidate in normalized_candidates:
        for header in headers:
            if header == candidate or candidate in header:
                return header
    return None


def _get(row, candidates, default=""):
    by_norm = {_norm(k): v for k, v in row.items()}
    col = _find_col(set(by_norm.keys()), candidates)
    return by_norm.get(col, default) if col else default


def _num(value):
    s = str(value or "").strip()
    if not s:
        return 0.0
    negative = s.startswith("(") and s.endswith(")")
    s = s.replace(",", "").replace("₹", "").replace("Rs.", "").replace("rs.", "")
    s = s.replace("(", "").replace(")", "").strip()
    try:
        value = float(s)
    except ValueError:
        return 0.0
    return -value if negative else value


def _extract_date_range(lines):
    joined = "\n".join(lines[:12])
    dates = re.findall(r"\b(\d{1,2}/\d{1,2}/\d{4})\b", joined)
    parsed = [_parse_any_date(d) for d in dates]
    parsed = [d for d in parsed if d]
    if len(parsed) >= 2:
        return parsed[0], parsed[1]
    if len(parsed) == 1:
        return parsed[0], parsed[0]
    return None, None


def _parse_any_date(value):
    value = str(value or "").strip().strip('"')
    if not value:
        return None
    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%d %b %Y", "%d %B %Y"):
        try:
            return datetime.strptime(value[:10] if fmt == "%Y-%m-%d" else value, fmt)
        except Exception:
            pass
    return None


def _fmt_date(dt):
    return dt.strftime("%d/%m/%Y") if dt else ""


def _parse_sheet_date(value):
    return _parse_any_date(value) or datetime.min


def _row_to_item(row, date_range):
    symbol = str(_get(row, ["symbol", "name", "trading symbol", "scrip", "contract"]) or "").strip()
    if not symbol:
        return None

    buy_qty = abs(_num(_get(row, ["buy qty", "buy quantity", "bought qty", "quantity buy"])))
    sell_qty = abs(_num(_get(row, ["sell qty", "sell quantity", "sold qty", "quantity sell"])))
    qty = max(buy_qty, sell_qty, abs(_num(_get(row, ["qty", "quantity", "net qty"]))))
    if qty <= 0:
        return None

    buy_avg = _num(_get(row, ["buy avg", "buy average", "average buy price", "buy price"]))
    sell_avg = _num(_get(row, ["sell avg", "sell average", "average sell price", "sell price"]))
    gross = _num(_get(row, ["gross pnl", "gross p&l", "gross profit", "gross realized pnl", "gross realised pnl"]))
    charges = abs(_num(_get(row, ["charges", "total charges", "expenses", "brokerage charges"])))
    net = _num(_get(row, ["net pnl", "net p&l", "realized pnl", "realised pnl", "net realized pnl", "net realised pnl"]))
    if not net and gross:
        net = gross - charges
    if not gross and (buy_avg or sell_avg):
        gross = round((sell_avg - buy_avg) * qty, 2)
    if not charges and gross and net:
        charges = round(abs(gross - net), 2)

    entry_dt = (
        _parse_any_date(_get(row, ["entry date", "buy date", "from date"]))
        or date_range[0]
        or _parse_any_date(_get(row, ["date", "trade date", "exit date"]))
    )
    exit_dt = (
        _parse_any_date(_get(row, ["exit date", "sell date", "trade date", "date", "realized date", "realised date"]))
        or date_range[1]
        or entry_dt
    )

    details = parse_instrument_details(symbol)
    underlying = details.get("underlying") or symbol
    lot_size = LOT_SIZES.get(underlying, 1)
    lots = max(int(round(qty / lot_size)), 1) if lot_size else 1

    return {
        "symbol": symbol,
        "underlying": underlying,
        "strike": details.get("strike", ""),
        "option_type": details.get("option_type", ""),
        "instrument_type": details.get("instrument_type", "OPTIONS"),
        "expiry_key": _expiry_key(symbol, details),
        "qty": qty,
        "lots": lots,
        "lot_size": lot_size,
        "buy_avg": buy_avg,
        "sell_avg": sell_avg,
        "gross": round(gross, 2),
        "charges": round(charges, 2),
        "net": round(net, 2),
        "entry_date": _fmt_date(entry_dt),
        "exit_date": _fmt_date(exit_dt),
    }


def _expiry_key(symbol, details):
    clean = symbol.split(":")[-1].upper()
    strike = str(details.get("strike", ""))
    opt = details.get("option_type", "")
    if opt and strike and clean.endswith(opt):
        stem = clean[: -len(opt)]
        if stem.endswith(strike):
            return stem[: -len(strike)]
    return clean


def _group_items(items):
    option_buckets = defaultdict(list)
    singles = []
    for item in items:
        if item.get("instrument_type") == "OPTIONS" and item.get("strike") and item.get("option_type") in {"CE", "PE"}:
            key = (
                item["entry_date"],
                item["exit_date"],
                item["underlying"],
                item["expiry_key"],
                item["strike"],
            )
            option_buckets[key].append(item)
        else:
            singles.append(item)

    trades = []
    used = set()
    for key, bucket in option_buckets.items():
        ces = [b for b in bucket if b["option_type"] == "CE"]
        pes = [b for b in bucket if b["option_type"] == "PE"]
        if ces and pes:
            group = ces + pes
            trades.append(_item_to_trade(group))
            used.update(id(x) for x in group)

    for item in items:
        if id(item) not in used and item not in singles:
            trades.append(_item_to_trade([item]))
    for item in singles:
        trades.append(_item_to_trade([item]))
    return trades


def _item_to_trade(items):
    if not items:
        return None

    underlying = items[0]["underlying"]
    strike = items[0].get("strike", "")
    opt_types = {i.get("option_type", "") for i in items}
    total_qty = sum(i["qty"] for i in items)
    total_gross = round(sum(i["gross"] for i in items), 2)
    total_charges = round(sum(i["charges"] for i in items), 2)
    total_net = round(sum(i["net"] for i in items), 2)
    lot_size = items[0].get("lot_size", 1)
    lots = max((i.get("lots", 1) for i in items), default=1)
    entry_date = min((i["entry_date"] for i in items if i["entry_date"]), default="")
    exit_date = max((i["exit_date"] for i in items if i["exit_date"]), default=entry_date)

    if opt_types == {"CE", "PE"} and strike:
        instrument = f"{underlying} {strike} CE+PE Realized"
        direction = "Realized Option Pair"
    elif len(items) == 1:
        item = items[0]
        suffix = f"{item.get('strike', '')}{item.get('option_type', '')}".strip()
        instrument = f"{underlying} {suffix}".strip()
        direction = f"Realized {item.get('option_type') or item.get('instrument_type', 'Trade')}".strip()
    else:
        instrument = f"{underlying} Realized Basket"
        direction = "Realized Basket"

    avg_entry = _weighted_avg([i["buy_avg"] for i in items], [i["qty"] for i in items])
    avg_exit = _weighted_avg([i["sell_avg"] for i in items], [i["qty"] for i in items])
    pl_points = round(total_gross / total_qty, 2) if total_qty else 0.0

    source_parts = sorted(i["symbol"] for i in items)
    source_id = f"REALIZED|{exit_date}|{'|'.join(source_parts)}|{total_net:.2f}"

    return {
        "entry_date": entry_date,
        "segment": "Index Options" if items[0].get("instrument_type") == "OPTIONS" else items[0].get("instrument_type", "OPTIONS"),
        "instrument": instrument,
        "long_short": direction,
        "status": "CLOSED",
        "lots": lots,
        "lot_size": lot_size,
        "entry_price": avg_entry,
        "exit_date": exit_date,
        "exit_price": avg_exit,
        "pl_points": pl_points,
        "actual_spot_points": "",
        "pl_rupees": total_gross,
        "total_charges": total_charges,
        "net_pl": total_net,
        "duration_display": "",
        "source_id": source_id,
        "comments": "Imported from Fyers Realized P&L",
    }


def _weighted_avg(values, weights):
    total_weight = sum(weights)
    if total_weight <= 0:
        return 0.0
    return round(sum(v * w for v, w in zip(values, weights)) / total_weight, 2)
