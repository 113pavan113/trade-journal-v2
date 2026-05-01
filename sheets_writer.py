"""
Google Sheets Writer — V2
Writes processed trades to the Trade_Journal_Enhanced spreadsheet.
Updates all 4 tabs on every sync.

Sheet 1 — Trade Log        (individual trade rows, columns A-U)
Sheet 2 — Parameters Summary (21 performance metrics)
Sheet 3 — Monthly & Weekly P&L Report (weekly performance table)
Sheet 4 — Drawdown Analysis (per-trade equity curve)
"""

import os
import re
import json
from datetime import datetime, timedelta
from collections import defaultdict

import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

load_dotenv()

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

GOOGLE_SHEET_ID  = os.getenv("GOOGLE_SHEET_ID", "1fQRwCUIpT9wGpMz0_xAPJZFDHsMcL_XLx17mqb7tGvQ")
STARTING_CAPITAL = 498_000


# ── Connection ────────────────────────────────────────────────────────────────

def get_sheets_client():
    """Authenticate and return the spreadsheet object."""
    creds_env = os.getenv("GOOGLE_CREDS_JSON", "")
    if creds_env:
        clean = creds_env.strip().strip("'")
        creds = Credentials.from_service_account_info(json.loads(clean), scopes=SCOPES)
    else:
        path = os.getenv("GOOGLE_CREDS_PATH", "google_creds.json")
        if not os.path.isabs(path):
            path = os.path.join(os.path.dirname(os.path.abspath(__file__)), path)
        creds = Credentials.from_service_account_file(path, scopes=SCOPES)

    client = gspread.authorize(creds)
    return client.open_by_key(GOOGLE_SHEET_ID)


# ── Main entry point ──────────────────────────────────────────────────────────

def sync_to_sheets(trades, spreadsheet=None):
    """
    Sync trade list (from csv_parser) to all 4 sheets.
    Returns dict: added, skipped, open_updated, errors
    """
    if spreadsheet is None:
        spreadsheet = get_sheets_client()

    result = {"added": 0, "skipped": 0, "open_updated": 0, "errors": []}

    try:
        trade_log = spreadsheet.worksheet("Trade Log")
    except gspread.exceptions.WorksheetNotFound:
        result["errors"].append("'Trade Log' worksheet not found.")
        return result

    _ensure_headers(trade_log)

    # ── 1. Close any existing OPEN rows matched by incoming data ──────────────
    open_updated, used_update_keys = _update_open_rows(trade_log, trades)
    result["open_updated"] = open_updated

    trades_to_add = [t for t in trades if _trade_key(t) not in used_update_keys]

    # ── 2. Append genuinely new trades ────────────────────────────────────────
    existing_keys = _get_existing_keys(trade_log)
    next_sno      = _get_next_sno(trade_log)
    rows_to_add   = []

    for trade in trades_to_add:
        key = _trade_key(trade)
        if trade.get("status") == "CLOSED" and key in existing_keys:
            result["skipped"] += 1
            continue
        if trade.get("status") == "OPEN" and _open_already_exists(trade_log, trade):
            result["skipped"] += 1
            continue

        long_short = trade.get("long_short", "")
        if trade.get("status") == "OPEN" and "[OPEN]" not in long_short:
            long_short = f"{long_short} [OPEN]"

        rows_to_add.append([
            next_sno + len(rows_to_add),       # A  S.No
            trade.get("entry_date", ""),        # B  Entry Date
            trade.get("segment", ""),           # C  Segment
            trade.get("instrument", ""),        # D  Instrument
            long_short,                         # E  Type
            trade.get("lots", 1),               # F  Lots
            trade.get("lot_size", 1),           # G  Lot Size
            trade.get("entry_price", 0),        # H  Entry Price
            trade.get("exit_date", ""),         # I  Exit Date
            trade.get("exit_price", 0),         # J  Exit/LTP Price
            trade.get("pl_points", 0),          # K  P/L (Points)
            trade.get("actual_spot_points", ""),# L  Actual Spot Points
            trade.get("pl_rupees", 0),          # M  P/L (₹)
            "",                                 # N  Drawdown % (recalculated)
            "",                                 # O  Cumulative P/L
            "",                                 # P  Monthly Profit/Loss
            "",                                 # Q  Cum Capital (No Charges)
            "",                                 # R  Comments
            trade.get("total_charges", 0),      # S  Total Charges
            trade.get("net_pl", 0),             # T  Net P/L
            trade.get("duration_display", ""),  # U  Duration
        ])

    if rows_to_add:
        trade_log.append_rows(rows_to_add, value_input_option="USER_ENTERED")
        result["added"] = len(rows_to_add)

    # ── 3. Recalculate Trade Log derived columns & reorder ────────────────────
    if result["added"] or result["open_updated"]:
        _move_open_rows_to_end(trade_log)

    _recalculate_trade_log(trade_log, spreadsheet)

    # ── 4. Update all summary sheets ──────────────────────────────────────────
    cap = _read_starting_capital(trade_log)
    _update_parameters_summary(trade_log, spreadsheet, cap)
    _update_weekly_performance(trade_log, spreadsheet, cap)
    _update_monthly_performance(trade_log, spreadsheet, cap)
    _update_drawdown_analysis(trade_log, spreadsheet, cap)

    return result


# ── Instrument normalisation (V1 ↔ V2 compatibility) ─────────────────────────

def _normalize_instrument(s):
    """
    Strip Fyers 5-digit expiry codes embedded in V1 instrument names.
    e.g. 'NIFTY 2631024200/2631024850 PE' → 'NIFTY 24200/24850 PE'
         'NIFTY 24400/25050 CE'            → unchanged
         'NIFTY 23100 Synthetic Long'       → unchanged
    """
    # 10-digit block = 5-digit expiry + 5-digit strike → keep only the strike
    s = re.sub(r'\b\d{5}(\d{5})\b', r'\1', s)
    # Collapse "Synthetic Long/Short" → "Synthetic" for dedup
    s = s.replace("Synthetic Long", "Synthetic").replace("Synthetic Short", "Synthetic")
    return s.strip()


def _trade_key(trade):
    instr = _normalize_instrument(trade.get("instrument", ""))
    return f"{trade.get('entry_date', '')}_{instr}"


# ── Duplicate-detection helpers ───────────────────────────────────────────────

def _get_existing_keys(sheet):
    try:
        data = sheet.get_all_values()
        keys = set()
        for row in data[3:]:
            if len(row) >= 4 and row[0]:
                keys.add(f"{row[1]}_{_normalize_instrument(row[3])}")
        return keys
    except Exception:
        return set()


def _get_next_sno(sheet):
    try:
        data  = sheet.get_all_values()
        max_n = 0
        for row in data[3:]:
            if row and row[0] and str(row[0]).strip().isdigit():
                max_n = max(max_n, int(row[0]))
        return max_n + 1
    except Exception:
        return 1


def _open_already_exists(sheet, trade):
    try:
        data = sheet.get_all_values()
        for row in data[3:]:
            if len(row) >= 5 and row[0] and "[OPEN]" in row[4]:
                if _instruments_match(row[3], trade.get("instrument", "")):
                    return True
        return False
    except Exception:
        return False


def _instruments_match(a, b):
    return _normalize_instrument(a) == _normalize_instrument(b)


# ── Close existing OPEN rows ──────────────────────────────────────────────────

def _update_open_rows(trade_log, trades):
    try:
        all_data = trade_log.get_all_values()
    except Exception:
        return 0, set()

    updated_count = 0
    used_keys     = set()

    for row_idx, row in enumerate(all_data[3:], start=4):
        if not row or not row[0]:
            continue
        if "[OPEN]" not in (row[4] if len(row) > 4 else ""):
            continue

        existing_instr = row[3] if len(row) > 3 else ""
        match = next(
            (t for t in trades
             if t.get("status") == "CLOSED"
             and _instruments_match(t.get("instrument", ""), existing_instr)),
            None,
        )
        if not match:
            continue

        updated_row = list(row)
        if len(updated_row) < 21:
            updated_row += [""] * (21 - len(updated_row))

        updated_row[4]  = match.get("long_short", updated_row[4].replace(" [OPEN]", ""))
        # Correct any manual-entry mistakes with authoritative tradebook values
        updated_row[5]  = match.get("lots",        updated_row[5]  if len(updated_row) > 5  else 1)
        updated_row[6]  = match.get("lot_size",    updated_row[6]  if len(updated_row) > 6  else 1)
        updated_row[7]  = match.get("entry_price", updated_row[7]  if len(updated_row) > 7  else 0)
        updated_row[8]  = match.get("exit_date", "")
        updated_row[9]  = match.get("exit_price", 0)
        updated_row[10] = match.get("pl_points", 0)
        updated_row[12] = match.get("pl_rupees", 0)
        updated_row[18] = match.get("total_charges", 0)
        updated_row[19] = match.get("net_pl", 0)
        updated_row[20] = match.get("duration_display", "")

        trade_log.update(f"A{row_idx}:U{row_idx}",
                         [updated_row[:21]], value_input_option="USER_ENTERED")
        used_keys.add(_trade_key(match))
        updated_count += 1

    return updated_count, used_keys


# ── Trade Log derived columns (N, O, P, Q) ────────────────────────────────────

def _recalculate_trade_log(trade_log, spreadsheet=None):
    """Recalculate Drawdown %, Cumulative P/L, Monthly P/L, Cum Capital for all rows."""
    try:
        all_data = trade_log.get_all_values()
    except Exception:
        return

    cap = _read_starting_capital(trade_log)
    cum_pl     = 0.0
    peak_cap   = cap
    month_pls  = defaultdict(float)
    batch      = []

    for row_idx, row in enumerate(all_data):
        if row_idx < 3 or not row or not row[0]:
            continue
        if "[OPEN]" in (row[4] if len(row) > 4 else ""):
            continue

        actual_row = row_idx + 1
        pl         = _safe_float(row[12]) if len(row) > 12 else 0.0
        date_str   = row[1] if len(row) > 1 else ""

        cum_pl  += pl
        cum_cap  = cap + cum_pl
        if cum_cap > peak_cap:
            peak_cap = cum_cap

        drawdown = ((peak_cap - cum_cap) / peak_cap * 100) if peak_cap > 0 else 0.0

        month_key = ""
        try:
            month_key = datetime.strptime(date_str, "%d/%m/%Y").strftime("%m/%Y")
        except Exception:
            pass
        if month_key:
            month_pls[month_key] += pl

        existing_comment = row[17] if len(row) > 17 else ""
        ath_label = "New All Time High" if drawdown == 0.0 else ""
        if existing_comment.strip() == "New All Time High":
            comment_val = ath_label   # clear it if no longer ATH
        elif not existing_comment.strip():
            comment_val = ath_label   # empty cell — write label or leave blank
        else:
            comment_val = existing_comment   # preserve manual comments

        batch.extend([
            {"range": f"N{actual_row}", "values": [[f"{round(drawdown, 2)}%"]]},
            {"range": f"O{actual_row}", "values": [[round(cum_pl, 2)]]},
            {"range": f"P{actual_row}", "values": [[round(month_pls.get(month_key, 0), 2)]]},
            {"range": f"Q{actual_row}", "values": [[round(cum_cap, 2)]]},
            {"range": f"R{actual_row}", "values": [[comment_val]]},
        ])

    if batch:
        trade_log.batch_update(batch, value_input_option="USER_ENTERED")

    # ── Color column Q: green at ATH (drawdown=0), light red in drawdown ──────
    sheet_id   = trade_log.id
    color_reqs = []
    cum_pl2    = 0.0
    peak2      = cap

    for row_idx, row in enumerate(all_data):
        if row_idx < 3 or not row or not row[0]:
            continue
        if "[OPEN]" in (row[4] if len(row) > 4 else ""):
            continue

        pl       = _safe_float(row[12]) if len(row) > 12 else 0.0
        cum_pl2 += pl
        cum_cap2 = cap + cum_pl2
        if cum_cap2 > peak2:
            peak2 = cum_cap2

        at_ath     = (cum_cap2 >= peak2)
        bg         = ({"red": 0.714, "green": 0.843, "blue": 0.659}
                      if at_ath else
                      {"red": 0.957, "green": 0.698, "blue": 0.698})
        actual_row = row_idx + 1
        color_reqs.append({
            "repeatCell": {
                "range": {
                    "sheetId":          sheet_id,
                    "startRowIndex":    actual_row - 1,
                    "endRowIndex":      actual_row,
                    "startColumnIndex": 16,   # column Q
                    "endColumnIndex":   17,
                },
                "cell": {"userEnteredFormat": {"backgroundColor": bg}},
                "fields": "userEnteredFormat.backgroundColor",
            }
        })

    if color_reqs and spreadsheet:
        spreadsheet.batch_update({"requests": color_reqs})


# ── Sheet 2 — Parameters Summary ─────────────────────────────────────────────

def _update_parameters_summary(trade_log, spreadsheet, starting_capital):
    try:
        ws       = spreadsheet.worksheet("Parameters Summary")
        all_data = trade_log.get_all_values()
    except Exception:
        return

    trades = []
    for row in all_data[3:]:
        if not row or not row[0]:
            continue
        if "[OPEN]" in (row[4] if len(row) > 4 else ""):
            continue
        trades.append({
            "pl":      _safe_float(row[12]),
            "points":  _safe_float(row[10]),
            "charges": _safe_float(row[18]),
            "date":    row[1] if len(row) > 1 else "",
        })

    if not trades:
        return

    # Date range
    dates = []
    for t in trades:
        try:
            dates.append(datetime.strptime(t["date"], "%d/%m/%Y"))
        except Exception:
            pass
    total_days = (max(dates) - min(dates)).days + 1 if len(dates) >= 2 else 1

    total_pl      = sum(t["pl"]      for t in trades)
    total_charges = sum(t["charges"] for t in trades)
    total_points  = sum(t["points"]  for t in trades)
    n_trades      = len(trades)

    winners = [t for t in trades if t["pl"] > 0]
    losers  = [t for t in trades if t["pl"] < 0]
    n_win   = len(winners)
    n_lose  = len(losers)

    avg_win  = sum(t["pl"] for t in winners) / n_win  if n_win  else 0.0
    avg_lose = sum(t["pl"] for t in losers)  / n_lose if n_lose else 0.0
    max_win  = max((t["pl"] for t in trades), default=0.0)
    max_lose = min((t["pl"] for t in trades), default=0.0)

    # Streaks
    max_w = max_l = cur_w = cur_l = 0
    for t in trades:
        if   t["pl"] > 0: cur_w += 1; cur_l  = 0
        elif t["pl"] < 0: cur_l += 1; cur_w  = 0
        else:              cur_w  = 0; cur_l  = 0
        max_w = max(max_w, cur_w)
        max_l = max(max_l, cur_l)

    total_win_pl  = sum(t["pl"] for t in winners)
    total_lose_pl = abs(sum(t["pl"] for t in losers))
    profit_factor = total_win_pl / total_lose_pl if total_lose_pl else 999.0
    exp_r         = abs(avg_win / avg_lose) if avg_lose else 0.0
    exp_rs        = total_pl / n_trades if n_trades else 0.0

    day_pls = defaultdict(float)
    for t in trades:
        day_pls[t["date"]] += t["pl"]
    win_days  = sum(1 for v in day_pls.values() if v > 0)
    lose_days = sum(1 for v in day_pls.values() if v < 0)

    cum_pct   = total_pl / starting_capital * 100
    win_rate  = n_win   / n_trades * 100 if n_trades else 0.0
    lose_rate = n_lose  / n_trades * 100 if n_trades else 0.0

    def _fmt_inr(v):
        return f"₹ {v:,.0f}" if v >= 0 else f"-₹ {abs(v):,.0f}"

    updates = [
        ("C4",  f"₹ {starting_capital:,.0f}"),
        ("C5",  total_days),
        ("C6",  _fmt_inr(total_pl)),
        ("C7",  f"{cum_pct:.2f}%"),
        ("C8",  win_days),
        ("C9",  lose_days),
        ("C10", n_trades),
        ("C11", f"{win_rate:.0f}%"),
        ("C12", f"{lose_rate:.0f}%"),
        ("C13", f"₹ {avg_win:,.2f}"),
        ("C14", f"-₹ {abs(avg_lose):,.2f}"),
        ("C15", f"₹ {max_win:,.0f}"),
        ("C16", f"-₹ {abs(max_lose):,.0f}"),
        ("C17", max_w),
        ("C18", max_l),
        ("C19", f"{exp_r:.1f}x"),
        ("C20", f"₹ {profit_factor:.2f}"),
        ("C21", round(exp_rs, 2)),
        ("C22", round(total_charges, 0)),
        ("C23", round(total_points, 2)),
        ("C24", _fmt_inr(total_pl - total_charges)),
    ]

    batch = [{"range": c, "values": [[v]]} for c, v in updates]
    ws.batch_update(batch, value_input_option="USER_ENTERED")


# ── Sheet 3 — Weekly Performance ─────────────────────────────────────────────

def _update_weekly_performance(trade_log, spreadsheet, starting_capital):
    try:
        ws       = spreadsheet.worksheet("Monthly & Weekly P&L Report")
        all_data = trade_log.get_all_values()
    except Exception:
        return

    weekly = defaultdict(float)
    for row in all_data[3:]:
        if not row or not row[0]:
            continue
        if "[OPEN]" in (row[4] if len(row) > 4 else ""):
            continue
        pl = _safe_float(row[19]) if len(row) > 19 else _safe_float(row[12])
        try:
            dt     = datetime.strptime(row[1], "%d/%m/%Y")
            monday = dt - timedelta(days=dt.weekday())
            weekly[monday] += pl
        except Exception:
            pass

    if not weekly:
        return

    rows    = []
    cum_pl  = 0.0
    for monday in sorted(weekly.keys()):
        sunday  = monday + timedelta(days=6)
        wpl     = weekly[monday]
        cum_pl += wpl
        ret_pct = wpl / starting_capital * 100

        def _w(v):
            return f"{'₹' if v >= 0 else '-₹'}{abs(v):,.0f}"

        rows.append([
            monday.strftime("%d-%b-%y"),
            sunday.strftime("%d-%b-%y"),
            _w(wpl),
            _w(cum_pl),
            f"{ret_pct:.2f}%",
        ])

    end_row = 2 + len(rows)
    ws.update("A1:E1", [["WEEKLY PERFORMANCE", "", "", "", ""]])
    ws.update("A2:E2", [["Week Start (Mon)", "Week End (Sun)",
                          "Weekly P/L (₹)", "Cumulative P/L (₹)", "Weekly Return %"]])
    ws.update(f"A3:E{end_row}", rows, value_input_option="USER_ENTERED")

    # Apply red/green color to Weekly P/L (col C=2) and Cumulative P/L (col D=3)
    try:
        sheet_id   = ws.id
        sorted_weeks = sorted(weekly.keys())
        color_requests = []
        cum = 0.0
        for i, monday in enumerate(sorted_weeks):
            wpl  = weekly[monday]
            cum += wpl
            row_idx = 2 + i  # 0-based; row3 = index 2
            for col_idx, val in [(2, wpl), (3, cum)]:
                if val >= 0:
                    bg = {"red": 0.714, "green": 0.843, "blue": 0.659}  # light green
                else:
                    bg = {"red": 0.957, "green": 0.698, "blue": 0.698}  # light red
                color_requests.append({
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": row_idx,
                            "endRowIndex": row_idx + 1,
                            "startColumnIndex": col_idx,
                            "endColumnIndex": col_idx + 1,
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "backgroundColor": bg
                            }
                        },
                        "fields": "userEnteredFormat.backgroundColor",
                    }
                })
        if color_requests:
            ws.spreadsheet.batch_update({"requests": color_requests})
    except Exception:
        pass

    # Clear stale rows below current data
    try:
        existing_rows = len(ws.get_all_values())
        if existing_rows > end_row:
            clear_range = f"A{end_row+1}:E{existing_rows}"
            ws.batch_clear([clear_range])
    except Exception:
        pass


def _update_monthly_performance(trade_log, spreadsheet, starting_capital):
    try:
        ws       = spreadsheet.worksheet("Monthly & Weekly P&L Report")
        all_data = trade_log.get_all_values()
    except Exception:
        return

    monthly = defaultdict(float)
    for row in all_data[3:]:
        if not row or not row[0]:
            continue
        if "[OPEN]" in (row[4] if len(row) > 4 else ""):
            continue
        exit_date_str = row[8].strip() if len(row) > 8 else ""
        net_pl_str    = row[19].strip() if len(row) > 19 else ""
        if not exit_date_str or not net_pl_str:
            continue
        try:
            exit_dt    = datetime.strptime(exit_date_str, "%d/%m/%Y")
            month_key  = exit_dt.strftime("%b %Y")
            monthly[month_key] += _safe_float(net_pl_str)
        except Exception:
            pass

    if not monthly:
        return

    sorted_months = sorted(monthly.keys(), key=lambda m: datetime.strptime(m, "%b %Y"))

    def _fmt(v):
        return f"{'₹' if v >= 0 else '-₹'}{abs(v):,.0f}"

    rows    = []
    cum_pl  = 0.0
    for m in sorted_months:
        mpl    = monthly[m]
        cum_pl = round(cum_pl + mpl, 2)
        ret    = mpl / starting_capital * 100
        rows.append([m, _fmt(mpl), _fmt(cum_pl), f"{ret:.2f}%", mpl])

    end_row = 2 + len(rows)
    ws.update(range_name="H1:K1", values=[["MONTHLY PERFORMANCE", "", "", ""]])
    ws.update(range_name="H2:K2", values=[["Month", "Monthly P/L (₹)", "Cumulative P/L (₹)", "Monthly Return %"]])
    ws.update(range_name=f"H3:K{end_row}", values=[[r[0], r[1], r[2], r[3]] for r in rows],
              value_input_option="USER_ENTERED")

    try:
        sheet_id = ws.id
        dark_navy  = {"red": 0.188, "green": 0.231, "blue": 0.310}
        dark_slate = {"red": 0.267, "green": 0.329, "blue": 0.412}
        white      = {"red": 1.0,   "green": 1.0,   "blue": 1.0}
        black      = {"red": 0.0,   "green": 0.0,   "blue": 0.0}
        green_bg   = {"red": 0.714, "green": 0.843, "blue": 0.659}
        red_bg     = {"red": 0.957, "green": 0.698, "blue": 0.698}
        white_bg   = {"red": 1.0,   "green": 1.0,   "blue": 1.0}

        fmt_reqs = [
            {"mergeCells": {
                "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1,
                          "startColumnIndex": 7, "endColumnIndex": 11},
                "mergeType": "MERGE_ALL"
            }},
            {"repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1,
                          "startColumnIndex": 7, "endColumnIndex": 11},
                "cell": {"userEnteredFormat": {
                    "backgroundColor": dark_navy,
                    "textFormat": {"foregroundColor": white, "bold": True, "fontSize": 12},
                    "horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE",
                }},
                "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,verticalAlignment)"
            }},
            {"repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 2,
                          "startColumnIndex": 7, "endColumnIndex": 11},
                "cell": {"userEnteredFormat": {
                    "backgroundColor": dark_slate,
                    "textFormat": {"foregroundColor": white, "bold": True, "fontSize": 10},
                    "horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE",
                }},
                "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,verticalAlignment)"
            }},
        ]

        for idx, row_data in enumerate(rows):
            ri  = 2 + idx
            bg  = green_bg if row_data[4] >= 0 else red_bg
            fmt_reqs.append({"repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": ri, "endRowIndex": ri + 1,
                          "startColumnIndex": 7, "endColumnIndex": 11},
                "cell": {"userEnteredFormat": {
                    "backgroundColor": bg,
                    "textFormat": {"foregroundColor": black, "bold": False, "fontSize": 10},
                    "horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE",
                }},
                "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,verticalAlignment)"
            }})
            # Month name col (H) — white bg, left-aligned
            fmt_reqs.append({"repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": ri, "endRowIndex": ri + 1,
                          "startColumnIndex": 7, "endColumnIndex": 8},
                "cell": {"userEnteredFormat": {
                    "backgroundColor": white_bg,
                    "textFormat": {"foregroundColor": black, "bold": False, "fontSize": 10},
                    "horizontalAlignment": "LEFT", "verticalAlignment": "MIDDLE",
                }},
                "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,verticalAlignment)"
            }})

        ws.spreadsheet.batch_update({"requests": fmt_reqs})
    except Exception:
        pass

    # Clear stale monthly rows below current data
    try:
        existing = ws.get_all_values()
        max_h_row = max((i for i, r in enumerate(existing) if len(r) > 7 and r[7]), default=end_row - 1)
        if max_h_row + 1 > end_row:
            ws.batch_clear([f"H{end_row+1}:K{max_h_row+1}"])
    except Exception:
        pass


# ── Sheet 4 — Drawdown Analysis ───────────────────────────────────────────────

def _update_drawdown_analysis(trade_log, spreadsheet, starting_capital):
    try:
        ws       = spreadsheet.worksheet("Drawdown Analysis")
        all_data = trade_log.get_all_values()
    except Exception:
        return

    rows     = []
    cum_pl   = 0.0
    peak_cap = starting_capital

    for row in all_data[3:]:
        if not row or not row[0]:
            continue
        if "[OPEN]" in (row[4] if len(row) > 4 else ""):
            continue

        pl         = _safe_float(row[12])
        date_str   = row[1] if len(row) > 1 else ""
        instrument = row[3] if len(row) > 3 else ""

        cum_pl  += pl
        capital  = starting_capital + cum_pl
        if capital > peak_cap:
            peak_cap = capital

        dd_amt = capital - peak_cap
        dd_pct = dd_amt / peak_cap * 100 if peak_cap > 0 else 0.0

        def _fmt(v, decimals=2):
            fmt = f":,.{decimals}f"
            return f"₹ {v:{fmt[1:]}}" if v >= 0 else f"-₹ {abs(v):{fmt[1:]}}"

        rows.append([
            len(rows) + 1,
            date_str,
            instrument,
            _fmt(pl),
            _fmt(cum_pl),
            _fmt(capital),
            _fmt(peak_cap),
            _fmt(dd_amt),
            f"{dd_pct:.2f}%",
        ])

    if not rows:
        return

    end_row = 1 + len(rows)
    ws.update(f"A2:I{end_row}", rows, value_input_option="USER_ENTERED")

    # Clear stale rows
    try:
        existing = len(ws.get_all_values())
        if existing > end_row:
            ws.batch_clear([f"A{end_row+1}:I{existing}"])
    except Exception:
        pass


# ── OPEN rows to bottom ───────────────────────────────────────────────────────

def _move_open_rows_to_end(trade_log):
    try:
        all_data = trade_log.get_all_values()
    except Exception:
        return
    if len(all_data) <= 3:
        return

    closed = [r for r in all_data[3:] if "[OPEN]" not in (r[4] if len(r) > 4 else "")]
    open_  = [r for r in all_data[3:] if "[OPEN]"     in (r[4] if len(r) > 4 else "")]
    reordered = closed + open_

    renumbered = []
    for idx, row in enumerate(reordered, start=1):
        m    = list(row)
        m[0] = str(idx)
        renumbered.append(m)

    if not renumbered:
        return

    max_cols = max(len(r) for r in renumbered)
    padded   = [r + [""] * (max_cols - len(r)) for r in renumbered]
    end_col  = chr(ord("A") + max_cols - 1)
    end_row  = 3 + len(padded)

    trade_log.update(f"A4:{end_col}{end_row}", padded, value_input_option="USER_ENTERED")


# ── Manual trade entry ────────────────────────────────────────────────────────

def add_manual_trade(trade, spreadsheet=None):
    """
    Write a single manually entered trade to Trade Log and update all summary sheets.
    Returns dict: added, errors
    """
    if spreadsheet is None:
        spreadsheet = get_sheets_client()

    result = {"added": 0, "errors": []}

    try:
        trade_log = spreadsheet.worksheet("Trade Log")
    except gspread.exceptions.WorksheetNotFound:
        result["errors"].append("'Trade Log' worksheet not found.")
        return result

    _ensure_headers(trade_log)

    # ── Duplicate check ───────────────────────────────────────────────────────
    if trade.get("status") == "CLOSED":
        existing_keys = _get_existing_keys(trade_log)
        if _trade_key(trade) in existing_keys:
            result["errors"].append(
                f"Trade already exists: {trade.get('instrument')} on {trade.get('entry_date')}"
            )
            return result
    elif trade.get("status") == "OPEN":
        if _open_already_exists(trade_log, trade):
            result["errors"].append(
                f"Open position already exists for: {trade.get('instrument')}"
            )
            return result

    # ── Build row ─────────────────────────────────────────────────────────────
    next_sno   = _get_next_sno(trade_log)
    long_short = trade.get("long_short", "")
    if trade.get("status") == "OPEN" and "[OPEN]" not in long_short:
        long_short = f"{long_short} [OPEN]"

    row = [
        next_sno,
        trade.get("entry_date", ""),         # B  Entry Date
        trade.get("segment", ""),            # C  Segment
        trade.get("instrument", ""),         # D  Instrument
        long_short,                          # E  Type
        trade.get("lots", 1),               # F  Lots
        trade.get("lot_size", 1),           # G  Lot Size
        trade.get("entry_price", 0),        # H  Entry Price
        trade.get("exit_date", ""),         # I  Exit Date
        trade.get("exit_price", 0),         # J  Exit Price
        trade.get("pl_points", 0),          # K  P/L (Points)
        trade.get("actual_spot_points", ""),# L  Actual Spot Points
        trade.get("pl_rupees", 0),          # M  P/L (₹)
        "",                                 # N  Drawdown % (recalculated)
        "",                                 # O  Cumulative P/L
        "",                                 # P  Monthly P/L
        "",                                 # Q  Cum Capital
        trade.get("comments", ""),          # R  Comments
        trade.get("total_charges", 0),      # S  Total Charges
        trade.get("net_pl", 0),             # T  Net P/L
        trade.get("duration_display", ""),  # U  Duration
    ]

    trade_log.append_rows([row], value_input_option="USER_ENTERED")
    result["added"] = 1

    # ── Keep OPEN rows at end, then recalculate everything ────────────────────
    _move_open_rows_to_end(trade_log)
    _recalculate_trade_log(trade_log, spreadsheet)

    cap = _read_starting_capital(trade_log)
    _update_parameters_summary(trade_log, spreadsheet, cap)
    _update_weekly_performance(trade_log, spreadsheet, cap)
    _update_monthly_performance(trade_log, spreadsheet, cap)
    _update_drawdown_analysis(trade_log, spreadsheet, cap)

    return result


# ── Header guard ──────────────────────────────────────────────────────────────

def _ensure_headers(trade_log):
    try:
        h = trade_log.row_values(3)
        if len(h) < 21 or "Total Charges" not in h:
            trade_log.update("S3:U3",
                             [["Total Charges", "Net P/L", "Duration"]],
                             value_input_option="USER_ENTERED")
    except Exception:
        pass


# ── Utilities ─────────────────────────────────────────────────────────────────

def _read_starting_capital(trade_log):
    try:
        row1 = trade_log.row_values(1)
        return float(str(row1[1]).replace(",", "").replace("₹", "").strip())
    except Exception:
        return STARTING_CAPITAL


def _safe_float(s):
    try:
        return float(str(s).replace(",", "").replace("₹", "")
                     .replace("(", "-").replace(")", "").strip())
    except (ValueError, TypeError):
        return 0.0
