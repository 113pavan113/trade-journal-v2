"""
Google Sheets Writer — V2
Writes processed trades from the CSV parser to the same Google Sheets template
used by V1 (Trade Log, Parameters Summary, Monthly & Weekly P&L, Drawdown).

Column map (Trade Log):
  A  S.No          B  Entry Date     C  Segment        D  Instrument
  E  Long/Short    F  Lots           G  Lot Size        H  Entry Price
  I  Exit Date     J  Exit Price     K  P/L (Points)    L  Actual Spot Points
  M  P/L (₹)       N  Drawdown %     O  Cumulative P/L  P  Monthly P/L
  Q  Cum Capital   R  Comments       S  Total Charges   T  Net P/L
  U  Duration
"""

import os
import re
import json
from datetime import datetime
from collections import defaultdict

import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

load_dotenv()

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

STARTING_CAPITAL = 498_000   # fallback if not set in sheet B1


# ── Connection ────────────────────────────────────────────────────────────────

def get_sheets_client():
    """Authenticate with Google Sheets and return the spreadsheet object."""
    sheet_id  = os.getenv("GOOGLE_SHEET_ID", "")
    creds_env = os.getenv("GOOGLE_CREDS_JSON", "")

    if creds_env:
        clean = creds_env.strip().strip("'")
        creds_dict = json.loads(clean)
        creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    else:
        creds_path = os.getenv("GOOGLE_CREDS_PATH", "google_creds.json")
        if not os.path.isabs(creds_path):
            creds_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), creds_path)
        creds = Credentials.from_service_account_file(creds_path, scopes=SCOPES)

    client      = gspread.authorize(creds)
    spreadsheet = client.open_by_key(sheet_id)
    return spreadsheet


# ── Main entry point ──────────────────────────────────────────────────────────

def sync_to_sheets(trades, spreadsheet=None):
    """
    Sync a list of trade dicts (from csv_parser) to Google Sheets.

    Returns:
        dict with keys: added, skipped, open_updated, errors
    """
    if spreadsheet is None:
        spreadsheet = get_sheets_client()

    result = {"added": 0, "skipped": 0, "open_updated": 0, "errors": []}

    try:
        trade_log = spreadsheet.worksheet("Trade Log")
    except gspread.exceptions.WorksheetNotFound:
        result["errors"].append("'Trade Log' worksheet not found in the spreadsheet.")
        return result

    _ensure_headers(trade_log)

    # ── Update any existing OPEN rows that are now closed ─────────────────────
    open_updated, still_open = _update_open_rows(trade_log, trades)
    result["open_updated"] = open_updated

    # Remove trades that were used to update open rows
    closed_keys   = {t["_update_key"] for t in still_open}
    trades_to_add = [t for t in trades if _trade_key(t) not in closed_keys]

    # ── Append genuinely new trades ───────────────────────────────────────────
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

        row = [
            next_sno + len(rows_to_add),    # A: S.No
            trade.get("entry_date", ""),     # B: Entry Date
            trade.get("segment", ""),        # C: Segment
            trade.get("instrument", ""),     # D: Instrument
            long_short,                      # E: Long/Short
            trade.get("lots", 1),            # F: Lots
            trade.get("lot_size", 1),        # G: Lot Size
            trade.get("entry_price", 0),     # H: Entry Price
            trade.get("exit_date", ""),      # I: Exit Date
            trade.get("exit_price", 0),      # J: Exit Price
            trade.get("pl_points", 0),       # K: P/L (Points)
            trade.get("actual_spot_points", ""),  # L: Actual Spot Points
            trade.get("pl_rupees", 0),       # M: P/L (₹)
            "",                              # N: Drawdown % (calculated below)
            "",                              # O: Cumulative P/L (calculated)
            "",                              # P: Monthly P/L (calculated)
            "",                              # Q: Cum Capital (calculated)
            "",                              # R: Comments (manual)
            trade.get("total_charges", 0),   # S: Total Charges
            trade.get("net_pl", 0),          # T: Net P/L
            trade.get("duration_display", ""),  # U: Duration
        ]
        rows_to_add.append(row)

    if rows_to_add:
        trade_log.append_rows(rows_to_add, value_input_option="USER_ENTERED")
        result["added"] = len(rows_to_add)

    # ── Recalculate derived columns & reorder ─────────────────────────────────
    if result["added"] or result["open_updated"]:
        _move_open_rows_to_end(trade_log)
        _recalculate_columns(trade_log, spreadsheet)

    return result


# ── Duplicate detection helpers ───────────────────────────────────────────────

def _trade_key(trade):
    instr = trade.get("instrument", "")
    instr = instr.replace("Synthetic Long", "Synthetic").replace("Synthetic Short", "Synthetic")
    return f"{trade.get('entry_date', '')}_{instr}"


def _get_existing_keys(sheet):
    """Return set of (entry_date + instrument) composite keys already in the sheet."""
    try:
        data = sheet.get_all_values()
        keys = set()
        for row in data[3:]:
            if len(row) >= 4 and row[0]:
                instr = row[3]
                instr = instr.replace("Synthetic Long", "Synthetic").replace("Synthetic Short", "Synthetic")
                keys.add(f"{row[1]}_{instr}")
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
    """Return True if an [OPEN] row for the same instrument exists."""
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
    """Fuzzy match — ignores Long/Short suffix."""
    def norm(s):
        return re.sub(r"\s+(Long|Short|OPEN|\[OPEN\])", "", s, flags=re.IGNORECASE).strip().upper()
    return norm(a) == norm(b)


# ── Open-row updater ──────────────────────────────────────────────────────────

def _update_open_rows(trade_log, trades):
    """
    For any [OPEN] row in the sheet that now has a matching CLOSED trade in
    the incoming batch, overwrite the row with the final closed values.

    Returns (count_updated, [trades_used_as_updates])
    """
    try:
        all_data = trade_log.get_all_values()
    except Exception:
        return 0, []

    updated_count = 0
    used_trades   = []

    for row_idx, row in enumerate(all_data[3:], start=4):
        if not row or not row[0]:
            continue
        long_short = row[4] if len(row) > 4 else ""
        if "[OPEN]" not in long_short:
            continue

        existing_instr = row[3] if len(row) > 3 else ""
        # Find a matching CLOSED incoming trade
        match = next(
            (t for t in trades
             if t.get("status") == "CLOSED"
             and _instruments_match(t.get("instrument", ""), existing_instr)),
            None
        )
        if not match:
            continue

        new_long_short = match.get("long_short", long_short.replace(" [OPEN]", ""))
        updated_row = list(row)  # copy
        # Patch the columns that change on close
        if len(updated_row) < 21:
            updated_row += [""] * (21 - len(updated_row))
        updated_row[4]  = new_long_short                         # E
        updated_row[8]  = match.get("exit_date", "")            # I
        updated_row[9]  = match.get("exit_price", 0)            # J
        updated_row[10] = match.get("pl_points", 0)             # K
        updated_row[12] = match.get("pl_rupees", 0)             # M
        updated_row[18] = match.get("total_charges", 0)         # S
        updated_row[19] = match.get("net_pl", 0)                # T
        updated_row[20] = match.get("duration_display", "")     # U

        trade_log.update(f"A{row_idx}:U{row_idx}",
                         [updated_row[:21]], value_input_option="USER_ENTERED")
        match["_update_key"] = _trade_key(match)
        used_trades.append(match)
        updated_count += 1

    return updated_count, used_trades


# ── Derived column recalculation ──────────────────────────────────────────────

def _recalculate_columns(trade_log, spreadsheet):
    """
    Recalculate columns N (Drawdown %), O (Cumulative P/L),
    P (Monthly P/L), Q (Cumulative Capital) for ALL rows.
    Reads starting capital from cell B1 of Trade Log.
    """
    try:
        all_data = trade_log.get_all_values()
    except Exception:
        return

    # Starting capital from B1
    starting_capital = STARTING_CAPITAL
    try:
        cap_str = all_data[0][1] if len(all_data[0]) > 1 else str(STARTING_CAPITAL)
        starting_capital = float(str(cap_str).replace(",", "").replace("₹", "").strip())
    except Exception:
        pass

    cumulative_pl = 0.0
    peak_capital  = starting_capital
    monthly_pls   = defaultdict(float)
    batch         = []

    for row_idx, row in enumerate(all_data):
        if row_idx < 3:
            continue
        if not row or not row[0] or not str(row[0]).strip():
            continue

        actual_row = row_idx + 1
        pl_rupees  = _safe_float(row[12]) if len(row) > 12 else 0.0
        entry_date = row[1] if len(row) > 1 else ""

        # Skip OPEN rows from cumulative calculations (unrealized)
        long_short = row[4] if len(row) > 4 else ""
        if "[OPEN]" in long_short:
            continue

        cumulative_pl += pl_rupees
        cum_capital    = starting_capital + cumulative_pl
        is_new_high    = cum_capital > peak_capital
        if is_new_high:
            peak_capital = cum_capital

        drawdown_pct = ((peak_capital - cum_capital) / peak_capital * 100) if peak_capital > 0 else 0.0

        month_key = ""
        try:
            parsed    = datetime.strptime(entry_date, "%d/%m/%Y")
            month_key = parsed.strftime("%m/%Y")
        except Exception:
            pass

        if month_key:
            monthly_pls[month_key] += pl_rupees
        monthly_pl = monthly_pls.get(month_key, 0.0)

        batch.extend([
            {"range": f"N{actual_row}", "values": [[f"{round(drawdown_pct, 2)}%"]]},
            {"range": f"O{actual_row}", "values": [[round(cumulative_pl, 2)]]},
            {"range": f"P{actual_row}", "values": [[round(monthly_pl, 2)]]},
            {"range": f"Q{actual_row}", "values": [[round(cum_capital, 2)]]},
        ])

    if batch:
        trade_log.batch_update(batch, value_input_option="USER_ENTERED")


# ── OPEN-rows-to-end reorder ──────────────────────────────────────────────────

def _move_open_rows_to_end(trade_log):
    """Keep [OPEN] rows at the bottom and renumber S.No sequentially."""
    try:
        all_data = trade_log.get_all_values()
    except Exception:
        return

    if len(all_data) <= 3:
        return

    data_rows    = all_data[3:]
    closed_rows  = [r for r in data_rows if "[OPEN]" not in (r[4] if len(r) > 4 else "")]
    open_rows    = [r for r in data_rows if "[OPEN]"     in (r[4] if len(r) > 4 else "")]
    reordered    = closed_rows + open_rows

    # Renumber
    renumbered = []
    for idx, row in enumerate(reordered, start=1):
        mutable    = list(row)
        mutable[0] = str(idx)
        renumbered.append(mutable)

    if not renumbered:
        return

    max_cols = max(len(r) for r in renumbered)
    padded   = [r + [""] * (max_cols - len(r)) for r in renumbered]
    end_col  = chr(ord("A") + max_cols - 1)
    end_row  = 3 + len(padded)

    trade_log.update(f"A4:{end_col}{end_row}", padded, value_input_option="USER_ENTERED")


# ── Header guard ──────────────────────────────────────────────────────────────

def _ensure_headers(trade_log):
    """Make sure the extended columns S-U have headers in row 3."""
    try:
        header_row = trade_log.row_values(3)
        if len(header_row) < 21 or "Total Charges" not in header_row:
            trade_log.update("S3:U3",
                             [["Total Charges", "Net P/L", "Duration"]],
                             value_input_option="USER_ENTERED")
    except Exception:
        pass


# ── Utilities ─────────────────────────────────────────────────────────────────

def _safe_float(s):
    try:
        return float(str(s).replace(",", "").replace("₹", "").strip())
    except (ValueError, TypeError):
        return 0.0
