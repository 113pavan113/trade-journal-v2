"""
Trade Journal V2 — Streamlit App
Upload a Fyers tradebook CSV → preview trades → sync to Google Sheets.
OR manually add/log a position directly.

Deploy: streamlit run app.py  (local)
        push to GitHub → deploy on Streamlit Community Cloud (free)
"""
import streamlit as st
import pandas as pd
from datetime import datetime, date

from csv_parser    import parse_fyers_csv, _fmt_duration, LOT_SIZES
from sheets_writer import get_sheets_client, sync_to_sheets, add_manual_trade
from charge_calculator import calculate_charges

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Trade Journal V2",
    page_icon="📊",
    layout="centered",
)

st.markdown("""
<style>
    .stApp { max-width: 860px; margin: auto; }
</style>
""", unsafe_allow_html=True)

# ── Header ────────────────────────────────────────────────────────────────────
st.title("📊 Trade Journal V2")
st.caption("Upload your Fyers tradebook CSV → journal updates automatically.")
st.divider()

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("⚙️ Status")
    if st.button("🔌 Test Sheets Connection", use_container_width=True):
        with st.spinner("Connecting..."):
            try:
                ss = get_sheets_client()
                st.success(f"Connected: **{ss.title}**")
                tabs = [ws.title for ws in ss.worksheets()]
                st.caption("Tabs: " + ", ".join(tabs))
            except Exception as e:
                st.error(f"Connection failed: {e}")
    st.divider()
    st.caption("**How to use:**")
    st.caption("1. Download tradebook from Fyers terminal")
    st.caption("2. Upload CSV (daily or weekly)")
    st.caption("3. Review the preview")
    st.caption("4. Click **Sync to Sheets**")
    st.divider()
    st.caption("Or use **Manual Entry** tab to log a position directly.")
    st.divider()
    st.caption("Overnight positions are handled automatically — upload the full week's CSV to pair entry + exit across days.")

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab1, tab2 = st.tabs(["📂 CSV Upload & Sync", "✏️ Manual Entry"])


# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — CSV Upload
# ══════════════════════════════════════════════════════════════════════════════
with tab1:

    uploaded_file = st.file_uploader(
        "📂 Drop your Fyers tradebook CSV here",
        type=["csv"],
        help="Download from Fyers terminal → Reports → Trade Book → Export CSV",
    )

    if uploaded_file is None:
        st.info("Upload a Fyers tradebook CSV to get started.")

    else:
        # ── Parse ─────────────────────────────────────────────────────────────
        trades = None
        with st.spinner("Parsing CSV..."):
            try:
                trades = parse_fyers_csv(uploaded_file)
            except Exception as e:
                st.error(f"Failed to parse CSV: {e}")

        if trades is not None and len(trades) == 0:
            st.warning("No trades found. Make sure it is a valid Fyers tradebook CSV.")
            trades = None

        if trades:
            # ── Summary metrics ───────────────────────────────────────────────
            closed_trades = [t for t in trades if t.get("status") == "CLOSED"]
            open_trades   = [t for t in trades if t.get("status") == "OPEN"]
            total_net_pl  = sum(t.get("net_pl", 0)        for t in closed_trades)
            total_charges = sum(t.get("total_charges", 0) for t in closed_trades)
            total_points  = sum(t.get("pl_points", 0)     for t in closed_trades)

            col1, col2, col3, col4, col5 = st.columns(5)
            col1.metric("Trades Found",       len(trades))
            col2.metric("Closed",             len(closed_trades))
            col3.metric("Open Positions",     len(open_trades))
            col4.metric("Total Pts (closed)", f"{total_points:+.2f}")
            col5.metric("Net P/L (closed)",   f"₹{total_net_pl:,.2f}",
                        delta=f"charges ₹{total_charges:,.2f}", delta_color="inverse")

            st.divider()

            # ── Trades preview table ──────────────────────────────────────────
            st.subheader("📋 Trade Preview")
            preview_rows = []
            for t in trades:
                preview_rows.append({
                    "Status":    "🟢 OPEN" if t.get("status") == "OPEN" else "✅ Closed",
                    "Entry Date": t.get("entry_date", ""),
                    "Instrument": t.get("instrument", ""),
                    "Direction":  t.get("long_short", ""),
                    "Lots":       t.get("lots", 1),
                    "Entry ₹":    t.get("entry_price", 0),
                    "Exit ₹":     t.get("exit_price", "") if t.get("status") == "CLOSED" else "—",
                    "P/L (Pts)":  t.get("pl_points", 0),
                    "P/L ₹":      t.get("pl_rupees", 0),
                    "Charges ₹":  t.get("total_charges", 0),
                    "Net P/L ₹":  t.get("net_pl", 0),
                    "Duration":   t.get("duration_display", ""),
                })

            st.dataframe(
                pd.DataFrame(preview_rows),
                use_container_width=True,
                hide_index=True,
                column_config={
                    "P/L (Pts)": st.column_config.NumberColumn(format="%.2f"),
                    "P/L ₹":     st.column_config.NumberColumn(format="₹%.2f"),
                    "Net P/L ₹": st.column_config.NumberColumn(format="₹%.2f"),
                    "Charges ₹": st.column_config.NumberColumn(format="₹%.2f"),
                    "Entry ₹":   st.column_config.NumberColumn(format="%.2f"),
                    "Exit ₹":    st.column_config.TextColumn(),
                }
            )

            # ── Charge breakdown ──────────────────────────────────────────────
            with st.expander("🧾 Charge Breakdown (closed trades only)"):
                if closed_trades:
                    charge_data = {
                        "Brokerage":    sum(t.get("brokerage", 0)    for t in closed_trades),
                        "STT":          sum(t.get("stt", 0)          for t in closed_trades),
                        "Exchange Txn": sum(t.get("exchange_txn", 0) for t in closed_trades),
                        "SEBI Fees":    sum(t.get("sebi_fees", 0)    for t in closed_trades),
                        "GST":          sum(t.get("gst", 0)          for t in closed_trades),
                        "Stamp Duty":   sum(t.get("stamp_duty", 0)   for t in closed_trades),
                        "Total":        total_charges,
                    }
                    st.dataframe(
                        pd.DataFrame([{"Component": k, "Amount (₹)": round(v, 2)}
                                      for k, v in charge_data.items()]),
                        use_container_width=True, hide_index=True,
                    )
                else:
                    st.info("No closed trades to show charges for.")

            st.divider()

            # ── Sync button ───────────────────────────────────────────────────
            st.subheader("🔄 Sync to Google Sheets")

            if open_trades:
                st.info(
                    f"ℹ️ **{len(open_trades)} open position(s)** found. "
                    "They will be written as `[OPEN]` rows. Upload next week's CSV "
                    "(with the exit) and they will be automatically closed and updated."
                )

            col_sync, col_gap = st.columns([1, 2])
            with col_sync:
                sync_clicked = st.button("🚀 Sync to Sheets", type="primary",
                                         use_container_width=True)

            if sync_clicked:
                with st.spinner("Syncing to Google Sheets..."):
                    try:
                        ss     = get_sheets_client()
                        result = sync_to_sheets(trades, ss)
                        if result.get("errors"):
                            for err in result["errors"]:
                                st.error(err)
                        else:
                            st.success(
                                f"✅ Done!  "
                                f"**{result['added']}** new rows added  •  "
                                f"**{result['open_updated']}** open rows closed  •  "
                                f"**{result['skipped']}** duplicates skipped"
                            )
                            st.balloons()
                    except Exception as e:
                        st.error(f"Sync failed: {e}")
                        st.caption("Check GOOGLE_SHEET_ID and GOOGLE_CREDS_JSON in your Streamlit secrets.")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — Manual Entry
# ══════════════════════════════════════════════════════════════════════════════
with tab2:
    st.subheader("✏️ Add Position Manually")
    st.caption(
        "Log a trade without uploading CSV. "
        "If values are wrong, just upload the tradebook later — "
        "the sync will correct entry price, lots, and P&L automatically."
    )

    _UNDERLYINGS = ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX", "BANKEX", "Other"]
    _DIRECTIONS  = [
        "Long CE", "Short CE",
        "Long PE", "Short PE",
        "Synthetic Long", "Synthetic Short",
        "Credit Put Spread", "Credit Call Spread",
        "Debit Put Spread",  "Debit Call Spread",
        "Long Futures", "Short Futures",
        "Long", "Short",
    ]
    _LONG_DIRECTIONS = {
        "Long CE", "Long PE", "Synthetic Long",
        "Debit Put Spread", "Debit Call Spread",
        "Long Futures", "Long",
    }

    with st.form("manual_trade_form", clear_on_submit=True):
        r1c1, r1c2, r1c3 = st.columns(3)

        with r1c1:
            underlying  = st.selectbox("Underlying", _UNDERLYINGS)
            entry_date  = st.date_input("Entry Date", value=date.today())
            segment     = st.selectbox("Segment", ["Index Options", "Index Futures", "Equity"])

        with r1c2:
            instrument_text = st.text_input(
                "Instrument",
                placeholder="e.g. NIFTY 25000 CE",
                help="What to display in the sheet — keep it consistent with existing entries."
            )
            direction = st.selectbox("Direction / Type", _DIRECTIONS)
            lots      = st.number_input("Lots", min_value=1, value=1, step=1)

        with r1c3:
            default_lot_size = LOT_SIZES.get(underlying, 1) if underlying != "Other" else 1
            lot_size    = st.number_input("Lot Size", min_value=1,
                                          value=default_lot_size, step=1)
            entry_price = st.number_input("Entry Price ₹", min_value=0.0,
                                          value=0.0, step=0.05, format="%.2f")
            status      = st.radio("Status", ["Open", "Closed"], horizontal=True)

        st.markdown("---")
        st.caption("Fill exit fields only if **Status = Closed**")
        r2c1, r2c2, r2c3 = st.columns(3)
        with r2c1:
            exit_date  = st.date_input("Exit Date", value=date.today())
        with r2c2:
            exit_price = st.number_input("Exit Price ₹", min_value=0.0,
                                         value=0.0, step=0.05, format="%.2f")
        with r2c3:
            comments = st.text_input("Comments (optional)", "")

        submitted = st.form_submit_button(
            "➕ Add to Google Sheets", type="primary", use_container_width=True
        )

    if submitted:
        errors = []
        if not instrument_text.strip():
            errors.append("Instrument name is required.")
        if entry_price <= 0:
            errors.append("Entry price must be greater than 0.")
        if status == "Closed" and exit_price <= 0:
            errors.append("Exit price must be greater than 0 for a closed trade.")

        if errors:
            for e in errors:
                st.error(e)
        else:
            is_long   = direction in _LONG_DIRECTIONS
            is_closed = status == "Closed"
            total_qty = lots * lot_size

            seg_map  = {"Index Options": "OPTIONS", "Index Futures": "FUTURES", "Equity": "EQUITY"}
            seg_code = seg_map.get(segment, "OPTIONS")

            if is_long:
                buy_value  = total_qty * entry_price
                sell_value = total_qty * exit_price if is_closed else 0.0
            else:
                sell_value = total_qty * entry_price
                buy_value  = total_qty * exit_price if is_closed else 0.0

            pl_rupees  = round(sell_value - buy_value, 2)
            pl_points  = round(pl_rupees / total_qty, 2) if total_qty else 0.0
            num_orders = 2 if is_closed else 1

            charges = calculate_charges(buy_value, sell_value, seg_code,
                                        num_orders=num_orders,
                                        trade_date=entry_date.strftime("%Y-%m-%d"))
            net_pl = round(pl_rupees - charges["total_charges"], 2)

            entry_date_str = entry_date.strftime("%d/%m/%Y")
            exit_date_str  = exit_date.strftime("%d/%m/%Y") if is_closed else ""

            duration_display = ""
            if is_closed:
                entry_dt_full = datetime.combine(entry_date, datetime.min.time())
                exit_dt_full  = datetime.combine(exit_date,  datetime.min.time())
                duration_display = _fmt_duration(entry_dt_full, exit_dt_full)

            trade = {
                "entry_date":         entry_date_str,
                "segment":            segment,
                "instrument":         instrument_text.strip(),
                "long_short":         direction,
                "status":             "CLOSED" if is_closed else "OPEN",
                "lots":               lots,
                "lot_size":           lot_size,
                "entry_price":        entry_price,
                "exit_date":          exit_date_str,
                "exit_price":         exit_price if is_closed else 0.0,
                "pl_points":          pl_points  if is_closed else 0.0,
                "actual_spot_points": "",
                "pl_rupees":          pl_rupees  if is_closed else 0.0,
                "total_charges":      charges["total_charges"] if is_closed else 0.0,
                "net_pl":             net_pl     if is_closed else 0.0,
                "duration_display":   duration_display,
                "comments":           comments,
            }

            st.markdown("**Preview:**")
            st.dataframe(pd.DataFrame([{
                "Instrument": trade["instrument"],
                "Direction":  trade["long_short"],
                "Status":     trade["status"],
                "Entry Date": trade["entry_date"],
                "Lots":       trade["lots"],
                "Entry ₹":    trade["entry_price"],
                "Exit ₹":     trade["exit_price"]     if is_closed else "—",
                "P/L ₹":      trade["pl_rupees"]      if is_closed else "—",
                "Net P/L ₹":  trade["net_pl"]         if is_closed else "—",
                "Charges ₹":  trade["total_charges"],
            }]), use_container_width=True, hide_index=True)

            with st.spinner("Adding to Google Sheets..."):
                try:
                    ss     = get_sheets_client()
                    result = add_manual_trade(trade, ss)
                    if result.get("errors"):
                        for err in result["errors"]:
                            st.error(err)
                    else:
                        label = "closed" if is_closed else "open"
                        st.success(
                            f"✅ Added {label} trade: **{instrument_text}** — "
                            f"{direction}, {lots} lot(s) @ ₹{entry_price:.2f}"
                        )
                        if is_closed:
                            st.caption(
                                f"Net P/L: ₹{net_pl:+,.2f}  |  "
                                f"Charges: ₹{charges['total_charges']:.2f}"
                            )
                        st.balloons()
                except Exception as e:
                    st.error(f"Failed to add trade: {e}")
                    st.caption("Check your Streamlit secrets (GOOGLE_SHEET_ID, GOOGLE_CREDS_JSON).")

# ── Footer ────────────────────────────────────────────────────────────────────
st.divider()
st.caption(
    "Trade Journal V2 • Fyers tradebook → Google Sheets • "
    "STT rates: Options 0.15%, Futures 0.05% (from Apr 1 2026)"
)
