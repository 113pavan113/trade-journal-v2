"""
Trade Journal V2 — Streamlit App
Upload a Fyers tradebook CSV → preview trades → sync to Google Sheets.

Deploy: streamlit run app.py  (local)
        push to GitHub → deploy on Streamlit Community Cloud (free)
"""
import streamlit as st
import pandas as pd
from datetime import datetime

from csv_parser   import parse_fyers_csv
from sheets_writer import get_sheets_client, sync_to_sheets

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Trade Journal V2",
    page_icon="📊",
    layout="centered",
)

# ── Minimal custom CSS for mobile-friendliness ────────────────────────────────
st.markdown("""
<style>
    .stApp { max-width: 860px; margin: auto; }
    .metric-box {
        background: #1e1e2e;
        border-radius: 12px;
        padding: 16px 20px;
        margin-bottom: 8px;
    }
    .profit  { color: #4caf50; font-weight: 700; }
    .loss    { color: #f44336; font-weight: 700; }
    .neutral { color: #aaa; }
</style>
""", unsafe_allow_html=True)

# ── Header ────────────────────────────────────────────────────────────────────
st.title("📊 Trade Journal V2")
st.caption("Upload your Fyers tradebook CSV → journal updates automatically.")

st.divider()

# ── Sidebar — Google Sheets status ────────────────────────────────────────────
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
    st.caption("2. Upload CSV below (daily or weekly)")
    st.caption("3. Review the preview")
    st.caption("4. Click **Sync to Sheets**")
    st.divider()
    st.caption("Overnight positions are handled automatically — upload the full week's CSV to pair entry + exit across days.")

# ── File Upload ───────────────────────────────────────────────────────────────
uploaded_file = st.file_uploader(
    "📂 Drop your Fyers tradebook CSV here",
    type=["csv"],
    help="Download from Fyers terminal → Reports → Trade Book → Export CSV",
)

if uploaded_file is None:
    st.info("Upload a Fyers tradebook CSV to get started.")
    st.stop()

# ── Parse ─────────────────────────────────────────────────────────────────────
with st.spinner("Parsing CSV..."):
    try:
        trades = parse_fyers_csv(uploaded_file)
    except Exception as e:
        st.error(f"Failed to parse CSV: {e}")
        st.stop()

if not trades:
    st.warning("No trades found in the uploaded file. Make sure it is a valid Fyers tradebook CSV.")
    st.stop()

# ── Summary metrics ───────────────────────────────────────────────────────────
closed_trades = [t for t in trades if t.get("status") == "CLOSED"]
open_trades   = [t for t in trades if t.get("status") == "OPEN"]
total_pl      = sum(t.get("pl_rupees", 0) for t in closed_trades)
total_net_pl  = sum(t.get("net_pl", 0)    for t in closed_trades)
total_charges = sum(t.get("total_charges", 0) for t in closed_trades)

col1, col2, col3, col4 = st.columns(4)
col1.metric("Trades Found",   len(trades))
col2.metric("Closed",         len(closed_trades))
col3.metric("Open Positions", len(open_trades))
pl_color = "normal" if total_net_pl == 0 else ("normal" if total_net_pl > 0 else "inverse")
col4.metric("Net P/L (closed)", f"₹{total_net_pl:,.2f}",
            delta=f"charges ₹{total_charges:,.2f}", delta_color="inverse")

st.divider()

# ── Trades preview table ──────────────────────────────────────────────────────
st.subheader("📋 Trade Preview")

preview_rows = []
for t in trades:
    status_badge = "🟢 OPEN" if t.get("status") == "OPEN" else "✅ Closed"
    net_pl_val   = t.get("net_pl", 0)
    pl_str       = f"₹{net_pl_val:+,.2f}"

    preview_rows.append({
        "Status":       status_badge,
        "Entry Date":   t.get("entry_date", ""),
        "Instrument":   t.get("instrument", ""),
        "Direction":    t.get("long_short", ""),
        "Lots":         t.get("lots", 1),
        "Entry ₹":      t.get("entry_price", 0),
        "Exit ₹":       t.get("exit_price", "") if t.get("status") == "CLOSED" else "—",
        "P/L ₹":        t.get("pl_rupees", 0),
        "Charges ₹":    t.get("total_charges", 0),
        "Net P/L ₹":    net_pl_val,
        "Duration":     t.get("duration_display", ""),
    })

df = pd.DataFrame(preview_rows)
st.dataframe(
    df,
    use_container_width=True,
    hide_index=True,
    column_config={
        "P/L ₹":     st.column_config.NumberColumn(format="₹%.2f"),
        "Net P/L ₹": st.column_config.NumberColumn(format="₹%.2f"),
        "Charges ₹": st.column_config.NumberColumn(format="₹%.2f"),
        "Entry ₹":   st.column_config.NumberColumn(format="%.2f"),
        "Exit ₹":    st.column_config.TextColumn(),
    }
)

# ── Charge breakdown expander ─────────────────────────────────────────────────
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
        charge_df = pd.DataFrame(
            [{"Component": k, "Amount (₹)": round(v, 2)} for k, v in charge_data.items()]
        )
        st.dataframe(charge_df, use_container_width=True, hide_index=True)
    else:
        st.info("No closed trades to show charges for.")

st.divider()

# ── Sync button ───────────────────────────────────────────────────────────────
st.subheader("🔄 Sync to Google Sheets")

if open_trades:
    st.info(
        f"ℹ️ **{len(open_trades)} open position(s)** found. "
        "They will be written as `[OPEN]` rows. Upload next week's CSV (with the exit) "
        "and they will be automatically closed and updated."
    )

col_sync, col_gap = st.columns([1, 2])
with col_sync:
    sync_clicked = st.button("🚀 Sync to Sheets", type="primary", use_container_width=True)

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
            st.caption("Check that GOOGLE_SHEET_ID and GOOGLE_CREDS_JSON are set correctly in your Streamlit secrets.")

# ── Footer ────────────────────────────────────────────────────────────────────
st.divider()
st.caption(
    "Trade Journal V2 • Fyers tradebook → Google Sheets • "
    f"STT rates: Options 0.15%, Futures 0.05% (from Apr 1 2026)"
)
