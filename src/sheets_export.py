"""
Gold -> Google Sheets (one spreadsheet per table).

Tableau Public reads a Google Sheet by asking Google Drive to export the whole
file as Excel, and Drive refuses exports above its size limit (~10 MB). So every
gold table Tableau needs gets its own spreadsheet, and wide tables are trimmed
to the columns the dashboard uses (the full tables stay in Databricks gold).
Each run: full replace, exact tab sizing, user-entered values so types are kept,
chunked writes with retry, then a test export to report each file's Excel size.
"""
import math
import time
import datetime as dt

import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from google.auth.transport.requests import AuthorizedSession

CATALOG = "power_prices"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets",
          "https://www.googleapis.com/auth/drive.readonly"]
CHUNK_ROWS = 10000
CELL_LIMIT = 10_000_000          # Google Sheets cells per spreadsheet
XLSX = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

# Gold table -> its own spreadsheet (ID from the sheet URL)
SHEETS = {
    "dim_zone_display":       "13xvvdYDflLTblPeotp0Pc3QYloWGpHuUhpp8miyuiRY",
    "agg_refresh_info":       "1LV7Tx6pAZzms_CJHVxcYxIbtJ4A_Q_ragcmrWmsAMjY",
    "agg_price_daily":        "19qMZHhjEUaWkEDWAW18RtlRyjwkEuwi8x3zsr-Ojo8w",
    "agg_price_profile":      "1SJpcQbrT7t8HpIemH46U8PijyfQhj9Ya1SSiIzbN8Ps",
    "agg_generation_monthly": "1_g10DHOicO58knqiOL06_MYCQwJWVDJE8i4SiE08ZVQ",
    "agg_border_monthly":     "1NEN2Ae9Mr85gw7Sqib6v6wCY_SjUVEga_Smssx1d2zc",
    "agg_tomorrow_hourly":    "1XgGQEbI2JuzMdoiMru-pVT-VxW05SeKnHqcRzwmrghw",
}

# Columns sent to Tableau for wide tables (others are sent in full)
EXPORT_COLUMNS = {
    "agg_price_daily": [
        "zone_code", "date",
        "avg_price_eur_mwh", "min_price_eur_mwh", "max_price_eur_mwh",
        "peak_price_eur_mwh", "offpeak_price_eur_mwh",
        "solar_window_price_eur_mwh", "evening_peak_price_eur_mwh",
        "negative_hours", "renewable_share",
    ],
}


def _retry(fn, *args, **kwargs):
    """Call a Sheets API function, backing off on rate limits and server errors."""
    for attempt in range(6):
        try:
            return fn(*args, **kwargs)
        except gspread.exceptions.APIError as e:
            code = e.response.status_code
            if code in (429, 500, 502, 503) and attempt < 5:
                time.sleep(5 * 2 ** attempt)
                continue
            raise


def _cell(v):
    """Convert one value to something the Sheets API accepts as user-entered input."""
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except (TypeError, ValueError):
        pass
    if hasattr(v, "item"):                 # numpy scalar -> Python scalar
        v = v.item()
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, dt.datetime):
        return v.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(v, dt.date):
        return v.isoformat()
    if isinstance(v, float) and math.isinf(v):
        return ""
    return v


def _values(pdf):
    rows = [list(pdf.columns)]
    for row in pdf.itertuples(index=False):
        rows.append([_cell(v) for v in row])
    return rows


def _export_size(session, sheet_id):
    """Ask Drive to export the file as Excel, the way Tableau does. Returns a short status string."""
    r = session.get(f"https://www.googleapis.com/drive/v3/files/{sheet_id}/export",
                    params={"mimeType": XLSX}, timeout=300)
    if r.status_code == 200:
        return f"OK, {len(r.content) / 1e6:.1f} MB"
    try:
        reason = r.json()["error"]["errors"][0].get("reason", r.status_code)
    except Exception:
        reason = r.status_code
    return f"REFUSED ({reason})"


def export(spark, sa_info, tables=None, measure=True):
    """Write gold tables to their spreadsheets. Returns [(table, data_rows, cols, cells, export_status)]."""
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    creds = Credentials.from_service_account_info(sa_info, scopes=SCOPES)
    gc = gspread.authorize(creds)
    session = AuthorizedSession(creds)

    summary = []
    for name in tables or list(SHEETS):
        sheet_id = SHEETS[name]
        df = spark.table(f"{CATALOG}.gold.{name}")
        if name in EXPORT_COLUMNS:
            df = df.select(*EXPORT_COLUMNS[name])
        values = _values(df.toPandas())
        n_rows, n_cols = len(values), len(values[0])
        tab_rows = max(n_rows, 2)          # header-only tabs keep one spare row so the header can be frozen

        sh = _retry(gc.open_by_key, sheet_id)
        tabs = _retry(sh.worksheets)
        ws = next((w for w in tabs if w.title == name), None)
        if ws is None:                     # new file: rename its first tab to the table name
            ws = tabs[0]
            _retry(ws.update_title, name)
        for w in tabs:                     # one tab per file
            if w.id != ws.id:
                _retry(sh.del_worksheet, w)

        _retry(ws.clear)
        _retry(ws.resize, rows=tab_rows, cols=n_cols)
        for i in range(0, n_rows, CHUNK_ROWS):
            _retry(ws.update, range_name=f"A{i + 1}", values=values[i:i + CHUNK_ROWS],
                   value_input_option="USER_ENTERED")
        _retry(ws.freeze, rows=1)

        status = _export_size(session, sheet_id) if measure else "not measured"
        summary.append((name, n_rows - 1, n_cols, tab_rows * n_cols, status))
    return summary