"""
Gold -> Google Sheets.

Writes each gold table Tableau needs to its own tab of the serving
spreadsheet: full replace, exact tab sizing (empty cells count toward
the 10M-cell limit), user-entered values so types are kept, chunked
writes with retry on rate limits.
"""
import math
import time
import datetime as dt

import gspread
import pandas as pd
from google.oauth2.service_account import Credentials

CATALOG = "power_prices"
SHEET_ID = "1bam2ByYUsriJbkfJp5o3SQXRBtwOWSNNFQ1lCziT1yc"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
CHUNK_ROWS = 10000
CELL_LIMIT = 10_000_000

TABLES = [
    "dim_zone_display",
    "agg_refresh_info",
    "agg_price_daily",
    "agg_price_profile",
    "agg_generation_monthly",
    "agg_border_monthly",
    "agg_tomorrow_hourly",
]
OBSOLETE_TABS = ["_connection_test", "Sheet1"]


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


def export(spark, sa_info, sheet_id=SHEET_ID, tables=None):
    """Write gold tables to their tabs. Returns [(table, data_rows, cols, cells)]."""
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    creds = Credentials.from_service_account_info(sa_info, scopes=SCOPES)
    sh = _retry(gspread.authorize(creds).open_by_key, sheet_id)
    existing = {ws.title: ws for ws in _retry(sh.worksheets)}

    summary = []
    for name in tables or TABLES:
        values = _values(spark.table(f"{CATALOG}.gold.{name}").toPandas())
        n_rows, n_cols = len(values), len(values[0])
        tab_rows = max(n_rows, 2)          # header-only tabs keep one spare row so the header can be frozen

        ws = existing.get(name)
        if ws is None:
            ws = _retry(sh.add_worksheet, title=name, rows=tab_rows, cols=n_cols)
        _retry(ws.clear)
        _retry(ws.resize, rows=tab_rows, cols=n_cols)
        for i in range(0, n_rows, CHUNK_ROWS):
            _retry(ws.update, range_name=f"A{i + 1}", values=values[i:i + CHUNK_ROWS],
                   value_input_option="USER_ENTERED")
        _retry(ws.freeze, rows=1)
        summary.append((name, n_rows - 1, n_cols, tab_rows * n_cols))

    # Remove tabs that are no longer used (a spreadsheet must keep at least one tab)
    for title in OBSOLETE_TABS:
        tabs = {w.title: w for w in _retry(sh.worksheets)}
        if title in tabs and len(tabs) > 1:
            _retry(sh.del_worksheet, tabs[title])

    return summary