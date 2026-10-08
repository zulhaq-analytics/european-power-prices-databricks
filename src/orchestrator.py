"""
Ingestion orchestrator.

Builds the list of series (zone datasets and directional border flows),
splits each into year-sized chunks from its watermark, calls the ENTSO-E
API, advances watermarks on success, checkpoints progress, then loads
new raw files into silver.
"""
import datetime as dt
from collections import Counter

from pyspark.sql import functions as F

from entsoe_client import EntsoeClient
import silver_loader

CATALOG = "power_prices"
WATERMARK = f"{CATALOG}.ops.load_watermark"
UTC = dt.timezone.utc

HISTORY_START = dt.datetime(2018, 12, 31, 23, 0, tzinfo=UTC)   # 2019-01-01 00:00 CET
LOOKBACK = dt.timedelta(days=3)                                 # daily mode re-fetch window
CHECKPOINT_EVERY = 50                                           # calls between checkpoints

ZONE_DATASETS = ["day_ahead_price", "actual_load", "load_forecast", "generation_actual", "installed_capacity"]
FLOW_DATASET = "physical_flow"
ALL_DATASETS = ZONE_DATASETS + [FLOW_DATASET]
FORWARD_DATASETS = {"day_ahead_price", "load_forecast"}         # published ahead of delivery


# ---------------------------------------------------------------
# Watermarks
# ---------------------------------------------------------------
def ensure_watermark_table(spark):
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {WATERMARK} (
        dataset STRING NOT NULL COMMENT 'Raw dataset name, e.g. day_ahead_price',
        series_key STRING NOT NULL COMMENT 'Zone code, or FROM__TO for flows',
        loaded_through TIMESTAMP COMMENT 'UTC time the series is loaded up to (exclusive)',
        updated_at TIMESTAMP COMMENT 'When the watermark last moved')
        COMMENT 'Load progress per series; drives backfill resume and daily increments'""")


def read_watermarks(spark):
    return {(r.dataset, r.series_key): r.loaded_through.replace(tzinfo=UTC)
            for r in spark.table(WATERMARK).collect() if r.loaded_through is not None}


def save_watermarks(spark, new_wm):
    if not new_wm:
        return
    now = dt.datetime.now(UTC)
    df = spark.createDataFrame(
        [(d, k, ts, now) for (d, k), ts in new_wm.items()],
        "dataset STRING, series_key STRING, loaded_through TIMESTAMP, updated_at TIMESTAMP")
    df.createOrReplaceTempView("_wm_src")
    spark.sql(f"""MERGE INTO {WATERMARK} t USING _wm_src s
                  ON t.dataset = s.dataset AND t.series_key = s.series_key
                  WHEN MATCHED AND s.loaded_through > t.loaded_through THEN UPDATE SET *
                  WHEN NOT MATCHED THEN INSERT *""")


# ---------------------------------------------------------------
# Planning
# ---------------------------------------------------------------
def build_series(spark, datasets, zones=None):
    """[(dataset, series_key, eic, eic_to)] for active zones and in-scope borders."""
    eic = {r.zone_code: r.eic_code for r in
           spark.table(f"{CATALOG}.gold.dim_bidding_zone").filter("is_active")
                .select("zone_code", "eic_code").collect()}
    if zones:
        eic = {z: e for z, e in eic.items() if z in zones}
    series = []
    for d in datasets:
        if d == FLOW_DATASET:
            for b in spark.table(f"{CATALOG}.gold.dim_border").orderBy("border_key").collect():
                for frm, to in [(b.zone_a, b.zone_b), (b.zone_b, b.zone_a)]:
                    if frm in eic and to in eic:
                        series.append((d, f"{frm}__{to}", eic[frm], eic[to]))
        else:
            for z in sorted(eic):
                series.append((d, z, eic[z], None))
    return series


def _year_boundary(year):
    """Midnight CET on 1 January of `year`, expressed in UTC."""
    return dt.datetime(year - 1, 12, 31, 23, 0, tzinfo=UTC)


def _chunks(start, end):
    cur = start
    while cur < end:
        y = cur.year + 1
        while _year_boundary(y) <= cur:
            y += 1
        nxt = min(_year_boundary(y), end)
        yield cur, nxt
        cur = nxt


def _target_end(dataset, now):
    if dataset in FORWARD_DATASETS:
        return now.replace(hour=0, minute=0, second=0, microsecond=0) + dt.timedelta(days=2)
    return now.replace(minute=0, second=0, microsecond=0)


def _start(mode, wm, now):
    if wm is None:
        return HISTORY_START
    if mode == "daily":
        return max(HISTORY_START, min(wm, now - LOOKBACK))
    return wm


def plan(spark, mode="daily", datasets=None, zones=None):
    """All calls a run would make: [(dataset, series_key, eic, eic_to, start, end)]."""
    ensure_watermark_table(spark)
    now = dt.datetime.now(UTC)
    wms = read_watermarks(spark)
    calls = []
    for d, key, eic, eic_to in build_series(spark, datasets or ALL_DATASETS, zones):
        for a, b in _chunks(_start(mode, wms.get((d, key)), now), _target_end(d, now)):
            calls.append((d, key, eic, eic_to, a, b))
    return calls


# ---------------------------------------------------------------
# Execution
# ---------------------------------------------------------------
def run(spark, token, mode="daily", datasets=None, zones=None, max_calls=None,
        max_consecutive_failures=3, load_silver=True):
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    now = dt.datetime.now(UTC)
    client = EntsoeClient(token=token, spark=spark)
    calls = plan(spark, mode, datasets, zones)

    stats, touched, new_wm = Counter(), set(), {}
    failed_series, consecutive_failures, made = set(), 0, 0
    stopped = None

    for d, key, eic, eic_to, a, b in calls:
        if (d, key) in failed_series:
            continue                                   # never skip past a failed chunk
        if max_calls is not None and made >= max_calls:
            stopped = f"max_calls={max_calls} reached"
            break

        outcome, _, _ = client.fetch(d, key, eic, a, b, eic_to=eic_to)
        made += 1
        stats[outcome] += 1

        if outcome in ("ok", "no_data"):
            consecutive_failures = 0
            new_wm[(d, key)] = min(b, now)
            if outcome == "ok":
                touched.add(d)
        else:
            consecutive_failures += 1
            failed_series.add((d, key))
            if consecutive_failures >= max_consecutive_failures:
                stopped = f"circuit breaker: {consecutive_failures} consecutive failures (last: {outcome})"
                break

        if made % CHECKPOINT_EVERY == 0:
            client.flush_log()
            save_watermarks(spark, new_wm)

    client.flush_log()
    save_watermarks(spark, new_wm)

    silver = silver_loader.run(spark, sorted(touched)) if (load_silver and touched) else []
    return {
        "run_id": client.run_id,
        "mode": mode,
        "planned_calls": len(calls),
        "calls_made": made,
        "outcomes": dict(stats),
        "series_advanced": len(new_wm),
        "series_failed": len(failed_series),
        "stopped": stopped or "completed",
        "silver": silver,
    }