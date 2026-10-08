"""
Bronze -> silver.

Reads raw ENTSO-E XML files from the bronze volume, parses them, normalizes
to hourly UTC, maps EIC codes to zone codes, adds local date/hour per zone,
and MERGEs into silver Delta tables. Processed files are tracked in
ops.silver_file_log so each run only handles new files.
"""
import glob
import datetime as dt

from pyspark.sql import functions as F
from pyspark.sql import Window

from entsoe_parser import parse

CATALOG = "power_prices"
RAW_ROOT = f"/Volumes/{CATALOG}/bronze/raw_files"
FILE_LOG = f"{CATALOG}.ops.silver_file_log"
DIM_ZONE = f"{CATALOG}.gold.dim_bidding_zone"

FIELDS = ["document_type", "series_mrid", "business_type", "in_domain", "out_domain", "psr_type",
          "curve_type", "unit", "currency", "measure", "resolution", "period_start", "position",
          "timestamp_utc", "value", "is_filled", "source_file"]
PARSED_SCHEMA = (
    "document_type STRING, series_mrid STRING, business_type STRING, in_domain STRING, "
    "out_domain STRING, psr_type STRING, curve_type STRING, unit STRING, currency STRING, "
    "measure STRING, resolution STRING, period_start TIMESTAMP, position INT, "
    "timestamp_utc TIMESTAMP, value DOUBLE, is_filled BOOLEAN, source_file STRING"
)

# Raw folder (dataset) -> silver table
DATASET_TABLE = {
    "day_ahead_price":    "price",
    "actual_load":        "load",
    "load_forecast":      "load",
    "generation_actual":  "generation",
    "physical_flow":      "flow",
    "installed_capacity": "capacity",
}

_LINEAGE = [
    ("source_resolution", "STRING",    "Original resolution, e.g. PT15M or PT60M"),
    ("points_in_hour",    "INT",       "Number of source points averaged into the hour"),
    ("filled_points",     "INT",       "Source points filled from the previous value (A03 curve)"),
    ("source_file",       "STRING",    "Raw file this row came from"),
    ("loaded_at",         "TIMESTAMP", "When the row was last written"),
]
_LOCAL = [
    ("local_date", "DATE", "Local calendar date in the zone time zone (DST-aware)"),
    ("local_hour", "INT",  "Local hour 0-23 in the zone time zone"),
]

TABLES = {
    "price": {
        "comment": "Hourly day-ahead prices per bidding zone",
        "keys": ["zone_code", "timestamp_utc"],
        "columns": [("zone_code", "STRING", "Bidding zone, FK to gold.dim_bidding_zone"),
                    ("timestamp_utc", "TIMESTAMP", "Start of the hour, UTC")] + _LOCAL +
                   [("price_eur_mwh", "DOUBLE", "Day-ahead price, EUR/MWh; hourly mean of sub-hourly prices")] + _LINEAGE,
    },
    "load": {
        "comment": "Hourly actual and day-ahead forecast load per bidding zone",
        "keys": ["zone_code", "load_type", "timestamp_utc"],
        "columns": [("zone_code", "STRING", "Bidding zone, FK to gold.dim_bidding_zone"),
                    ("load_type", "STRING", "actual or forecast"),
                    ("timestamp_utc", "TIMESTAMP", "Start of the hour, UTC")] + _LOCAL +
                   [("load_mw", "DOUBLE", "Average load in the hour, MW (equals MWh for the hour)")] + _LINEAGE,
    },
    "generation": {
        "comment": "Hourly actual generation per bidding zone and fuel type",
        "keys": ["zone_code", "psr_code", "flow_type", "timestamp_utc"],
        "columns": [("zone_code", "STRING", "Bidding zone, FK to gold.dim_bidding_zone"),
                    ("psr_code", "STRING", "Fuel type code, FK to gold.dim_fuel_type"),
                    ("flow_type", "STRING", "generation, or consumption (e.g. pumped storage pumping)"),
                    ("timestamp_utc", "TIMESTAMP", "Start of the hour, UTC")] + _LOCAL +
                   [("generation_mw", "DOUBLE", "Average output in the hour, MW (equals MWh for the hour)")] + _LINEAGE,
    },
    "flow": {
        "comment": "Hourly cross-border physical flows between bidding zones (directional)",
        "keys": ["from_zone", "to_zone", "timestamp_utc"],
        "columns": [("from_zone", "STRING", "Sending zone, FK to gold.dim_bidding_zone"),
                    ("to_zone", "STRING", "Receiving zone, FK to gold.dim_bidding_zone"),
                    ("timestamp_utc", "TIMESTAMP", "Start of the hour, UTC"),
                    ("flow_mw", "DOUBLE", "Average physical flow in the hour, MW")] + _LINEAGE,
    },
    "capacity": {
        "comment": "Installed generation capacity per bidding zone, fuel type and year",
        "keys": ["zone_code", "psr_code", "year"],
        "columns": [("zone_code", "STRING", "Bidding zone, FK to gold.dim_bidding_zone"),
                    ("psr_code", "STRING", "Fuel type code, FK to gold.dim_fuel_type"),
                    ("year", "INT", "Year the capacity applies to"),
                    ("capacity_mw", "DOUBLE", "Installed capacity, MW"),
                    ("source_file", "STRING", "Raw file this row came from"),
                    ("loaded_at", "TIMESTAMP", "When the row was last written")],
    },
}


# ---------------------------------------------------------------
# Table setup
# ---------------------------------------------------------------
def ensure_tables(spark):
    for name, spec in TABLES.items():
        cols = ",\n  ".join(
            f"{c} {t}{' NOT NULL' if c in spec['keys'] else ''} COMMENT '{cm}'"
            for c, t, cm in spec["columns"]
        )
        spark.sql(f"CREATE TABLE IF NOT EXISTS {CATALOG}.silver.{name} (\n  {cols}\n) "
                  f"COMMENT '{spec['comment']}'")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {FILE_LOG} (
        file_path STRING, dataset STRING, rows_parsed BIGINT, processed_at TIMESTAMP)
        COMMENT 'Raw files already processed into silver'""")


# ---------------------------------------------------------------
# Transformations
# ---------------------------------------------------------------
def _res_minutes(col):
    return (F.when(col.rlike(r"^PT\d+M$"), F.regexp_extract(col, r"^PT(\d+)M$", 1).cast("int"))
             .when(col.rlike(r"^PT\d+H$"), F.regexp_extract(col, r"^PT(\d+)H$", 1).cast("int") * 60))


def _hourly(df, group_cols):
    """Keep the finest resolution per group and hour, then average to hourly."""
    df = (df.withColumn("hour_utc", F.date_trunc("hour", "timestamp_utc"))
            .withColumn("res_min", _res_minutes(F.col("resolution"))))
    w = Window.partitionBy(*group_cols, "hour_utc")
    df = df.withColumn("min_res", F.min("res_min").over(w)).filter(F.col("res_min") == F.col("min_res"))
    return (df.groupBy(*group_cols, "hour_utc")
              .agg(F.avg("value").alias("value"),
                   F.count("*").cast("int").alias("points_in_hour"),
                   F.sum(F.col("is_filled").cast("int")).cast("int").alias("filled_points"),
                   F.first("resolution").alias("source_resolution"),
                   F.max("source_file").alias("source_file"))
              .withColumnRenamed("hour_utc", "timestamp_utc"))


def _build(spark, table, dataset, raw):
    zones = spark.table(DIM_ZONE).select("eic_code", "zone_code", "timezone")
    eic = zones.select(F.col("eic_code").alias("_eic"), "zone_code")
    tz = zones.select("zone_code", "timezone")

    def add_local(df):
        local_ts = F.from_utc_timestamp("timestamp_utc", F.col("timezone"))
        return (df.join(tz, "zone_code")
                  .withColumn("local_date", F.to_date(local_ts))
                  .withColumn("local_hour", F.hour(local_ts))
                  .drop("timezone"))

    if table == "price":
        df = raw.join(eic, raw.in_domain == eic._eic)
        df = add_local(_hourly(df, ["zone_code"]).withColumnRenamed("value", "price_eur_mwh"))

    elif table == "load":
        df = (raw.join(eic, raw.out_domain == eic._eic)
                 .withColumn("load_type", F.lit("actual" if dataset == "actual_load" else "forecast")))
        df = add_local(_hourly(df, ["zone_code", "load_type"]).withColumnRenamed("value", "load_mw"))

    elif table == "generation":
        df = (raw.withColumn("_zone_eic", F.coalesce("in_domain", "out_domain"))
                 .withColumn("flow_type", F.when(F.col("in_domain").isNotNull(), "generation")
                                           .otherwise("consumption"))
                 .withColumnRenamed("psr_type", "psr_code"))
        df = df.join(eic, df._zone_eic == eic._eic)
        df = add_local(_hourly(df, ["zone_code", "psr_code", "flow_type"])
                       .withColumnRenamed("value", "generation_mw"))

    elif table == "flow":
        frm = zones.select(F.col("eic_code").alias("_from_eic"), F.col("zone_code").alias("from_zone"))
        to = zones.select(F.col("eic_code").alias("_to_eic"), F.col("zone_code").alias("to_zone"))
        df = raw.join(frm, raw.out_domain == frm._from_eic).join(to, raw.in_domain == to._to_eic)
        df = _hourly(df, ["from_zone", "to_zone"]).withColumnRenamed("value", "flow_mw")

    elif table == "capacity":
        df = (raw.join(eic, raw.in_domain == eic._eic)
                 .withColumnRenamed("psr_type", "psr_code")
                 .join(tz, "zone_code"))
        df = (df.withColumn("year", F.year(F.from_utc_timestamp("timestamp_utc", F.col("timezone"))))
                .groupBy("zone_code", "psr_code", "year")
                .agg(F.max("value").alias("capacity_mw"), F.max("source_file").alias("source_file")))

    cols = [c for c, _, _ in TABLES[table]["columns"]]
    return df.withColumn("loaded_at", F.current_timestamp()).select(*cols)


def _merge(spark, df, table):
    keys = TABLES[table]["keys"]
    view = f"_src_{table}"
    df.createOrReplaceTempView(view)
    on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    spark.sql(f"""MERGE INTO {CATALOG}.silver.{table} t USING {view} s ON {on}
                  WHEN MATCHED THEN UPDATE SET *
                  WHEN NOT MATCHED THEN INSERT *""")


# ---------------------------------------------------------------
# Run
# ---------------------------------------------------------------
def pending_files(spark, dataset, reprocess=False):
    files = sorted(glob.glob(f"{RAW_ROOT}/{dataset}/**/*.xml", recursive=True))
    if reprocess:
        return files
    done = {r.file_path for r in spark.table(FILE_LOG).filter(F.col("dataset") == dataset)
                                          .select("file_path").collect()}
    return [f for f in files if f not in done]


def run(spark, datasets=None, reprocess=False, batch_size=100):
    """Process new raw files into silver. Returns [(dataset, files, rows_parsed, rows_written)]."""
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    ensure_tables(spark)
    summary = []
    for dataset in datasets or list(DATASET_TABLE):
        table = DATASET_TABLE[dataset]
        files = pending_files(spark, dataset, reprocess)
        n_rows, n_written = 0, 0
        for i in range(0, len(files), batch_size):
            batch = files[i:i + batch_size]
            rows, per_file = [], []
            for path in batch:
                with open(path, "rb") as f:
                    parsed = parse(f.read())
                for r in parsed:
                    r["source_file"] = path
                rows.extend(parsed)
                per_file.append((path, dataset, len(parsed)))
            if rows:
                raw = spark.createDataFrame([tuple(r[k] for k in FIELDS) for r in rows], PARSED_SCHEMA)
                df = _build(spark, table, dataset, raw)
                n_written += df.count()
                _merge(spark, df, table)
            n_rows += len(rows)
            now = dt.datetime.now(dt.timezone.utc)
            (spark.createDataFrame([(p, d, n, now) for p, d, n in per_file],
                                   "file_path STRING, dataset STRING, rows_parsed BIGINT, processed_at TIMESTAMP")
                  .write.mode("append").saveAsTable(FILE_LOG))
        summary.append((dataset, len(files), n_rows, n_written))
    return summary