"""
ENTSO-E Transparency Platform API client.

Builds requests for each dataset, retries transient failures with backoff,
throttles calls, saves raw XML to the bronze volume and logs every call
to power_prices.ops.api_call_log.
"""
import os
import time
import uuid
import datetime as dt

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://web-api.tp.entsoe.eu/api"
RAW_ROOT = "/Volumes/power_prices/bronze/raw_files"
LOG_TABLE = "power_prices.ops.api_call_log"

# Request parameters per dataset. eic = main zone; eic_to = receiving zone (flows only).
DATASETS = {
    "day_ahead_price":    lambda eic, eic_to=None: {"documentType": "A44", "in_Domain": eic, "out_Domain": eic,
                                                     "contract_MarketAgreement.type": "A01"},
    "actual_load":        lambda eic, eic_to=None: {"documentType": "A65", "processType": "A16",
                                                     "outBiddingZone_Domain": eic},
    "load_forecast":      lambda eic, eic_to=None: {"documentType": "A65", "processType": "A01",
                                                     "outBiddingZone_Domain": eic},
    "generation_actual":  lambda eic, eic_to=None: {"documentType": "A75", "processType": "A16",
                                                     "in_Domain": eic},
    "installed_capacity": lambda eic, eic_to=None: {"documentType": "A68", "processType": "A33",
                                                     "in_Domain": eic},
    # Physical flow from eic (sending, out_Domain) to eic_to (receiving, in_Domain)
    "physical_flow":      lambda eic, eic_to=None: {"documentType": "A11", "out_Domain": eic,
                                                     "in_Domain": eic_to},
}

LOG_SCHEMA = """
    run_id STRING, called_at TIMESTAMP, dataset STRING, series_key STRING,
    period_start TIMESTAMP, period_end TIMESTAMP, http_status INT, outcome STRING,
    bytes BIGINT, duration_ms BIGINT, file_path STRING, error_message STRING
"""


def _fmt(ts: dt.datetime) -> str:
    """ENTSO-E period format: yyyyMMddHHmm in UTC."""
    return ts.astimezone(dt.timezone.utc).strftime("%Y%m%d%H%M")


class EntsoeClient:
    def __init__(self, token: str, spark, run_id: str = None, min_interval_s: float = 0.25):
        self.token = token
        self.spark = spark
        self.run_id = run_id or str(uuid.uuid4())
        self.min_interval_s = min_interval_s
        self._last_call = 0.0
        self._log_rows = []

        retry = Retry(
            total=5, backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"], respect_retry_after_header=True,
            raise_on_status=False,
        )
        self.session = requests.Session()
        self.session.mount("https://", HTTPAdapter(max_retries=retry))

    def _throttle(self):
        wait = self.min_interval_s - (time.time() - self._last_call)
        if wait > 0:
            time.sleep(wait)
        self._last_call = time.time()

    def _redact(self, text: str) -> str:
        return text.replace(self.token, "***") if self.token else text

    def _raw_path(self, dataset, series_key, start, end) -> str:
        folder = f"{RAW_ROOT}/{dataset}/{series_key}/{start:%Y}"
        os.makedirs(folder, exist_ok=True)
        return f"{folder}/{dataset}_{series_key}_{_fmt(start)}_{_fmt(end)}.xml"

    def fetch(self, dataset: str, series_key: str, eic: str,
              start: dt.datetime, end: dt.datetime, eic_to: str = None, save: bool = True):
        """
        Call the API for one dataset, series and period.
        Returns (outcome, xml_bytes or None, file_path or None).
        outcome: ok | no_data | unauthorized | error
        """
        params = DATASETS[dataset](eic, eic_to)
        params.update({"periodStart": _fmt(start), "periodEnd": _fmt(end), "securityToken": self.token})

        self._throttle()
        t0 = time.time()
        status, body, path, err = None, b"", None, None
        try:
            r = self.session.get(BASE_URL, params=params, timeout=90)
            status, body = r.status_code, r.content
            head = r.text[:2000]
            is_ack = "Acknowledgement_MarketDocument" in head

            # Order matters: ENTSO-E uses reason code 999 for both bad tokens and missing data
            if status in (401, 403):
                outcome = "unauthorized"
            elif status == 200 and not is_ack:
                outcome = "ok"
            elif is_ack and ("No matching data found" in head or "<code>999</code>" in head):
                outcome = "no_data"
            else:
                outcome = "error"
                err = self._redact(head[:500])

            if outcome == "ok" and save:
                path = self._raw_path(dataset, series_key, start, end)
                with open(path, "wb") as f:
                    f.write(body)
        except Exception as e:
            outcome, err = "error", self._redact(f"{type(e).__name__}: {e}")[:500]

        self._log_rows.append({
            "run_id": self.run_id,
            "called_at": dt.datetime.now(dt.timezone.utc),
            "dataset": dataset,
            "series_key": series_key,
            "period_start": start,
            "period_end": end,
            "http_status": status,
            "outcome": outcome,
            "bytes": len(body),
            "duration_ms": int((time.time() - t0) * 1000),
            "file_path": path,
            "error_message": err,
        })
        return outcome, (body if outcome == "ok" else None), path

    def flush_log(self) -> int:
        """Append buffered call records to the ops log table. Returns rows written."""
        if not self._log_rows:
            return 0
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {LOG_TABLE} ({LOG_SCHEMA})
            COMMENT 'One row per ENTSO-E API call: outcome, timing, raw file path'
        """)
        df = self.spark.createDataFrame(self._log_rows, schema=LOG_SCHEMA)
        df.write.mode("append").saveAsTable(LOG_TABLE)
        n = len(self._log_rows)
        self._log_rows = []
        return n