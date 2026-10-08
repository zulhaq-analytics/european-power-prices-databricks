"""
ENTSO-E XML parser.

Turns a raw API response (bytes) into a list of flat rows, one per timestamp:
TimeSeries -> Period -> Point, with timestamps computed from period start,
position and resolution. Fills points omitted under curve type A03.
"""
import re
import datetime as dt
import xml.etree.ElementTree as ET


def _local(tag: str) -> str:
    """Strip the XML namespace: '{urn:...}TimeSeries' -> 'TimeSeries'."""
    return tag.rsplit("}", 1)[-1]


def _child(el, name):
    if el is None:
        return None
    for c in el:
        if _local(c.tag) == name:
            return c
    return None


def _children(el, name):
    return [c for c in el if _local(c.tag) == name]


def _text(el, name):
    c = _child(el, name)
    return c.text.strip() if c is not None and c.text else None


def _parse_ts(s: str) -> dt.datetime:
    """'2025-06-01T22:00Z' -> timezone-aware UTC datetime."""
    return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))


def _step(resolution: str):
    """ISO 8601 resolution -> timedelta. None for calendar-based (P1M, P1Y)."""
    m = re.fullmatch(r"PT(\d+)M", resolution)
    if m:
        return dt.timedelta(minutes=int(m.group(1)))
    m = re.fullmatch(r"PT(\d+)H", resolution)
    if m:
        return dt.timedelta(hours=int(m.group(1)))
    if resolution == "P1D":
        return dt.timedelta(days=1)
    if resolution == "P7D":
        return dt.timedelta(days=7)
    return None


def parse(xml_bytes: bytes) -> list:
    """Parse one ENTSO-E document into flat rows. Acknowledgement (no data) -> []."""
    root = ET.fromstring(xml_bytes)
    document_type = _local(root.tag)
    if document_type == "Acknowledgement_MarketDocument":
        return []

    rows = []
    for ts in _children(root, "TimeSeries"):
        in_domain = _text(ts, "in_Domain.mRID") or _text(ts, "inBiddingZone_Domain.mRID")
        out_domain = _text(ts, "out_Domain.mRID") or _text(ts, "outBiddingZone_Domain.mRID")
        meta = {
            "document_type": document_type,
            "series_mrid":   _text(ts, "mRID"),
            "business_type": _text(ts, "businessType"),
            "in_domain":     in_domain,
            "out_domain":    out_domain,
            "psr_type":      _text(_child(ts, "MktPSRType"), "psrType"),
            "curve_type":    _text(ts, "curveType") or "A01",
            "unit":          _text(ts, "price_Measure_Unit.name") or _text(ts, "quantity_Measure_Unit.name"),
            "currency":      _text(ts, "currency_Unit.name"),
        }

        for period in _children(ts, "Period"):
            interval = _child(period, "timeInterval")
            start = _parse_ts(_text(interval, "start"))
            end = _parse_ts(_text(interval, "end"))
            resolution = _text(period, "resolution")
            step = _step(resolution)

            points, measure = {}, None
            for p in _children(period, "Point"):
                pos = int(_text(p, "position"))
                value = _text(p, "price.amount")
                measure = "price" if value is not None else "quantity"
                if value is None:
                    value = _text(p, "quantity")
                if value is not None:
                    points[pos] = float(value)
            if not points:
                continue

            expected = int((end - start) / step) if step else max(points)
            last = None
            for pos in range(1, expected + 1):
                if pos in points:
                    last, filled = points[pos], False
                elif meta["curve_type"] == "A03" and last is not None:
                    filled = True          # omitted point: value unchanged from previous
                else:
                    continue               # genuine gap (A01): leave for data quality checks
                rows.append({
                    **meta,
                    "measure":       measure,
                    "resolution":    resolution,
                    "period_start":  start,
                    "position":      pos,
                    "timestamp_utc": start + step * (pos - 1) if step else start,
                    "value":         last,
                    "is_filled":     filled,
                })
    return rows