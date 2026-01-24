import csv
import json
import os
from pathlib import Path
from datetime import datetime, timezone

import requests


ARCHIVE_CSV_PATH = Path("data/uganda_pm25_archive.csv")
RECENT_JSON_PATH = Path("data/uganda_recent.json")

CSV_COLUMNS = [
    "datetime",
    "device_name",
    "frequency",
    "humidity",
    "latitude",
    "longitude",
    "network",
    "pm10",
    "pm10_calibrated_value",
    "pm2_5",
    "pm2_5_calibrated_value",
    "site_name",
    "temperature",
]


def _iso(dt_like) -> str:
    if not dt_like:
        return ""
    if isinstance(dt_like, (int, float)):
        try:
            return datetime.fromtimestamp(float(dt_like) / 1000.0, tz=timezone.utc).isoformat()
        except Exception:
            return ""
    s = str(dt_like).strip()
    if not s:
        return ""
    try:
        # allow "Z"
        if s.endswith("Z"):
            s2 = s[:-1] + "+00:00"
            return datetime.fromisoformat(s2).astimezone(timezone.utc).isoformat()
        return datetime.fromisoformat(s).astimezone(timezone.utc).isoformat()
    except Exception:
        return s


def _num(x):
    try:
        if x is None:
            return ""
        v = float(x)
        if v != v:  # NaN
            return ""
        return v
    except Exception:
        return ""


def _pick(d: dict, *keys, default=None):
    for k in keys:
        if isinstance(d, dict) and k in d:
            return d[k]
    return default


def _extract_pollutant(item: dict, keys):
    """
    Returns (raw_value, calibrated_value) as floats or "".
    Handles shapes like:
      pm2_5: { value: 12.3, calibratedValue: 11.9 }
      pm2_5: { value: 12.3 }
      pm2_5: 12.3
      pm2_5: { calibrated_value: 11.9 }
    """
    obj = None
    for k in keys:
        if k in item:
            obj = item[k]
            break

    if obj is None:
        return "", ""

    if isinstance(obj, dict):
        raw = _pick(obj, "value", "rawValue", "raw_value", "raw", default="")
        cal = _pick(
            obj,
            "calibratedValue",
            "calibrated_value",
            "calibrated",
            "calibratedValueUg",
            "calibratedValueUG",
            default="",
        )
        raw_n = _num(raw)
        cal_n = _num(cal)
        if cal_n == "" and raw_n != "":
            cal_n = raw_n
        return raw_n, cal_n

    # numeric or string
    raw_n = _num(obj)
    cal_n = raw_n if raw_n != "" else ""
    return raw_n, cal_n


def _extract_row(item: dict) -> dict:
    device = _pick(item, "device", "device_id", "deviceId", "name", default="")
    site_name = _pick(item, "site_name", "siteName", "site", default="")

    dt = _pick(item, "time", "timestamp", "created_at", "createdAt", default="")
    dt_iso = _iso(dt)

    humidity = _pick(item, "humidity", "rh", default="")
    temperature = _pick(item, "temperature", "temp", default="")

    latitude = _pick(item, "latitude", "lat", default="")
    longitude = _pick(item, "longitude", "lon", "lng", default="")

    network = _pick(item, "network", default="")

    frequency = _pick(item, "frequency", default="")

    pm25_raw, pm25_cal = _extract_pollutant(item, ("pm2_5", "pm25", "pm_2_5"))
    pm10_raw, pm10_cal = _extract_pollutant(item, ("pm10", "pm_10"))

    row = {
        "datetime": dt_iso,
        "device_name": str(device) if device is not None else "",
        "frequency": str(frequency) if frequency is not None else "",
        "humidity": humidity if humidity != "" else "",
        "latitude": latitude if latitude != "" else "",
        "longitude": longitude if longitude != "" else "",
        "network": str(network) if network is not None else "",
        "pm10": pm10_raw if pm10_raw != "" else "",
        "pm10_calibrated_value": pm10_cal if pm10_cal != "" else "",
        "pm2_5": pm25_raw if pm25_raw != "" else "",
        "pm2_5_calibrated_value": pm25_cal if pm25_cal != "" else "",
        "site_name": str(site_name) if site_name is not None else "",
        "temperature": temperature if temperature != "" else "",
    }

    # stringify all for csv writer
    out = {}
    for k in CSV_COLUMNS:
        v = row.get(k, "")
        if isinstance(v, float):
            out[k] = f"{v:.4f}".rstrip("0").rstrip(".")
        else:
            out[k] = "" if v is None else str(v)
    return out


def _load_existing_keys(path: Path):
    keys = set()
    if not path.exists():
        return keys
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            dt = (r.get("datetime") or "").strip()
            dev = (r.get("device_name") or "").strip()
            if dt and dev:
                keys.add((dt, dev))
    return keys


def main():
    token = os.environ.get("AIRQO_TOKEN", "").strip()
    cohort_id = os.environ.get("AIRQO_COHORT_ID", "").strip()

    if not token:
        raise SystemExit("Missing AIRQO_TOKEN env var")
    if not cohort_id:
        raise SystemExit("Missing AIRQO_COHORT_ID env var")

    Path("data").mkdir(parents=True, exist_ok=True)
    Path("scripts").mkdir(parents=True, exist_ok=True)

    url = f"https://api.airqo.net/api/v2/devices/measurements/cohorts/{cohort_id}"

    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {token}",
    }

    resp = requests.get(url, headers=headers, timeout=45)
    resp.raise_for_status()
    data = resp.json()

    # Always write the full recent response for the website to read
    with RECENT_JSON_PATH.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    measurements = data.get("measurements") or data.get("results") or []
    if not isinstance(measurements, list):
        measurements = []

    existing = _load_existing_keys(ARCHIVE_CSV_PATH)

    file_exists = ARCHIVE_CSV_PATH.exists()
    if not file_exists:
        with ARCHIVE_CSV_PATH.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            writer.writeheader()

    new_rows = []
    for item in measurements:
        if not isinstance(item, dict):
            continue
        row = _extract_row(item)
        key = (row["datetime"].strip(), row["device_name"].strip())
        if not key[0] or not key[1]:
            continue
        if key in existing:
            continue
        existing.add(key)
        new_rows.append(row)

    if new_rows:
        with ARCHIVE_CSV_PATH.open("a", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            writer.writerows(new_rows)

    print(f"Wrote recent JSON: {RECENT_JSON_PATH}")
    print(f"Appended {len(new_rows)} new archive rows to: {ARCHIVE_CSV_PATH}")


if __name__ == "__main__":
    main()
