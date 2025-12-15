import os
import csv
import time
import datetime as dt
import requests

ARCHIVE_PATH = "data/uganda_pm25_archive.csv"

TOKEN = os.environ.get("AIRQO_TOKEN", "").strip()
COHORT_ID = os.environ.get("AIRQO_COHORT_ID", "").strip()
if not TOKEN or not COHORT_ID:
    raise SystemExit("Missing AIRQO_TOKEN or AIRQO_COHORT_ID environment variables")

HIST_URL = f"https://api.airqo.net/api/v2/devices/measurements/cohorts/{COHORT_ID}/historical"

# We re-pull a rolling window to avoid gaps and then dedupe.
WINDOW_DAYS = 2

def iso_z(d: dt.datetime) -> str:
    # AirQo examples vary; ISO8601 with Z is the safest.
    return d.replace(microsecond=0).isoformat() + "Z"

def safe_get(d, *keys, default=None):
    cur = d
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur

def fetch_all_pages(start_iso: str, end_iso: str):
    items = []
    page = 1
    pages = 1

    while page <= pages:
        params = {
            "token": TOKEN,
            "startTime": start_iso,
            "endTime": end_iso,
            "page": page,
        }
        r = requests.get(HIST_URL, params=params, timeout=60)
        if r.status_code != 200:
            raise RuntimeError(f"AirQo request failed HTTP {r.status_code}: {r.text[:300]}")
        data = r.json()

        batch = data.get("measurements") or data.get("results") or data.get("data") or []
        if isinstance(batch, list):
            items.extend(batch)

        meta = data.get("meta") or {}
        pages = int(meta.get("pages") or 1)
        page += 1
        time.sleep(0.2)

    return items

def normalize_row(item):
    # Keep an “export-like” format because your existing file exports look like this.
    # We store both calibrated and raw if present.
    device_name = (
        item.get("device_name")
        or item.get("device")
        or item.get("device_id")
        or item.get("deviceId")
        or ""
    )

    # AirQo timestamps vary by endpoint; try common fields.
    ts = item.get("datetime") or item.get("time") or item.get("created_at") or item.get("timestamp") or ""

    # PM2.5 may appear as pm2_5.value or pm2_5_calibrated.value, etc.
    pm25_raw = safe_get(item, "pm2_5", "value", default=None)
    if pm25_raw is None:
        pm25_raw = item.get("pm2_5") or item.get("pm25")

    pm25_cal = safe_get(item, "pm2_5_calibrated", "value", default=None)
    if pm25_cal is None:
        pm25_cal = item.get("pm2_5_calibrated_value") or item.get("pm2_5_calibrated")

    site_name = item.get("site_name") or item.get("site") or item.get("name") or ""
    network = item.get("network") or ""

    def to_num(x):
        try:
            if x is None or x == "":
                return ""
            return str(float(x))
        except Exception:
            return ""

    return {
        "datetime": ts,
        "device_name": str(device_name),
        "site_name": str(site_name),
        "network": str(network),
        "pm2_5_calibrated_value": to_num(pm25_cal),
        "pm2_5": to_num(pm25_raw),
    }

def read_archive(path):
    if not os.path.exists(path):
        return [], None

    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        return rows, reader.fieldnames

def write_archive(path, fieldnames, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

def main():
    end = dt.datetime.utcnow()
    start = end - dt.timedelta(days=WINDOW_DAYS)

    start_iso = iso_z(start)
    end_iso = iso_z(end)

    new_items = fetch_all_pages(start_iso, end_iso)
    new_rows = [normalize_row(it) for it in new_items]

    existing_rows, existing_fields = read_archive(ARCHIVE_PATH)

    # Define/lock header order
    fieldnames = existing_fields or [
        "datetime",
        "device_name",
        "site_name",
        "network",
        "pm2_5_calibrated_value",
        "pm2_5",
    ]

    # Merge then dedupe. Keying on datetime + device_name + network is usually stable.
    merged = []
    seen = {}

    def score(row):
        # Prefer rows that have calibrated values; otherwise keep whatever.
        return (1 if row.get("pm2_5_calibrated_value") not in ("", None) else 0)

    for row in existing_rows + new_rows:
        dtv = (row.get("datetime") or "").strip()
        dev = (row.get("device_name") or "").strip()
        net = (row.get("network") or "").strip()
        if not dtv or not dev:
            continue
        k = (dtv, dev, net)
        if k not in seen:
            seen[k] = row
        else:
            if score(row) > score(seen[k]):
                seen[k] = row

    merged = list(seen.values())

    # Sort by datetime text; most of your datetimes are ISO-ish so lexical sort works well
    merged.sort(key=lambda r: (r.get("datetime", ""), r.get("device_name", ""), r.get("network", "")))

    # Ensure all fields exist
    for r in merged:
        for fn in fieldnames:
            r.setdefault(fn, "")

    write_archive(ARCHIVE_PATH, fieldnames, merged)

if __name__ == "__main__":
    main()
