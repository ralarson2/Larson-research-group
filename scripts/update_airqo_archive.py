import csv
import json
import os
import time
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
        if v != v:
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
    obj = None
    for k in keys:
        if k in item:
            obj = item[k]
            break

    if obj is None:
        return "", ""

    if isinstance(obj, dict):
        raw = _pick(obj, "value", "rawValue", "raw_value", "raw", default="")
        cal = _pick(obj, "calibratedValue", "calibrated_value", "calibrated", default="")
        raw_n = _num(raw)
        cal_n = _num(cal)
        if cal_n == "" and raw_n != "":
            cal_n = raw_n
        return raw_n, cal_n

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


def _try_fetch(url: str, token: str):
    # Try header auth, then query-token auth
    r1 = requests.get(url, headers={"Accept": "application/json", "Authorization": f"Bearer {token}"}, timeout=45)
    if r1.ok:
        return r1, "bearer"
    r2 = requests.get(f"{url}?token={token}", headers={"Accept": "application/json"}, timeout=45)
    if r2.ok:
        return r2, "query"
    return r2, f"failed(bearer={r1.status_code},query={r2.status_code})"


def fetch_with_retries(url: str, token: str, attempts: int = 4):
    last_resp = None
    last_mode = ""
    for i in range(attempts):
        resp, mode = _try_fetch(url, token)
        last_resp, last_mode = resp, mode
        if resp.ok:
            return resp, mode
        # backoff: 3s, 9s, 18s...
        time.sleep(3 * (i + 1) ** 2)
    return last_resp, last_mode


def main():
    token = os.environ.get("AIRQO_TOKEN", "").strip()
    cohort_id = os.environ.get("AIRQO_COHORT_ID", "").strip()
    if not token or not cohort_id:
        print("Missing AIRQO_TOKEN or AIRQO_COHORT_ID; exiting without failure to avoid notification spam.")
        return

    Path("data").mkdir(parents=True, exist_ok=True)
    url = f"https://api.airqo.net/api/v2/devices/measurements/cohorts/{cohort_id}"

    resp, mode = fetch_with_retries(url, token, attempts=4)

    if resp is None or not resp.ok:
        preview = ""
        status = None
        if resp is not None:
            status = resp.status_code
            preview = (resp.text or "")[:500]
        RECENT_JSON_PATH.write_text(
            json.dumps(
                {
                    "error": "AirQo fetch failed on this run",
                    "status_code": status,
                    "auth_mode": mode,
                    "note": "Workflow will try again next hour. This run exited successfully to avoid email spam.",
                    "response_text_preview": preview,
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        print(f"AirQo fetch failed (mode={mode}, status={status}). Exiting 0.")
        return

    data = resp.json()

    with RECENT_JSON_PATH.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    measurements = data.get("measurements") or data.get("results") or []
    if not isinstance(measurements, list):
        measurements = []

    existing = _load_existing_keys(ARCHIVE_CSV_PATH)

    if not ARCHIVE_CSV_PATH.exists():
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

    print(f"AirQo fetch OK using {mode}. Appended {len(new_rows)} rows. Recent JSON updated.")


if __name__ == "__main__":
    main()


