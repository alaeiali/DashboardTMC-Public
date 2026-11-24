#!/usr/bin/env python3
"""Time-windowed, read-only TSETMC collector for instrument 17914401175772326.

All timestamps are Asia/Tehran. Raw JSON responses are retained in CSV rows.
Every request is logged, while raw payloads are written only when they change.
No authentication, account access, order placement, or trading actions are used.
"""
from __future__ import annotations

import concurrent.futures
import csv
import datetime as dt
import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

INS_CODE = os.getenv("INS_CODE", "17914401175772326")
try:
    TZ = ZoneInfo("Asia/Tehran")
except ZoneInfoNotFoundError:
    # Windows runners may not ship the IANA database; Iran uses UTC+03:30 year-round.
    TZ = dt.timezone(dt.timedelta(hours=3, minutes=30), name="Asia/Tehran")
START_TIME = dt.time(8, 45)
CONTINUOUS_END = dt.time(12, 30)
END_TIME = dt.time(12, 45)
POLL_SECONDS = 2.0
REQUEST_TIMEOUT = 10
MAX_RETRIES = 1
OUT = Path("reports") / f"ahrom_session_{dt.datetime.now(TZ):%Y%m%d}"
OUT.mkdir(parents=True, exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/126.0.0.0 Safari/537.36",
    "Accept": "application/json,text/plain,*/*",
    "Referer": "https://www.tsetmc.com/",
}
ENDPOINTS = {
    "quote": f"https://cdn.tsetmc.com/api/ClosingPrice/GetClosingPriceInfo/{INS_CODE}",
    "orderbook": f"https://cdn.tsetmc.com/api/BestLimits/{INS_CODE}",
    "trades": f"https://cdn.tsetmc.com/api/Trade/GetTrade/{INS_CODE}",
}
WRAPPED_KEYS = {"quote": "closingPriceInfo", "orderbook": "bestLimits", "trades": "trade"}
LOCAL_FIELDS = ["local_time", "phase", "endpoint", "http_status", "elapsed_ms", "bytes", "changed", "error"]
RAW_FIELDS = ["local_time", "phase", "endpoint", "http_status", "elapsed_ms", "bytes", "raw_response"]

def now_local() -> dt.datetime:
    return dt.datetime.now(TZ)

def phase_at(t: dt.time) -> str:
    if t < dt.time(9, 0):
        return "PREOPEN"
    if t < CONTINUOUS_END:
        return "CONTINUOUS"
    return "POST_CLOSE_OBSERVATION"

def ensure_csv(path: Path, fields: list[str]) -> None:
    if not path.exists():
        with path.open("w", newline="", encoding="utf-8-sig") as f:
            csv.DictWriter(f, fieldnames=fields).writeheader()

def append_csv(path: Path, fields: list[str], row: dict) -> None:
    ensure_csv(path, fields)
    with path.open("a", newline="", encoding="utf-8-sig") as f:
        csv.DictWriter(f, fieldnames=fields).writerow(row)

def request_one(name: str, url: str) -> dict:
    last_error = ""
    for attempt in range(MAX_RETRIES + 1):
        started = time.perf_counter()
        stamp = now_local().isoformat(timespec="milliseconds")
        req = urllib.request.Request(url, headers=HEADERS, method="GET")
        try:
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as res:
                body = res.read(2_000_000)
                elapsed = round((time.perf_counter() - started) * 1000, 1)
                text = body.decode("utf-8-sig", errors="replace")
                try:
                    parsed = json.loads(text)
                    valid_json = True
                    wrapped = WRAPPED_KEYS[name]
                    payload = parsed.get(wrapped) if isinstance(parsed, dict) else None
                    shape_ok = payload is not None
                    item_count = len(payload) if isinstance(payload, (list, dict)) else None
                except Exception:
                    valid_json, shape_ok, item_count = False, False, None
                return {
                    "local_time": stamp, "name": name, "http_status": res.status,
                    "elapsed_ms": elapsed, "bytes": len(body), "raw": text,
                    "error": "", "valid_json": valid_json, "shape_ok": shape_ok,
                    "item_count": item_count, "attempt": attempt + 1,
                }
        except urllib.error.HTTPError as e:
            last_error = f"HTTPError {e.code}: {e.reason}"
            try:
                detail = e.read(2000).decode("utf-8", errors="replace")
            except Exception:
                detail = ""
            status = e.code
        except Exception as e:
            last_error = f"{type(e).__name__}: {e}"
            detail, status = "", ""
        elapsed = round((time.perf_counter() - started) * 1000, 1)
        if attempt < MAX_RETRIES:
            time.sleep(0.5)
    return {
        "local_time": stamp, "name": name, "http_status": status,
        "elapsed_ms": elapsed, "bytes": len(detail.encode("utf-8")),
        "raw": detail, "error": last_error, "valid_json": False,
        "shape_ok": False, "item_count": None, "attempt": MAX_RETRIES + 1,
    }

def write_result(result: dict, phase: str, last_payloads: dict[str, str], is_preflight: bool = False) -> None:
    name = result["name"]
    payload = result["raw"]
    changed = payload != last_payloads.get(name)
    log_row = {
        "local_time": result["local_time"], "phase": "PREFLIGHT" if is_preflight else phase,
        "endpoint": name, "http_status": result["http_status"],
        "elapsed_ms": result["elapsed_ms"], "bytes": result["bytes"],
        "changed": str(changed).lower(), "error": result["error"],
    }
    append_csv(OUT / "requests_log.csv", LOCAL_FIELDS, log_row)
    if payload and changed:
        raw_row = {
            "local_time": result["local_time"], "phase": "PREFLIGHT" if is_preflight else phase,
            "endpoint": name, "http_status": result["http_status"],
            "elapsed_ms": result["elapsed_ms"], "bytes": result["bytes"],
            "raw_response": payload,
        }
        append_csv(OUT / f"{name}_raw.csv", RAW_FIELDS, raw_row)
        last_payloads[name] = payload
    status = result["http_status"] if result["http_status"] != "" else "ERR"
    print(
        f'{result["local_time"]} | {phase if not is_preflight else "PREFLIGHT"} | '
        f'{name} | HTTP {status} | {result["elapsed_ms"]}ms | '
        f'{result["bytes"]}B | JSON={result["valid_json"]} | '
        f'SHAPE={result["shape_ok"]} | ITEMS={result["item_count"]} | '
        f'CHANGED={changed} | {result["error"]}',
        flush=True,
    )

def main() -> int:
    print(f"Instrument={INS_CODE}; timezone=Asia/Tehran; output={OUT}", flush=True)
    print("Endpoints: " + json.dumps(ENDPOINTS, ensure_ascii=False), flush=True)
    print("Preflight checks begin now; collection window is 08:45-12:45 Tehran time.", flush=True)

    last_payloads: dict[str, str] = {}
    manifest = {
        "instrument": "اهرم", "instrument_code": INS_CODE,
        "timezone": "Asia/Tehran", "planned_start": "08:45",
        "continuous_session_end": "12:30", "planned_end": "12:45",
        "poll_interval_seconds": POLL_SECONDS,
        "phases": {
            "PREOPEN": "08:45-09:00",
            "CONTINUOUS": "09:00-12:30",
            "POST_CLOSE_OBSERVATION": "12:30-12:45",
        },
        "important_note": "اهرم طبق اطلاعات کاربر مشمول جلسه TAL نیست؛ بازه 12:30-12:45 صرفاً پایش پس از پایان معاملات پیوسته است و TAL نام‌گذاری نشده است.",
        "endpoints": ENDPOINTS,
        "scope": "Read-only public GET requests only. No login, account, order placement, or trade actions.",
    }
    (OUT / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    # Preflight is deliberately before 08:45 when the workflow is started early.
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(ENDPOINTS)) as pool:
        futures = {pool.submit(request_one, name, url): name for name, url in ENDPOINTS.items()}
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            write_result(result, "PREFLIGHT", last_payloads, is_preflight=True)

    start_dt = now_local().replace(hour=8, minute=45, second=0, microsecond=0)
    end_dt = now_local().replace(hour=12, minute=45, second=0, microsecond=0)
    current = now_local()
    if current >= end_dt:
        print("Current Tehran time is already past 12:45; no session collection started.", flush=True)
        return 2
    if current < start_dt:
        wait_seconds = (start_dt - current).total_seconds()
        print(f"Waiting {int(wait_seconds)} seconds until 08:45 Tehran time.", flush=True)
        time.sleep(wait_seconds)
    else:
        print("Workflow started after 08:45; collecting immediately and marking the late start in logs.", flush=True)

    cycles = 0
    errors = 0
    while now_local() < end_dt:
        cycle_started = time.perf_counter()
        phase = phase_at(now_local().time())
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(ENDPOINTS)) as pool:
            futures = {pool.submit(request_one, name, url): name for name, url in ENDPOINTS.items()}
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                if result["error"]:
                    errors += 1
                write_result(result, phase, last_payloads)
        cycles += 1
        remaining = POLL_SECONDS - (time.perf_counter() - cycle_started)
        if remaining > 0:
            time.sleep(remaining)

    summary = {
        "finished_at": now_local().isoformat(),
        "cycles": cycles,
        "requests_expected": cycles * len(ENDPOINTS),
        "request_errors": errors,
        "unique_payloads": {k: bool(v) for k, v in last_payloads.items()},
        "output_directory": str(OUT),
    }
    (OUT / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print("COLLECTION_FINISHED " + json.dumps(summary, ensure_ascii=False), flush=True)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
