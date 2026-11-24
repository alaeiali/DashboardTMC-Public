#!/usr/bin/env python3
"""Read-only availability/latency probe for public TSETMC endpoints."""
from __future__ import annotations
import json, time, datetime, urllib.request, urllib.error, ssl, os
from pathlib import Path

INS_CODE = "17914401175772326"
OUT = Path("reports")
OUT.mkdir(exist_ok=True)
NOW = datetime.datetime.now(datetime.timezone.utc)
ENDPOINTS = [
    ("tsetmc_old_quote", f"https://old.tsetmc.com/tsev2/data/instinfofast.aspx?i={INS_CODE}&c=68%20&e=1"),
    ("tsetmc_old_orderbook", f"https://old.tsetmc.com/tsev2/data/instinfodata.aspx?i={INS_CODE}&c=27"),
    ("tsetmc_old_trades", f"https://old.tsetmc.com/tsev2/data/TradeDetail.aspx?i={INS_CODE}"),
    ("tsetmc_cdn_orderbook", f"https://cdn.tsetmc.com/api/BestLimits/{INS_CODE}"),
    ("tsetmc_cdn_trades", f"https://cdn.tsetmc.com/api/Trade/GetTrade/{INS_CODE}"),
]
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; TSETMC-read-only-probe/1.0)",
    "Accept": "application/json,text/plain,*/*",
    "Referer": "https://www.tsetmc.com/",
}
def probe(name: str, url: str) -> dict:
    started = time.perf_counter()
    row = {"name": name, "url": url, "checked_at_utc": datetime.datetime.now(datetime.timezone.utc).isoformat()}
    req = urllib.request.Request(url, headers=HEADERS, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=15, context=ssl.create_default_context()) as res:
            body = res.read(2_000_000)
            row.update(http_status=res.status, content_type=res.headers.get("Content-Type"),
                       bytes_received=len(body), elapsed_ms=round((time.perf_counter()-started)*1000, 1),
                       preview=body[:1200].decode("utf-8", errors="replace"))
            try:
                parsed = json.loads(body.decode("utf-8-sig"))
                row["json_valid"] = True
                if isinstance(parsed, dict):
                    row["top_level_keys"] = list(parsed.keys())[:40]
                elif isinstance(parsed, list):
                    row["top_level_type"] = "list"
                    row["item_count"] = len(parsed)
                else:
                    row["top_level_type"] = type(parsed).__name__
            except Exception:
                row["json_valid"] = False
    except urllib.error.HTTPError as e:
        row.update(http_status=e.code, error=f"HTTPError: {e.reason}",
                   preview=e.read(1000).decode("utf-8", errors="replace"))
        row["elapsed_ms"] = round((time.perf_counter()-started)*1000, 1)
    except Exception as e:
        row.update(error=f"{type(e).__name__}: {e}",
                   elapsed_ms=round((time.perf_counter()-started)*1000, 1))
    return row

results = [probe(name, url) for name, url in ENDPOINTS]
report = {
    "project": "DashboardTMC-Public",
    "instrument": "اهرم",
    "instrument_code": INS_CODE,
    "generated_at_utc": NOW.isoformat(),
    "scope": "GET-only public endpoints; no authentication, orders, trades, or account actions",
    "results": results,
}
stamp = NOW.strftime("%Y%m%dT%H%M%SZ")
path = OUT / f"tsetmc_dual_source_{stamp}.json"
path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
print(json.dumps(report, ensure_ascii=False, indent=2))
print(f"\nREPORT_FILE={path}")
