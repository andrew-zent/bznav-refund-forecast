"""
Pipedrive API에서 deals 전건을 추출하여 로컬 JSON으로 저장.

환경변수:
  PIPEDRIVE_API_TOKEN: Pipedrive API 토큰
  PIPEDRIVE_DOMAIN: 회사 도메인 (예: bizzep)

출력:
  data/deals_raw.json: 전체 deals 원본
  data/deal_fields.json: 커스텀 필드 매핑

Incremental 모드:
  data/.last_sync.txt 파일이 있으면 그 시점 이후 변경분만 가져옴.
  --full 플래그 시 전건 재추출.
"""
import os
import sys
import json
import time
import argparse
import urllib.request
import urllib.parse
import urllib.error
from datetime import datetime, timezone
from pathlib import Path

API_TOKEN = os.environ.get("PIPEDRIVE_API_TOKEN")
DOMAIN = os.environ.get("PIPEDRIVE_DOMAIN")

if not API_TOKEN or not DOMAIN:
    print("ERROR: PIPEDRIVE_API_TOKEN, PIPEDRIVE_DOMAIN 환경변수를 설정하세요.", file=sys.stderr)
    sys.exit(1)

BASE_URL = f"https://{DOMAIN}.pipedrive.com/api/v1"
DATA_DIR = Path(__file__).resolve().parent.parent / "data"
DATA_DIR.mkdir(exist_ok=True, parents=True)
LAST_SYNC_FILE = DATA_DIR / ".last_sync.txt"


def http_get(path: str, params: dict, retries: int = 3, backoff: float = 2.0):
    """GET with retry and rate-limit handling."""
    params = {**params, "api_token": API_TOKEN}
    url = f"{BASE_URL}{path}?{urllib.parse.urlencode(params)}"
    last_err = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "biznep-forecast/1.0"})
            with urllib.request.urlopen(req, timeout=60) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            last_err = e
            if e.code == 429:  # rate limited
                wait = backoff * (2 ** attempt)
                print(f"  rate limit, waiting {wait:.0f}s ...", file=sys.stderr)
                time.sleep(wait)
            elif e.code >= 500:
                time.sleep(backoff * (attempt + 1))
            else:
                raise
        except urllib.error.URLError as e:
            last_err = e
            time.sleep(backoff * (attempt + 1))
    raise last_err


def fetch_deal_fields():
    """커스텀 필드 정의 (key → name 매핑)."""
    print("[1/3] Fetching deal field definitions ...")
    fields = {}
    start = 0
    while True:
        data = http_get("/dealFields", {"start": start, "limit": 500})
        for f in data.get("data") or []:
            fields[f["key"]] = {
                "name": f["name"],
                "field_type": f.get("field_type"),
                "options": {opt["id"]: opt["label"] for opt in (f.get("options") or [])},
            }
        if not data.get("additional_data", {}).get("pagination", {}).get("more_items_in_collection"):
            break
        start = data["additional_data"]["pagination"]["next_start"]
    out = DATA_DIR / "deal_fields.json"
    out.write_text(json.dumps(fields, ensure_ascii=False, indent=2))
    print(f"      {len(fields)} fields → {out}")
    return fields


def fetch_deals(since=None):
    """전체 deals 추출 (페이지네이션). since=ISO 시점 이후만."""
    print(f"[2/3] Fetching deals {'since ' + since if since else '(full sync)'} ...")
    all_deals = []
    start = 0
    LIMIT = 500
    page = 0
    while True:
        page += 1
        params = {
            "start": start,
            "limit": LIMIT,
            "status": "all_not_deleted",
            "sort": "update_time ASC",
        }
        data = http_get("/deals", params)
        chunk = data.get("data") or []
        if since:
            chunk = [d for d in chunk if (d.get("update_time") or "") >= since]
        all_deals.extend(chunk)
        if page % 5 == 0:
            print(f"      page {page}: {len(all_deals):,} deals so far")
        pag = data.get("additional_data", {}).get("pagination", {})
        if not pag.get("more_items_in_collection"):
            break
        start = pag["next_start"]
        time.sleep(0.1)  # gentle pacing
    print(f"      total: {len(all_deals):,} deals")
    return all_deals


def merge_with_existing(new_deals: list[dict]):
    """Incremental sync: 기존 데이터에 변경분 병합 (id 기준 upsert)."""
    raw_path = DATA_DIR / "deals_raw.json"
    if not raw_path.exists():
        return new_deals
    existing = json.loads(raw_path.read_text())
    by_id = {d["id"]: d for d in existing}
    for d in new_deals:
        by_id[d["id"]] = d  # upsert
    merged = list(by_id.values())
    print(f"      merged: {len(existing):,} existing + {len(new_deals):,} updated = {len(merged):,} total")
    return merged


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--full", action="store_true", help="전건 재추출 (기본: 증분)")
    args = parser.parse_args()

    fetch_deal_fields()

    since = None
    if not args.full and LAST_SYNC_FILE.exists():
        since = LAST_SYNC_FILE.read_text().strip()
        print(f"  Incremental mode (last sync: {since})")
    else:
        print("  Full sync mode")

    new_deals = fetch_deals(since=since)

    if since:
        deals = merge_with_existing(new_deals)
    else:
        deals = new_deals

    print("[3/3] Writing output ...")
    raw_path = DATA_DIR / "deals_raw.json"
    raw_path.write_text(json.dumps(deals, ensure_ascii=False))
    print(f"      → {raw_path} ({raw_path.stat().st_size / 1e6:.1f} MB)")

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    LAST_SYNC_FILE.write_text(now)
    print(f"      sync timestamp: {now}")
    print(f"\nDone. Total deals in dataset: {len(deals):,}")


if __name__ == "__main__":
    main()
