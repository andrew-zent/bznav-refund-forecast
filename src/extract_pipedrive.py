"""
Pipedrive API에서 deals를 추출하여 필요한 필드만 저장.

최적화:
  - 163개 필드 중 10개만 추출 → 데이터 4.1GB → ~50MB
  - Incremental: 마지막 sync 이후 변경분만 가져와 병합
  - --full: 전건 재추출

환경변수:
  PIPEDRIVE_API_TOKEN, PIPEDRIVE_DOMAIN

출력:
  data/deals_slim.json  (필요 필드만, ~50MB)
  data/deal_fields.json (커스텀 필드 매핑)
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
SLIM_PATH = DATA_DIR / "deals_slim.json"

# 우리 모델에 필요한 Pipedrive custom field keys
NEEDED_KEYS = {
    "d63b4b92c9490208976c2fdd430cb55ee558b15e": "apply_date",      # ✔ 신청일자
    "ae58f328fb0ae8dc48428ef1166271f087c89443": "payment_amount",   # 📍 결제금액
    "c86a14fb9b30df3535753929014a22cc4d44a1aa": "decision_amount",  # ✍ 결정 환급액
    "18f5d8f72f30db7d6abdc4aa862f64b9cb96409b": "apply_amount",    # ✔ 조회 환급액
    "84aa730356cdaeb238992c90f18eeef43beef0c8": "filing_date",      # ✔ 신고일자
    "ada49fdb068665ee3c437ab4c996fce9ed2cde79": "filing_amount",    # ✍ 신고 환급액
    "9a3b01b6f929c9f4ea9ee51e0523b405412850c2": "decision_date",    # ✍ 결정일자
    "897afed52d7b6a78a08599c33323d548218359ac": "payment_date",     # 💸 결제일자
    "3a7d95a418826be9d6facd9f660a8315bb6bb14a": "is_only_gam",     # 감면only 여부
}

# Pipedrive standard field keys to keep
STD_KEYS = {"id", "status", "pipeline_id", "update_time", "lost_reason", "lost_time"}


def http_get(path, params, retries=3, backoff=2.0):
    params = {**params, "api_token": API_TOKEN}
    url = f"{BASE_URL}{path}?{urllib.parse.urlencode(params)}"
    last_err = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "bznav-refund/1.0"})
            with urllib.request.urlopen(req, timeout=60) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            last_err = e
            if e.code == 429:
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


def fetch_pipeline_names():
    """pipeline_id → name 매핑."""
    data = http_get("/pipelines", {})
    return {p["id"]: p["name"] for p in (data.get("data") or [])}


def slim_deal(deal, pipe_names):
    """원본 deal에서 필요한 필드만 추출 → 경량 dict."""
    rec = {}
    for raw_key, var_name in NEEDED_KEYS.items():
        rec[var_name] = deal.get(raw_key)
    # standard fields (STD_KEYS 기반)
    for k in STD_KEYS:
        if k == "pipeline_id":
            rec["pipeline"] = pipe_names.get(deal.get(k), str(deal.get(k, "")))
        else:
            rec[k] = deal.get(k)
    return rec


def fetch_deals_slim(pipe_names, since=None):
    """deals를 페이지네이션으로 가져오되 즉시 slim 변환.

    incremental: DESC 정렬 → 오래된 건 만나면 즉시 중단 (전건 스캔 방지)
    full: ASC 정렬 → 전건 순회
    """
    mode = "incremental" if since else "full sync"
    print(f"[2/3] Fetching deals ({mode}) ...")
    all_deals = []
    start = 0
    LIMIT = 500
    page = 0
    sort_order = "update_time DESC" if since else "update_time ASC"
    while True:
        page += 1
        params = {
            "start": start,
            "limit": LIMIT,
            "status": "all_not_deleted",
            "sort": sort_order,
        }
        data = http_get("/deals", params)
        chunk = data.get("data") or []
        done = False
        for deal in chunk:
            if since and (deal.get("update_time") or "") < since:
                done = True
                break
            all_deals.append(slim_deal(deal, pipe_names))
        if done:
            break
        if page % 50 == 0:
            print(f"      page {page}: {len(all_deals):,} deals so far")
        pag = data.get("additional_data", {}).get("pagination", {})
        if not pag.get("more_items_in_collection"):
            break
        start = pag["next_start"]
        time.sleep(0.05)
    print(f"      total: {len(all_deals):,} deals")
    return all_deals


def merge_with_existing(new_deals):
    """기존 slim 데이터에 변경분 병합 (id 기준 upsert)."""
    if not SLIM_PATH.exists():
        return new_deals
    existing = json.loads(SLIM_PATH.read_text())
    by_id = {d["id"]: d for d in existing}
    updated = 0
    for d in new_deals:
        if d["id"] in by_id:
            updated += 1
        by_id[d["id"]] = d
    merged = list(by_id.values())
    print(f"      merged: {len(existing):,} existing + {len(new_deals):,} new/updated ({updated} updates) = {len(merged):,} total")
    return merged


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--full", action="store_true", help="전건 재추출 (기본: 증분)")
    args = parser.parse_args()

    print("[1/3] Fetching pipeline names ...")
    pipe_names = fetch_pipeline_names()
    print(f"      pipelines: {pipe_names}")

    since = None
    if not args.full and LAST_SYNC_FILE.exists():
        since = LAST_SYNC_FILE.read_text().strip()
        print(f"  Incremental mode (last sync: {since})")
    else:
        print("  Full sync mode")

    new_deals = fetch_deals_slim(pipe_names, since=since)

    if since and SLIM_PATH.exists():
        deals = merge_with_existing(new_deals)
    else:
        deals = new_deals

    print("[3/3] Writing output ...")
    SLIM_PATH.write_text(json.dumps(deals, ensure_ascii=False))
    size_mb = SLIM_PATH.stat().st_size / 1e6
    print(f"      → {SLIM_PATH} ({size_mb:.1f} MB)")

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    LAST_SYNC_FILE.write_text(now)
    print(f"      sync timestamp: {now}")
    print(f"\nDone. {len(deals):,} deals ({size_mb:.0f} MB — 기존 4.1GB 대비 {4100/max(size_mb,1):.0f}x 절감)")


if __name__ == "__main__":
    main()
