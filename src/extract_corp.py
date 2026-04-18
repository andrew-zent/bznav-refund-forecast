"""
법인 Pipedrive API에서 deals를 추출하여 개인과 동일한 slim 포맷으로 저장.

차이점 (개인 대비):
  - 별도 API 토큰/도메인 (CORP_PIPEDRIVE_API_TOKEN, CORP_PIPEDRIVE_DOMAIN)
  - 필드 key가 다름
  - 결제금액 = 결정환급액 × 최종수수료율 (헥토 필드 거의 비어있음)
  - 파이프라인: 법인(1), 법인-추심(4), 법인-취소(3)

출력:
  data/deals_corp_slim.json
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

API_TOKEN = os.environ.get("CORP_PIPEDRIVE_API_TOKEN")
DOMAIN = os.environ.get("CORP_PIPEDRIVE_DOMAIN", "api")

if not API_TOKEN:
    print("ERROR: CORP_PIPEDRIVE_API_TOKEN 환경변수를 설정하세요.", file=sys.stderr)
    sys.exit(1)

BASE_URL = f"https://{DOMAIN}.pipedrive.com/api/v1"
DATA_DIR = Path(__file__).resolve().parent.parent / "data"
DATA_DIR.mkdir(exist_ok=True, parents=True)
LAST_SYNC_FILE = DATA_DIR / ".last_sync_corp.txt"
SLIM_PATH = DATA_DIR / "deals_corp_slim.json"

# 법인 Pipedrive 필드 매핑
NEEDED_KEYS = {
    "763fdb0697376420280693965f28c4123dd72d5b": "apply_date",       # ✔ 신청일자
    "ea8d36f6b730cc7f872d6de4d4bf9425785b8729": "filing_date",      # ✔ 신고일자
    "4b9446b4c68d81bab6f598b66aa5061d0baa194c": "decision_amount",  # *결정 환급액
    "dd68046f0536492917e28d1f03b0e0549e44c813": "payment_date",     # 💸 결제일자
    "a416fad6ab7cbc198dfd5b05abe8a1f757c42eb3": "apply_amount",     # ✔ 환급액(zent) — 조회 환급액 대용
    "83a0cc7ad74ae684f6bbeb3133eb2866dad677b0": "filing_amount",    # *검토환급액 — 신고 환급액 대용
    "30adbcf37881a14b5a01952994fe8be1b473a5ea": "fee_rate",         # *최종 수수료율
    "57b3e5836348d6639373859c08ff7b916e8c75c9": "hecto_account",    # 헥토계좌 결제금액
    "8f3e1c66651fdabdeadbf78bad9777345312b334": "hecto_card",       # 헥토카드 결제금액
    # 실패/취소 진단용
    "dbf353faf17cf72c0bf336a994d945f7eec4a803": "cancel_reason",    # 신청취소사유 (varchar)
    # 유입 채널 추적 - 신청 시점
    "0008b09a5d8a125726edf3640f0307b3d9cfac89": "utm_source",
    "daca31f5cef6433f0a8142d534a5358dac5419a3": "utm_medium",
    "18ca8b6cdce3a7d3a4d444121ec0bff82d967ea0": "utm_campaign",
    # 유입 채널 추적 - 조회 시점
    "495d87ec4af535181a7c2b362103b3f3db713650": "utm_source_query",
    "87d7e5f7419b91917a3e6989c1a17cee2736336c": "utm_medium_query",
    "343320bc84878113458a3b7bc9a75c10fb6e9a2a": "utm_campaign_query",
}

STD_KEYS = {"id", "status", "pipeline_id", "update_time", "lost_reason", "lost_time", "channel", "channel_id"}

# 법인 파이프라인 이름 매핑 (개인과 통일된 네이밍)
PIPE_NAME_MAP = {
    "법인": "법인-환급",
    "법인-추심": "법인-추심",
    "법인-취소": "법인-취소",
    "테스트": "테스트",
}


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
    data = http_get("/pipelines", {})
    raw = {p["id"]: p["name"] for p in (data.get("data") or [])}
    # 개인과 통일된 이름으로 매핑
    return {pid: PIPE_NAME_MAP.get(name, name) for pid, name in raw.items()}


def slim_deal(deal, pipe_names):
    """법인 deal → 개인과 동일한 slim 포맷 변환."""
    rec = {}
    for raw_key, var_name in NEEDED_KEYS.items():
        val = deal.get(raw_key)
        # monetary 타입은 숫자만 추출
        if isinstance(val, str) and "," in val:
            val = val.replace(",", "")
        rec[var_name] = val

    # 결제금액 산출: 헥토 > 결정환급액 × 수수료율
    hecto = 0
    for k in ("hecto_account", "hecto_card"):
        v = rec.pop(k, None)
        if v:
            try:
                hecto += float(v)
            except (ValueError, TypeError):
                pass

    if hecto > 0:
        rec["payment_amount"] = hecto
    else:
        dec = 0
        fee = 0
        try:
            dec = float(rec.get("decision_amount") or 0)
        except (ValueError, TypeError):
            pass
        try:
            fee = float(rec.pop("fee_rate", None) or 30) / 100  # 기본 30%
        except (ValueError, TypeError):
            fee = 0.30
        rec["payment_amount"] = dec * fee if dec > 0 else 0

    # fee_rate 제거 (이미 payment_amount에 반영)
    rec.pop("fee_rate", None)

    # standard fields (STD_KEYS 기반)
    for k in STD_KEYS:
        if k == "pipeline_id":
            rec["pipeline"] = pipe_names.get(deal.get(k), str(deal.get(k, "")))
        else:
            rec[k] = deal.get(k)
    rec["source"] = "corp"  # 개인과 구분
    return rec


def fetch_deals_slim(pipe_names, since=None):
    mode = "incremental" if since else "full sync"
    print(f"[2/3] Fetching corp deals ({mode}) ...")
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
        if page % 10 == 0:
            print(f"      page {page}: {len(all_deals):,} deals so far")
        pag = data.get("additional_data", {}).get("pagination", {})
        if not pag.get("more_items_in_collection"):
            break
        start = pag["next_start"]
        time.sleep(0.05)
    print(f"      total: {len(all_deals):,} deals")
    return all_deals


def merge_with_existing(new_deals):
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
    print(f"      merged: {len(existing):,} + {len(new_deals):,} ({updated} updates) = {len(merged):,} total")
    return merged


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--full", action="store_true")
    args = parser.parse_args()

    print("[1/3] Fetching corp pipeline names ...")
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
    print(f"\nDone. {len(deals):,} corp deals ({size_mb:.1f} MB)")


if __name__ == "__main__":
    main()
