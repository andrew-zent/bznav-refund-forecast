"""스냅샷 + 시계열 생성.

forecast.json + field_catalog.json을 읽어서:
- output/snapshots/YYYY-MM-DD.json (단일 스냅샷, 상세)
- output/timeline.json (rolling history, 대시보드용)

매주 model.py 실행 후 자동 호출 (workflow)."""
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = ROOT / "output"
SNAPSHOT_DIR = OUTPUT_DIR / "snapshots"
SNAPSHOT_DIR.mkdir(exist_ok=True, parents=True)


def build_id_label_map(field_catalog):
    """(field_key, option_id str) → label."""
    m = {}
    for f in field_catalog.get("all_fields", []):
        key = f.get("key")
        for o in f.get("options_full") or []:
            oid = o.get("id")
            if oid is not None:
                m[(key, str(oid))] = o.get("label", "")
    return m


def translate(id_map, field_key, raw_value):
    if raw_value is None or raw_value in ("", "(미기재)", "None"):
        return "(미기재)"
    s = str(raw_value).strip()
    if "," in s:
        parts = [p.strip() for p in s.split(",")]
        return " + ".join(id_map.get((field_key, p), p) for p in parts)
    return id_map.get((field_key, s), s)


# 필드 key (개인 Pipedrive)
KEYS = {
    "cancel_reason": "ebdd813efc921dcb6a90be9156642c824589aced",
    "hold_reason": "430f49c344b73aaa29622d1fa50e33f75a79ad80",
    "hold_reason_2": "314ea8de88a7dda7349215ddc0975216b9662ad9",
    "customer_type": "0ec37f587ba626b05d5db916d9e2f185e47f1abc",
    "channel": "channel",
}


def sum_dim(diag, dim_key, months):
    totals = {}
    for k, rows in diag.get(dim_key, {}).items():
        apply_amt = sum(r["apply_amount"] for r in rows if r["month"] in months)
        n = sum(r["deal_count"] for r in rows if r["month"] in months)
        if apply_amt > 0 or n > 0:
            totals[k] = {"apply": apply_amt, "n": n}
    return sorted(totals.items(), key=lambda x: -x[1]["apply"])


def mature_months(current_label):
    """2024-11 ~ current-6 완성 코호트 월 리스트."""
    y, m = map(int, current_label.split("-"))
    cur_idx = y * 12 + (m - 1)
    # 가장 최근 완성월 = 현재월 -6
    end_idx = cur_idx - 6
    # 12개월 window
    start_idx = end_idx - 11
    return [f"{mi // 12:04d}-{mi % 12 + 1:02d}" for mi in range(start_idx, end_idx + 1)]


def dual_translate(id_map, raw):
    s = str(raw).strip()
    parts = [p.strip() for p in s.split(",")] if "," in s else [s]
    labels = []
    for p in parts:
        l = id_map.get((KEYS["hold_reason"], p)) or id_map.get((KEYS["hold_reason_2"], p)) or p
        labels.append(l)
    return " + ".join(labels)


def build_snapshot(forecast, field_catalog):
    id_map = build_id_label_map(field_catalog)
    diag = forecast.get("diagnostic_breakdown", {})

    current_label = forecast.get("data_range", "... ~ ?").split("~")[-1].strip()
    months = mature_months(current_label)

    # 코호트 전환율
    all_coh = forecast.get("apply_to_pay_cohort", {}).get("all", [])
    unfilt_coh = forecast.get("apply_to_pay_cohort", {}).get("unfiltered", [])

    def cohort_conversion(rows):
        mature = [r for r in rows if r["apply_month"] in months and r.get("apply_amount", 0) > 0]
        sa = sum(r["apply_amount"] for r in mature)
        sp = sum(r["paid_total"] for r in mature)
        return {
            "apply_sum": round(sa, 2),
            "paid_sum": round(sp, 2),
            "conversion_pct": round(sp / sa * 100, 2) if sa > 0 else 0,
            "months": f"{months[0]} ~ {months[-1]}",
            "n_months": len(months),
        }

    # Pipeline 분포
    pipe_totals = {}
    for name, rows in diag.get("by_pipeline", {}).items():
        sa = sum(r["apply_amount"] for r in rows if r["month"] in months)
        sp = sum(r["paid"] for r in rows if r["month"] in months)
        if sa > 0 or sp > 0:
            pipe_totals[name] = {"apply": round(sa, 1), "paid": round(sp, 2),
                                  "conv_pct": round(sp / sa * 100, 2) if sa > 0 else 0}

    # Top N
    def top_n(dim, n=10, translator=None):
        sorted_rows = sum_dim(diag, dim, months)
        result = []
        total = sum(v["apply"] for _, v in sorted_rows)
        for k, v in sorted_rows[:n]:
            label = translator(k) if translator else k
            result.append({
                "label": label,
                "raw_key": k,
                "apply_amount": round(v["apply"], 2),
                "deal_count": v["n"],
                "share_pct": round(v["apply"] / total * 100, 2) if total > 0 else 0,
            })
        return {"total": round(total, 2), "top": result}

    # Collection pool 최근 3개월 평균
    pool_trend = forecast.get("collection_pool_trend", [])
    recent_pool = pool_trend[-4:-1] if len(pool_trend) >= 4 else pool_trend[-3:] if len(pool_trend) >= 3 else pool_trend
    pool_3mo_avg_paid = round(
        sum(p.get("paid", 0) for p in recent_pool) / len(recent_pool), 2
    ) if recent_pool else 0

    snapshot = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "data_range": forecast.get("data_range"),
        "mature_window": {"start": months[0], "end": months[-1]},
        "conversion": {
            "filtered_all": cohort_conversion(all_coh),
            "unfiltered": cohort_conversion(unfilt_coh),
        },
        "pipeline_distribution": pipe_totals,
        "top_lost_reasons": top_n("by_lost_reason", 10),
        "top_cancel_reasons": top_n(
            "by_cancel_reason", 10,
            translator=lambda k: translate(id_map, KEYS["cancel_reason"], k),
        ),
        "top_hold_reasons": top_n(
            "by_hold_reason", 10,
            translator=lambda k: dual_translate(id_map, k),
        ),
        "top_customer_types": top_n(
            "by_customer_type", 10,
            translator=lambda k: translate(id_map, KEYS["customer_type"], k),
        ),
        "top_channels": top_n(
            "by_channel", 5,
            translator=lambda k: translate(id_map, KEYS["channel"], k),
        ),
        "top_utm_sources": top_n("by_utm_source", 10),
        "collection_pool": {
            "balance": forecast.get("collection_pool", {}).get("balance", 0),
            "utilization_rate_pct": forecast.get("collection_pool", {}).get("utilization_rate_pct", 0),
            "recent_3mo_avg_paid": pool_3mo_avg_paid,
        },
    }
    return snapshot


def condense_for_timeline(snap):
    """timeline.json용 축약 (대시보드 chart 데이터만)."""
    def top_shares(items, n=5):
        return [{"label": x["label"], "share": x["share_pct"]} for x in items.get("top", [])[:n]]
    return {
        "generated_at": snap["generated_at"],
        "data_range": snap["data_range"],
        "mature_window_end": snap["mature_window"]["end"],
        "conversion_filtered_pct": snap["conversion"]["filtered_all"]["conversion_pct"],
        "conversion_unfiltered_pct": snap["conversion"]["unfiltered"]["conversion_pct"],
        "pipeline_shares": {
            name: round(v["apply"] / max(sum(x["apply"] for x in snap["pipeline_distribution"].values()), 1) * 100, 2)
            for name, v in snap["pipeline_distribution"].items()
        },
        "pool_balance": snap["collection_pool"]["balance"],
        "pool_monthly_paid_3mo_avg": snap["collection_pool"]["recent_3mo_avg_paid"],
        "top_lost_reason_shares": top_shares(snap["top_lost_reasons"]),
        "top_cancel_reason_shares": top_shares(snap["top_cancel_reasons"]),
    }


def main():
    forecast_path = OUTPUT_DIR / "forecast.json"
    fc_path = OUTPUT_DIR / "field_catalog.json"
    if not forecast_path.exists():
        print("ERROR: output/forecast.json not found. Run model.py first.", file=sys.stderr)
        sys.exit(1)

    forecast = json.loads(forecast_path.read_text())
    field_catalog = json.loads(fc_path.read_text()) if fc_path.exists() else {"all_fields": []}

    snapshot = build_snapshot(forecast, field_catalog)

    # 1. 상세 스냅샷 저장 (일자별)
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    snap_path = SNAPSHOT_DIR / f"{date_str}.json"
    snap_path.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2))
    print(f"→ {snap_path} ({snap_path.stat().st_size / 1024:.1f} KB)")

    # 2. timeline.json 업데이트 (append)
    timeline_path = OUTPUT_DIR / "timeline.json"
    if timeline_path.exists():
        timeline = json.loads(timeline_path.read_text())
    else:
        timeline = {"entries": []}

    condensed = condense_for_timeline(snapshot)
    condensed["date"] = date_str

    # 같은 날짜 entry 있으면 교체
    entries = [e for e in timeline["entries"] if e.get("date") != date_str]
    entries.append(condensed)
    # date 기준 정렬
    entries.sort(key=lambda e: e.get("date", ""))
    timeline["entries"] = entries
    timeline["updated_at"] = snapshot["generated_at"]

    timeline_path.write_text(json.dumps(timeline, ensure_ascii=False, indent=2))
    print(f"→ {timeline_path} ({len(entries)} entries)")


if __name__ == "__main__":
    main()
