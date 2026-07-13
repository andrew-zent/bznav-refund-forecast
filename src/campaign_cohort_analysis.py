"""
캠페인 코호트 추적 — utm_campaign(캠페인 식별값) 단위로 주간 코호트 신청/취소율을
집계하고, 과거 트레일링 기준선(baseline) 대비 최근 구간의 이탈을 감지한다.

[Z21-RPT] 환급 IMC·Paid 서비스운영 영향도 감지 방법론 —
B(추세모니터링+기준선) + D(캠페인 코호트 태깅) 조합의 1차 구현.
VOC는 별도 소스(채널톡 API) 연동 예정 — 이 스크립트는 Pipedrive 신청/취소만 다룬다.

입력: data/deals_slim.json (extract_pipedrive.py 전건 추출 결과, 개인 정기 파이프라인만 사용)
출력: output/campaign_cohort_analysis.json

CLI: python src/campaign_cohort_analysis.py
"""
from __future__ import annotations
import json
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data" / "deals_slim.json"
OUT = ROOT / "output" / "campaign_cohort_analysis.json"

PIPELINE_REGULAR = "B(젠트)-환급"

# 임계값 — 실측 데이터 없이 잡은 초기값. 운영하면서 조정 필요.
MIN_DEALS_PER_WEEK = 10        # 신뢰 가능한 최소 신청 건수 (baseline/current 구간 합산 기준)
MATURATION_DAYS = 30           # 취소 여부가 대부분 확정되는 신청 후 경과일
BASELINE_WEEKS = 8             # 기준선으로 삼을 트레일링 과거 주 수
CANCEL_RATE_DELTA_WARN = 5.0   # 기준선 대비 취소율 상승 경보 임계값(%p)
VOLUME_DROP_WARN_PCT = -30.0   # 기준선 대비 주당 신청량 급감 경보 임계값(%)


def parse_date(v):
    if not v:
        return None
    s = str(v).strip()[:10]
    for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    return None


def cancel_date(d):
    """취소 이벤트 날짜 — 취소요청일 우선, 없으면 lost_time(+cancel_reason 존재) fallback."""
    cd = parse_date(d.get("cancel_request_date"))
    if cd:
        return cd
    if str(d.get("status")) == "lost" and d.get("cancel_reason"):
        return parse_date(d.get("lost_time"))
    return None


def campaign_key(d):
    camp = (d.get("utm_campaign") or "").strip()
    if camp:
        return camp
    src = (d.get("utm_source") or "").strip()
    return f"(no-campaign)/{src or '(none)'}"


def week_start(d: date) -> date:
    return d - timedelta(days=d.weekday())  # 월요일 기준 ISO 주


def load_deals():
    if not DATA.exists():
        print(f"ERROR: {DATA} 없음. python src/extract_pipedrive.py --full 먼저 실행하세요.", file=sys.stderr)
        sys.exit(1)
    deals = json.loads(DATA.read_text())
    return [d for d in deals if str(d.get("pipeline", "")) == PIPELINE_REGULAR]


def build_weekly_cohorts(deals):
    """campaign → week_start(iso) → {applied, cancelled, apply_amt}"""
    cohorts: dict[str, dict[str, dict]] = defaultdict(
        lambda: defaultdict(lambda: {"applied": 0, "cancelled": 0, "apply_amt": 0.0})
    )
    for d in deals:
        ad = parse_date(d.get("apply_date"))
        if not ad:
            continue
        camp = campaign_key(d)
        wk = week_start(ad).isoformat()
        bucket = cohorts[camp][wk]
        bucket["applied"] += 1
        bucket["apply_amt"] += float(d.get("apply_amount") or 0)
        if cancel_date(d) is not None:
            bucket["cancelled"] += 1
    return cohorts


def summarize_campaign(camp: str, weeks: dict, today: date):
    mature_weeks = sorted(
        wk for wk in weeks
        if (today - date.fromisoformat(wk)).days >= MATURATION_DAYS
    )
    if len(mature_weeks) < 2:
        return None

    # 최근 성숙 구간(current) vs 그 이전 트레일링 기준선(baseline)
    current_weeks = mature_weeks[-2:]
    baseline_weeks = mature_weeks[-(2 + BASELINE_WEEKS):-2] or mature_weeks[:-2]
    if not baseline_weeks:
        return None

    def agg(wk_list):
        applied = sum(weeks[w]["applied"] for w in wk_list)
        cancelled = sum(weeks[w]["cancelled"] for w in wk_list)
        apply_amt = sum(weeks[w]["apply_amt"] for w in wk_list)
        return applied, cancelled, apply_amt

    b_applied, b_cancelled, b_amt = agg(baseline_weeks)
    c_applied, c_cancelled, c_amt = agg(current_weeks)

    if b_applied < MIN_DEALS_PER_WEEK or c_applied < MIN_DEALS_PER_WEEK:
        return None

    b_weekly_avg = b_applied / len(baseline_weeks)
    c_weekly_avg = c_applied / len(current_weeks)
    b_cancel_rate = 100 * b_cancelled / b_applied
    c_cancel_rate = 100 * c_cancelled / c_applied
    cancel_delta = round(c_cancel_rate - b_cancel_rate, 1)
    volume_delta_pct = round(100 * (c_weekly_avg - b_weekly_avg) / b_weekly_avg, 1) if b_weekly_avg else None

    flags = []
    if cancel_delta >= CANCEL_RATE_DELTA_WARN:
        flags.append(f"취소율 기준선 대비 +{cancel_delta}%p 상승")
    if volume_delta_pct is not None and volume_delta_pct <= VOLUME_DROP_WARN_PCT:
        flags.append(f"신청량 기준선 대비 {volume_delta_pct}% 급감")

    return {
        "campaign": camp,
        "baseline_weeks": baseline_weeks,
        "current_weeks": current_weeks,
        "baseline_applied": b_applied,
        "current_applied": c_applied,
        "baseline_weekly_avg": round(b_weekly_avg, 1),
        "current_weekly_avg": round(c_weekly_avg, 1),
        "baseline_cancel_rate_pct": round(b_cancel_rate, 1),
        "current_cancel_rate_pct": round(c_cancel_rate, 1),
        "cancel_rate_delta_ppt": cancel_delta,
        "volume_delta_pct": volume_delta_pct,
        "baseline_apply_oku": round(b_amt / 1e8, 2),
        "current_apply_oku": round(c_amt / 1e8, 2),
        "flags": flags,
    }


def run():
    deals = load_deals()
    today = date.today()
    cohorts = build_weekly_cohorts(deals)

    results = []
    for camp, weeks in cohorts.items():
        s = summarize_campaign(camp, weeks, today)
        if s:
            results.append(s)

    results.sort(key=lambda r: r["current_apply_oku"], reverse=True)
    flagged = [r for r in results if r["flags"]]

    out = {
        "generated_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "as_of": today.isoformat(),
        "params": {
            "maturation_days": MATURATION_DAYS,
            "baseline_weeks": BASELINE_WEEKS,
            "min_deals_per_week": MIN_DEALS_PER_WEEK,
            "cancel_rate_delta_warn_ppt": CANCEL_RATE_DELTA_WARN,
            "volume_drop_warn_pct": VOLUME_DROP_WARN_PCT,
        },
        "note": (
            "캠페인 식별값(utm_campaign)이 없는 딜은 utm_source로 대체 그룹핑. "
            "취소율은 신청 후 코호트 성숙 기준(MATURATION_DAYS일) 이상 경과한 주만 집계 — "
            "최근 미성숙 주는 취소가 아직 발생하지 않았을 뿐일 수 있어 제외. "
            "VOC 신호는 미포함 (채널톡 API 연동 후 별도 스크립트에서 결합 예정)."
        ),
        "campaigns": results,
        "flagged": flagged,
    }
    OUT.parent.mkdir(exist_ok=True)
    OUT.write_text(json.dumps(out, ensure_ascii=False, indent=2))
    print(f"campaigns analyzed: {len(results)}, flagged: {len(flagged)}")
    for r in flagged[:20]:
        print(
            f"  [{r['campaign']}] {', '.join(r['flags'])} "
            f"(취소율 {r['baseline_cancel_rate_pct']}% → {r['current_cancel_rate_pct']}%, n={r['current_applied']})"
        )
    print(f"\n→ {OUT}")
    return out


if __name__ == "__main__":
    run()
