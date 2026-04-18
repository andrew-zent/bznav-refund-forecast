"""
Phase 1: Watcher Agent — 데이터 품질 감시.

추출된 deals 데이터의 품질을 검증하고 이상 시 알림.
독립 실행: python src/agents/watcher.py [deals_slim.json 경로]
"""
import json
import sys
from pathlib import Path
from datetime import datetime

import numpy as np

# 임계값
THRESHOLDS = {
    "min_deal_count": 200_000,       # 개인 최소 건수
    "min_corp_deal_count": 5_000,    # 법인 최소 건수
    "max_null_rate": 0.05,           # 필수 필드 null 허용률 5%
    "zscore_threshold": 2.5,         # 월별 금액 이상치 Z-score
    "min_monthly_amount": 5e8,       # 월 최소 금액 (5억)
    "max_amount_single": 1e10,       # 단건 최대 금액 (100억)
}

REQUIRED_FIELDS = ["apply_date", "status", "pipeline"]
AMOUNT_FIELDS = ["apply_amount", "filing_amount", "decision_amount", "payment_amount"]
DATE_FIELDS = ["apply_date", "filing_date", "decision_date", "payment_date"]


def check_deal_count(deals: list[dict], min_count: int) -> dict:
    """건수 검증."""
    count = len(deals)
    ok = count >= min_count
    return {
        "check": "건수 검증",
        "ok": ok,
        "detail": f"{count:,}건 (최소 {min_count:,}건)",
        "value": count,
    }


def check_null_rates(deals: list[dict]) -> list[dict]:
    """필수 필드 null 비율 검증."""
    results = []
    n = len(deals)
    if n == 0:
        return [{"check": "null 검증", "ok": False, "detail": "데이터 없음"}]

    for field in REQUIRED_FIELDS + DATE_FIELDS[:1]:  # apply_date는 필수
        null_count = sum(1 for d in deals if not d.get(field))
        rate = null_count / n
        ok = rate <= THRESHOLDS["max_null_rate"]
        results.append({
            "check": f"null율: {field}",
            "ok": ok,
            "detail": f"{rate:.1%} ({null_count:,}/{n:,}건)",
            "value": rate,
        })
    return results


def check_amount_outliers(deals: list[dict]) -> list[dict]:
    """단건 금액 이상치 탐지."""
    results = []
    max_amt = THRESHOLDS["max_amount_single"]

    for field in AMOUNT_FIELDS:
        values = [_to_num(d.get(field)) for d in deals if d.get(field)]
        if not values:
            continue
        outliers = [v for v in values if v > max_amt]
        ok = len(outliers) == 0
        results.append({
            "check": f"금액 이상치: {field}",
            "ok": ok,
            "detail": f"{len(outliers)}건 > {max_amt/1e8:.0f}억" if outliers else "정상",
            "value": len(outliers),
        })
    return results


def check_monthly_distribution(deals: list[dict], lookback_months: int = 12) -> list[dict]:
    """최근 월별 금액 분포 Z-score 이상 감지."""
    from collections import defaultdict

    monthly = defaultdict(float)
    for d in deals:
        ad = d.get("apply_date")
        amt = _to_num(d.get("apply_amount"))
        if ad and amt > 0:
            ym = str(ad)[:7]  # "2026-04"
            monthly[ym] += amt

    if len(monthly) < 6:
        return [{"check": "월별 분포", "ok": True, "detail": "데이터 부족 (6개월 미만)"}]

    sorted_months = sorted(monthly.keys())
    recent = sorted_months[-lookback_months:]
    values = [monthly[m] for m in recent]

    if len(values) < 3:
        return [{"check": "월별 분포", "ok": True, "detail": "비교 데이터 부족"}]

    mean = float(np.mean(values[:-1]))
    std = float(np.std(values[:-1]))
    latest = values[-1]
    latest_month = recent[-1]

    results = []
    if std > 0:
        zscore = (latest - mean) / std
        ok = abs(zscore) <= THRESHOLDS["zscore_threshold"]
        results.append({
            "check": f"월별 분포: {latest_month}",
            "ok": ok,
            "detail": f"Z={zscore:+.2f} ({latest/1e8:.1f}억, 평균 {mean/1e8:.1f}억±{std/1e8:.1f}억)",
            "value": zscore,
        })
    else:
        results.append({"check": f"월별 분포: {latest_month}", "ok": True, "detail": "표준편차 0"})

    # 최소 금액 검증
    low_months = [(m, monthly[m]) for m in recent if monthly[m] < THRESHOLDS["min_monthly_amount"]]
    if low_months:
        for m, v in low_months:
            results.append({
                "check": f"월 최소금액: {m}",
                "ok": False,
                "detail": f"{v/1e8:.2f}억 < {THRESHOLDS['min_monthly_amount']/1e8:.0f}억",
                "value": v,
            })
    return results


def check_schema_fields(deals: list[dict]) -> dict:
    """필드 스키마 일관성 검증."""
    expected = set(REQUIRED_FIELDS + AMOUNT_FIELDS + DATE_FIELDS + ["is_only_gam"])
    if not deals:
        return {"check": "스키마 검증", "ok": False, "detail": "데이터 없음"}

    actual = set(deals[0].keys())
    missing = expected - actual
    ok = len(missing) == 0
    return {
        "check": "스키마 검증",
        "ok": ok,
        "detail": f"누락: {missing}" if missing else f"정상 ({len(actual)}개 필드)",
        "value": list(missing),
    }


def run_all_checks(deals, min_count=None):
    """전체 검증 실행. 결과 딕셔너리 반환."""
    if min_count is None:
        min_count = THRESHOLDS["min_deal_count"]

    results = []
    results.append(check_deal_count(deals, min_count))
    results.append(check_schema_fields(deals))
    results.extend(check_null_rates(deals))
    results.extend(check_amount_outliers(deals))
    results.extend(check_monthly_distribution(deals))

    failures = [r for r in results if not r["ok"]]
    severity = "critical" if len(failures) >= 3 else "warn" if failures else "info"

    return {
        "agent": "watcher",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "total_checks": len(results),
        "passed": len(results) - len(failures),
        "failed": len(failures),
        "severity": severity,
        "results": results,
    }


def _to_num(v) -> float:
    if v is None or v == "":
        return 0.0
    try:
        return float(v)
    except (ValueError, TypeError):
        return 0.0


def main():
    """CLI 진입점."""
    root = Path(__file__).resolve().parent.parent.parent
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else root / "data" / "deals_slim.json"

    if not path.exists():
        print(f"ERROR: {path} not found")
        sys.exit(1)

    deals = json.loads(path.read_text())
    print(f"Watcher: checking {len(deals):,} deals from {path.name}")

    report = run_all_checks(deals)

    print(f"\n[결과] {report['passed']}/{report['total_checks']} passed (severity: {report['severity']})")
    for r in report["results"]:
        tag = "✅" if r["ok"] else "❌"
        print(f"  {tag} {r['check']}: {r['detail']}")

    # 알림
    if report["severity"] != "info":
        try:
            from agents.alerts import format_report, send_slack
        except ModuleNotFoundError:
            sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
            from agents.alerts import format_report, send_slack
        msg = format_report("Watcher 데이터 품질 검증", report["results"], report["severity"])
        send_slack(msg, report["severity"])

    return report


if __name__ == "__main__":
    main()
