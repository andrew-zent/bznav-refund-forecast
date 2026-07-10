"""Agent 시스템 전체 테스트."""
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# src를 import path에 추가
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from agents.alerts import format_report, SEVERITY_EMOJI
from agents.watcher import (
    check_deal_count, check_null_rates, check_amount_outliers,
    check_monthly_distribution, check_schema_fields, run_all_checks,
    THRESHOLDS,
)
from agents.verifier import (
    check_mape, check_backtest_bias, check_backtest_outliers,
    check_distribution_stability, check_pool_health, check_forecast_range,
    run_all_checks as verifier_run_all,
)
from agents.orchestrator import (
    StepStatus, ErrorPolicy, StepResult, PipelineState, PIPELINE_STEPS,
)
from agents.filing_rate_monitor import (
    daily_flow, backlog_aging, cohort_conversion, cancel_reason_breakdown,
    citation_stats, hold_reason_breakdown, run_all_checks as filing_rate_run_all,
)
from snapshot import build_id_label_map


# ── 테스트 픽스처 ─────────────────────────────────────

def make_deals(n=1000, with_nulls=False, with_outlier=False):
    """테스트용 deals 생성."""
    deals = []
    for i in range(n):
        month = f"2026-{(i % 12) + 1:02d}-15"
        d = {
            "apply_date": month,
            "filing_date": month,
            "decision_date": month,
            "payment_date": month if i % 3 == 0 else None,
            "apply_amount": 5_000_000 + (i * 1000),
            "filing_amount": 4_000_000 + (i * 1000),
            "decision_amount": 3_500_000 + (i * 1000),
            "payment_amount": 1_000_000 if i % 3 == 0 else 0,
            "status": "진행 중",
            "pipeline": "B(젠트)-환급",
            "is_only_gam": None,
        }
        if with_nulls and i < 10:
            d["apply_date"] = None
            d["status"] = None
        if with_outlier and i == 0:
            d["apply_amount"] = 20_000_000_000  # 200억 (이상치)
        deals.append(d)
    return deals


def make_forecast():
    """테스트용 forecast.json 데이터."""
    return {
        "generated_at": "2026-04-14T06:00:00Z",
        "total_claims": 233236,
        "total_corp_claims": 8174,
        "mape": 5.73,
        "distributions": {
            "a2f": {"0": 29.02, "1": 29.31, "2": 1.76, "3": 0.75, "4": 0.26},
            "f2d": {"0": 14.22, "1": 41.79, "2": 37.98, "3": 3.16, "4": 2.08},
            "d2p": {"0": 18.61, "1": 14.29, "2": 1.54, "3": 0.07},
        },
        "collection_pool": {
            "balance": 149.4,
            "utilization_rate": 1.109,
            "monthly_delta": -0.9,
        },
        "forecast": [
            {"month": "2026-04", "individual": {"regular": 14.39, "collection": 1.66,
             "total": 16.05, "season_adj": 0, "adjusted": 16.05}, "corporate": {"total": 0.245}, "grand_total": 16.30},
            {"month": "2026-05", "individual": {"regular": 16.45, "collection": 1.65,
             "total": 18.10, "season_adj": 0, "adjusted": 18.10}, "corporate": {"total": 0.24}, "grand_total": 18.34},
        ],
        "backtest": [
            {"month": "2025-04", "actual": 16.93, "predicted": 16.11, "error_pct": -4.8},
            {"month": "2025-05", "actual": 12.10, "predicted": 12.14, "error_pct": 0.3},
            {"month": "2025-06", "actual": 14.91, "predicted": 13.26, "error_pct": -11.1},
            {"month": "2025-07", "actual": 15.69, "predicted": 14.73, "error_pct": -6.1},
            {"month": "2025-08", "actual": 12.33, "predicted": 13.93, "error_pct": 13.0},
            {"month": "2025-09", "actual": 17.51, "predicted": 17.08, "error_pct": -2.5},
        ],
    }


# ── Alerts 테스트 ─────────────────────────────────────

def test_format_report():
    items = [
        {"check": "건수", "ok": True, "detail": "1000건"},
        {"check": "null", "ok": False, "detail": "10% 초과"},
    ]
    msg = format_report("테스트", items, "warn")
    assert "⚠️" in msg
    assert "테스트" in msg
    assert "✅" in msg
    assert "❌" in msg
    print("  ✅ test_format_report")


def test_severity_emoji():
    assert "info" in SEVERITY_EMOJI
    assert "warn" in SEVERITY_EMOJI
    assert "critical" in SEVERITY_EMOJI
    print("  ✅ test_severity_emoji")


# ── Watcher 테스트 ────────────────────────────────────

def test_check_deal_count():
    deals = make_deals(1000)
    r = check_deal_count(deals, 500)
    assert r["ok"] is True
    r = check_deal_count(deals, 2000)
    assert r["ok"] is False
    print("  ✅ test_check_deal_count")


def test_check_null_rates():
    deals = make_deals(100, with_nulls=True)
    results = check_null_rates(deals)
    assert len(results) > 0
    # 10/100 = 10% null > 5% threshold for apply_date
    null_result = [r for r in results if "apply_date" in r["check"]][0]
    assert null_result["ok"] is False
    print("  ✅ test_check_null_rates")


def test_check_null_rates_clean():
    deals = make_deals(100)
    results = check_null_rates(deals)
    for r in results:
        assert r["ok"] is True
    print("  ✅ test_check_null_rates_clean")


def test_check_amount_outliers():
    deals = make_deals(100, with_outlier=True)
    results = check_amount_outliers(deals)
    outlier_found = any(not r["ok"] for r in results)
    assert outlier_found, "Should detect 200억 outlier"
    print("  ✅ test_check_amount_outliers")


def test_check_amount_outliers_clean():
    deals = make_deals(100)
    results = check_amount_outliers(deals)
    for r in results:
        assert r["ok"] is True
    print("  ✅ test_check_amount_outliers_clean")


def test_check_schema_fields():
    deals = make_deals(10)
    r = check_schema_fields(deals)
    assert r["ok"] is True

    # 필드 누락
    deals_bad = [{"apply_date": "2026-01-01"}]
    r = check_schema_fields(deals_bad)
    assert r["ok"] is False
    print("  ✅ test_check_schema_fields")


def test_watcher_run_all():
    deals = make_deals(500)
    report = run_all_checks(deals, min_count=100)
    assert report["agent"] == "watcher"
    assert report["total_checks"] > 0
    assert "severity" in report
    print(f"  ✅ test_watcher_run_all ({report['passed']}/{report['total_checks']} passed)")


# ── Filing Rate Monitor 테스트 ─────────────────────────

def make_filing_deals(today):
    """테스트용 신고율 관리 deals 생성 (today 기준 상대 날짜)."""
    def d(days_ago):
        return str(today - timedelta(days=days_ago))

    deals = [
        # 어제 신청/신고완료/취소 각 1건
        {"pipeline": "B(젠트)-환급", "apply_date": d(40), "filing_date": d(1), "status": "won"},
        {"pipeline": "B(젠트)-환급", "apply_date": d(1), "filing_date": None, "status": "open"},
        {"pipeline": "B(젠트)-환급", "apply_date": d(35), "filing_date": None,
         "cancel_request_date": d(1), "cancel_reason": "106", "status": "lost"},
        # 성숙 코호트(45~75일 전): filed 4 / cancelled 2 / pending 1
        *[{"pipeline": "B(젠트)-환급", "apply_date": d(60), "filing_date": d(30), "status": "won"} for _ in range(4)],
        *[{"pipeline": "B(젠트)-환급", "apply_date": d(60), "filing_date": None,
           "cancel_request_date": d(20), "cancel_reason": "107", "status": "lost"} for _ in range(2)],
        {"pipeline": "B(젠트)-환급", "apply_date": d(60), "filing_date": None, "status": "open"},
        # 백로그 에이징 + 보류
        {"pipeline": "B(젠트)-환급", "apply_date": d(90), "filing_date": None, "status": "open",
         "hold_status": "9001", "hold_reason": "5001", "hold_activity_date": d(15)},
        # 인용확인: 완료 1건 + 기한 경과 미확인 2건 (상태 다름)
        {"pipeline": "B(젠트)-환급", "apply_date": d(200), "filing_date": d(150),
         "decision_date": d(100), "citation_confirmed_date": d(1)},
        {"pipeline": "B(젠트)-환급", "apply_date": d(200), "filing_date": d(150),
         "decision_date": d(100), "citation_due_date": d(5), "citation_status": "1507"},
        {"pipeline": "B(젠트)-환급", "apply_date": d(200), "filing_date": d(150),
         "decision_date": d(100), "citation_due_date": d(5), "citation_status": "598"},
        # 다른 파이프라인 — 제외되어야 함
        {"pipeline": "C(젠트)-추심", "apply_date": d(1), "filing_date": d(1)},
    ]
    return deals


def make_filing_field_catalog():
    return {
        "all_fields": [
            {"key": "ebdd813efc921dcb6a90be9156642c824589aced",
             "options_full": [{"id": 106, "label": "기존 세무대리인"}, {"id": 107, "label": "수수료"}]},
            {"key": "430f49c344b73aaa29622d1fa50e33f75a79ad80",
             "options_full": [{"id": 5001, "label": "환급액 변동"}]},
            {"key": "6a4c5816ff87fa993ea6c4affe4ce82636b09714",
             "options_full": [{"id": 9001, "label": "보류 중"}, {"id": 9002, "label": "보류 완료"}]},
            {"key": "8e057c4b5b8a2a57e4ad2579c150b197f1017506",
             "options_full": [{"id": 1507, "label": "세무서 비협조"}, {"id": 598, "label": "대응 필요"}]},
        ]
    }


def test_filing_rate_daily_flow():
    now = datetime.now(timezone.utc)
    today = now.date()
    deals = make_filing_deals(today)
    report_date = today - timedelta(days=1)
    flow = daily_flow(deals, report_date)
    # 다른 파이프라인 필터링은 run_all_checks에서 처리되므로 여기선 raw deals 그대로 집계됨
    assert flow["applied"] >= 1
    print("  ✅ test_filing_rate_daily_flow")


def test_filing_rate_cohort_conversion():
    today = datetime.now(timezone.utc).date()
    deals = make_filing_deals(today)
    conv = cohort_conversion(deals, today)
    assert conv["n"] == 7
    assert conv["filed"] == 4
    assert conv["cancelled"] == 2
    assert conv["pending"] == 1
    print("  ✅ test_filing_rate_cohort_conversion")


def test_filing_rate_cancel_reason_breakdown():
    today = datetime.now(timezone.utc).date()
    deals = make_filing_deals(today)
    catalog = make_filing_field_catalog()
    id_map = build_id_label_map(catalog)
    result = cancel_reason_breakdown(deals, id_map, today)
    assert result["total"] == 3  # 어제 1건 + 성숙 코호트 2건
    labels = {r["reason"] for r in result["top_reasons"]}
    assert labels == {"기존 세무대리인", "수수료"}
    print("  ✅ test_filing_rate_cancel_reason_breakdown")


def test_filing_rate_citation_stats():
    today = datetime.now(timezone.utc).date()
    deals = make_filing_deals(today)
    catalog = make_filing_field_catalog()
    id_map = build_id_label_map(catalog)
    report_date = today - timedelta(days=1)
    cite = citation_stats(deals, id_map, report_date, today)
    assert cite["confirmed_total"] == 1
    assert cite["confirmed_today"] == 1
    assert cite["sla_overdue"] == 2
    print("  ✅ test_filing_rate_citation_stats")


def test_filing_rate_hold_reason_breakdown():
    today = datetime.now(timezone.utc).date()
    deals = make_filing_deals(today)
    catalog = make_filing_field_catalog()
    id_map = build_id_label_map(catalog)
    result = hold_reason_breakdown(deals, id_map, today)
    assert result["total_on_hold"] == 1
    assert result["top_reasons"][0]["reason"] == "환급액 변동"
    assert result["oldest_hold_days"] == 15
    print("  ✅ test_filing_rate_hold_reason_breakdown")


def test_filing_rate_run_all():
    now = datetime.now(timezone.utc)
    today = now.date()
    deals = make_filing_deals(today)
    catalog = make_filing_field_catalog()
    report = filing_rate_run_all(deals, catalog, as_of=now)

    assert report["agent"] == "filing_rate_monitor"
    assert report["total_checks"] > 0
    assert report["severity"] in ("info", "warn", "critical")

    # 다른 파이프라인(C(젠트)-추심) 제외 확인: 어제 신고완료는 1건만 (B파이프라인)
    assert report["daily_flow"]["filed"] == 1
    assert report["daily_flow"]["cancelled"] == 1

    # 성숙 코호트: 7건 중 filed 4, cancelled 2, pending 1
    conv = report["cohort_conversion"]
    assert conv["n"] == 7
    assert conv["filed"] == 4
    assert conv["cancelled"] == 2
    assert conv["pending"] == 1

    # 취소 사유 id → label 변환 확인
    reasons = {r["reason"] for r in report["cancel_reasons"]["top_reasons"]}
    assert "수수료" in reasons or "기존 세무대리인" in reasons

    # 보류 사유 브레이크다운
    assert report["hold_summary"]["total_on_hold"] == 1
    assert report["hold_summary"]["top_reasons"][0]["reason"] == "환급액 변동"
    assert report["hold_summary"]["oldest_hold_days"] == 15

    # 인용확인: 오늘/누적 1건, 기한경과 미확인 2건, 상태별 2건
    cite = report["citation"]
    assert cite["confirmed_total"] == 1
    assert cite["sla_overdue"] == 2
    statuses = {r["status"] for r in cite["status_breakdown"]}
    assert statuses == {"세무서 비협조", "대응 필요"}

    print(f"  ✅ test_filing_rate_run_all ({report['passed']}/{report['total_checks']} passed, {report['severity']})")


def test_filing_rate_backlog_aging():
    today = datetime.now(timezone.utc).date()
    deals = [
        {"apply_date": str(today - timedelta(days=3)), "filing_date": None},
        {"apply_date": str(today - timedelta(days=70)), "filing_date": None},
        {"apply_date": str(today - timedelta(days=70)), "filing_date": str(today)},  # 이미 신고완료 → 제외
        {"apply_date": str(today - timedelta(days=70)), "filing_date": None,
         "cancel_request_date": str(today), "status": "lost"},  # 취소 → 제외
    ]
    aging = backlog_aging(deals, today)
    assert aging["0-7"] == 1
    assert aging["60+"] == 1
    assert sum(aging.values()) == 2
    print("  ✅ test_filing_rate_backlog_aging")


# ── Verifier 테스트 ───────────────────────────────────

def test_check_mape():
    data = make_forecast()
    r = check_mape(data)
    assert r["ok"] is True  # 5.73% < 10%

    data["mape"] = 15.0
    r = check_mape(data)
    assert r["ok"] is False  # 15% >= 10%
    print("  ✅ test_check_mape")


def test_check_backtest_bias():
    data = make_forecast()
    r = check_backtest_bias(data)
    assert r["ok"] is True  # 오차 방향이 혼재

    # 모두 음수 편향
    data["backtest"] = [{"error_pct": -5}, {"error_pct": -3}, {"error_pct": -8}, {"error_pct": -2}]
    r = check_backtest_bias(data)
    assert r["ok"] is False  # 4개월 연속 음수
    print("  ✅ test_check_backtest_bias")


def test_check_backtest_outliers():
    data = make_forecast()
    results = check_backtest_outliers(data)
    # 2025-08 오차 13%, 2025-06 오차 -11.1% — 둘 다 ±15% 이내
    # But check threshold: single_error_warn = 15%
    # -11.1% and 13.0% are within ±15%
    # So all should pass... unless there's one >15%
    # In our fixture there's no >15% error
    for r in results:
        if "개별 월" in r["check"]:
            assert r["ok"] is True
    print("  ✅ test_check_backtest_outliers")


def test_check_distribution_stability():
    data = make_forecast()
    results = check_distribution_stability(data)
    # 현재 분산 = 참조 분산이므로 안정
    for r in results:
        assert r["ok"] is True

    # 급변 시뮬레이션
    data["distributions"]["d2p"]["0"] = 30.0  # 18.61 → 30.0 (61% 변화)
    results = check_distribution_stability(data)
    unstable = [r for r in results if not r["ok"]]
    assert len(unstable) > 0
    print("  ✅ test_check_distribution_stability")


def test_check_pool_health():
    data = make_forecast()
    results = check_pool_health(data)
    for r in results:
        assert r["ok"] is True  # 정상 범위

    # 급변
    data["collection_pool"]["monthly_delta"] = -5.0
    results = check_pool_health(data)
    delta_warn = [r for r in results if "급변" in r["check"]]
    assert len(delta_warn) > 0 and not delta_warn[0]["ok"]
    print("  ✅ test_check_pool_health")


def test_check_forecast_range():
    data = make_forecast()
    results = check_forecast_range(data)
    for r in results:
        assert r["ok"] is True

    # 비정상 예측
    data["forecast"][0]["grand_total"] = 2.0  # 너무 낮음
    results = check_forecast_range(data)
    low = [r for r in results if not r["ok"]]
    assert len(low) > 0
    print("  ✅ test_check_forecast_range")


def test_verifier_run_all():
    data = make_forecast()
    report = verifier_run_all(data)
    assert report["agent"] == "verifier"
    assert report["total_checks"] > 0
    assert report["severity"] in ("info", "warn", "critical")
    print(f"  ✅ test_verifier_run_all ({report['passed']}/{report['total_checks']} passed)")


# ── Orchestrator 테스트 ───────────────────────────────

def test_pipeline_state():
    state = PipelineState(run_id="test_001", status="running")
    state.steps.append(StepResult(name="test_step", status=StepStatus.SUCCESS))
    d = state.to_dict()
    assert d["run_id"] == "test_001"
    assert len(d["steps"]) == 1
    assert d["steps"][0]["status"] == "success"
    print("  ✅ test_pipeline_state")


def test_pipeline_steps_defined():
    names = [s["name"] for s in PIPELINE_STEPS]
    assert "extract_individual" in names
    assert "watch_data" in names
    assert "run_model" in names
    assert "verify_forecast" in names
    assert "notify_slack" in names
    print("  ✅ test_pipeline_steps_defined")


def test_step_error_policies():
    policies = {s["name"]: s["error_policy"] for s in PIPELINE_STEPS}
    assert policies["watch_data"] == ErrorPolicy.ABORT
    assert policies["extract_corp"] == ErrorPolicy.SKIP
    assert policies["extract_individual"] == ErrorPolicy.RETRY
    print("  ✅ test_step_error_policies")


# ── Verifier를 실제 forecast.json으로 실행 ────────────

def test_verifier_with_real_forecast():
    path = ROOT / "output" / "forecast.json"
    if not path.exists():
        print("  ⏭️  test_verifier_with_real_forecast (forecast.json 없음)")
        return
    data = json.loads(path.read_text())
    report = verifier_run_all(data)
    print(f"  ✅ test_verifier_with_real_forecast ({report['passed']}/{report['total_checks']} passed, {report['severity']})")
    for r in report["results"]:
        if not r["ok"]:
            print(f"     ❌ {r['check']}: {r['detail']}")


# ── 메인 ─────────────────────────────────────────────

def main():
    tests = [
        # Alerts
        ("Alerts", [test_format_report, test_severity_emoji]),
        # Watcher
        ("Watcher", [
            test_check_deal_count, test_check_null_rates, test_check_null_rates_clean,
            test_check_amount_outliers, test_check_amount_outliers_clean,
            test_check_schema_fields, test_watcher_run_all,
        ]),
        # Verifier
        ("Verifier", [
            test_check_mape, test_check_backtest_bias, test_check_backtest_outliers,
            test_check_distribution_stability, test_check_pool_health,
            test_check_forecast_range, test_verifier_run_all,
        ]),
        # Filing Rate Monitor
        ("FilingRateMonitor", [
            test_filing_rate_daily_flow, test_filing_rate_backlog_aging,
            test_filing_rate_cohort_conversion, test_filing_rate_cancel_reason_breakdown,
            test_filing_rate_citation_stats, test_filing_rate_hold_reason_breakdown,
            test_filing_rate_run_all,
        ]),
        # Orchestrator
        ("Orchestrator", [test_pipeline_state, test_pipeline_steps_defined, test_step_error_policies]),
        # Integration
        ("Integration", [test_verifier_with_real_forecast]),
    ]

    total = 0
    passed = 0
    failed = 0

    for group_name, group_tests in tests:
        print(f"\n{'─' * 40}")
        print(f"[{group_name}]")
        for t in group_tests:
            total += 1
            try:
                t()
                passed += 1
            except Exception as e:
                failed += 1
                print(f"  ❌ {t.__name__}: {e}")

    print(f"\n{'=' * 40}")
    print(f"결과: {passed}/{total} passed, {failed} failed")
    return failed == 0


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
