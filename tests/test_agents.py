"""Agent 시스템 전체 테스트."""
import json
import sys
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
