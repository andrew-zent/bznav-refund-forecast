"""
Phase 2: Verifier Agent — 예측 결과 자동 검증.

forecast.json의 예측 결과를 검증하고 이상 시 알림.
독립 실행: python src/agents/verifier.py [forecast.json 경로]
"""
import json
import sys
from pathlib import Path
from datetime import datetime

# 임계값
THRESHOLDS = {
    "mape_warn": 10.0,           # MAPE 경고 임계값 (%)
    "mape_critical": 20.0,       # MAPE 위험 임계값 (%)
    "single_error_warn": 15.0,   # 단월 오차 경고 (%)
    "bias_consecutive": 3,       # 연속 동일 방향 편향 (개월)
    "dist_change_pct": 30.0,     # 분산 비율 급변 경고 (%)
    "pool_delta_warn": 3.0,      # 풀 순변동 급변 경고 (억/월)
    "forecast_min": 5.0,         # 최소 예측값 (억)
    "forecast_max": 40.0,        # 최대 예측값 (억)
}

# 기준 분산 비율 (안정적 참조값)
REFERENCE_DISTS = {
    "d2p": {"0": 18.61, "1": 14.29, "2": 1.54, "3": 0.07},
    "f2d": {"0": 14.22, "1": 41.79, "2": 37.98, "3": 3.16, "4": 2.08},
    "a2f": {"0": 29.02, "1": 29.31, "2": 1.76, "3": 0.75, "4": 0.26},
}


def check_mape(data: dict) -> dict:
    """MAPE 수준 검증."""
    mape = data.get("mape", 0)
    if mape >= THRESHOLDS["mape_critical"]:
        return {"check": "MAPE 수준", "ok": False,
                "detail": f"{mape:.1f}% >= {THRESHOLDS['mape_critical']}% (CRITICAL)", "value": mape}
    if mape >= THRESHOLDS["mape_warn"]:
        return {"check": "MAPE 수준", "ok": False,
                "detail": f"{mape:.1f}% >= {THRESHOLDS['mape_warn']}% (WARN)", "value": mape}
    return {"check": "MAPE 수준", "ok": True, "detail": f"{mape:.1f}%", "value": mape}


def check_backtest_bias(data: dict) -> dict:
    """백테스트 편향 검증 — 연속 동일 방향 오차 감지."""
    bt = data.get("backtest", [])
    if len(bt) < THRESHOLDS["bias_consecutive"]:
        return {"check": "편향 검증", "ok": True, "detail": "백테스트 데이터 부족"}

    signs = [1 if r["error_pct"] > 0 else -1 for r in bt]
    max_consecutive = 1
    current = 1
    for i in range(1, len(signs)):
        if signs[i] == signs[i - 1]:
            current += 1
            max_consecutive = max(max_consecutive, current)
        else:
            current = 1

    threshold = THRESHOLDS["bias_consecutive"]
    ok = max_consecutive < threshold
    direction = "과대" if signs[-1] > 0 else "과소"
    return {
        "check": "편향 검증",
        "ok": ok,
        "detail": f"최대 연속 동일방향 {max_consecutive}개월" + (f" ({direction}추정)" if not ok else ""),
        "value": max_consecutive,
    }


def check_backtest_outliers(data: dict) -> list[dict]:
    """백테스트 개별 월 이상치."""
    bt = data.get("backtest", [])
    results = []
    threshold = THRESHOLDS["single_error_warn"]
    for r in bt:
        if abs(r["error_pct"]) > threshold:
            results.append({
                "check": f"백테스트 이상: {r['month']}",
                "ok": False,
                "detail": f"오차 {r['error_pct']:+.1f}% (임계 ±{threshold}%)",
                "value": r["error_pct"],
            })
    if not results:
        results.append({"check": "백테스트 개별 월", "ok": True,
                        "detail": f"전체 ±{threshold}% 이내"})
    return results


def check_distribution_stability(data: dict) -> list[dict]:
    """분산 비율(a2f/f2d/d2p) 안정성 검증."""
    dists = data.get("distributions", {})
    results = []
    threshold = THRESHOLDS["dist_change_pct"]

    for name, ref in REFERENCE_DISTS.items():
        current = dists.get(name, {})
        if not current:
            continue
        for offset, ref_val in ref.items():
            cur_val = current.get(str(offset), 0)
            if ref_val > 0:
                change_pct = abs(cur_val - ref_val) / ref_val * 100
                if change_pct > threshold:
                    results.append({
                        "check": f"분산 급변: {name}[{offset}]",
                        "ok": False,
                        "detail": f"{ref_val:.2f}% → {cur_val:.2f}% (변화 {change_pct:.0f}%)",
                        "value": change_pct,
                    })
    if not results:
        results.append({"check": "분산 비율 안정성", "ok": True, "detail": "전체 안정"})
    return results


def check_pool_health(data: dict) -> list[dict]:
    """채권풀 건강도 검증."""
    pool = data.get("collection_pool", {})
    results = []

    balance = pool.get("balance", 0)
    delta = pool.get("monthly_delta", 0)
    util_rate = pool.get("utilization_rate", 0)

    # 순변동 급변
    if abs(delta) > THRESHOLDS["pool_delta_warn"]:
        results.append({
            "check": "풀 순변동 급변",
            "ok": False,
            "detail": f"월 {delta:+.1f}억 (임계 ±{THRESHOLDS['pool_delta_warn']}억)",
            "value": delta,
        })

    # 회수율 음수 또는 비정상
    if util_rate <= 0:
        results.append({
            "check": "회수율 이상", "ok": False,
            "detail": f"{util_rate}% (0 이하)", "value": util_rate,
        })

    # 풀잔액 음수 위험 (12개월 내)
    if balance > 0 and delta < 0:
        months_to_zero = balance / abs(delta)
        if months_to_zero < 24:
            results.append({
                "check": "풀 소진 예상",
                "ok": True,  # 소진 자체는 경고가 아닌 info
                "detail": f"{months_to_zero:.0f}개월 후 소진 예상 (잔액 {balance}억, 월 {delta:+.1f}억)",
                "value": months_to_zero,
            })

    if not results:
        results.append({"check": "채권풀 건강도", "ok": True,
                        "detail": f"잔액 {balance}억, 순변동 {delta:+.1f}억/월, 회수율 {util_rate}%"})
    return results


def check_forecast_range(data: dict) -> list[dict]:
    """예측값 범위 검증."""
    fc = data.get("forecast", [])
    results = []
    for f in fc:
        gt = f.get("grand_total", 0)
        month = f.get("month", "?")
        if gt < THRESHOLDS["forecast_min"]:
            results.append({
                "check": f"예측 하한: {month}",
                "ok": False,
                "detail": f"{gt:.2f}억 < {THRESHOLDS['forecast_min']}억",
                "value": gt,
            })
        elif gt > THRESHOLDS["forecast_max"]:
            results.append({
                "check": f"예측 상한: {month}",
                "ok": False,
                "detail": f"{gt:.2f}억 > {THRESHOLDS['forecast_max']}억",
                "value": gt,
            })
    if not results:
        totals = [f.get("grand_total", 0) for f in fc]
        results.append({"check": "예측 범위", "ok": True,
                        "detail": f"{min(totals):.1f}~{max(totals):.1f}억 (정상)"})
    return results


def run_all_checks(data: dict) -> dict:
    """전체 검증 실행."""
    results = []
    results.append(check_mape(data))
    results.append(check_backtest_bias(data))
    results.extend(check_backtest_outliers(data))
    results.extend(check_distribution_stability(data))
    results.extend(check_pool_health(data))
    results.extend(check_forecast_range(data))

    failures = [r for r in results if not r["ok"]]
    severity = "critical" if len(failures) >= 5 else "warn" if failures else "info"

    return {
        "agent": "verifier",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "total_checks": len(results),
        "passed": len(results) - len(failures),
        "failed": len(failures),
        "severity": severity,
        "results": results,
    }


def main():
    """CLI 진입점."""
    root = Path(__file__).resolve().parent.parent.parent
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else root / "output" / "forecast.json"

    if not path.exists():
        print(f"ERROR: {path} not found")
        sys.exit(1)

    data = json.loads(path.read_text())
    print(f"Verifier: checking forecast from {data.get('generated_at', '?')}")

    report = run_all_checks(data)

    print(f"\n[결과] {report['passed']}/{report['total_checks']} passed (severity: {report['severity']})")
    for r in report["results"]:
        tag = "✅" if r["ok"] else "❌"
        print(f"  {tag} {r['check']}: {r['detail']}")

    # 리포트 저장
    report_path = root / "output" / "verification_report.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2))
    print(f"\n→ {report_path}")

    # 알림
    if report["severity"] != "info":
        try:
            from agents.alerts import format_report, send_slack
        except ModuleNotFoundError:
            sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
            from agents.alerts import format_report, send_slack
        msg = format_report("Verifier 예측 검증", report["results"], report["severity"])
        send_slack(msg, report["severity"])

    return report


if __name__ == "__main__":
    main()
