"""
분기별 전환율 재검증 (Quarterly Recalibrator).

현재 config.py 기준치 대비 실측 코호트 전환율 드리프트를 체크하고,
임계값 초과 시 Slack 경보 + 권장 값 제안.

실행: python src/agents/recalibrator.py
출력: output/recalibration_report.json
"""
from __future__ import annotations

import json
import sqlite3
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
DB = Path("/tmp/history.sqlite")
OUT = ROOT / "output" / "recalibration_report.json"

# config.py 기준치 (하드코딩 — config 변경 시 여기도 갱신)
BASELINES = {
    "apply_to_pay_filtered_pct": 20.33,   # B+C+corp filtered base
    "filing_to_pay_pct":         30.0,    # 신고→결제 비율
    "decision_to_pay_pct":       31.0,    # 결정→결제 비율
    "apply_to_pay_unfiltered_pct": 4.7,   # 마케팅팀 계수 (unfiltered 전체 기준)
}

DRIFT_WARN_PCT  = 10.0   # ±10% 변동 시 warn
DRIFT_ALERT_PCT = 20.0   # ±20% 변동 시 alert

# 유효 유입 파이프라인 (A(지수), D/B-취소 제외)
PIPELINE_FILTERED = (
    "B(젠트)-환급", "C(젠트)-추심", "E(가은)-미수채권",
    "법인-환급", "법인-추심",
)


def latest_as_of(con: sqlite3.Connection) -> str:
    return con.execute("SELECT MAX(as_of_date) FROM deal_history").fetchone()[0]


def cohort_window(as_of: str) -> tuple[str, str]:
    """최근 완성 12M 코호트 (as_of 기준 13~1개월 전 신청 → 12개월 경과 보장)."""
    end_d = date.fromisoformat(as_of) - timedelta(days=30)
    start_d = end_d - timedelta(days=365)
    return start_d.isoformat(), end_d.isoformat()


def compute_rates(con: sqlite3.Connection, as_of: str) -> dict:
    start, end = cohort_window(as_of)
    rows = con.execute("""
        SELECT apply_amount, filing_amount, decision_amount, payment_amount, pipeline
        FROM deal_history
        WHERE as_of_date = ?
          AND apply_date BETWEEN ? AND ?
    """, (as_of, start, end)).fetchall()

    if not rows:
        return {}

    filtered = [r for r in rows if r[4] in PIPELINE_FILTERED]

    # apply → pay (filtered)
    apply_f = sum(r[0] or 0 for r in filtered)
    pay_f   = sum(r[3] or 0 for r in filtered)

    # apply → pay (unfiltered, 모든 pipeline)
    apply_all = sum(r[0] or 0 for r in rows)
    pay_all   = sum(r[3] or 0 for r in rows)

    # filing → pay (filing_amount가 있는 건만)
    filing_rows = [(r[1], r[3]) for r in filtered if (r[1] or 0) > 0]
    filing_src  = sum(r[0] for r in filing_rows)
    filing_pay  = sum(r[1] for r in filing_rows)

    # decision → pay
    decision_rows = [(r[2], r[3]) for r in filtered if (r[2] or 0) > 0]
    decision_src  = sum(r[0] for r in decision_rows)
    decision_pay  = sum(r[1] for r in decision_rows)

    return {
        "apply_to_pay_filtered_pct":   round(pay_f   / apply_f   * 100, 2) if apply_f   > 0 else None,
        "apply_to_pay_unfiltered_pct": round(pay_all / apply_all * 100, 2) if apply_all > 0 else None,
        "filing_to_pay_pct":           round(filing_pay   / filing_src   * 100, 2) if filing_src   > 0 else None,
        "decision_to_pay_pct":         round(decision_pay / decision_src * 100, 2) if decision_src > 0 else None,
        "cohort_window": {"start": start, "end": end},
        "deals_filtered": len(filtered),
        "deals_total": len(rows),
        "apply_filtered_oku": round(apply_f / 1e8, 1),
        "pay_filtered_oku":   round(pay_f   / 1e8, 1),
    }


def check_drift(current: dict) -> list[dict]:
    checks = []
    for key, baseline in BASELINES.items():
        actual = current.get(key)
        if actual is None:
            checks.append({"metric": key, "baseline": baseline, "actual": None,
                           "drift_pct": None, "severity": "warn", "message": "데이터 없음"})
            continue
        drift = (actual - baseline) / baseline * 100
        if abs(drift) >= DRIFT_ALERT_PCT:
            sev = "alert"
        elif abs(drift) >= DRIFT_WARN_PCT:
            sev = "warn"
        else:
            sev = "ok"
        checks.append({
            "metric":    key,
            "baseline":  baseline,
            "actual":    actual,
            "drift_pct": round(drift, 1),
            "severity":  sev,
            "message":   (
                f"{baseline:.1f}% → {actual:.1f}% ({drift:+.1f}%p)"
                + (" ⚠️ 재검토 권장" if sev == "warn" else " 🔴 즉시 검토" if sev == "alert" else "")
            ),
        })
    return checks


def _notify_slack(report: dict) -> None:
    import os, urllib.request
    url = os.getenv("SLACK_WEBHOOK_URL")
    if not url:
        return
    alerts = [c for c in report["checks"] if c["severity"] in ("warn", "alert")]
    if not alerts:
        return
    lines = ["*📊 분기별 전환율 재검증 — 드리프트 감지*"]
    for c in alerts:
        icon = "🔴" if c["severity"] == "alert" else "⚠️"
        lines.append(f"{icon} `{c['metric']}`: {c['message']}")
    lines.append(f"기준 윈도우: {report['cohort_window']['start']} ~ {report['cohort_window']['end']}")
    payload = json.dumps({"text": "\n".join(lines)}).encode()
    req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
    try:
        urllib.request.urlopen(req, timeout=10)
    except Exception:
        pass


def run() -> dict:
    if not DB.exists():
        print(f"SKIP: {DB} 없음 (history archive 미다운로드)")
        return {"skipped": True}

    con = sqlite3.connect(DB)
    as_of = latest_as_of(con)
    print(f"as_of={as_of}")

    current = compute_rates(con, as_of)
    con.close()

    if not current:
        print("SKIP: 코호트 데이터 없음")
        return {"skipped": True}

    checks = check_drift(current)

    n_alert = sum(1 for c in checks if c["severity"] == "alert")
    n_warn  = sum(1 for c in checks if c["severity"] == "warn")
    n_ok    = sum(1 for c in checks if c["severity"] == "ok")
    overall = "alert" if n_alert else ("warn" if n_warn else "ok")

    report = {
        "generated_at":   datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "as_of_date":     as_of,
        "cohort_window":  current["cohort_window"],
        "overall":        overall,
        "summary":        f"{n_ok} ok / {n_warn} warn / {n_alert} alert",
        "checks":         checks,
        "current_rates":  {k: v for k, v in current.items() if k not in ("cohort_window",)},
        "baselines":      BASELINES,
    }

    OUT.parent.mkdir(exist_ok=True)
    OUT.write_text(json.dumps(report, ensure_ascii=False, indent=2))

    print(f"\n=== Recalibration Report ({overall.upper()}) ===")
    for c in checks:
        print(f"  {c['message']}")
    print(f"\n→ {OUT}")

    _notify_slack(report)
    return report


if __name__ == "__main__":
    result = run()
    if result.get("skipped"):
        sys.exit(0)
    sys.exit(0 if result["overall"] == "ok" else 1)
