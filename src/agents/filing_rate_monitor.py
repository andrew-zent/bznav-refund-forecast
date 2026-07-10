"""
신고율 관리 Agent — 신청완료→신고완료 전환/실패율 + 인용확인 현황 일일 모니터링.

매일 실행: 어제자 신청/신고완료/취소 건수, 신청완료 상태로 정체된 백로그 에이징,
성숙 코호트(45~75일 전 신청) 기준 전환율/취소율/취소사유, 인용확인 완료건수와
기한(6주) 경과 미확인 건 + 상태별(세무서 비협조 등) 브레이크다운을 계산한다.

입력: data/deals_slim.json (extract_pipedrive.py 출력, 개인 파이프라인만 사용)
      output/field_catalog.json (dump_fields.py 출력 — enum/set 필드 id→label 변환용, 없으면 raw id로 표기)
출력: output/filing_rate_report.json (최신)
      output/filing_rate_snapshots/YYYY-MM-DD.json (일자별 이력)

독립 실행: python src/agents/filing_rate_monitor.py [deals_slim.json 경로]
"""
import json
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

from config import PIPELINE_REGULAR
from snapshot import build_id_label_map, translate


def parse_date(v):
    if not v:
        return None
    s = str(v).strip()[:10]
    for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    return None

# Pipedrive 커스텀 필드 key — enum/set 값 id→label 변환용
TRANSLATE_FIELDS = {
    "cancel_reason": "ebdd813efc921dcb6a90be9156642c824589aced",
    "hold_reason": "430f49c344b73aaa29622d1fa50e33f75a79ad80",
    "hold_reason_2": "314ea8de88a7dda7349215ddc0975216b9662ad9",
    "hold_status": "6a4c5816ff87fa993ea6c4affe4ce82636b09714",
    "citation_status": "8e057c4b5b8a2a57e4ad2579c150b197f1017506",
}

# 임계값 (실측 데이터 없이 설정한 초기값 — 운영 후 조정 필요)
MATURATION_WINDOW_START = 75   # 코호트 시작: 신청 후 75일 전
MATURATION_WINDOW_END = 45     # 코호트 종료: 신청 후 45일 전 (그 사이 신청 건 = 대부분 결과 확정)
STALE_BACKLOG_DAYS = 60        # 신청완료 후 이 일수 초과 미해결 = 정체
STALE_BACKLOG_WARN_COUNT = 100
CANCEL_RATE_WARN_PCT = 15.0
CITATION_SLA_WARN_COUNT = 20
BAD_CITATION_STATUS = {"세무서 비협조", "대응 필요"}
BAD_CITATION_WARN_COUNT = 15


def _cancel_date(d):
    """취소 이벤트 날짜 — 취소요청일 우선, 없으면 lost_time(+cancel_reason 존재) fallback."""
    cd = parse_date(d.get("cancel_request_date"))
    if cd:
        return cd
    if str(d.get("status")) == "lost" and d.get("cancel_reason"):
        return parse_date(d.get("lost_time"))
    return None


def _is_cancelled(d):
    return _cancel_date(d) is not None


def _regular_deals(deals):
    return [d for d in deals if PIPELINE_REGULAR in str(d.get("pipeline", ""))]


def daily_flow(deals, report_date):
    """report_date 하루 동안 신청/신고완료/취소 건수."""
    applied = filed = cancelled = 0
    for d in deals:
        ad = parse_date(d.get("apply_date"))
        fd = parse_date(d.get("filing_date"))
        cd = _cancel_date(d)
        if ad and ad.date() == report_date:
            applied += 1
        if fd and fd.date() == report_date:
            filed += 1
        if cd and cd.date() == report_date:
            cancelled += 1
    return {"date": str(report_date), "applied": applied, "filed": filed, "cancelled": cancelled}


def backlog_aging(deals, today):
    """신청완료 후 아직 신고완료도 취소도 안 된 건을 경과일수 구간별로 집계."""
    buckets = {"0-7": 0, "8-14": 0, "15-30": 0, "31-60": 0, "60+": 0}
    for d in deals:
        if parse_date(d.get("filing_date")) or _is_cancelled(d):
            continue
        ad = parse_date(d.get("apply_date"))
        if not ad:
            continue
        age = (today - ad.date()).days
        if age < 0:
            continue
        if age <= 7:
            buckets["0-7"] += 1
        elif age <= 14:
            buckets["8-14"] += 1
        elif age <= 30:
            buckets["15-30"] += 1
        elif age <= 60:
            buckets["31-60"] += 1
        else:
            buckets["60+"] += 1
    return buckets


def hold_reason_breakdown(deals, id_map, today, top_n=5):
    """정체 백로그 중 '보류 중' 상태인 건의 사유 브레이크다운 + 최장 보류 경과일."""
    counter = Counter()
    total = 0
    oldest_days = 0
    for d in deals:
        if parse_date(d.get("filing_date")) or _is_cancelled(d):
            continue
        if translate(id_map, TRANSLATE_FIELDS["hold_status"], d.get("hold_status")) != "보류 중":
            continue
        total += 1
        raw = d.get("hold_reason")
        raw2 = d.get("hold_reason_2")
        if raw:
            label = translate(id_map, TRANSLATE_FIELDS["hold_reason"], raw)
        elif raw2:
            label = translate(id_map, TRANSLATE_FIELDS["hold_reason_2"], raw2)
        else:
            label = "(미기재)"
        counter[label] += 1
        activity = parse_date(d.get("hold_activity_date"))
        if activity:
            oldest_days = max(oldest_days, (today - activity.date()).days)
    top = [{"reason": k, "count": v} for k, v in counter.most_common(top_n)]
    return {"total_on_hold": total, "oldest_hold_days": oldest_days, "top_reasons": top}


def cohort_conversion(deals, today, window_start=MATURATION_WINDOW_START, window_end=MATURATION_WINDOW_END):
    """신청 후 window_end~window_start일 지난 코호트의 신고완료/취소/진행중 비율."""
    lo = today - timedelta(days=window_start)
    hi = today - timedelta(days=window_end)
    cohort = []
    for d in deals:
        ad = parse_date(d.get("apply_date"))
        if ad and lo <= ad.date() <= hi:
            cohort.append(d)

    n = len(cohort)
    filed = cancelled = 0
    for d in cohort:
        if parse_date(d.get("filing_date")):
            filed += 1
        elif _is_cancelled(d):
            cancelled += 1
    pending = n - filed - cancelled

    def pct(x):
        return round(100 * x / n, 1) if n else 0.0

    return {
        "window_label": f"신청 {window_start}~{window_end}일 전",
        "n": n,
        "filed": filed,
        "cancelled": cancelled,
        "pending": pending,
        "filed_pct": pct(filed),
        "cancel_pct": pct(cancelled),
        "pending_pct": pct(pending),
    }


def cancel_reason_breakdown(deals, id_map, today, lookback_days=90, top_n=5):
    """최근 lookback_days 이내 취소된 건의 사유 브레이크다운."""
    lo = today - timedelta(days=lookback_days)
    counter = Counter()
    total = 0
    for d in deals:
        cd = _cancel_date(d)
        if not cd or cd.date() < lo:
            continue
        total += 1
        raw = d.get("cancel_reason")
        if raw:
            label = translate(id_map, TRANSLATE_FIELDS["cancel_reason"], raw)
        else:
            label = d.get("cancel_reason_auto") or "(미기재)"
        counter[label] += 1
    top = [{"reason": k, "count": v, "pct": round(100 * v / total, 1) if total else 0} for k, v in counter.most_common(top_n)]
    return {"window_days": lookback_days, "total": total, "top_reasons": top}


def citation_stats(deals, id_map, report_date, today):
    """인용확인 완료 건수 + 기한 경과 미확인 건 + 상태별 브레이크다운."""
    confirmed_today = confirmed_total = sla_overdue = 0
    status_counter = Counter()
    for d in deals:
        conf = parse_date(d.get("citation_confirmed_date"))
        if conf:
            confirmed_total += 1
            if conf.date() == report_date:
                confirmed_today += 1
            continue
        due = parse_date(d.get("citation_due_date"))
        if due and due.date() < today:
            sla_overdue += 1
        if parse_date(d.get("decision_date")):
            label = translate(id_map, TRANSLATE_FIELDS["citation_status"], d.get("citation_status"))
            if label and label != "(미기재)":
                status_counter[label] += 1
    return {
        "confirmed_today": confirmed_today,
        "confirmed_total": confirmed_total,
        "sla_overdue": sla_overdue,
        "status_breakdown": [{"status": k, "count": v} for k, v in status_counter.most_common()],
    }


def run_all_checks(deals, field_catalog=None, as_of=None):
    """전체 지표 계산 + severity 판정. 결과 딕셔너리 반환."""
    field_catalog = field_catalog or {"all_fields": []}
    id_map = build_id_label_map(field_catalog)

    now = as_of or datetime.now(timezone.utc)
    today = now.date()
    report_date = today - timedelta(days=1)

    regular = _regular_deals(deals)
    backlog = [d for d in regular if not parse_date(d.get("filing_date")) and not _is_cancelled(d)]

    flow = daily_flow(regular, report_date)
    aging = backlog_aging(regular, today)
    hold = hold_reason_breakdown(regular, id_map, today)
    conv = cohort_conversion(regular, today)
    cancels = cancel_reason_breakdown(regular, id_map, today)
    citation = citation_stats(regular, id_map, report_date, today)

    results = [
        {
            "check": f"일일 전환 흐름 ({flow['date']})",
            "ok": True,
            "detail": f"신청 {flow['applied']}건 · 신고완료 {flow['filed']}건 · 취소 {flow['cancelled']}건",
            "value": flow,
        },
        {
            "check": f"정체 백로그 (신청 후 {STALE_BACKLOG_DAYS}일+)",
            "ok": aging["60+"] <= STALE_BACKLOG_WARN_COUNT,
            "detail": f"{aging['60+']}건 (임계 {STALE_BACKLOG_WARN_COUNT}건) · 전체 백로그 {len(backlog):,}건",
            "value": aging["60+"],
        },
        {
            "check": f"성숙 코호트 취소율 ({conv['window_label']})",
            "ok": conv["cancel_pct"] <= CANCEL_RATE_WARN_PCT,
            "detail": f"취소 {conv['cancel_pct']}% ({conv['cancelled']}/{conv['n']}건) · 신고완료 {conv['filed_pct']}%",
            "value": conv["cancel_pct"],
        },
        {
            "check": "인용확인 기한(6주) 경과 미확인",
            "ok": citation["sla_overdue"] <= CITATION_SLA_WARN_COUNT,
            "detail": f"{citation['sla_overdue']}건 (임계 {CITATION_SLA_WARN_COUNT}건)",
            "value": citation["sla_overdue"],
        },
    ]
    bad_citation = sum(r["count"] for r in citation["status_breakdown"] if r["status"] in BAD_CITATION_STATUS)
    results.append({
        "check": "인용확인 대응필요 (세무서 비협조/대응 필요)",
        "ok": bad_citation <= BAD_CITATION_WARN_COUNT,
        "detail": f"{bad_citation}건 (임계 {BAD_CITATION_WARN_COUNT}건)",
        "value": bad_citation,
    })

    failures = [r for r in results if not r["ok"]]
    severity = "critical" if len(failures) >= 3 else "warn" if failures else "info"

    return {
        "agent": "filing_rate_monitor",
        "timestamp": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "report_date": str(report_date),
        "total_checks": len(results),
        "passed": len(results) - len(failures),
        "failed": len(failures),
        "severity": severity,
        "results": results,
        "daily_flow": flow,
        "backlog_aging": aging,
        "hold_summary": hold,
        "cohort_conversion": conv,
        "cancel_reasons": cancels,
        "citation": citation,
    }


def build_digest(report):
    """Slack 일일 다이제스트 메시지 (체크리스트가 아닌 데이터 요약)."""
    emoji = {"info": "✅", "warn": "⚠️", "critical": "🚨"}.get(report["severity"], "ℹ️")
    flow = report["daily_flow"]
    aging = report["backlog_aging"]
    conv = report["cohort_conversion"]
    cite = report["citation"]
    hold = report["hold_summary"]
    cancels = report["cancel_reasons"]
    backlog_total = sum(aging.values())

    lines = [f"{emoji} *신고율 관리 일일 리포트* ({flow['date']})", ""]
    lines.append(f"*오늘 흐름*  신청 {flow['applied']}건 → 신고완료 {flow['filed']}건 · 취소 {flow['cancelled']}건")
    lines.append("")
    lines.append(f"*백로그 에이징* (신청완료 후 미해결 총 {backlog_total:,}건)")
    lines.append(
        f"  0-7일 {aging['0-7']} · 8-14일 {aging['8-14']} · 15-30일 {aging['15-30']} · "
        f"31-60일 {aging['31-60']} · 60일+ {aging['60+']}"
    )
    if hold["total_on_hold"]:
        top_hold = ", ".join(f"{r['reason']}({r['count']})" for r in hold["top_reasons"][:3])
        lines.append(f"  ㄴ 보류 중 {hold['total_on_hold']}건 (최장 {hold['oldest_hold_days']}일) — {top_hold}")
    lines.append("")
    lines.append(f"*성숙 코호트 전환율* ({conv['window_label']}, {conv['n']}건)")
    lines.append(f"  신고완료 {conv['filed_pct']}% · 취소 {conv['cancel_pct']}% · 진행중 {conv['pending_pct']}%")
    if cancels["top_reasons"]:
        top_cancel = ", ".join(f"{r['reason']}({r['count']})" for r in cancels["top_reasons"])
        lines.append(f"  취소 사유 top: {top_cancel}")
    lines.append("")
    lines.append(
        f"*인용확인 현황*  오늘 완료 {cite['confirmed_today']}건 · 누적 {cite['confirmed_total']:,}건 · "
        f"기한경과 미확인 {cite['sla_overdue']}건"
    )
    if cite["status_breakdown"]:
        top_status = ", ".join(f"{r['status']}({r['count']})" for r in cite["status_breakdown"])
        lines.append(f"  상태별: {top_status}")

    return "\n".join(lines)


def main():
    """CLI 진입점."""
    deals_path = Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / "data" / "deals_slim.json"
    catalog_path = ROOT / "output" / "field_catalog.json"

    if not deals_path.exists():
        print(f"ERROR: {deals_path} not found")
        sys.exit(1)

    deals = json.loads(deals_path.read_text())
    field_catalog = json.loads(catalog_path.read_text()) if catalog_path.exists() else {"all_fields": []}
    if not catalog_path.exists():
        print(f"WARN: {catalog_path} 없음 — 사유/상태 값이 raw id로 표기됩니다 (python src/dump_fields.py 먼저 실행 권장)")

    print(f"Filing Rate Monitor: checking {len(deals):,} deals from {deals_path.name}")
    report = run_all_checks(deals, field_catalog)

    print(f"\n[결과] {report['passed']}/{report['total_checks']} passed (severity: {report['severity']})")
    for r in report["results"]:
        tag = "✅" if r["ok"] else "❌"
        print(f"  {tag} {r['check']}: {r['detail']}")

    out_dir = ROOT / "output"
    out_dir.mkdir(exist_ok=True)
    report_path = out_dir / "filing_rate_report.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2))
    print(f"\n→ {report_path}")

    snap_dir = out_dir / "filing_rate_snapshots"
    snap_dir.mkdir(exist_ok=True, parents=True)
    snap_path = snap_dir / f"{report['report_date']}.json"
    snap_path.write_text(json.dumps(report, ensure_ascii=False, indent=2))

    try:
        from agents.alerts import send_slack
    except ModuleNotFoundError:
        sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
        from agents.alerts import send_slack
    send_slack(build_digest(report), report["severity"])

    return report


if __name__ == "__main__":
    main()
