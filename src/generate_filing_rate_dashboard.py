"""
신고율 관리 대시보드 생성 — filing_rate_report.json (+ snapshots) → HTML.

입력: output/filing_rate_report.json (최신)
      output/filing_rate_snapshots/*.json (일별 이력, 트렌드 차트용)
출력: output/filing_rate_dashboard.html

독립 실행: python src/generate_filing_rate_dashboard.py
"""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = ROOT / "output"
SNAP_DIR = OUTPUT_DIR / "filing_rate_snapshots"

COLOR = {"green": "#3fb950", "amber": "#d29922", "red": "#f85149", "blue": "#58a6ff", "muted": "#8b949e"}
SEV_COLOR = {"info": COLOR["green"], "warn": COLOR["amber"], "critical": COLOR["red"]}
SEV_LABEL = {"info": "정상", "warn": "주의", "critical": "위험"}


def load_report(path: Path) -> dict:
    return json.loads(path.read_text())


def load_history(snap_dir: Path, limit: int = 30) -> list[dict]:
    if not snap_dir.exists():
        return []
    files = sorted(snap_dir.glob("*.json"))[-limit:]
    return [json.loads(f.read_text()) for f in files]


def _backlog_chart(aging: dict) -> tuple[str, str]:
    buckets = ["0-7", "8-14", "15-30", "31-60", "60+"]
    values = [aging.get(b, 0) for b in buckets]
    colors = [COLOR["green"], COLOR["green"], COLOR["amber"], COLOR["amber"], COLOR["red"]]
    labels_js = json.dumps([f"{b}일" for b in buckets])
    values_js = json.dumps(values)
    colors_js = json.dumps(colors)
    html = '<div class="chart-c" style="height:220px"><canvas id="agingChart"></canvas></div>'
    js = (
        f"new Chart(document.getElementById('agingChart'),{{type:'bar',data:{{labels:{labels_js},"
        f"datasets:[{{data:{values_js},backgroundColor:{colors_js},borderRadius:4,"
        f"datalabels:{{}}}}]}},options:{{maintainAspectRatio:false,responsive:true,"
        f"plugins:{{legend:{{display:false}},tooltip:{{callbacks:{{label:c=>c.raw+'건'}}}}}},"
        f"scales:{{y:{{...bs,title:{{display:true,text:'건수'}}}},x:{{...bs}}}}}}}});"
    )
    return html, js


def _composition_bar(conv: dict) -> str:
    n = conv.get("n", 0)
    if n == 0:
        return '<div style="color:#6e7681;font-size:12px">성숙 코호트 데이터 없음</div>'
    segs = [
        ("신고완료", conv["filed_pct"], COLOR["green"]),
        ("취소", conv["cancel_pct"], COLOR["red"]),
        ("진행중", conv["pending_pct"], COLOR["amber"]),
    ]
    bar = "".join(
        f'<div style="width:{pct}%;background:{c};display:flex;align-items:center;'
        f'justify-content:center;color:#0d1117;font-size:11px;font-weight:700;min-width:2px;'
        f'overflow:hidden;white-space:nowrap;">{f"{label} {pct}%" if pct >= 12 else ""}</div>'
        for label, pct, c in segs
    )
    legend = "".join(
        f'<span style="margin-right:14px"><span style="display:inline-block;width:9px;height:9px;'
        f'border-radius:2px;background:{c};margin-right:4px"></span>{label} {pct}%</span>'
        for label, pct, c in segs
    )
    return (
        f'<div style="display:flex;height:32px;border-radius:6px;overflow:hidden;margin-bottom:8px">{bar}</div>'
        f'<div style="font-size:11px;color:#8b949e">{legend}</div>'
    )


def _reason_bars(items: list[dict], key: str, count_key: str = "count", max_n: int = 6) -> str:
    if not items:
        return '<div style="color:#6e7681;font-size:12px">데이터 없음</div>'
    top = items[:max_n]
    max_v = max(r[count_key] for r in top) or 1
    rows = ""
    for r in top:
        pct = round(100 * r[count_key] / max_v)
        rows += (
            f'<div style="margin-bottom:7px">'
            f'<div style="display:flex;justify-content:space-between;font-size:12px;margin-bottom:2px">'
            f'<span>{r[key]}</span><span style="color:#8b949e">{r[count_key]}건</span></div>'
            f'<div style="background:#21262d;border-radius:3px;height:8px">'
            f'<div style="width:{pct}%;background:{COLOR["blue"]};height:8px;border-radius:3px"></div>'
            f'</div></div>'
        )
    return rows


def _trend_charts(history: list[dict]) -> tuple[str, str]:
    if len(history) < 2:
        return "", ""
    dates = json.dumps([h["report_date"] for h in history])
    applied = json.dumps([h["daily_flow"]["applied"] for h in history])
    filed = json.dumps([h["daily_flow"]["filed"] for h in history])
    cancelled = json.dumps([h["daily_flow"]["cancelled"] for h in history])
    stale = json.dumps([h["backlog_aging"].get("60+", 0) for h in history])
    cancel_pct = json.dumps([h["cohort_conversion"].get("cancel_pct", 0) for h in history])

    html = """
<div class="grid-2">
  <div class="card"><h2>📈 일별 흐름 추이</h2><div class="chart-c"><canvas id="flowTrend"></canvas></div></div>
  <div class="card"><h2>📊 정체 백로그 · 취소율 추이</h2><div class="chart-c"><canvas id="staleTrend"></canvas></div></div>
</div>
"""
    js = f"""
new Chart(document.getElementById('flowTrend'),{{type:'line',data:{{labels:{dates},datasets:[
  {{label:'신청',data:{applied},borderColor:'{COLOR["blue"]}',tension:.3,pointRadius:3,borderWidth:2}},
  {{label:'신고완료',data:{filed},borderColor:'{COLOR["green"]}',tension:.3,pointRadius:3,borderWidth:2}},
  {{label:'취소',data:{cancelled},borderColor:'{COLOR["red"]}',tension:.3,pointRadius:3,borderWidth:2}}
]}},options:{{maintainAspectRatio:false,responsive:true,plugins:{{legend:{{position:'top',labels:{{boxWidth:10}}}}}},scales:{{y:{{...bs,title:{{display:true,text:'건수'}}}},x:{{...bs}}}}}}}});
new Chart(document.getElementById('staleTrend'),{{type:'line',data:{{labels:{dates},datasets:[
  {{label:'60일+ 정체(건)',data:{stale},borderColor:'{COLOR["red"]}',backgroundColor:'rgba(248,81,73,0.1)',fill:true,tension:.3,pointRadius:3,borderWidth:2,yAxisID:'y'}},
  {{label:'성숙코호트 취소율(%)',data:{cancel_pct},borderColor:'{COLOR["amber"]}',tension:.3,pointRadius:3,borderWidth:2,yAxisID:'y1'}}
]}},options:{{maintainAspectRatio:false,responsive:true,plugins:{{legend:{{position:'top',labels:{{boxWidth:10}}}}}},scales:{{y:{{...bs,position:'left',title:{{display:true,text:'정체 건수'}}}},y1:{{...bs,position:'right',grid:{{drawOnChartArea:false}},title:{{display:true,text:'취소율 %'}}}},x:{{...bs}}}}}}}});
"""
    return html, js


def build_html(report: dict, history: list[dict]) -> str:
    flow = report["daily_flow"]
    aging = report["backlog_aging"]
    conv = report["cohort_conversion"]
    cite = report["citation"]
    hold = report["hold_summary"]
    cancels = report["cancel_reasons"]
    backlog_total = sum(aging.values())
    sev = report["severity"]
    sev_color = SEV_COLOR.get(sev, COLOR["blue"])

    aging_html, aging_js = _backlog_chart(aging)
    trend_html, trend_js = _trend_charts(history)

    checks_rows = "".join(
        f'<tr><td>{r["check"]}</td><td>{"🟢" if r["ok"] else "🔴"}</td><td>{r["detail"]}</td></tr>'
        for r in report["results"]
    )

    return f"""<!DOCTYPE html>
<html lang="ko"><head><meta charset="UTF-8">
<title>신고율 관리 대시보드</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
*{{box-sizing:border-box}}body{{font-family:-apple-system,"Apple SD Gothic Neo",sans-serif;margin:0;padding:20px;background:#0d1117;color:#e6edf3;font-size:13px;line-height:1.5}}
.container{{max-width:1200px;margin:0 auto}}
h1{{font-size:24px;margin:0 0 4px 0}}h2{{font-size:14px;margin:0 0 10px 0}}
.meta{{font-size:11px;color:#6e7681;margin-bottom:16px}}
.meta span{{padding:2px 8px;background:#21262d;border-radius:4px;margin-right:6px}}
.badge{{padding:2px 10px;border-radius:12px;font-weight:700;color:#0d1117}}
.kpi-row{{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:16px}}
.kpi{{background:#161b22;border:1px solid #30363d;padding:14px;border-radius:8px}}
.kpi-label{{color:#8b949e;font-size:10px;text-transform:uppercase}}.kpi-value{{font-size:22px;font-weight:700}}.kpi-sub{{font-size:10px;color:#8b949e;margin-top:4px}}
.grid-2{{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-bottom:14px}}
.card{{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:14px}}
table{{width:100%;border-collapse:collapse;font-size:12px}}th,td{{padding:5px 6px;text-align:right;border-bottom:1px solid #21262d}}th:first-child,td:first-child{{text-align:left}}th{{color:#8b949e;font-size:10px}}
.chart-c{{position:relative;height:280px}}
footer{{margin-top:20px;text-align:center;font-size:10px;color:#6e7681}}
@media (max-width:900px){{.kpi-row{{grid-template-columns:repeat(2,1fr)}}.grid-2{{grid-template-columns:1fr}}}}
</style></head><body>
<div class="container">
<h1>신고율 관리 대시보드</h1>
<div class="meta">
  <span>기준일: {report['report_date']}</span>
  <span>갱신: {report['timestamp']}</span>
  <span class="badge" style="background:{sev_color}">{SEV_LABEL.get(sev, sev)} ({report['passed']}/{report['total_checks']})</span>
</div>

<div class="kpi-row">
  <div class="kpi" style="border-color:{COLOR['blue']}">
    <div class="kpi-label">어제 흐름</div>
    <div class="kpi-value" style="color:{COLOR['blue']}">{flow['applied']}<span style="font-size:11px;color:#8b949e"> 신청</span></div>
    <div class="kpi-sub">신고완료 {flow['filed']}건 · 취소 {flow['cancelled']}건</div>
  </div>
  <div class="kpi" style="border-color:{COLOR['red'] if aging['60+'] else COLOR['green']}">
    <div class="kpi-label">정체 백로그 (60일+)</div>
    <div class="kpi-value" style="color:{COLOR['red'] if aging['60+'] else COLOR['green']}">{aging['60+']}<span style="font-size:11px;color:#8b949e">건</span></div>
    <div class="kpi-sub">전체 백로그 {backlog_total:,}건 중</div>
  </div>
  <div class="kpi" style="border-color:{COLOR['amber']}">
    <div class="kpi-label">성숙 코호트 취소율</div>
    <div class="kpi-value" style="color:{COLOR['amber']}">{conv['cancel_pct']}<span style="font-size:11px;color:#8b949e">%</span></div>
    <div class="kpi-sub">{conv['window_label']} · {conv['n']}건 기준</div>
  </div>
  <div class="kpi" style="border-color:{COLOR['red'] if cite['sla_overdue'] else COLOR['green']}">
    <div class="kpi-label">인용확인 기한경과 미확인</div>
    <div class="kpi-value" style="color:{COLOR['red'] if cite['sla_overdue'] else COLOR['green']}">{cite['sla_overdue']}<span style="font-size:11px;color:#8b949e">건</span></div>
    <div class="kpi-sub">완료 누적 {cite['confirmed_total']:,}건 (오늘 {cite['confirmed_today']}건)</div>
  </div>
</div>

<div class="grid-2">
  <div class="card">
    <h2>📦 백로그 에이징 (신청완료 후 미해결)</h2>
    {aging_html}
  </div>
  <div class="card">
    <h2>🧭 성숙 코호트 결과 분포 ({conv['window_label']})</h2>
    {_composition_bar(conv)}
    <div style="margin-top:14px;font-size:11px;color:#8b949e">보류 중 {hold['total_on_hold']}건 (최장 {hold['oldest_hold_days']}일)</div>
  </div>
</div>

<div class="grid-2">
  <div class="card">
    <h2>❌ 취소 사유 top ({cancels['window_days']}일 이내, 총 {cancels['total']}건)</h2>
    {_reason_bars(cancels['top_reasons'], 'reason')}
  </div>
  <div class="card">
    <h2>📋 인용확인 상태별 (미확인 건)</h2>
    {_reason_bars(cite['status_breakdown'], 'status')}
  </div>
</div>

{trend_html}

<div class="card">
  <h2>✅ 체크 상세</h2>
  <table><thead><tr><th>항목</th><th></th><th>상세</th></tr></thead><tbody>{checks_rows}</tbody></table>
</div>

<footer>지엔터프라이즈 · 신고율 관리 Agent · 매일 09:00 KST 자동 갱신</footer>
</div>
<script>
Chart.defaults.color='#8b949e';Chart.defaults.borderColor='#21262d';
const g={{color:'#21262d',drawBorder:false}},bs={{grid:g,ticks:{{color:'#8b949e'}}}};
{aging_js}
{trend_js}
</script></body></html>"""


def main():
    report_path = OUTPUT_DIR / "filing_rate_report.json"
    if not report_path.exists():
        print("ERROR: output/filing_rate_report.json not found. Run src/agents/filing_rate_monitor.py first.", file=sys.stderr)
        sys.exit(1)

    report = load_report(report_path)
    history = load_history(SNAP_DIR)

    html = build_html(report, history)
    out_path = OUTPUT_DIR / "filing_rate_dashboard.html"
    out_path.write_text(html)
    print(f"→ {out_path} ({out_path.stat().st_size / 1024:.0f} KB)")


if __name__ == "__main__":
    main()
