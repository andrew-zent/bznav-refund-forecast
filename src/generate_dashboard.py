"""
forecast.json → HTML 대시보드 자동 생성.
templates/dashboard_template.html에서 {{DATA}} 치환.
"""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = ROOT / "output"
TEMPLATE_DIR = ROOT / "templates"


def main():
    forecast_path = OUTPUT_DIR / "forecast.json"
    if not forecast_path.exists():
        raise FileNotFoundError("output/forecast.json not found. Run model.py first.")

    data = json.loads(forecast_path.read_text())

    # 대시보드 HTML에 JSON 데이터 주입
    template_path = TEMPLATE_DIR / "dashboard_template.html"
    if not template_path.exists():
        print("templates/dashboard_template.html not found. Using inline generation.")
        html = _generate_inline(data)
    else:
        html = template_path.read_text().replace("{{FORECAST_DATA}}", json.dumps(data, ensure_ascii=False))

    out_path = OUTPUT_DIR / "index.html"
    out_path.write_text(html)
    print(f"→ {out_path} ({out_path.stat().st_size / 1024:.0f} KB)")


def _marketing_section(data: dict) -> tuple[str, str]:
    """마케팅 cohort LTV 섹션 HTML + 추가 chart JS 반환."""
    ms = data.get("monthly_series") or {}
    coh = data.get("apply_to_pay_cohort", {}).get("all") or []
    fc_coh = data.get("filing_to_pay_cohort", {}).get("all") or []
    dc_coh = data.get("decision_to_pay_cohort", {}).get("all") or []
    pool = data.get("collection_pool") or {}
    pool_trend = data.get("collection_pool_trend") or []

    if not coh:
        return ("", "")

    # 완성 코호트 (2024-11 ~ 2025-10 등 M+6 경과분) 평균 전환율
    # current_m 유추: 마지막 apply_month
    last_m = coh[-1]["apply_month"]
    # 단순 기준: 최근 6개 cohort 제외한 나머지를 성숙으로 간주
    mature_coh = [r for r in coh[:-6] if r["apply_amount"] > 0]
    if mature_coh:
        sa = sum(r["apply_amount"] for r in mature_coh)
        sp = sum(r["paid_total"] for r in mature_coh)
        mature_rate = sp / sa * 100 if sa > 0 else 0
    else:
        mature_rate = 20.0

    # In-flight 예측: offset 분포 기반
    off_rate = {}
    for r in mature_coh:
        for x in r["paid_by_offset"]:
            off_rate[x["off"]] = off_rate.get(x["off"], 0) + x["paid"]
    sa_m = sum(r["apply_amount"] for r in mature_coh)
    for o in off_rate:
        off_rate[o] = off_rate[o] / sa_m if sa_m > 0 else 0

    # 최근 6개 cohort 예측 테이블
    current_apply = coh[-1]
    current_m_label = current_apply["apply_month"]
    year, month = map(int, current_m_label.split("-"))
    current_m_idx = year * 12 + (month - 1)
    inflight_rows = ""
    for r in coh[-6:]:
        y, m = map(int, r["apply_month"].split("-"))
        apply_m_idx = y * 12 + (m - 1)
        elapsed = current_m_idx - apply_m_idx
        app = r["apply_amount"]
        if app <= 0:
            continue
        by_off = {x["off"]: x["paid"] for x in r["paid_by_offset"]}
        observed = sum(v for o, v in by_off.items() if o <= elapsed)
        remaining = app * sum(off_rate.get(o, 0) for o in range(elapsed + 1, 25))
        predicted = observed + remaining
        pred_pct = predicted / app * 100
        mark = "🟡" if elapsed < 6 else "🟢"
        inflight_rows += (
            f"<tr><td>{r['apply_month']}</td><td>{app:.1f}</td><td>M+{elapsed}</td>"
            f"<td>{observed:.2f}</td><td>{remaining:.2f}</td>"
            f"<td style='color:#58a6ff;font-weight:700;'>{predicted:.2f}</td>"
            f"<td>{pred_pct:.1f}%</td><td>{mark}</td></tr>"
        )

    # 단계별 정밀화 비교 (최근 완전월)
    if len(coh) >= 2:
        ref = coh[-2]  # 직전월 (partial이 아닌)
    else:
        ref = coh[-1]
    ref_month = ref["apply_month"]
    ref_apply = ref["apply_amount"]
    ref_filing = next((x["source_amount"] for x in fc_coh if x.get("source_month") == ref_month), 0)
    ref_dec = next((x["source_amount"] for x in dc_coh if x.get("source_month") == ref_month), 0)
    stage_rows = (
        f"<tr><td>T+0 당일</td><td>신청환급금</td><td>{ref_apply:.1f}억</td><td>× {mature_rate:.2f}%</td>"
        f"<td style='color:#58a6ff'>{ref_apply * mature_rate / 100:.2f}억</td></tr>"
        f"<tr><td>T+7 신고 후</td><td>신고환급액</td><td>{ref_filing:.1f}억</td><td>× 30%</td>"
        f"<td style='color:#58a6ff'>{ref_filing * 0.30:.2f}억</td></tr>"
        f"<tr><td>T+14 결정 후</td><td>결정환급액</td><td>{ref_dec:.1f}억</td><td>× 31%</td>"
        f"<td style='color:#58a6ff'>{ref_dec * 0.31:.2f}억</td></tr>"
    )

    # Pool 추이 chart data
    pool_labels = json.dumps([p["month"] for p in pool_trend])
    pool_balance_data = json.dumps([p["balance"] for p in pool_trend])
    pool_paid_data = json.dumps([p["paid"] for p in pool_trend])

    # KPI
    pool_bal = pool.get("balance", 0)
    pool_util = pool.get("utilization_rate_pct", 0)
    monthly_harvest = round(pool_bal * pool_util / 100, 2)

    html = f"""
<div style="margin:24px 0 8px 0; padding-top:24px; border-top:2px solid #388bfd;">
  <h1 style="font-size:18px; color:#3fb950;">🎯 마케팅 Cohort LTV (신청 기준)</h1>
  <div style="font-size:11px; color:#8b949e; margin-bottom:12px;">
    ⚠ 아래 값은 해당 월 신청 cohort의 <b>lifetime 예상 결제액</b>이며, 상단 월별 cash inflow와는 별개 지표입니다.
    12개월 성숙 코호트 평균 전환율: <b style="color:#3fb950">{mature_rate:.2f}%</b>
    (마케팅 base 환산: <b>{mature_rate / 4.77:.2f}%</b>)
  </div>
</div>

<div class="kpi-row">
  <div class="kpi" style="border-color:#3fb950;">
    <div class="kpi-label">{current_m_label} 신청환급금 (내부 base)</div>
    <div class="kpi-value" style="color:#3fb950;">{current_apply['apply_amount']:.1f}<span style="font-size:12px;color:#8b949e">억</span></div>
    <div class="kpi-sub">예상 LTV: {current_apply['apply_amount'] * mature_rate / 100:.1f}억</div>
  </div>
  <div class="kpi" style="border-color:#3fb950;">
    <div class="kpi-label">결정금액 풀 잔액</div>
    <div class="kpi-value" style="color:#3fb950;">{pool_bal:.1f}<span style="font-size:12px;color:#8b949e">억</span></div>
    <div class="kpi-sub">월 회수율 {pool_util:.2f}% → {monthly_harvest:.2f}억/월</div>
  </div>
  <div class="kpi" style="border-color:#3fb950;">
    <div class="kpi-label">누수율 (B결정→추심)</div>
    <div class="kpi-value" style="color:#3fb950;">{pool.get('leak_pct', 0):.1f}<span style="font-size:12px;color:#8b949e">%</span></div>
    <div class="kpi-sub">유입률 {pool.get('inflow_rate_pct', 0):.1f}%</div>
  </div>
  <div class="kpi" style="border-color:#3fb950;">
    <div class="kpi-label">월간 cohort 기여 공식</div>
    <div class="kpi-value" style="color:#3fb950; font-size:14px; line-height:1.3;">신청×{mature_rate:.1f}%<br>+ 풀×{pool_util:.2f}%</div>
    <div class="kpi-sub">하이브리드 forecast</div>
  </div>
</div>

<div class="grid-2">
  <div class="card">
    <h2>📋 단계별 정밀화 ({ref_month} 기준)</h2>
    <table>
      <thead><tr><th>시점</th><th>입력</th><th>금액</th><th>계수</th><th>예상결제액</th></tr></thead>
      <tbody>{stage_rows}</tbody>
    </table>
    <div style="font-size:10px; color:#6e7681; margin-top:8px;">
      시간 경과에 따라 자동 replace: T+0 신청 rough → T+7 신고 정밀 → T+14 결정 확정
    </div>
  </div>
  <div class="card">
    <h2>🔄 In-flight Cohort 예측 (최근 6개월)</h2>
    <table>
      <thead><tr><th>신청월</th><th>신청</th><th>경과</th><th>관찰</th><th>남은</th><th>예측최종</th><th>예측%</th><th></th></tr></thead>
      <tbody>{inflight_rows}</tbody>
    </table>
  </div>
</div>

<div class="card">
  <h2>💰 결정금액 풀 추이 (24개월)</h2>
  <div class="chart-c"><canvas id="poolChart"></canvas></div>
  <div style="font-size:11px; color:#8b949e; margin-top:6px;">
    풀 잔액 = 결정났지만 미결제된 추심 건의 합. 풀이 증가하면 향후 매출 ↑, 감소하면 향후 매출 ↓ 선행지표.
  </div>
</div>
"""
    chart_js = f"""
new Chart(document.getElementById('poolChart'),{{type:'line',data:{{labels:{pool_labels},datasets:[
  {{label:'풀 잔액 (억)',data:{pool_balance_data},borderColor:'#3fb950',backgroundColor:'rgba(63,185,80,0.1)',fill:true,tension:.3,pointRadius:3,borderWidth:2,yAxisID:'y'}},
  {{label:'월 결제 회수 (억)',data:{pool_paid_data},borderColor:'#d29922',tension:.3,pointRadius:3,borderWidth:2,yAxisID:'y1'}}
]}},options:{{maintainAspectRatio:false,responsive:true,plugins:{{legend:{{position:'top',labels:{{boxWidth:10}}}}}},scales:{{y:{{...bs,position:'left',title:{{display:true,text:'풀 잔액'}}}},y1:{{...bs,position:'right',grid:{{drawOnChartArea:false}},title:{{display:true,text:'월 회수'}}}},x:{{...bs}}}}}}}});
"""
    return html, chart_js


def _diagnosis_section() -> tuple[str, str]:
    """timeline.json + 최신 snapshot 기반 진단 KPI + 시계열 차트."""
    timeline_path = OUTPUT_DIR / "timeline.json"
    if not timeline_path.exists():
        return ("", "")
    timeline = json.loads(timeline_path.read_text())
    entries = timeline.get("entries", [])
    if not entries:
        return ("", "")
    latest = entries[-1]

    # 최신 snapshot에서 Top 사유들 가져오기
    date_str = latest.get("date", "")
    snap_path = OUTPUT_DIR / "snapshots" / f"{date_str}.json"
    snap = json.loads(snap_path.read_text()) if snap_path.exists() else None

    # 시계열 chart 데이터
    dates = json.dumps([e["date"] for e in entries])
    conv_u = json.dumps([e.get("conversion_unfiltered_pct", 0) for e in entries])
    conv_f = json.dumps([e.get("conversion_filtered_pct", 0) for e in entries])
    pool_bal = json.dumps([e.get("pool_balance", 0) for e in entries])
    pool_paid = json.dumps([e.get("pool_monthly_paid_3mo_avg", 0) for e in entries])

    # Pipeline 비중 변화 (주요 pipeline만)
    main_pipes = ["A(지수)", "B(젠트)-환급", "D(젠트)-취소", "C(젠트)-추심"]
    pipe_datasets = []
    pipe_colors = {"A(지수)": "#f85149", "B(젠트)-환급": "#3fb950",
                   "D(젠트)-취소": "#d29922", "C(젠트)-추심": "#58a6ff"}
    for p in main_pipes:
        data_vals = [e.get("pipeline_shares", {}).get(p, 0) for e in entries]
        pipe_datasets.append({"label": p, "data": data_vals, "color": pipe_colors.get(p, "#888")})

    # KPI 카드
    conv = snap["conversion"]["unfiltered"] if snap else {"conversion_pct": latest.get("conversion_unfiltered_pct", 0)}
    a_share = latest.get("pipeline_shares", {}).get("A(지수)", 0)
    b_share = latest.get("pipeline_shares", {}).get("B(젠트)-환급", 0)

    # Top 3 lost + cancel reasons
    lost_rows = ""
    cancel_rows = ""
    if snap:
        for r in snap["top_lost_reasons"]["top"][:5]:
            lost_rows += f'<tr><td>{r["label"]}</td><td>{r["apply_amount"]:.1f}</td><td>{r["deal_count"]:,}</td><td>{r["share_pct"]:.1f}%</td></tr>'
        for r in snap["top_cancel_reasons"]["top"][:5]:
            cancel_rows += f'<tr><td>{r["label"]}</td><td>{r["apply_amount"]:.1f}</td><td>{r["deal_count"]:,}</td><td>{r["share_pct"]:.1f}%</td></tr>'

    html = f"""
<div style="margin:24px 0 8px 0; padding-top:24px; border-top:2px solid #d29922;">
  <h1 style="font-size:18px; color:#d29922;">📊 진단 KPI + 시계열 (주간 스냅샷)</h1>
  <div style="font-size:11px; color:#8b949e; margin-bottom:12px;">
    매주 월요일 자동 스냅샷. 완성 코호트 12개월 기준. 최신: {date_str} · 누적 {len(entries)}주.
  </div>
</div>

<div class="kpi-row">
  <div class="kpi" style="border-color:#d29922;">
    <div class="kpi-label">Unfiltered 전환율</div>
    <div class="kpi-value" style="color:#d29922;">{conv.get("conversion_pct", 0):.2f}<span style="font-size:12px;color:#8b949e">%</span></div>
    <div class="kpi-sub">마케팅 base × 4.7% 권장</div>
  </div>
  <div class="kpi" style="border-color:#d29922;">
    <div class="kpi-label">A(지수) pipeline 비중</div>
    <div class="kpi-value" style="color:#f85149;">{a_share:.1f}<span style="font-size:12px;color:#8b949e">%</span></div>
    <div class="kpi-sub">dead deal 공급처 — 낮출수록 좋음</div>
  </div>
  <div class="kpi" style="border-color:#d29922;">
    <div class="kpi-label">B(젠트)-환급 비중</div>
    <div class="kpi-value" style="color:#3fb950;">{b_share:.1f}<span style="font-size:12px;color:#8b949e">%</span></div>
    <div class="kpi-sub">실제 매출 pipeline</div>
  </div>
  <div class="kpi" style="border-color:#d29922;">
    <div class="kpi-label">풀 3개월 평균 결제</div>
    <div class="kpi-value" style="color:#d29922;">{latest.get("pool_monthly_paid_3mo_avg", 0):.2f}<span style="font-size:12px;color:#8b949e">억</span></div>
    <div class="kpi-sub">rolling 추심 회수</div>
  </div>
</div>

<div class="grid-2">
  <div class="card"><h2>📈 전환율 시계열</h2><div class="chart-c"><canvas id="convTrend"></canvas></div></div>
  <div class="card"><h2>📊 Pipeline 비중 시계열</h2><div class="chart-c"><canvas id="pipeTrend"></canvas></div></div>
</div>

<div class="grid-2">
  <div class="card">
    <h2>🚫 실패 사유 Top 5 ({date_str})</h2>
    <table>
      <thead><tr><th>사유</th><th>신청액(억)</th><th>건수</th><th>비중</th></tr></thead>
      <tbody>{lost_rows}</tbody>
    </table>
  </div>
  <div class="card">
    <h2>❌ 취소 사유 Top 5 ({date_str})</h2>
    <table>
      <thead><tr><th>사유</th><th>신청액(억)</th><th>건수</th><th>비중</th></tr></thead>
      <tbody>{cancel_rows}</tbody>
    </table>
  </div>
</div>
"""
    pipe_ds_js = "[" + ",".join(
        f'{{label:"{d["label"]}",data:{json.dumps(d["data"])},borderColor:"{d["color"]}",tension:.3,pointRadius:3,borderWidth:2}}'
        for d in pipe_datasets
    ) + "]"

    chart_js = f"""
new Chart(document.getElementById('convTrend'),{{type:'line',data:{{labels:{dates},datasets:[
  {{label:'Unfiltered (마케팅 base)',data:{conv_u},borderColor:'#d29922',backgroundColor:'rgba(210,153,34,0.1)',fill:true,tension:.3,pointRadius:4,borderWidth:2.5}},
  {{label:'Filtered (내부 base)',data:{conv_f},borderColor:'#3fb950',tension:.3,pointRadius:4,borderWidth:2}}
]}},options:{{maintainAspectRatio:false,responsive:true,plugins:{{legend:{{position:'top',labels:{{boxWidth:10}}}}}},scales:{{y:{{...bs,title:{{display:true,text:'전환율 %'}}}},x:{{...bs}}}}}}}});
new Chart(document.getElementById('pipeTrend'),{{type:'line',data:{{labels:{dates},datasets:{pipe_ds_js}}},options:{{maintainAspectRatio:false,responsive:true,plugins:{{legend:{{position:'top',labels:{{boxWidth:10}}}}}},scales:{{y:{{...bs,title:{{display:true,text:'비중 %'}}}},x:{{...bs}}}}}}}});
"""
    return html, chart_js


def _generate_inline(data: dict) -> str:
    """forecast.json을 기반으로 간략 대시보드 HTML 생성."""
    fc = data["forecast"]
    bt = data["backtest"]
    mape = data["mape"]
    gen = data["generated_at"]
    has_corp = bool(fc and "individual" in fc[0])
    marketing_html, marketing_js = _marketing_section(data)
    diagnosis_html, diagnosis_js = _diagnosis_section()

    if has_corp:
        fc_rows = "".join(
            f'<tr><td><b>{f["month"]}</b></td>'
            f'<td>{f["individual"]["regular"]}</td><td>{f["individual"]["collection"]}</td>'
            f'<td>{f["individual"]["adjusted"]}</td>'
            f'<td>{f["corporate"]["total"]}</td>'
            f'<td style="color:#58a6ff;font-weight:700;">{f["grand_total"]}</td></tr>'
            for f in fc
        )
    else:
        fc_rows = "".join(
            f'<tr><td><b>{f["month"]}</b></td><td>{f["regular"]}</td><td>{f["collection"]}</td>'
            f'<td><b>{f["total"]}</b></td><td>{f["season_adj"]:+.0%}</td>'
            f'<td style="color:#58a6ff;font-weight:700;">{f["adjusted"]}</td></tr>'
            for f in fc
        )
    bt_rows = "".join(
        f'<tr><td>{r["month"]}</td><td>{r["actual"]}</td><td>{r["predicted"]}</td>'
        f'<td style="color:{"#3fb950" if abs(r["error_pct"])<=10 else ("#d29922" if abs(r["error_pct"])<=20 else "#f85149")}">'
        f'{r["error_pct"]:+.1f}%</td>'
        f'<td>{"🟢" if abs(r["error_pct"])<=10 else ("🟡" if abs(r["error_pct"])<=20 else "🔴")}</td></tr>'
        for r in bt
    )

    # Chart data
    bt_labels = json.dumps([r["month"] for r in bt])
    bt_actual = json.dumps([r["actual"] for r in bt])
    bt_pred = json.dumps([r["predicted"] for r in bt])
    fc_labels = json.dumps([f["month"] for f in fc])
    if has_corp:
        fc_total = json.dumps([f["individual"]["adjusted"] for f in fc])
        fc_adj = json.dumps([f["grand_total"] for f in fc])
        fc_corp = json.dumps([f["corporate"]["total"] for f in fc])
    else:
        fc_total = json.dumps([f["total"] for f in fc])
        fc_adj = json.dumps([f["adjusted"] for f in fc])
        fc_corp = json.dumps([0] * len(fc))

    return f"""<!DOCTYPE html>
<html lang="ko"><head><meta charset="UTF-8">
<title>비즈넵 결제예측 대시보드</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
*{{box-sizing:border-box}}body{{font-family:-apple-system,"Apple SD Gothic Neo",sans-serif;margin:0;padding:20px;background:#0d1117;color:#e6edf3;font-size:13px;line-height:1.5}}
.container{{max-width:1200px;margin:0 auto}}
h1{{font-size:24px;margin:0 0 4px 0}}h2{{font-size:14px;margin:12px 0 6px 0}}
.meta{{font-size:11px;color:#6e7681;margin-bottom:16px}}
.meta span{{padding:2px 8px;background:#21262d;border-radius:4px;margin-right:6px}}
.kpi-row{{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:16px}}
.kpi{{background:#161b22;border:1px solid #30363d;padding:14px;border-radius:8px}}
.kpi.hl{{border-color:#388bfd;background:#1f2d3d}}.kpi-label{{color:#8b949e;font-size:10px;text-transform:uppercase}}.kpi-value{{font-size:22px;font-weight:700}}.kpi-sub{{font-size:10px;color:#8b949e;margin-top:4px}}
.grid-2{{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-bottom:14px}}
.card{{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:14px}}
table{{width:100%;border-collapse:collapse;font-size:12px}}th,td{{padding:5px 6px;text-align:right;border-bottom:1px solid #21262d}}th:first-child,td:first-child{{text-align:left}}th{{color:#8b949e;font-size:10px}}
.chart-c{{position:relative;height:280px}}
footer{{margin-top:20px;text-align:center;font-size:10px;color:#6e7681}}
</style></head><body>
<div class="container">
<h1>비즈넵 결제 예측 대시보드</h1>
<div class="meta"><span>갱신: {gen}</span><span>개인 MAPE {mape}%</span><span>개인: {data['total_claims']:,}건</span>{f'<span>법인: {data.get("total_corp_claims",0):,}건</span>' if data.get('total_corp_claims') else ''}</div>

<div class="kpi-row">
{''.join(f'<div class="kpi hl"><div class="kpi-label">{f["month"]} 통합</div><div class="kpi-value">{f["grand_total"]}<span style="font-size:12px;color:#8b949e">억</span></div><div class="kpi-sub">개인 {f["individual"]["adjusted"]}억 + 법인 {f["corporate"]["total"]}억</div></div>' for f in fc[:3]) if has_corp else ''.join(f'<div class="kpi hl"><div class="kpi-label">{f["month"]} 권장</div><div class="kpi-value">{f["adjusted"]}<span style="font-size:12px;color:#8b949e">억</span></div><div class="kpi-sub">모델 {f["total"]}억</div></div>' for f in fc[:3])}
<div class="kpi"><div class="kpi-label">12개월 MAPE</div><div class="kpi-value">{mape}%</div></div>
</div>

<div class="grid-2">
<div class="card"><h2>📊 향후 예측</h2><div class="chart-c"><canvas id="fcChart"></canvas></div></div>
<div class="card"><h2>🎯 예측값 (개인+법인)</h2>
<table><thead><tr><th>월</th><th>개인정기</th><th>개인추심</th><th>개인소계</th><th>법인</th><th style="color:#58a6ff">통합</th></tr></thead>
<tbody>{fc_rows}</tbody></table></div>
</div>

<div class="grid-2">
<div class="card"><h2>📈 12개월 백테스트</h2><div class="chart-c"><canvas id="btChart"></canvas></div></div>
<div class="card"><h2>📋 백테스트 상세</h2>
<table><thead><tr><th>월</th><th>실제</th><th>예측</th><th>오차</th><th></th></tr></thead>
<tbody>{bt_rows}</tbody></table></div>
</div>

{marketing_html}

{diagnosis_html}

<footer>지엔터프라이즈 · Phase 2 v2 코호트 분산 모델 · 자동 갱신</footer>
</div>
<script>
Chart.defaults.color='#8b949e';Chart.defaults.borderColor='#21262d';
const g={{color:'#21262d',drawBorder:false}},bs={{grid:g,ticks:{{color:'#8b949e'}}}};
new Chart(document.getElementById('fcChart'),{{type:'bar',data:{{labels:{fc_labels},datasets:[
{{label:'개인',data:{fc_total},backgroundColor:'#3fb950',borderRadius:4,stack:'s'}},
{{label:'법인',data:{fc_corp},backgroundColor:'#d29922',borderRadius:4,stack:'s'}},
{{label:'통합',data:{fc_adj},type:'line',borderColor:'#58a6ff',backgroundColor:'transparent',tension:.3,pointRadius:6,pointStyle:'rectRot',borderWidth:2.5}}
]}},options:{{maintainAspectRatio:false,responsive:true,plugins:{{legend:{{position:'top',labels:{{boxWidth:10}}}}}},scales:{{y:{{...bs,stacked:true}},x:{{...bs,stacked:true}}}}}}}});
new Chart(document.getElementById('btChart'),{{type:'line',data:{{labels:{bt_labels},datasets:[
{{label:'실제',data:{bt_actual},borderColor:'#58a6ff',backgroundColor:'rgba(88,166,255,0.2)',fill:true,tension:.3,pointRadius:4,borderWidth:2.5}},
{{label:'예측',data:{bt_pred},borderColor:'#3fb950',tension:.3,pointRadius:4,pointStyle:'triangle'}}
]}},options:{{maintainAspectRatio:false,responsive:true,plugins:{{legend:{{position:'top',labels:{{boxWidth:10}}}}}},scales:{{y:{{...bs}},x:{{...bs}}}}}}}});
{marketing_js}
{diagnosis_js}
</script></body></html>"""


if __name__ == "__main__":
    main()
