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


def _generate_inline(data: dict) -> str:
    """forecast.json을 기반으로 간략 대시보드 HTML 생성."""
    fc = data["forecast"]
    bt = data["backtest"]
    mape = data["mape"]
    gen = data["generated_at"]
    has_corp = bool(fc and "individual" in fc[0])

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
</script></body></html>"""


if __name__ == "__main__":
    main()
