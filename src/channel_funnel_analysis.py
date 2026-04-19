"""
채널 퍼널 분석 — 신청→접수→결정→결제 단계별 전환율, 분기 트렌드, 자력전환율

출력: output/channel_funnel.json
CLI:  python src/channel_funnel_analysis.py
"""
from __future__ import annotations
import json, sqlite3
from datetime import date, timedelta
from pathlib import Path

DB   = Path("/tmp/history.sqlite")
ROOT = Path(__file__).resolve().parents[1]
OUT  = ROOT / "output" / "channel_funnel.json"
EXCL = ("A(지수)",)
CRM_SOURCES = ("alrimtalk", "sms", "kakaochannel")
MIN_DEALS = 100  # 퍼널 신뢰도 최소 딜 수


def excl():
    q = ",".join(f"'{p}'" for p in EXCL)
    return f"AND pipeline NOT IN ({q})"


def run():
    con = sqlite3.connect(DB)
    as_of = con.execute("SELECT MAX(as_of_date) FROM deal_history").fetchone()[0]
    print(f"as_of = {as_of}")

    # ── 1. 채널별 퍼널 전환율 (12M 코호트) ─────────────────────────────────
    rows = con.execute(f"""
        SELECT
            COALESCE(NULLIF(utm_source,''), '(none)') src,
            COUNT(*) deals,
            SUM(apply_amount) apply,
            SUM(CASE WHEN filing_date  IS NOT NULL THEN 1 ELSE 0 END) filed,
            SUM(CASE WHEN decision_date IS NOT NULL THEN 1 ELSE 0 END) decided,
            SUM(CASE WHEN status='won' THEN 1 ELSE 0 END) won,
            SUM(CASE WHEN payment_date IS NOT NULL THEN 1 ELSE 0 END) paid,
            SUM(payment_amount) pay_amt
        FROM deal_history
        WHERE as_of_date=?
          AND apply_date BETWEEN '2024-11-01' AND '2025-10-31'
          {excl()}
        GROUP BY src
        HAVING deals >= {MIN_DEALS}
        ORDER BY apply DESC
    """, (as_of,)).fetchall()

    funnel = []
    for src, deals, apply, filed, decided, won, paid, pay_amt in rows:
        funnel.append({
            "channel":        src,
            "deals":          deals,
            "apply_oku":      round((apply or 0) / 1e8, 2),
            "filing_n":       filed,
            "decision_n":     decided,
            "won_n":          won,
            "payment_n":      paid,
            "filing_rate":    round(100 * filed   / deals, 1),
            "decision_rate":  round(100 * decided / deals, 1),
            "won_rate":       round(100 * won     / deals, 1),
            "payment_rate":   round(100 * paid    / deals, 1),
            "yield_pct":      round(100 * (pay_amt or 0) / apply, 2) if apply else None,
        })
    print(f"  funnel channels: {len(funnel)}")

    # ── 2. 분기별 채널 수익률 트렌드 ────────────────────────────────────────
    quarters = [
        ("2024Q4", "2024-10-01", "2024-12-31"),
        ("2025Q1", "2025-01-01", "2025-03-31"),
        ("2025Q2", "2025-04-01", "2025-06-30"),
        ("2025Q3", "2025-07-01", "2025-09-30"),
        ("2025Q4", "2025-10-01", "2025-12-31"),
    ]
    # 상위 15개 채널 (12M 신청액 기준)
    top_channels = [r["channel"] for r in funnel[:15]]

    trend = {ch: [] for ch in top_channels}
    for q_label, fr, to in quarters:
        q_rows = con.execute(f"""
            SELECT
                COALESCE(NULLIF(utm_source,''), '(none)') src,
                COUNT(*) deals,
                SUM(apply_amount) apply,
                SUM(payment_amount) pay
            FROM deal_history
            WHERE as_of_date=? AND apply_date BETWEEN ? AND ? {excl()}
            GROUP BY src
        """, (as_of, fr, to)).fetchall()
        by_ch = {r[0]: r for r in q_rows}
        for ch in top_channels:
            r = by_ch.get(ch)
            if r and r[2] and r[2] > 0:
                trend[ch].append({
                    "quarter": q_label,
                    "deals":   r[1],
                    "apply_oku": round(r[2] / 1e8, 2),
                    "yield_pct": round(100 * (r[3] or 0) / r[2], 2),
                })
            else:
                trend[ch].append({"quarter": q_label, "deals": 0, "apply_oku": 0, "yield_pct": None})

    print(f"  trend channels: {len(trend)}, quarters: {len(quarters)}")

    # ── 3. 자력전환율 — CRM 재유입 없이 자체 결제한 비율 ───────────────────
    self_conv_rows = con.execute(f"""
        SELECT
            COALESCE(NULLIF(utm_source_query,''), COALESCE(NULLIF(utm_source,''),'(none)')) first_ch,
            CASE WHEN utm_source != utm_source_query
                  AND (utm_source LIKE '%alrimtalk%' OR utm_source LIKE '%sms%'
                       OR utm_source LIKE '%kakaochannel%')
                 THEN 'crm' ELSE 'self' END as touch_type,
            COUNT(*) deals,
            SUM(apply_amount) apply,
            SUM(CASE WHEN payment_date IS NOT NULL THEN 1 ELSE 0 END) paid,
            SUM(payment_amount) pay_amt
        FROM deal_history
        WHERE as_of_date=?
          AND apply_date BETWEEN '2024-11-01' AND '2025-10-31'
          {excl()}
        GROUP BY first_ch, touch_type
    """, (as_of,)).fetchall()

    self_map: dict[str, dict] = {}
    for ch, touch, deals, apply, paid, pay_amt in self_conv_rows:
        if ch not in self_map:
            self_map[ch] = {"self_deals": 0, "crm_deals": 0,
                            "self_paid": 0, "crm_paid": 0,
                            "self_apply": 0, "crm_apply": 0,
                            "self_pay": 0, "crm_pay": 0}
        m = self_map[ch]
        m[f"{touch}_deals"] += deals
        m[f"{touch}_paid"]  += paid
        m[f"{touch}_apply"] += (apply or 0)
        m[f"{touch}_pay"]   += (pay_amt or 0)

    self_conv = []
    for ch, m in self_map.items():
        total = m["self_deals"] + m["crm_deals"]
        if total < MIN_DEALS:
            continue
        self_pct  = round(100 * m["self_deals"] / total, 1) if total else None
        crm_pct   = round(100 * m["crm_deals"]  / total, 1) if total else None
        self_yield = round(100 * m["self_pay"] / m["self_apply"], 2) if m["self_apply"] > 0 else None
        crm_yield  = round(100 * m["crm_pay"]  / m["crm_apply"],  2) if m["crm_apply"]  > 0 else None
        self_conv.append({
            "channel":        ch,
            "total_deals":    total,
            "self_deals":     m["self_deals"],
            "crm_deals":      m["crm_deals"],
            "self_pct":       self_pct,
            "crm_pct":        crm_pct,
            "self_yield":     self_yield,
            "crm_yield":      crm_yield,
            "apply_oku":      round((m["self_apply"] + m["crm_apply"]) / 1e8, 2),
        })
    self_conv.sort(key=lambda r: r["apply_oku"], reverse=True)
    print(f"  self-conv channels: {len(self_conv)}")

    con.close()

    result = {
        "generated_at": __import__("datetime").datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "as_of": as_of,
        "min_deals": MIN_DEALS,
        "funnel": funnel,
        "quarterly_trend": trend,
        "quarters": [q[0] for q in quarters],
        "self_conversion": self_conv,
    }
    OUT.parent.mkdir(exist_ok=True)
    OUT.write_text(json.dumps(result, ensure_ascii=False, indent=2))
    print(f"\n→ {OUT}")
    return result


if __name__ == "__main__":
    run()
