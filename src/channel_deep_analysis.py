"""[2~4/4] 채널 정밀 분석 — 신규/리마인드 분리, campaign A/B, 멀티터치 기여도.

산출:
  output/channel_deep_analysis.json
  output/channel_deep_analysis.csv
"""
from __future__ import annotations
import csv, json, sqlite3
from datetime import date, timedelta
from pathlib import Path

DB = Path("/tmp/history.sqlite")
ROOT = Path(__file__).resolve().parents[1]
OUT_JSON = ROOT / "output" / "channel_deep_analysis.json"
OUT_CSV  = ROOT / "output" / "channel_deep_analysis.csv"

# ── 리마인드 채널 정의 ─────────────────────────────────────────────
REMIND_SOURCES = {
    "alrimtalk", "alrimtalk_manual", "alrimtalk.toss.join",
    "sms", "kakaochannel", "kakaobrandmsg", "friendtalk",
}
REMIND_MEDIUMS = {"crm", "sms"}
EXCL_PIPELINE  = ("A(지수)",)

def channel_type(src: str, med: str) -> str:
    s, m = (src or "").lower(), (med or "").lower()
    if s in REMIND_SOURCES or m in REMIND_MEDIUMS:
        return "remind"
    if not s or s == "(none)":
        return "unknown"
    return "new"

def latest(con): return con.execute("SELECT MAX(as_of_date) FROM deal_history").fetchone()[0]

def excl():
    q = ",".join(f"'{p}'" for p in EXCL_PIPELINE)
    return f"AND pipeline NOT IN ({q})"

def window_dates(as_of: str):
    d = date.fromisoformat(as_of)
    return {
        "12M": ("2024-11-01", "2025-10-31", True),
        "3M":  ((d-timedelta(days=90)).isoformat(), as_of, False),
        "1M":  ((d-timedelta(days=30)).isoformat(), as_of, False),
    }

# ══ [2/4] 신규 vs 리마인드 ══════════════════════════════════════════
def new_vs_remind(con, as_of):
    rows = con.execute(f"""
        SELECT
            COALESCE(NULLIF(utm_source,''),'(none)') src,
            COALESCE(NULLIF(utm_medium,''),'(none)') med,
            apply_date,
            apply_amount, payment_amount,
            CASE WHEN status='won' THEN 1 ELSE 0 END won,
            CASE WHEN payment_date IS NOT NULL THEN 1 ELSE 0 END paid
        FROM deal_history
        WHERE as_of_date=? {excl()}
    """, (as_of,)).fetchall()

    wins = window_dates(as_of)
    result = {}
    for wk, (fr, to, mature) in wins.items():
        buckets = {"new":{}, "remind":{}, "unknown":{}}
        for src,med,apply_date,apply_amt,pay_amt,won,paid in rows:
            if not apply_date or not (fr <= apply_date[:10] <= to): continue
            ct = channel_type(src, med)
            key = f"{src}|{med}"
            b = buckets[ct].setdefault(key, dict(src=src,med=med,deals=0,apply=0,pay=0,won=0,paid=0))
            b["deals"]+=1; b["apply"]+=apply_amt or 0
            b["pay"]+=pay_amt or 0; b["won"]+=won; b["paid"]+=paid

        out = {}
        for ct, items in buckets.items():
            rows_sorted = sorted(items.values(), key=lambda r:-r["apply"])
            total_deals = sum(r["deals"] for r in rows_sorted)
            total_apply = sum(r["apply"] for r in rows_sorted)
            total_pay   = sum(r["pay"]   for r in rows_sorted)
            total_won   = sum(r["won"]   for r in rows_sorted)
            for r in rows_sorted:
                a = r["apply"]; s = r["deals"]
                r["apply_oku"]   = round(a/1e8, 3)
                r["payment_oku"] = round(r["pay"]/1e8, 3)
                r["yield_pct"]   = round(100*r["pay"]/a, 2) if a>0 else None
                r["won_rate"]    = round(100*r["won"]/s,1)  if s>0 else None
                r["paid_rate"]   = round(100*r["paid"]/s,1) if s>0 else None
            out[ct] = {
                "total_deals": total_deals,
                "total_apply_oku": round(total_apply/1e8,2),
                "total_payment_oku": round(total_pay/1e8,2),
                "yield_pct": round(100*total_pay/total_apply,2) if total_apply>0 else None,
                "won_rate": round(100*total_won/total_deals,1) if total_deals>0 else None,
                "top_sources": rows_sorted[:15],
                "matured": mature,
            }
        result[wk] = out
    return result

# ══ [3/4] utm_campaign A/B ══════════════════════════════════════════
def campaign_ab(con, as_of):
    rows = con.execute(f"""
        SELECT
            COALESCE(NULLIF(utm_source,''),'(none)') src,
            COALESCE(NULLIF(utm_medium,''),'(none)') med,
            COALESCE(NULLIF(utm_campaign,''),'(none)') camp,
            COUNT(DISTINCT deal_id)               deals,
            ROUND(SUM(apply_amount)/1e8,3)        apply_oku,
            ROUND(SUM(payment_amount)/1e8,3)      pay_oku,
            SUM(CASE WHEN status='won' THEN 1 ELSE 0 END) won,
            SUM(CASE WHEN payment_date IS NOT NULL THEN 1 ELSE 0 END) paid
        FROM deal_history
        WHERE as_of_date=?
          AND apply_date BETWEEN '2024-11-01' AND '2025-10-31'
          {excl()}
          AND utm_campaign IS NOT NULL AND utm_campaign != ''
        GROUP BY 1,2,3
        HAVING deals >= 10
        ORDER BY apply_oku DESC
    """, (as_of,)).fetchall()

    out = []
    for src,med,camp,deals,apply_oku,pay_oku,won,paid in rows:
        ct = channel_type(src,med)
        apply = apply_oku*1e8 if apply_oku else 0
        pay   = pay_oku*1e8   if pay_oku   else 0
        out.append({
            "channel_type": ct,
            "utm_source": src, "utm_medium": med, "utm_campaign": camp,
            "deals": deals,
            "apply_oku": apply_oku,
            "payment_oku": pay_oku,
            "yield_pct": round(100*pay/apply,2) if apply>0 else None,
            "won_rate":  round(100*won/deals,1)  if deals>0 else None,
            "paid_rate": round(100*paid/deals,1) if deals>0 else None,
        })
    return out

# ══ [4/4] 멀티터치 기여도 ═══════════════════════════════════════════
def multitouch(con, as_of):
    rows = con.execute(f"""
        SELECT
            COALESCE(NULLIF(utm_source,''),'(none)')       last_src,
            COALESCE(NULLIF(utm_source_query,''),'(none)') first_src,
            COALESCE(NULLIF(utm_medium,''),'(none)')       last_med,
            COALESCE(NULLIF(utm_medium_query,''),'(none)') first_med,
            COUNT(DISTINCT deal_id)                        deals,
            ROUND(SUM(apply_amount)/1e8,3)                 apply_oku,
            ROUND(SUM(payment_amount)/1e8,3)               pay_oku,
            ROUND(AVG(apply_amount)/1e4,1)                 avg_apply_manwon
        FROM deal_history
        WHERE as_of_date=?
          AND apply_date BETWEEN '2024-11-01' AND '2025-10-31'
          {excl()}
        GROUP BY 1,2,3,4
        HAVING deals >= 5
        ORDER BY apply_oku DESC
        LIMIT 60
    """, (as_of,)).fetchall()

    same, diff = [], []
    for r in rows:
        last_src, first_src, last_med, first_med, deals, apply_oku, pay_oku, avg = r
        apply = (apply_oku or 0)*1e8; pay = (pay_oku or 0)*1e8
        rec = {
            "last_touch_src": last_src, "first_touch_src": first_src,
            "last_touch_med": last_med, "first_touch_med": first_med,
            "touch_match": (last_src==first_src),
            "deals": deals,
            "apply_oku": apply_oku,
            "payment_oku": pay_oku,
            "yield_pct": round(100*pay/apply,2) if apply>0 else None,
            "avg_apply_manwon": avg,
        }
        (same if last_src==first_src else diff).append(rec)

    # 채널 조합별 유형 집계
    combo_stats = {}
    for r in rows:
        last_src, first_src = r[0], r[1]
        is_match = (last_src == first_src)
        k = f"{first_src} → {last_src}" if not is_match else f"[단일] {last_src}"
        b = combo_stats.setdefault(k, dict(deals=0, apply=0, pay=0, is_multitouch=not is_match,
                                           first=first_src, last=last_src))
        b["deals"] += r[4]; b["apply"] += (r[5] or 0)*1e8; b["pay"] += (r[6] or 0)*1e8

    combos = sorted(combo_stats.values(), key=lambda x:-x["apply"])
    for c in combos:
        a = c["apply"]
        c["apply_oku"] = round(a/1e8, 3)
        c["yield_pct"] = round(100*c["pay"]/a, 2) if a>0 else None
        del c["apply"], c["pay"]

    total = sum(r[4] for r in rows)
    multi = sum(r[4] for r in rows if r[0]!=r[1])
    return {
        "multitouch_rate_pct": round(100*multi/total,1) if total else None,
        "single_touch_count": total-multi,
        "multitouch_count": multi,
        "top_combos": combos[:20],
        "single_touch_detail": same[:15],
        "multitouch_detail": diff[:20],
    }

# ══ Main ════════════════════════════════════════════════════════════
def run():
    con = sqlite3.connect(DB)
    as_of = latest(con)
    print(f"as_of={as_of}")

    result = {
        "as_of_date": as_of,
        "new_vs_remind": new_vs_remind(con, as_of),
        "campaign_ab":   campaign_ab(con, as_of),
        "multitouch":    multitouch(con, as_of),
    }
    OUT_JSON.parent.mkdir(exist_ok=True)
    OUT_JSON.write_text(json.dumps(result, ensure_ascii=False, indent=2))

    # long CSV (campaign_ab용)
    rows = result["campaign_ab"]
    if rows:
        with OUT_CSV.open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader(); w.writerows(rows)
    print(f"→ {OUT_JSON}\n→ {OUT_CSV}")
    return result

if __name__ == "__main__":
    r = run()
    # ── 2/4 요약 ──
    print("\n═══ [2/4] 신규 vs 리마인드 (12M) ═══")
    for ct, d in r["new_vs_remind"]["12M"].items():
        print(f"  {ct:<8} deals={d['total_deals']:>6,}  apply={d['total_apply_oku']:>6.1f}억  yield={d['yield_pct']}%  won={d['won_rate']}%")
    # ── 3/4 요약 ──
    print("\n═══ [3/4] 캠페인 A/B Top 20 (12M, new 채널) ═══")
    for r2 in [x for x in r["campaign_ab"] if x["channel_type"]=="new"][:20]:
        print(f"  {r2['utm_source']:<20} {r2['utm_campaign'][:30]:<30} apply={r2['apply_oku']}억  yield={r2['yield_pct']}%")
    # ── 4/4 요약 ──
    mt = r["multitouch"]
    print(f"\n═══ [4/4] 멀티터치 ═══")
    print(f"  전체 deal(12M 표본): {mt['single_touch_count']+mt['multitouch_count']:,}")
    print(f"  멀티터치 비율: {mt['multitouch_rate_pct']}%")
    print(f"\n  Top 멀티터치 경로 (first→last):")
    for c in [x for x in mt["top_combos"] if x["is_multitouch"]][:10]:
        print(f"    {c['first']:<20} → {c['last']:<20}  deals={c['deals']:>5}  apply={c['apply_oku']}억  yield={c['yield_pct']}%")
