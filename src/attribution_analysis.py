"""
Attribution Analysis — Multi-touch, CRM Lift, Volume×Yield

모델:
  - First Touch  : utm_source_query (없으면 utm_source 대체)
  - Last Touch   : utm_source (없으면 utm_source_query 대체)
  - Linear 50:50 : 멀티터치면 첫/마지막 0.5씩, 싱글이면 1.0

출력:
  output/attribution_analysis.json

CLI:
  python src/attribution_analysis.py
"""
from __future__ import annotations
import json
import sqlite3
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

DB = Path("/tmp/history.sqlite")
ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "output" / "attribution_analysis.json"

EXCL = ("A(지수)",)

WINDOWS = {
    "12M": ("2024-11-01", "2025-10-31"),
    "6M":  None,   # relative, computed at runtime
    "3M":  None,
    "1M":  None,
    "4W":  None,
    "1W":  None,
}


# ── helpers ──────────────────────────────────────────────────────────────────

def excl_clause():
    q = ",".join(f"'{p}'" for p in EXCL)
    return f"AND pipeline NOT IN ({q})"


def rel_date(as_of: str, days: int) -> str:
    return (date.fromisoformat(as_of) - timedelta(days=days)).isoformat()


def window_range(key: str, as_of: str):
    ranges = {
        "12M": ("2024-11-01", "2025-10-31"),
        "6M":  (rel_date(as_of, 180), as_of),
        "3M":  (rel_date(as_of, 90),  as_of),
        "1M":  (rel_date(as_of, 30),  as_of),
        "4W":  (rel_date(as_of, 28),  as_of),
        "1W":  (rel_date(as_of, 7),   as_of),
    }
    return ranges[key]


def clean(v) -> str:
    return (v or "").strip() or "(none)"


# ── core attribution engine ───────────────────────────────────────────────────

class ChannelBucket:
    def __init__(self):
        self.deals = 0
        self.apply = 0.0
        self.payment = 0.0
        self.won = 0
        self.paid = 0

    def add(self, weight: float, apply_amt: float, pay_amt: float, won: int, paid: int):
        self.deals  += weight
        self.apply  += weight * apply_amt
        self.payment += weight * pay_amt
        self.won    += weight * won
        self.paid   += weight * paid

    def to_dict(self, ch: str) -> dict:
        apply = self.apply
        pay   = self.payment
        return {
            "channel":     ch,
            "deals":       round(self.deals, 2),
            "apply_oku":   round(apply / 1e8, 2),
            "pay_oku":     round(pay / 1e8, 2),
            "yield_pct":   round(100 * pay / apply, 2) if apply > 0 else None,
            "won_rate":    round(100 * self.won  / self.deals, 1) if self.deals > 0 else None,
            "paid_rate":   round(100 * self.paid / self.deals, 1) if self.deals > 0 else None,
        }


def compute_attribution(rows: list[tuple]) -> dict:
    """
    rows = (utm_source_query, utm_source, apply_amount, pay_amount, won, paid)
    Returns {model: {channel: ChannelBucket}}
    """
    models = {m: defaultdict(ChannelBucket) for m in ("first", "last", "linear")}

    for fq, ls, apply_amt, pay_amt, won, paid in rows:
        first = clean(fq or ls)   # fallback to last if no first
        last  = clean(ls or fq)   # fallback to first if no last
        apply_amt = apply_amt or 0.0
        pay_amt   = pay_amt   or 0.0

        # first touch
        models["first"][first].add(1.0, apply_amt, pay_amt, won, paid)

        # last touch
        models["last"][last].add(1.0, apply_amt, pay_amt, won, paid)

        # linear
        if first == last:
            models["linear"][first].add(1.0, apply_amt, pay_amt, won, paid)
        else:
            models["linear"][first].add(0.5, apply_amt, pay_amt, won, paid)
            models["linear"][last].add(0.5,  apply_amt, pay_amt, won, paid)

    result = {}
    for model_name, buckets in models.items():
        rows_out = [b.to_dict(ch) for ch, b in buckets.items()]
        rows_out.sort(key=lambda r: r["apply_oku"], reverse=True)
        result[model_name] = rows_out
    return result


def compute_crm_lift(rows: list[tuple], crm_sources=("alrimtalk", "sms", "kakaochannel")) -> list[dict]:
    """
    CRM 재유입 여부별 acquisition 채널 수익률 비교.
    returns: [{acquisition, organic_yield, crm_yield, lift, deals_organic, deals_crm}]
    """
    acq: dict[str, dict] = {}   # acq_ch → {organic: bucket, crm: bucket}

    for fq, ls, apply_amt, pay_amt, won, paid in rows:
        first = clean(fq or ls)
        last  = clean(ls or fq)
        apply_amt = apply_amt or 0.0
        pay_amt   = pay_amt   or 0.0

        if first not in acq:
            acq[first] = {"organic": ChannelBucket(), "crm": ChannelBucket()}

        is_crm = any(c in last for c in crm_sources) and first != last
        key = "crm" if is_crm else "organic"
        acq[first][key].add(1.0, apply_amt, pay_amt, won, paid)

    out = []
    for ch, buckets in acq.items():
        org = buckets["organic"]
        crm = buckets["crm"]
        if org.deals < 10 or crm.deals < 5:
            continue
        org_yield = 100 * org.payment / org.apply if org.apply > 0 else None
        crm_yield = 100 * crm.payment / crm.apply if crm.apply > 0 else None
        lift = round(crm_yield - org_yield, 2) if (org_yield and crm_yield) else None
        out.append({
            "acquisition": ch,
            "organic_deals":  round(org.deals),
            "organic_apply":  round(org.apply / 1e8, 2),
            "organic_yield":  round(org_yield, 2) if org_yield else None,
            "crm_deals":      round(crm.deals),
            "crm_apply":      round(crm.apply / 1e8, 2),
            "crm_yield":      round(crm_yield, 2) if crm_yield else None,
            "crm_lift_ppt":   lift,
        })
    out.sort(key=lambda r: r.get("organic_apply") or 0, reverse=True)
    return out


def compute_journey_matrix(rows: list[tuple], top_n=15) -> dict:
    """first × last touch 매트릭스."""
    matrix: dict[tuple, ChannelBucket] = defaultdict(ChannelBucket)
    channel_apply: dict[str, float] = defaultdict(float)

    for fq, ls, apply_amt, pay_amt, won, paid in rows:
        first = clean(fq or ls)
        last  = clean(ls or fq)
        apply_amt = apply_amt or 0.0
        pay_amt   = pay_amt   or 0.0
        matrix[(first, last)].add(1.0, apply_amt, pay_amt, won, paid)
        channel_apply[first] += apply_amt

    # top_n acquisition channels by apply volume
    top_first = [ch for ch, _ in sorted(channel_apply.items(), key=lambda x: -x[1])[:top_n]]

    cells = []
    for (first, last), b in matrix.items():
        if first not in top_first:
            continue
        cells.append({
            "first": first,
            "last": last,
            "deals": round(b.deals),
            "apply_oku": round(b.apply / 1e8, 2),
            "yield_pct": round(100 * b.payment / b.apply, 1) if b.apply > 0 else None,
        })
    cells.sort(key=lambda r: r["apply_oku"], reverse=True)
    return {"top_first_channels": top_first, "cells": cells}


# ── main ──────────────────────────────────────────────────────────────────────

def run():
    con = sqlite3.connect(DB)
    as_of = con.execute("SELECT MAX(as_of_date) FROM deal_history").fetchone()[0]
    print(f"as_of = {as_of}")

    results = {}

    for win_key in ("12M", "6M", "3M", "1M", "4W", "1W"):
        fr, to = window_range(win_key, as_of)
        print(f"\n[{win_key}] {fr} ~ {to}")

        rows = con.execute(f"""
            SELECT
                utm_source_query, utm_source,
                apply_amount, payment_amount,
                CASE WHEN status='won' THEN 1 ELSE 0 END,
                CASE WHEN payment_date IS NOT NULL THEN 1 ELSE 0 END
            FROM deal_history
            WHERE as_of_date=?
              AND apply_date BETWEEN ? AND ?
              {excl_clause()}
        """, (as_of, fr, to)).fetchall()

        print(f"  deals: {len(rows):,}")
        attr   = compute_attribution(rows)
        lift   = compute_crm_lift(rows)
        matrix = compute_journey_matrix(rows)

        # summary per model
        for model, ch_rows in attr.items():
            total_apply = sum(r["apply_oku"] for r in ch_rows)
            total_pay   = sum(r["pay_oku"]   for r in ch_rows)
            print(f"  [{model}] channels={len(ch_rows)}  apply={total_apply:.1f}억  pay={total_pay:.1f}억")

        results[win_key] = {
            "window": win_key,
            "date_from": fr,
            "date_to": to,
            "total_deals": len(rows),
            "attribution": attr,
            "crm_lift": lift,
            "journey_matrix": matrix,
        }

    con.close()

    out = {
        "generated_at": __import__("datetime").datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "as_of": as_of,
        "attribution_note": (
            "utm_source_query = first touch (URL query string at application). "
            "utm_source = last touch (CRM or direct). "
            "조회(browse) UTM은 Pipedrive에 없음 — GA4 연동 필요."
        ),
        "windows": results,
    }

    OUT.parent.mkdir(exist_ok=True)
    OUT.write_text(json.dumps(out, ensure_ascii=False, indent=2))
    print(f"\n→ {OUT}")
    return out


if __name__ == "__main__":
    run()
