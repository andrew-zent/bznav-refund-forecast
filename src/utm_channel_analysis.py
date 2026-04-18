"""UTM 채널별 마케팅 효율 분석 — 전체기간/3M/1M × source × medium.

산출:
  - output/utm_channel_analysis.json
  - output/utm_channel_analysis.csv
의존:
  - /tmp/history.sqlite (history-archive Release asset)

설계 메모:
  - A(지수) pipeline은 1차 심사 탈락 전용 → 분모 희석. 기본 제외.
  - 12M = 완성 코호트(2024-11~2025-10), payment lag 성숙.
  - 3M/1M = recency window. payment lag 미성숙 → "신청량/속도" 위주로 해석.
  - 신규 채널 vs 리마인드(CRM/sms) 분리는 보고서 단계에서.
"""
from __future__ import annotations

import csv
import json
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

DB_PATH = Path("/tmp/history.sqlite")
ROOT = Path(__file__).resolve().parents[1]
OUT_JSON = ROOT / "output" / "utm_channel_analysis.json"
OUT_CSV = ROOT / "output" / "utm_channel_analysis.csv"

EXCLUDE_PIPELINES = ("A(지수)",)  # 분모 희석 제거. None 으로 두면 포함.


@dataclass
class Window:
    key: str
    label: str
    apply_from: str
    apply_to: str
    matured: bool  # payment lag 성숙 여부


def build_windows(as_of: str) -> list[Window]:
    as_of_d = date.fromisoformat(as_of)
    return [
        Window("12M_cohort", "완성 코호트 12M (2024-11~2025-10)",
               "2024-11-01", "2025-10-31", matured=True),
        Window("3M_recent", f"최근 3개월 (~{as_of})",
               (as_of_d - timedelta(days=90)).isoformat(), as_of, matured=False),
        Window("1M_recent", f"최근 1개월 (~{as_of})",
               (as_of_d - timedelta(days=30)).isoformat(), as_of, matured=False),
    ]


def latest_as_of(con: sqlite3.Connection) -> str:
    return con.execute("SELECT MAX(as_of_date) FROM deal_history").fetchone()[0]


def _exclude_clause() -> str:
    if not EXCLUDE_PIPELINES:
        return ""
    quoted = ",".join(f"'{p}'" for p in EXCLUDE_PIPELINES)
    return f" AND pipeline NOT IN ({quoted})"


def aggregate(con: sqlite3.Connection, as_of: str, w: Window, group_cols: list[str]) -> list[dict]:
    """group_cols 별 1행씩 집계. NULL UTM은 '(none)' 으로 치환."""
    select_groups = ", ".join(
        f"COALESCE(NULLIF({c}, ''), '(none)') AS {c}" for c in group_cols
    )
    group_by = ", ".join(group_cols)
    sql = f"""
    WITH base AS (
      SELECT *
      FROM deal_history
      WHERE as_of_date = ?
        AND apply_date BETWEEN ? AND ?
        {_exclude_clause()}
    )
    SELECT
      {select_groups},
      COUNT(DISTINCT deal_id)                                AS deals,
      ROUND(SUM(apply_amount)/1e8, 2)                        AS apply_oku,
      ROUND(SUM(filing_amount)/1e8, 2)                       AS filing_oku,
      ROUND(SUM(decision_amount)/1e8, 2)                     AS decision_oku,
      ROUND(SUM(payment_amount)/1e8, 2)                      AS payment_oku,
      SUM(CASE WHEN filing_date   IS NOT NULL THEN 1 ELSE 0 END) AS filing_n,
      SUM(CASE WHEN decision_date IS NOT NULL THEN 1 ELSE 0 END) AS decision_n,
      SUM(CASE WHEN payment_date  IS NOT NULL THEN 1 ELSE 0 END) AS payment_n,
      SUM(CASE WHEN status = 'won'  THEN 1 ELSE 0 END)       AS won_n,
      SUM(CASE WHEN status = 'lost' THEN 1 ELSE 0 END)       AS lost_n,
      SUM(CASE WHEN status = 'open' THEN 1 ELSE 0 END)       AS open_n,
      ROUND(AVG(CASE WHEN payment_date IS NOT NULL AND apply_date IS NOT NULL
            THEN julianday(payment_date) - julianday(apply_date) END), 1) AS avg_lag_days
    FROM base
    GROUP BY {group_by}
    ORDER BY deals DESC;
    """
    rows = con.execute(sql, (as_of, w.apply_from, w.apply_to)).fetchall()
    cols = [d[0] for d in con.execute(sql, (as_of, w.apply_from, w.apply_to)).description]
    out = []
    for r in rows:
        d = dict(zip(cols, r))
        deals = d["deals"] or 0
        apply_oku = d["apply_oku"] or 0
        # 비율 (분모 0 방지)
        d["filing_rate_pct"] = round(100 * (d["filing_n"] or 0) / deals, 1) if deals else None
        d["decision_rate_pct"] = round(100 * (d["decision_n"] or 0) / deals, 1) if deals else None
        d["payment_rate_pct"] = round(100 * (d["payment_n"] or 0) / deals, 1) if deals else None
        d["won_rate_pct"] = round(100 * (d["won_n"] or 0) / deals, 1) if deals else None
        d["lost_rate_pct"] = round(100 * (d["lost_n"] or 0) / deals, 1) if deals else None
        d["open_rate_pct"] = round(100 * (d["open_n"] or 0) / deals, 1) if deals else None
        # yield = 회수액 / 신청액 (1원당 회수). 결제 lag 성숙 코호트에서만 신뢰 가능.
        d["yield_pct"] = round(100 * (d["payment_oku"] or 0) / apply_oku, 2) if apply_oku else None
        # 평균 신청금액 (만원)
        d["avg_apply_manwon"] = round(apply_oku * 1e4 / deals, 1) if deals else None
        # 결제건당 평균 회수 (만원)
        d["avg_payment_per_deal_manwon"] = (
            round((d["payment_oku"] or 0) * 1e4 / (d["payment_n"] or 1), 1)
            if (d["payment_n"] or 0) else None
        )
        out.append(d)
    return out


def add_share(rows: list[dict]) -> None:
    """deals/apply_oku 의 전체 대비 share% 추가 (in-place)."""
    total_deals = sum(r["deals"] or 0 for r in rows)
    total_apply = sum(r["apply_oku"] or 0 for r in rows)
    for r in rows:
        r["deals_share_pct"] = (
            round(100 * (r["deals"] or 0) / total_deals, 1) if total_deals else None
        )
        r["apply_share_pct"] = (
            round(100 * (r["apply_oku"] or 0) / total_apply, 1) if total_apply else None
        )


def run() -> dict:
    con = sqlite3.connect(DB_PATH)
    as_of = latest_as_of(con)
    windows = build_windows(as_of)

    result = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "as_of_date": as_of,
        "exclude_pipelines": list(EXCLUDE_PIPELINES),
        "windows": [w.__dict__ for w in windows],
        "by_dimension": {},
    }

    dimensions = {
        "utm_source": ["utm_source"],
        "utm_medium": ["utm_medium"],
        "utm_source_medium": ["utm_source", "utm_medium"],
    }

    for dim_key, cols in dimensions.items():
        result["by_dimension"][dim_key] = {}
        for w in windows:
            rows = aggregate(con, as_of, w, cols)
            add_share(rows)
            result["by_dimension"][dim_key][w.key] = rows

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(result, ensure_ascii=False, indent=2))

    write_long_csv(result)
    return result


def write_long_csv(result: dict) -> None:
    """롱 포맷 CSV — 엑셀/시각화에 바로 쓸 수 있도록."""
    fields = [
        "dimension", "window", "matured",
        "utm_source", "utm_medium",
        "deals", "deals_share_pct",
        "apply_oku", "apply_share_pct",
        "filing_oku", "decision_oku", "payment_oku",
        "filing_rate_pct", "decision_rate_pct", "payment_rate_pct",
        "won_rate_pct", "lost_rate_pct", "open_rate_pct",
        "yield_pct", "avg_apply_manwon", "avg_payment_per_deal_manwon",
        "avg_lag_days",
    ]
    win_meta = {w["key"]: w for w in result["windows"]}
    with OUT_CSV.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for dim, by_window in result["by_dimension"].items():
            for win_key, rows in by_window.items():
                for r in rows:
                    writer.writerow({
                        **r,
                        "dimension": dim,
                        "window": win_key,
                        "matured": win_meta[win_key]["matured"],
                    })


if __name__ == "__main__":
    out = run()
    n_rows = sum(
        len(rows) for by_w in out["by_dimension"].values() for rows in by_w.values()
    )
    print(f"as_of={out['as_of_date']}  rows={n_rows}")
    print(f"  json -> {OUT_JSON}")
    print(f"  csv  -> {OUT_CSV}")
