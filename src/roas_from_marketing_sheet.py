"""마케팅팀 Daily Report 엑셀 기반 ROAS 분석.

입력:
  data/비즈넵환급_Daily Report_*.xlsx  (가장 최근 파일 자동 선택)
  - sheet "3.슬라이서_daily_raw" : 일별 × 채널 × 매체 — 광고비 + 신청/예상결제 결합

산출:
  output/roas_marketing.json
  output/roas_marketing.csv  (long format)

지표 (모두 금액 기준):
  ROAS_expected = 예상결제액 / 광고비(VAT 포함)
  공헌이익      = 예상결제액 − 광고비
  CPL          = 광고비 / 신청완료
  CTR          = 클릭 / 노출
  CPC          = 광고비 / 클릭
  신청전환율    = 신청완료 / 클릭

  * 예상결제액은 마케팅팀이 신청금에 cohort 보정한 추정값.
  * 실측 ROAS는 별도 후속 분석에서 SQLite payment_amount와 결합.

윈도우:
  12M_cohort: 2024-11-01 ~ 2025-10-31  (lag 성숙)
  3M_recent : 데이터 최신 ~ -90일
  1M_recent : 데이터 최신 ~ -30일
"""
from __future__ import annotations

import csv
import glob
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
OUT_JSON = ROOT / "output" / "roas_marketing.json"
OUT_CSV = ROOT / "output" / "roas_marketing.csv"
SHEET_NAME = "3.슬라이서_daily_raw"

NUMERIC_COLS = [
    "조회완료", "조회환급금", "신청완료", "신청환급금",
    "예상결제액 ", "공헌이익", "PV(에어브릿지)", "UV(에어브릿지)",
    "노출", "클릭", "광고비vat제외", "광고비",
]


def latest_report() -> Path:
    matches = sorted(DATA_DIR.glob("*Daily Report*.xlsx"))
    if not matches:
        sys.exit(f"❌ {DATA_DIR}/*Daily Report*.xlsx 없음")
    return matches[-1]


def load_slicer(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=SHEET_NAME)
    for c in NUMERIC_COLS:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["날짜"] = pd.to_datetime(df["날짜"], errors="coerce")
    df = df.dropna(subset=["날짜"])
    return df


def windows(latest_date: pd.Timestamp) -> list[dict]:
    return [
        dict(key="12M_cohort", label="완성 코호트 12M (2024-11~2025-10)",
             start="2024-11-01", end="2025-10-31", matured=True),
        dict(key="3M_recent", label=f"최근 3개월 (~{latest_date.date()})",
             start=(latest_date - timedelta(days=90)).strftime("%Y-%m-%d"),
             end=latest_date.strftime("%Y-%m-%d"), matured=False),
        dict(key="1M_recent", label=f"최근 1개월 (~{latest_date.date()})",
             start=(latest_date - timedelta(days=30)).strftime("%Y-%m-%d"),
             end=latest_date.strftime("%Y-%m-%d"), matured=False),
    ]


def aggregate(df: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    agg = df.groupby(group_cols, dropna=False).agg(
        광고비=("광고비", "sum"),
        광고비vat제외=("광고비vat제외", "sum"),
        예상결제=("예상결제액 ", "sum"),
        공헌이익=("공헌이익", "sum"),
        신청환급금=("신청환급금", "sum"),
        조회환급금=("조회환급금", "sum"),
        신청건수=("신청완료", "sum"),
        조회건수=("조회완료", "sum"),
        노출=("노출", "sum"),
        클릭=("클릭", "sum"),
    ).reset_index()
    # ROAS / CPL / CPC / CTR / 전환율
    spend = agg["광고비"]
    deals = agg["신청건수"]
    clicks = agg["클릭"]
    impressions = agg["노출"]
    agg["ROAS_expected"] = (agg["예상결제"] / spend).where(spend > 0).round(3)
    agg["공헌이익"] = agg["공헌이익"].round(0)
    agg["CPL_krw"] = (spend / deals).where(deals > 0).round(0)
    agg["CPC_krw"] = (spend / clicks).where(clicks > 0).round(0)
    agg["CTR_pct"] = (100 * clicks / impressions).where(impressions > 0).round(3)
    agg["신청전환율_pct"] = (100 * deals / clicks).where(clicks > 0).round(3)
    return agg.sort_values("광고비", ascending=False).reset_index(drop=True)


def to_json_records(df: pd.DataFrame, group_cols: list[str]) -> list[dict]:
    out = []
    for _, r in df.iterrows():
        rec = {c: (None if pd.isna(r[c]) else r[c]) for c in df.columns}
        # 정수형 캐스팅 (json 깔끔)
        for c in ["광고비", "광고비vat제외", "예상결제", "공헌이익",
                  "신청환급금", "조회환급금", "신청건수", "조회건수", "노출", "클릭",
                  "CPL_krw", "CPC_krw"]:
            v = rec.get(c)
            if v is not None and not pd.isna(v):
                rec[c] = int(v)
            else:
                rec[c] = None
        out.append(rec)
    return out


def run() -> dict:
    src = latest_report()
    print(f"reading {src.name} ...")
    df = load_slicer(src)
    latest = df["날짜"].max()
    wins = windows(latest)

    result = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "source_file": src.name,
        "data_max_date": latest.strftime("%Y-%m-%d"),
        "windows": wins,
        "by_window": {},
        "totals_12M": {},
    }

    long_rows = []
    for w in wins:
        sub = df[(df["날짜"] >= w["start"]) & (df["날짜"] <= w["end"])]
        # 채널별
        ch = aggregate(sub, ["채널"])
        # 매체별 (광고비 0 제외)
        med = aggregate(sub[sub["광고비"] > 0], ["채널", "매체"])
        # 종합
        spend_total = float(sub["광고비"].sum())
        exp_pay = float(sub["예상결제액 "].sum())
        summary = {
            "광고비_vat포함": int(spend_total),
            "광고비_vat제외": int(sub["광고비vat제외"].sum()),
            "예상결제액": int(exp_pay),
            "공헌이익": int(sub["공헌이익"].sum()),
            "신청환급금": int(sub["신청환급금"].sum()),
            "신청건수": int(sub["신청완료"].sum()),
            "ROAS_expected": round(exp_pay / spend_total, 3) if spend_total > 0 else None,
            "matured": w["matured"],
        }
        result["by_window"][w["key"]] = {
            "summary": summary,
            "by_channel": to_json_records(ch, ["채널"]),
            "by_media": to_json_records(med, ["채널", "매체"]),
        }
        for r in to_json_records(med, ["채널", "매체"]):
            r["window"] = w["key"]
            r["matured"] = w["matured"]
            long_rows.append(r)

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(result, ensure_ascii=False, indent=2))
    _write_csv(long_rows)
    return result


def _write_csv(rows: list[dict]) -> None:
    if not rows:
        OUT_CSV.write_text("")
        return
    fields = ["window", "matured", "채널", "매체",
              "광고비", "광고비vat제외", "예상결제", "공헌이익",
              "신청환급금", "조회환급금",
              "신청건수", "조회건수", "노출", "클릭",
              "ROAS_expected", "CPL_krw", "CPC_krw", "CTR_pct", "신청전환율_pct"]
    with OUT_CSV.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


if __name__ == "__main__":
    out = run()
    print(f"\nsource = {out['source_file']}")
    print(f"data through = {out['data_max_date']}\n")
    for k in ["12M_cohort", "3M_recent", "1M_recent"]:
        s = out["by_window"][k]["summary"]
        flag = "" if s["matured"] else "  (lag 미성숙)"
        print(f"[{k}]{flag}")
        print(f"  광고비(VAT포함) {s['광고비_vat포함']:>14,}원")
        print(f"  예상결제액      {s['예상결제액']:>14,}원")
        print(f"  공헌이익        {s['공헌이익']:>14,}원")
        print(f"  ROAS            {s['ROAS_expected']}배")
        print()
    print(f"→ {OUT_JSON}")
    print(f"→ {OUT_CSV}")
