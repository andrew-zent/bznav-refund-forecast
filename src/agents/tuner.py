"""자동 하이퍼파라미터 튜닝 (최적화 버전)

전략:
  Phase A — ROLLING_WINDOW(3~12) × COLLECTION_MA_WINDOW(1~6) 그리드 서치
  Phase B — 월별 SEASON_ADJUSTMENT 좌표 강하법

최적화:
  - claims 행렬을 한 번만 pre-compute → 그리드 탐색 시 O(1) 슬라이싱
  - 채권풀 balance/pay를 dict로 pre-compute → 백테스트 O(1) 조회

출력:
  output/tuner_result.json
  config.py 업데이트 (MAPE 개선 시)

독립 실행: python src/agents/tuner.py [--dry-run]
"""
from __future__ import annotations
import json
import re
import sqlite3
import sys
from collections import defaultdict
from copy import deepcopy
from datetime import datetime
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parent.parent.parent
SRC  = ROOT / "src"
sys.path.insert(0, str(SRC))

import config as cfg
from model import ym, ym_label, month_of, parse_date, to_num, STATUS_EXCLUDE, \
    PIPELINE_REGULAR, PIPELINE_COLLECTION, CHAIN_DIST_MAX_OFF

DB  = Path("/tmp/history.sqlite")
OUT = ROOT / "output" / "tuner_result.json"

ROLLING_WINDOW_RANGE = range(3, 13)
COLLECTION_MA_RANGE  = range(1, 7)
SEASON_CANDIDATES    = [-0.15, -0.10, -0.05, 0.0, 0.05, 0.10, 0.15, 0.20]
BACKTEST_MONTHS      = 12


# ── 데이터 로드 ───────────────────────────────────────────────────────────────
def load_claims() -> list[dict]:
    slim = ROOT / "data" / "deals_slim.json"
    if slim.exists():
        raw = json.loads(slim.read_text())
        status_map = {"open": "진행 중", "won": "성사됨", "lost": "실패"}
        for d in raw:
            d["status"] = status_map.get(d.get("status", ""), d.get("status", ""))
        return raw
    con = sqlite3.connect(DB)
    as_of = con.execute("SELECT MAX(as_of_date) FROM deal_history").fetchone()[0]
    rows = con.execute("""
        SELECT apply_date,apply_amount,filing_date,filing_amount,
               decision_date,decision_amount,payment_date,payment_amount,
               status,pipeline
        FROM deal_history WHERE as_of_date=?
    """, (as_of,)).fetchall()
    con.close()
    cols = ["apply_date","apply_amount","filing_date","filing_amount",
            "decision_date","decision_amount","payment_date","payment_amount",
            "status","pipeline"]
    sm = {"open":"진행 중","won":"성사됨","lost":"실패"}
    result = [dict(zip(cols, r)) for r in rows]
    for d in result:
        d["status"] = sm.get(d["status"], d["status"])
    print(f"  Loaded {len(result):,} deals from SQLite (as_of={as_of})")
    return result


# ── Pre-computation ───────────────────────────────────────────────────────────
def precompute_chain(claims, src_key, src_amt, tgt_key, tgt_amt,
                     max_off, pipe_filter="B") -> tuple[dict, dict]:
    """행렬을 한 번만 계산. returns (src_total, matrix)."""
    src_total: dict[int, float] = defaultdict(float)
    matrix: dict[int, dict[int, float]] = defaultdict(lambda: defaultdict(float))
    for c in claims:
        status = str(c.get("status", ""))
        pipe   = str(c.get("pipeline", ""))
        if STATUS_EXCLUDE in status:
            continue
        if pipe_filter == "B" and PIPELINE_REGULAR not in pipe:
            continue
        if pipe_filter == "C" and not any(p in pipe for p in PIPELINE_COLLECTION):
            continue
        sd = parse_date(c.get(src_key))
        td = parse_date(c.get(tgt_key))
        sa = to_num(c.get(src_amt))
        ta = to_num(c.get(tgt_amt))
        if sd and sa > 0:
            src_total[ym(sd)] += sa
        if sd and td and sa > 0 and ta > 0:
            off = ym(td) - ym(sd)
            if 0 <= off <= max_off:
                matrix[ym(sd)][off] += ta
    return dict(src_total), {k: dict(v) for k, v in matrix.items()}


def fast_fit(src_total, matrix, max_off, last_complete_m, window) -> dict[int, float]:
    """Pre-computed 행렬에서 window만 슬라이싱."""
    valid_max = last_complete_m - max_off
    valid_min = valid_max - window + 1
    offs: dict[int, list] = defaultdict(list)
    for src_m, row in matrix.items():
        if src_m > valid_max or src_m < valid_min:
            continue
        sa = src_total.get(src_m, 0)
        if sa < 1e8:
            continue
        for off in range(max_off + 1):
            offs[off].append(row.get(off, 0) / sa * 100)
    return {off: float(np.mean(v)) for off, v in offs.items() if v}


def precompute_series(claims) -> dict:
    """월별 집계 시계열 (B/C 파이프라인)."""
    series = {
        "B": {"app": defaultdict(float), "fil": defaultdict(float),
              "dec": defaultdict(float), "pay": defaultdict(float)},
        "C": {"pay": defaultdict(float)},
    }
    for c in claims:
        status = str(c.get("status", ""))
        pipe   = str(c.get("pipeline", ""))
        if STATUS_EXCLUDE in status:
            continue
        ad = parse_date(c.get("apply_date"))
        fd = parse_date(c.get("filing_date"))
        dd = parse_date(c.get("decision_date"))
        pd_ = parse_date(c.get("payment_date"))
        if PIPELINE_REGULAR in pipe:
            if ad: series["B"]["app"][ym(ad)] += to_num(c.get("apply_amount"))
            if fd: series["B"]["fil"][ym(fd)] += to_num(c.get("filing_amount"))
            if dd: series["B"]["dec"][ym(dd)] += to_num(c.get("decision_amount"))
            if pd_: series["B"]["pay"][ym(pd_)] += to_num(c.get("payment_amount"))
        elif any(p in pipe for p in PIPELINE_COLLECTION):
            if pd_: series["C"]["pay"][ym(pd_)] += to_num(c.get("payment_amount"))
    return series


def precompute_pool(claims) -> tuple[dict, dict]:
    """채권풀 잔액/결제를 월별 dict로 pre-compute."""
    col_deals = []
    for c in claims:
        if STATUS_EXCLUDE in str(c.get("status", "")):
            continue
        if not any(p in str(c.get("pipeline", "")) for p in PIPELINE_COLLECTION):
            continue
        ad  = parse_date(c.get("apply_date"))
        pd_ = parse_date(c.get("payment_date"))
        dec = to_num(c.get("decision_amount"))
        pay = to_num(c.get("payment_amount"))
        if not ad:
            continue
        col_deals.append({
            "apply_m": ym(ad),
            "dec_amt": dec,
            "pay_m":   ym(pd_) if pd_ and pay > 0 else None,
            "pay_amt": pay if pd_ else 0,
        })

    # 관련 월 범위 파악
    all_months = set()
    for d in col_deals:
        all_months.add(d["apply_m"])
        if d["pay_m"]:
            all_months.add(d["pay_m"])

    if not all_months:
        return {}, {}

    mn, mx = min(all_months), max(all_months) + 12
    pool_balance: dict[int, float] = {}
    actual_pay:   dict[int, float] = defaultdict(float)

    for T in range(mn, mx + 1):
        pool_balance[T] = sum(
            d["dec_amt"] for d in col_deals
            if d["apply_m"] < T and (d["pay_m"] is None or d["pay_m"] >= T)
        )
    for d in col_deals:
        if d["pay_m"]:
            actual_pay[d["pay_m"]] += d["pay_amt"]

    return pool_balance, dict(actual_pay)


# ── 평가 함수 ─────────────────────────────────────────────────────────────────
def backtest_fast(chains, series, pool_balance, actual_pay,
                  current_m, rw, cma, season_adj=None) -> tuple[float, list]:
    """Pre-computed 데이터로 빠른 백테스트."""
    a2f_data, f2d_data, d2p_data = chains

    results = []
    errors  = []
    for i in range(BACKTEST_MONTHS, 0, -1):
        tgt = current_m - i
        lc  = tgt - 1

        d2p = fast_fit(*d2p_data, CHAIN_DIST_MAX_OFF["d2p"], lc, rw)
        f2d = fast_fit(*f2d_data, CHAIN_DIST_MAX_OFF["f2d"], lc, rw)

        bdec = series["B"]["dec"]
        bfil = series["B"]["fil"]
        pred_b = 0
        for off, r in d2p.items():
            sm = tgt - off
            d = bdec.get(sm, 0)
            if d == 0 and sm > lc:
                d = sum(bfil.get(sm - o2, 0) * f2d.get(o2, 0) / 100 for o2 in f2d)
            pred_b += d * r / 100

        # collection: pre-computed pool lookup
        pool = pool_balance.get(tgt, 0)
        rates = []
        for j in range(1, cma + 1):
            T = tgt - j
            p = pool_balance.get(T, 0)
            a = actual_pay.get(T, 0)
            if p > 0:
                rates.append(a / p)
        col_rate = float(np.mean(rates)) if rates else 0
        pred_c = pool * col_rate

        pred   = (pred_b + pred_c) / 1e8
        actual = (series["B"]["pay"].get(tgt, 0) + series["C"]["pay"].get(tgt, 0)) / 1e8
        mon    = month_of(tgt)

        if season_adj:
            pred = pred * (1 + season_adj.get(mon, 0))

        err = (pred - actual) / actual * 100 if actual > 0 else 0
        errors.append(abs(err))
        results.append({"month": ym_label(tgt), "actual": round(actual, 2),
                        "predicted": round(pred, 2), "error_pct": round(err, 1),
                        "season_month": mon})

    mape = round(float(np.mean(errors)), 3) if errors else 999.0
    return mape, results


# ── Phase A ───────────────────────────────────────────────────────────────────
def grid_search(chains, series, pool_balance, actual_pay, current_m):
    print("\n[Phase A] ROLLING_WINDOW × COLLECTION_MA_WINDOW 그리드 서치", flush=True)
    best = {"mape": 999, "rw": cfg.ROLLING_WINDOW, "cma": cfg.COLLECTION_MA_WINDOW}
    grid = []
    total = len(list(ROLLING_WINDOW_RANGE)) * len(list(COLLECTION_MA_RANGE))
    done  = 0

    for rw in ROLLING_WINDOW_RANGE:
        for cma in COLLECTION_MA_RANGE:
            mape, _ = backtest_fast(chains, series, pool_balance, actual_pay, current_m, rw, cma)
            grid.append({"rw": rw, "cma": cma, "mape": mape})
            done += 1
            if mape < best["mape"]:
                best = {"mape": mape, "rw": rw, "cma": cma}
            print(f"  [{done:02d}/{total}] rw={rw} cma={cma} → {mape}%  (best: rw={best['rw']} cma={best['cma']} {best['mape']}%)", flush=True)

    grid.sort(key=lambda x: x["mape"])
    print(f"  → 최적: rw={best['rw']} cma={best['cma']} MAPE={best['mape']}%", flush=True)
    return best, grid


# ── Phase B ───────────────────────────────────────────────────────────────────
def tune_season(chains, series, pool_balance, actual_pay,
                current_m, rw, cma, init_adj) -> tuple[dict, float]:
    print(f"\n[Phase B] 시즌 보정 좌표 강하 (rw={rw}, cma={cma})", flush=True)
    adj = deepcopy(init_adj)
    base_mape, _ = backtest_fast(chains, series, pool_balance, actual_pay,
                                 current_m, rw, cma, adj)
    print(f"  초기 MAPE: {base_mape}%", flush=True)

    improved   = True
    iterations = 0
    while improved and iterations < 20:
        improved   = False
        iterations += 1
        for mon in range(1, 13):
            cur    = adj.get(mon, 0.0)
            best_v = cur
            best_m = base_mape
            for delta in SEASON_CANDIDATES:
                if abs(delta - cur) < 1e-6:
                    continue
                cand = deepcopy(adj)
                cand[mon] = delta
                mape, _ = backtest_fast(chains, series, pool_balance, actual_pay,
                                        current_m, rw, cma, cand)
                if mape < best_m - 1e-4:
                    best_m, best_v = mape, delta
            if abs(best_v - cur) > 1e-6:
                adj[mon]   = round(best_v, 3)
                base_mape  = best_m
                improved   = True

    print(f"  최종 MAPE: {base_mape}%  ({iterations}회 반복)", flush=True)
    return adj, base_mape


# ── config.py 업데이트 ────────────────────────────────────────────────────────
def update_config(rw, cma, season_adj):
    comments = {1:"연초",2:"설 연휴",3:"1Q",4:"평월",5:"종소세 신고기",
                6:"종소세 결정지연",7:"종소세 결정지연",8:"평월",
                9:"평월",10:"평월",11:"평월",12:"연말"}
    lines = []
    for m in range(1, 13):
        v = season_adj.get(m, 0.0)
        lines.append(f"    {m}: {v:+.2f},   # {comments[m]}")
    season_str = "SEASON_ADJUSTMENT = {\n" + "\n".join(lines) + "\n}"

    path = SRC / "config.py"
    text = path.read_text()
    text = re.sub(r"ROLLING_WINDOW\s*=\s*\d+", f"ROLLING_WINDOW = {rw}", text)
    text = re.sub(r"COLLECTION_MA_WINDOW\s*=\s*\d+", f"COLLECTION_MA_WINDOW = {cma}", text)
    text = re.sub(r"SEASON_ADJUSTMENT\s*=\s*\{[^}]+\}", season_str, text, flags=re.DOTALL)
    path.write_text(text)
    print(f"  → config.py 업데이트 완료 (rw={rw}, cma={cma})")


# ── main ──────────────────────────────────────────────────────────────────────
def run(dry_run=False) -> dict:
    print("Tuner 시작 ...", flush=True)
    claims = load_claims()

    # 한 번만 계산
    print("  행렬 pre-compute 중 ...", flush=True)
    a2f_data = precompute_chain(claims, "apply_date","apply_amount","filing_date","filing_amount", CHAIN_DIST_MAX_OFF["a2f"])
    f2d_data = precompute_chain(claims, "filing_date","filing_amount","decision_date","decision_amount", CHAIN_DIST_MAX_OFF["f2d"])
    d2p_data = precompute_chain(claims, "decision_date","decision_amount","payment_date","payment_amount", CHAIN_DIST_MAX_OFF["d2p"])
    chains   = (a2f_data, f2d_data, d2p_data)
    series   = precompute_series(claims)
    pool_bal, actual_pay = precompute_pool(claims)

    all_ms   = {m for grp in series.values() for s in grp.values() for m in s}
    current_m = max(all_ms)
    print(f"  current_m={current_m}  pool months={len(pool_bal)}", flush=True)

    # baseline
    baseline_mape, _ = backtest_fast(chains, series, pool_bal, actual_pay,
                                     current_m, cfg.ROLLING_WINDOW, cfg.COLLECTION_MA_WINDOW,
                                     cfg.SEASON_ADJUSTMENT)
    print(f"  baseline MAPE: {baseline_mape}%  (rw={cfg.ROLLING_WINDOW}, cma={cfg.COLLECTION_MA_WINDOW})", flush=True)

    best_ab, grid = grid_search(chains, series, pool_bal, actual_pay, current_m)
    rw_best, cma_best = best_ab["rw"], best_ab["cma"]

    best_adj, final_mape = tune_season(
        chains, series, pool_bal, actual_pay, current_m,
        rw_best, cma_best, init_adj=deepcopy(cfg.SEASON_ADJUSTMENT),
    )

    _, final_bt = backtest_fast(chains, series, pool_bal, actual_pay,
                                current_m, rw_best, cma_best, best_adj)

    result = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "baseline": {"rw": cfg.ROLLING_WINDOW, "cma": cfg.COLLECTION_MA_WINDOW,
                     "season_adj": cfg.SEASON_ADJUSTMENT, "mape": baseline_mape},
        "best": {"rw": rw_best, "cma": cma_best, "mape": final_mape,
                 "season_adj": best_adj, "backtest": final_bt},
        "improvement_pct": round(baseline_mape - final_mape, 3),
        "grid_top10": grid[:10],
        "dry_run": dry_run,
    }
    OUT.parent.mkdir(exist_ok=True)
    OUT.write_text(json.dumps(result, ensure_ascii=False, indent=2))
    print(f"\n→ {OUT}", flush=True)

    improved = final_mape < baseline_mape - 0.05
    print(f"\n결과: {baseline_mape}% → {final_mape}%  ({'개선 +' + str(round(baseline_mape-final_mape,3)) + '%p' if improved else '변화 없음'})", flush=True)

    if improved and not dry_run:
        update_config(rw_best, cma_best, best_adj)
    elif not improved:
        print("개선 없음 — config.py 유지")
    else:
        print("[dry-run] config.py 업데이트 생략")

    return result


if __name__ == "__main__":
    dry_run = "--dry-run" in sys.argv
    r = run(dry_run=dry_run)
    print(f"\nMAPE: {r['baseline']['mape']}% → {r['best']['mape']}%  ({r['improvement_pct']:+.3f}%p)")
