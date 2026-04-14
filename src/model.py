"""
Phase 2 v2 코호트 분산 모델 — 학습 + 예측 + 검증.

입력: data/deals_raw.json (extract_pipedrive.py 출력)
출력: output/forecast.json, output/backtest.json
"""
import json
import sys
from datetime import datetime, timezone
from collections import defaultdict
from pathlib import Path

import numpy as np

from config import (
    FIELD_MAP_BY_NAME, PIPELINE_REGULAR, PIPELINE_COLLECTION,
    STATUS_EXCLUDE, CHAIN_DIST_MAX_OFF, ROLLING_WINDOW,
    APP_FALLBACK_WINDOW, COLLECTION_MA_WINDOW, SEASON_ADJUSTMENT,
)

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
OUTPUT_DIR = ROOT / "output"
OUTPUT_DIR.mkdir(exist_ok=True)


# ── helpers ──────────────────────────────────────────
def ym(d: datetime) -> int:
    return d.year * 12 + (d.month - 1)

def ym_label(m: int) -> str:
    return f"{m // 12:04d}-{m % 12 + 1:02d}"

def month_of(m: int) -> int:
    return m % 12 + 1

def parse_date(v):
    if not v:
        return None
    if isinstance(v, datetime):
        return v
    s = str(v).strip()[:10]
    for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    return None

def to_num(v) -> float:
    if v is None or v == "":
        return 0.0
    try:
        return float(v)
    except (ValueError, TypeError):
        return 0.0


# ── data loading ─────────────────────────────────────
def load_deals() -> list[dict]:
    """JSON 또는 xlsx에서 deals 로드 → 내부 포맷."""
    raw_path = DATA_DIR / "deals_raw.json"
    if raw_path.exists():
        return _load_json(raw_path)
    # xlsx fallback
    xlsx_files = list(DATA_DIR.glob("*.xlsx"))
    if xlsx_files:
        return _load_xlsx(xlsx_files[0])
    print("ERROR: data/deals_raw.json 또는 data/*.xlsx 필요", file=sys.stderr)
    sys.exit(1)


def _load_json(path: Path) -> list[dict]:
    """Pipedrive API JSON → 내부 포맷.
    필드 key는 deal_fields.json으로 매핑."""
    print(f"  Loading JSON: {path.name}")

    # deal_fields.json → hash key to our variable name
    fields_path = DATA_DIR / "deal_fields.json"
    key_to_var = {}
    pipeline_map = {}  # pipeline_id -> name
    if fields_path.exists():
        fields = json.loads(fields_path.read_text())
        # Build name_fragment → var mapping
        name_fragments = {
            "✔ 신청일자": "apply_date",
            "📍 결제금액-알림톡발송": "payment_amount",
            "✍ 결정 환급액-알림톡발송": "decision_amount",
            "✔ 조회 환급액": "apply_amount",
            "✔ 신고일자": "filing_date",
            "✍ 신고 환급액-알림톡발송": "filing_amount",
            "✍ 결정일자": "decision_date",
            "💸 결제일자": "payment_date",
            "감면only 여부": "is_only_gam",
        }
        for key, info in fields.items():
            # Skip currency/통화 fields — they contain 'KRW' not amounts
            if info.get("field_type") == "varchar" and ("통화" in info["name"] or key.endswith("_currency")):
                continue
            for frag, var_name in name_fragments.items():
                if frag in info["name"]:
                    key_to_var[key] = var_name
                    break
            # pipeline field options
            if info["name"] == "파이프라인" and info.get("options"):
                pipeline_map = info["options"]

    print(f"      mapped {len(key_to_var)} custom fields")

    # Pipedrive pipeline_id → name mapping via API response
    # (pipelines are in deal['pipeline_id'] as int)
    # We'll build this from the first few deals + deal_fields
    # For now, read all deals
    raw = json.loads(path.read_text())

    # Build pipeline_id → name from deals (Pipedrive includes pipeline info)
    pid_to_name = {}
    for deal in raw[:100]:
        pid = deal.get("pipeline_id")
        # pipeline name might be in a nested object or we need to infer
        if pid and pid not in pid_to_name:
            # Try to find from stage info or we'll map later
            pass

    # Map status: Pipedrive uses "open", "won", "lost"
    # Our data uses "진행 중", "성사됨", "실패"
    status_map = {"open": "진행 중", "won": "성사됨", "lost": "실패"}

    # Need pipeline names — fetch from Pipedrive API or use deal_fields
    # Since we have deal_fields with pipeline options, try that
    # Actually, pipeline_id maps to pipeline names via /pipelines endpoint
    # For now, let's check what pipeline_id values exist and map them
    if not pid_to_name:
        pid_counts = {}
        for deal in raw[:5000]:
            pid = deal.get("pipeline_id")
            if pid:
                pid_counts[pid] = pid_counts.get(pid, 0) + 1
        print(f"      pipeline_ids found: {pid_counts}")
        # We need to call API or use known mapping
        # Let's try to get pipeline names from the API
        import os, urllib.request, urllib.parse
        token = os.environ.get("PIPEDRIVE_API_TOKEN", "")
        domain = os.environ.get("PIPEDRIVE_DOMAIN", "")
        if token and domain:
            try:
                url = f"https://{domain}.pipedrive.com/api/v1/pipelines?api_token={token}"
                resp = urllib.request.urlopen(url, timeout=15)
                pdata = json.loads(resp.read())
                for p in (pdata.get("data") or []):
                    pid_to_name[p["id"]] = p["name"]
                print(f"      pipeline names: {pid_to_name}")
            except Exception as e:
                print(f"      pipeline fetch failed: {e}")

    claims = []
    for deal in raw:
        rec = {}
        for key, var_name in key_to_var.items():
            rec[var_name] = deal.get(key)
        # standard fields
        raw_status = deal.get("status", "")
        rec["status"] = status_map.get(raw_status, raw_status)
        pid = deal.get("pipeline_id")
        rec["pipeline"] = pid_to_name.get(pid, str(pid))
        claims.append(rec)
    print(f"      loaded {len(claims):,} deals from JSON")
    return claims


def _load_xlsx(path: Path) -> list[dict]:
    """xlsx 직접 로드 (offline fallback)."""
    import openpyxl
    print(f"  Loading XLSX: {path.name}")
    # 컬럼 인덱스 매핑 (이전 분석에서 확정)
    IDX = {
        0: "apply_date", 3: "status", 8: "payment_amount",
        9: "decision_amount", 11: "apply_amount", 18: "filing_date",
        45: "filing_amount", 101: "decision_date", 127: "payment_date",
        153: "pipeline", 62: "is_only_gam",
    }
    wb = openpyxl.load_workbook(str(path), read_only=True, data_only=True)
    ws = wb[wb.sheetnames[0]]
    it = ws.iter_rows(values_only=True)
    next(it)  # header
    claims = []
    for row in it:
        rec = {}
        for idx, var in IDX.items():
            rec[var] = row[idx] if idx < len(row) else None
        claims.append(rec)
    wb.close()
    print(f"      loaded {len(claims):,} deals from XLSX")
    return claims


# ── aggregate to monthly series ──────────────────────
def aggregate(claims: list[dict]):
    """claims → 월별 집계 시리즈 (정기 B / 추심 C)."""
    series = {
        grp: {k: defaultdict(float) for k in ["app", "fil", "dec", "pay"]}
        for grp in ("B", "C")
    }
    for c in claims:
        status = str(c.get("status", ""))
        pipe = str(c.get("pipeline", ""))
        if STATUS_EXCLUDE in status:
            continue
        if PIPELINE_REGULAR in pipe:
            grp = "B"
        elif any(p in pipe for p in PIPELINE_COLLECTION):
            grp = "C"
        else:
            continue
        ad = parse_date(c.get("apply_date"))
        fd = parse_date(c.get("filing_date"))
        dd = parse_date(c.get("decision_date"))
        pd_ = parse_date(c.get("payment_date"))
        if ad: series[grp]["app"][ym(ad)] += to_num(c.get("apply_amount"))
        if fd: series[grp]["fil"][ym(fd)] += to_num(c.get("filing_amount"))
        if dd: series[grp]["dec"][ym(dd)] += to_num(c.get("decision_amount"))
        if pd_: series[grp]["pay"][ym(pd_)] += to_num(c.get("payment_amount"))
    return series


# ── cohort distribution fitting ──────────────────────
def fit_dist(claims, src_date_key, src_amt_key, tgt_date_key, tgt_amt_key,
             max_off, last_complete_m, window=None, min_amt=1e8, pipe_filter="B"):
    """Cohort distribution with optional rolling window."""
    src_total = defaultdict(float)
    matrix = defaultdict(lambda: defaultdict(float))
    for c in claims:
        status = str(c.get("status", ""))
        pipe = str(c.get("pipeline", ""))
        if STATUS_EXCLUDE in status:
            continue
        if pipe_filter == "B" and PIPELINE_REGULAR not in pipe:
            continue
        if pipe_filter == "C" and not any(p in pipe for p in PIPELINE_COLLECTION):
            continue
        sd = parse_date(c.get(src_date_key))
        td = parse_date(c.get(tgt_date_key))
        sa = to_num(c.get(src_amt_key))
        ta = to_num(c.get(tgt_amt_key))
        if sd and sa > 0:
            src_total[ym(sd)] += sa
        if sd and td and sa > 0 and ta > 0:
            off = ym(td) - ym(sd)
            if 0 <= off <= max_off:
                matrix[ym(sd)][off] += ta
    offs = defaultdict(list)
    valid_max = last_complete_m - max_off
    valid_min = (valid_max - window + 1) if window else None
    for src_m, row in matrix.items():
        if src_m > valid_max:
            continue
        if valid_min is not None and src_m < valid_min:
            continue
        sa = src_total[src_m]
        if sa < min_amt:
            continue
        for off in range(max_off + 1):
            offs[off].append(row.get(off, 0) / sa * 100)
    return {off: float(np.mean(v)) for off, v in offs.items() if v}


# ── prediction engine ────────────────────────────────
class ForecastEngine:
    def __init__(self, claims, series, current_partial_m, today_day=None):
        self.claims = claims
        self.series = series
        self.current = current_partial_m
        self.last_complete = current_partial_m - 1
        self.today_day = today_day or datetime.now().day

        # fit distributions
        lc = self.last_complete
        w = ROLLING_WINDOW
        self.a2f = fit_dist(claims, "apply_date", "apply_amount", "filing_date", "filing_amount",
                            CHAIN_DIST_MAX_OFF["a2f"], lc, window=w)
        self.f2d = fit_dist(claims, "filing_date", "filing_amount", "decision_date", "decision_amount",
                            CHAIN_DIST_MAX_OFF["f2d"], lc, window=w)
        self.d2p = fit_dist(claims, "decision_date", "decision_amount", "payment_date", "payment_amount",
                            CHAIN_DIST_MAX_OFF["d2p"], lc, window=w)

        # fallback: recent avg application
        app = series["B"]["app"]
        recent = [app.get(m, 0) for m in [lc - 2, lc - 1, lc]]
        self.app_avg = float(np.mean([v for v in recent if v > 0])) if any(v > 0 for v in recent) else 0

        # collection MA
        cpay = series["C"]["pay"]
        self.col_ma = float(np.mean([cpay.get(lc - i, 0) for i in range(COLLECTION_MA_WINDOW)]))

    def _get_app(self, m):
        s = self.series["B"]["app"]
        if m <= self.last_complete and m in s:
            return s[m]
        if m == self.current and m in s:
            return s[m] * 30 / self.today_day
        return self.app_avg

    def _get_fil(self, m):
        s = self.series["B"]["fil"]
        if m <= self.last_complete and m in s:
            return s[m]
        if m == self.current and m in s:
            return s[m] * 30 / self.today_day
        return sum(self._get_app(m - off) * self.a2f.get(off, 0) / 100 for off in self.a2f)

    def _get_dec(self, m):
        s = self.series["B"]["dec"]
        if m <= self.last_complete and m in s:
            return s[m]
        if m == self.current and m in s:
            return s[m] * 30 / self.today_day
        return sum(self._get_fil(m - off) * self.f2d.get(off, 0) / 100 for off in self.f2d)

    def predict(self, target_m) -> dict:
        """단일 월 예측."""
        pred_reg = 0
        breakdown = []
        for off, r in self.d2p.items():
            src_m = target_m - off
            d = self._get_dec(src_m)
            contrib = d * r / 100
            pred_reg += contrib
            is_actual = src_m <= self.last_complete and src_m in self.series["B"]["dec"]
            breakdown.append({
                "off": off, "src": ym_label(src_m),
                "dec_amount": round(d / 1e8, 2), "rate": r,
                "contrib": round(contrib / 1e8, 2),
                "source": "실측" if is_actual else "추정"
            })
        col = self.col_ma
        pred_total = pred_reg + col
        mon = month_of(target_m)
        adj = SEASON_ADJUSTMENT.get(mon, 0)
        adjusted = pred_total * (1 + adj)
        return {
            "month": ym_label(target_m),
            "regular": round(pred_reg / 1e8, 2),
            "collection": round(col / 1e8, 2),
            "total": round(pred_total / 1e8, 2),
            "season_adj": adj,
            "adjusted": round(adjusted / 1e8, 2),
            "breakdown": breakdown,
        }

    def forecast(self, n_months=5) -> list[dict]:
        """현재월부터 n_months개 예측."""
        return [self.predict(self.current + i) for i in range(n_months)]

    def backtest(self, n_months=12) -> list[dict]:
        """과거 n_months Walk-Forward 검증."""
        results = []
        for i in range(n_months, 0, -1):
            tgt = self.current - i
            actual_b = self.series["B"]["pay"].get(tgt, 0) / 1e8
            actual_c = self.series["C"]["pay"].get(tgt, 0) / 1e8
            actual = actual_b + actual_c

            # refit at that time
            lc = tgt - 1
            w = ROLLING_WINDOW
            d2p_t = fit_dist(self.claims, "decision_date", "decision_amount",
                             "payment_date", "payment_amount",
                             CHAIN_DIST_MAX_OFF["d2p"], lc, window=w)
            f2d_t = fit_dist(self.claims, "filing_date", "filing_amount",
                             "decision_date", "decision_amount",
                             CHAIN_DIST_MAX_OFF["f2d"], lc, window=w)
            # simple prediction using actual series
            pred_b = 0
            bdec = self.series["B"]["dec"]
            bfil = self.series["B"]["fil"]
            for off, r in d2p_t.items():
                sm = tgt - off
                d = bdec.get(sm, 0)
                if d == 0 and sm > lc:
                    d = sum(bfil.get(sm - o2, 0) * f2d_t.get(o2, 0) / 100 for o2 in f2d_t)
                pred_b += d * r / 100

            cpay = self.series["C"]["pay"]
            pred_c = float(np.mean([cpay.get(tgt - j, 0) for j in range(1, 4)]))
            pred = (pred_b + pred_c) / 1e8
            err = (pred - actual) / actual * 100 if actual > 0 else 0
            mon = month_of(tgt)
            results.append({
                "month": ym_label(tgt),
                "actual": round(actual, 2),
                "predicted": round(pred, 2),
                "error_pct": round(err, 1),
                "season_month": mon,
                "is_season_outlier": abs(err) > 15,
            })
        return results


# ── main ─────────────────────────────────────────────
def main():
    print("=" * 60)
    print("비즈넵 결제 예측 모델 — Phase 2 v2 코호트 분산")
    print("=" * 60)

    claims = load_deals()
    print(f"  Total claims: {len(claims):,}")

    series = aggregate(claims)
    # Determine current partial month
    all_ms = set()
    for grp in series.values():
        for s in grp.values():
            all_ms.update(s.keys())
    current_m = max(all_ms)
    print(f"  Data range: ... ~ {ym_label(current_m)}")

    engine = ForecastEngine(claims, series, current_m)

    # Distributions
    dists = {
        "a2f": {str(k): round(v, 2) for k, v in engine.a2f.items()},
        "f2d": {str(k): round(v, 2) for k, v in engine.f2d.items()},
        "d2p": {str(k): round(v, 2) for k, v in engine.d2p.items()},
    }
    print(f"\n[분산 비율]")
    for name, d in dists.items():
        print(f"  {name}: {d}")

    # Forecast
    print(f"\n[향후 5개월 예측]")
    fc = engine.forecast(5)
    for f in fc:
        print(f"  {f['month']}: 통합 {f['total']}억 → 시즌보정 {f['adjusted']}억 (adj {f['season_adj']:+.0%})")

    # Backtest
    print(f"\n[12개월 백테스트]")
    bt = engine.backtest(12)
    mape = float(np.mean([abs(r["error_pct"]) for r in bt]))
    for r in bt:
        tag = "★" if abs(r["error_pct"]) <= 10 else ("⚠" if abs(r["error_pct"]) <= 20 else "✗")
        print(f"  {r['month']}: 실제 {r['actual']}억 vs 예측 {r['predicted']}억 ({r['error_pct']:+.1f}%) {tag}")
    print(f"  MAPE: {mape:.1f}%")

    # Save outputs
    output = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "data_range": f"... ~ {ym_label(current_m)}",
        "total_claims": len(claims),
        "distributions": dists,
        "season_adjustments": SEASON_ADJUSTMENT,
        "forecast": fc,
        "backtest": bt,
        "mape": round(mape, 2),
    }
    out_path = OUTPUT_DIR / "forecast.json"
    out_path.write_text(json.dumps(output, ensure_ascii=False, indent=2))
    print(f"\n→ {out_path}")


if __name__ == "__main__":
    main()
