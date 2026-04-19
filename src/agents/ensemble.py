"""
Multi-model ensemble forecaster.

Models:
  - Cohort distribution (Phase 2 v2) — primary, MAPE ~3.94%
  - ARIMA(1,1,1) — statsmodels
  - ETS Holt-Winters (additive trend) — statsmodels

Blend weights: inverse of each model's recent 3M MAPE on backtest.

Output: output/ensemble_forecast.json
CLI:    python src/agents/ensemble.py
"""
import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT / "src"))

OUTPUT_DIR = ROOT / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

DB = Path("/tmp/history.sqlite")


def load_claims() -> list[dict]:
    con = sqlite3.connect(DB)
    as_of = con.execute("SELECT MAX(as_of_date) FROM deal_history").fetchone()[0]
    rows = con.execute("""
        SELECT apply_date, apply_amount, filing_date, filing_amount,
               decision_date, decision_amount, payment_date, payment_amount,
               status, pipeline
        FROM deal_history WHERE as_of_date=?
    """, (as_of,)).fetchall()
    con.close()
    cols = ["apply_date", "apply_amount", "filing_date", "filing_amount",
            "decision_date", "decision_amount", "payment_date", "payment_amount",
            "status", "pipeline"]
    sm = {"open": "진행 중", "won": "성사됨", "lost": "실패"}
    result = [dict(zip(cols, r)) for r in rows]
    for d in result:
        d["status"] = sm.get(d["status"], d["status"])
    return result

try:
    from statsmodels.tsa.arima.model import ARIMA
    from statsmodels.tsa.holtwinters import ExponentialSmoothing
    STATSMODELS_AVAILABLE = True
except ImportError:
    STATSMODELS_AVAILABLE = False


# ── helpers ──────────────────────────────────────────────────────────────────

def ym_label(m: int) -> str:
    return f"{m // 12:04d}-{m % 12 + 1:02d}"


def extract_pay_series(series: dict, last_n: int = 24) -> tuple[list[int], list[float]]:
    """B + C monthly payments → sorted (ym_int, amount_in_won) for last_n months."""
    pay_b = series["B"]["pay"]
    pay_c = series["C"]["pay"]
    all_months = sorted(set(pay_b.keys()) | set(pay_c.keys()))
    if not all_months:
        return [], []
    cutoff = all_months[-1] - last_n + 1
    months = [m for m in all_months if m >= cutoff]
    amounts = [(pay_b.get(m, 0.0) + pay_c.get(m, 0.0)) / 1e8 for m in months]
    return months, amounts


def mape_last_n(actual: list[float], predicted: list[float], n: int = 3) -> float:
    pairs = [(a, p) for a, p in zip(actual, predicted) if a > 0]
    recent = pairs[-n:]
    if not recent:
        return 100.0
    return float(sum(abs(p - a) / a * 100 for a, p in recent) / len(recent))


def inverse_weight_normalize(mapes: list[float]) -> list[float]:
    """Convert MAPEs to normalized inverse weights; handles zero MAPE."""
    eps = 1e-6
    inv = [1.0 / (m + eps) for m in mapes]
    total = sum(inv)
    return [w / total for w in inv]


# ── ARIMA forecaster ─────────────────────────────────────────────────────────

def fit_arima(amounts: list[float], n_forecast: int) -> list[float]:
    model = ARIMA(amounts, order=(1, 1, 1))
    fit = model.fit()
    fc = fit.forecast(steps=n_forecast)
    return [max(float(v), 0.0) for v in fc]


def backtest_arima(amounts: list[float], backtest_n: int = 12) -> list[float]:
    """Walk-forward backtest — refit at each step, return predicted values."""
    preds = []
    for i in range(backtest_n, 0, -1):
        train = amounts[: len(amounts) - i]
        if len(train) < 4:
            preds.append(amounts[-i] if amounts else 0.0)
            continue
        try:
            fit = ARIMA(train, order=(1, 1, 1)).fit()
            preds.append(max(float(fit.forecast(steps=1)[0]), 0.0))
        except Exception:
            preds.append(float(train[-1]) if train else 0.0)
    return preds


# ── ETS forecaster ────────────────────────────────────────────────────────────

def fit_ets(amounts: list[float], n_forecast: int) -> list[float]:
    model = ExponentialSmoothing(amounts, trend="add", seasonal=None)
    fit = model.fit(optimized=True)
    fc = fit.forecast(steps=n_forecast)
    return [max(float(v), 0.0) for v in fc]


def backtest_ets(amounts: list[float], backtest_n: int = 12) -> list[float]:
    preds = []
    for i in range(backtest_n, 0, -1):
        train = amounts[: len(amounts) - i]
        if len(train) < 4:
            preds.append(amounts[-i] if amounts else 0.0)
            continue
        try:
            fit = ExponentialSmoothing(train, trend="add", seasonal=None).fit(optimized=True)
            preds.append(max(float(fit.forecast(steps=1)[0]), 0.0))
        except Exception:
            preds.append(float(train[-1]) if train else 0.0)
    return preds


# ── main runner ───────────────────────────────────────────────────────────────

def run():
    from model import aggregate, ForecastEngine

    print("=" * 60)
    print("Ensemble Forecaster — Cohort + ARIMA + ETS")
    print("=" * 60)

    claims = load_claims()
    series = aggregate(claims)

    # Determine current partial month from data
    all_ms: set[int] = set()
    for grp in series.values():
        for s in grp.values():
            all_ms.update(s.keys())
    current_m = max(all_ms)
    last_complete = current_m - 1
    print(f"  Data range: ... ~ {ym_label(current_m)}")

    N_FORECAST = 5
    BACKTEST_N = 12
    RECENT_MAPE_N = 3  # months used for weight calculation

    # ── cohort model ─────────────────────────────────────────────────────────
    engine = ForecastEngine(claims, series, current_m)
    cohort_fc_raw = engine.forecast(N_FORECAST)
    # grand_total combines individual adjusted + corp if available; fall back to adjusted
    cohort_preds = [r.get("grand_total", r.get("adjusted", r.get("total", 0.0))) for r in cohort_fc_raw]
    cohort_months = [r["month"] for r in cohort_fc_raw]

    cohort_bt = engine.backtest(BACKTEST_N)
    cohort_bt_actual = [r["actual"] for r in cohort_bt]
    cohort_bt_pred   = [r["predicted"] for r in cohort_bt]
    cohort_mape_full = float(
        sum(abs(p - a) / a * 100 for a, p in zip(cohort_bt_actual, cohort_bt_pred) if a > 0)
        / max(1, sum(1 for a in cohort_bt_actual if a > 0))
    )
    cohort_mape_3m = mape_last_n(cohort_bt_actual, cohort_bt_pred, RECENT_MAPE_N)
    print(f"  Cohort MAPE (12M): {cohort_mape_full:.2f}%  (3M): {cohort_mape_3m:.2f}%")

    # ── time series (B+C, last 24M) ───────────────────────────────────────────
    ts_months, ts_amounts = extract_pay_series(series, last_n=24)
    print(f"  Time series length: {len(ts_amounts)} months")

    # ── ARIMA & ETS ───────────────────────────────────────────────────────────
    if STATSMODELS_AVAILABLE and len(ts_amounts) >= 8:
        print("  Fitting ARIMA(1,1,1) ...")
        arima_preds = fit_arima(ts_amounts, N_FORECAST)
        arima_bt_pred = backtest_arima(ts_amounts, BACKTEST_N)
        # align backtest actuals with time-series actuals (last BACKTEST_N of ts)
        ts_bt_actual = ts_amounts[-BACKTEST_N:] if len(ts_amounts) >= BACKTEST_N else ts_amounts
        arima_bt_actual_aligned = ts_bt_actual[-len(arima_bt_pred):]
        arima_mape_3m = mape_last_n(arima_bt_actual_aligned, arima_bt_pred, RECENT_MAPE_N)
        arima_mape_full = (
            sum(abs(p - a) / a * 100 for a, p in zip(arima_bt_actual_aligned, arima_bt_pred) if a > 0)
            / max(1, sum(1 for a in arima_bt_actual_aligned if a > 0))
        )
        print(f"  ARIMA MAPE (bt): {arima_mape_full:.2f}%  (3M): {arima_mape_3m:.2f}%")

        print("  Fitting ETS (additive trend) ...")
        ets_preds = fit_ets(ts_amounts, N_FORECAST)
        ets_bt_pred = backtest_ets(ts_amounts, BACKTEST_N)
        ets_mape_3m = mape_last_n(arima_bt_actual_aligned, ets_bt_pred, RECENT_MAPE_N)
        ets_mape_full = (
            sum(abs(p - a) / a * 100 for a, p in zip(arima_bt_actual_aligned, ets_bt_pred) if a > 0)
            / max(1, sum(1 for a in arima_bt_actual_aligned if a > 0))
        )
        print(f"  ETS  MAPE (bt): {ets_mape_full:.2f}%  (3M): {ets_mape_3m:.2f}%")

        mapes_3m = [cohort_mape_3m, arima_mape_3m, ets_mape_3m]
        weights = inverse_weight_normalize(mapes_3m)
        w_cohort, w_arima, w_ets = weights

        ensemble_preds = [
            round(w_cohort * c + w_arima * a + w_ets * e, 2)
            for c, a, e in zip(cohort_preds, arima_preds, ets_preds)
        ]

        models_used = ["cohort", "arima", "ets"]
        fallback = False

    else:
        reason = "statsmodels not available" if not STATSMODELS_AVAILABLE else "insufficient time series data"
        print(f"  WARNING: {reason} — falling back to cohort-only")
        arima_preds = [None] * N_FORECAST
        ets_preds   = [None] * N_FORECAST
        arima_mape_3m = None
        ets_mape_3m   = None
        arima_mape_full = None
        ets_mape_full   = None
        w_cohort, w_arima, w_ets = 1.0, 0.0, 0.0
        ensemble_preds = [round(v, 2) for v in cohort_preds]
        models_used = ["cohort"]
        fallback = True

    # ── assemble output ───────────────────────────────────────────────────────
    print(f"\n[Ensemble Forecast — weights: cohort={w_cohort:.3f} arima={w_arima:.3f} ets={w_ets:.3f}]")

    monthly = []
    for i, month in enumerate(cohort_months):
        row = {
            "month": month,
            "cohort": round(cohort_preds[i], 2),
            "arima":  round(arima_preds[i], 2) if arima_preds[i] is not None else None,
            "ets":    round(ets_preds[i], 2)   if ets_preds[i]   is not None else None,
            "ensemble": ensemble_preds[i],
        }
        print(f"  {month}: cohort={row['cohort']} arima={row['arima']} ets={row['ets']} → ensemble={row['ensemble']}")
        monthly.append(row)

    output = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "current_month": ym_label(current_m),
        "models_used": models_used,
        "fallback_cohort_only": fallback,
        "weights": {
            "cohort": round(w_cohort, 4),
            "arima":  round(w_arima, 4),
            "ets":    round(w_ets, 4),
        },
        "weight_basis": f"inverse of recent {RECENT_MAPE_N}M backtest MAPE",
        "mape": {
            "cohort_12m":    round(cohort_mape_full, 2),
            "cohort_3m":     round(cohort_mape_3m, 2),
            "arima_12m":     round(arima_mape_full, 2) if arima_mape_full is not None else None,
            "arima_3m":      round(arima_mape_3m, 2)  if arima_mape_3m  is not None else None,
            "ets_12m":       round(ets_mape_full, 2)  if ets_mape_full  is not None else None,
            "ets_3m":        round(ets_mape_3m, 2)    if ets_mape_3m    is not None else None,
        },
        "forecast": monthly,
        "units": "억원 (100M KRW)",
    }

    out_path = OUTPUT_DIR / "ensemble_forecast.json"
    out_path.write_text(json.dumps(output, ensure_ascii=False, indent=2))
    print(f"\n→ {out_path}")
    return output


if __name__ == "__main__":
    run()
