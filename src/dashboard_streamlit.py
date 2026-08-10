import io
import json
import os
import sqlite3
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

BASE = Path(__file__).parent.parent
OUTPUT = BASE / "output"
DB_PATH = Path("/tmp/history.sqlite")

st.set_page_config(page_title="비즈넵 환급 예측 대시보드", layout="wide")
st.title("비즈넵 환급 예측 대시보드")


def load_json(filename: str):
    path = OUTPUT / filename
    if not path.exists():
        return None
    with open(path) as f:
        return json.load(f)


@st.cache_data
def get_forecast():
    return load_json("forecast.json")


@st.cache_data
def get_verification():
    return load_json("verification_report.json")


@st.cache_data
def get_utm():
    return load_json("utm_channel_analysis.json")


@st.cache_data
def get_channel_deep():
    return load_json("channel_deep_analysis.json")


@st.cache_data
def get_roas():
    return load_json("roas_marketing.json")


@st.cache_data
def get_pipeline():
    return load_json("pipeline_state.json")


@st.cache_data
def get_attribution():
    return load_json("attribution_analysis.json")


@st.cache_data
def get_funnel():
    return load_json("channel_funnel.json")


def fmt_krw(v) -> str:
    """원 단위 천단위 콤마 포맷."""
    try:
        return f"{int(round(float(v))):,}원"
    except (TypeError, ValueError):
        return "—"


# ── 주니어 마케터 가이드 다이얼로그 ──────────────────────────────────────────
GUIDES = {
    "funnel": {
        "title": "📖 퍼널 전환율 읽는 법",
        "body": """
## 퍼널이란?
고객이 "신청"부터 "결제"까지 거치는 단계입니다.

```
신청 → 접수(신고) → 결정(국세청 확정) → 수주 → 결제
```

## 각 지표 의미

| 지표 | 계산식 | 해석 |
|------|--------|------|
| **접수율** | 신고 완료 딜 ÷ 신청 딜 | 신청 후 세무서 신고까지 간 비율 |
| **결정율** | 국세청 결정 딜 ÷ 신청 딜 | 환급 금액이 확정된 비율 |
| **수주율** | won 딜 ÷ 신청 딜 | 최종 성사 비율 |
| **결제율** | 실결제 딜 ÷ 신청 딜 | 돈이 실제로 들어온 비율 |
| **수익률** | 결제액 ÷ 신청액 | 신청 금액 중 실제로 받은 % |

## 어떻게 활용하나요?
- **수주율이 높은데 결제율이 낮은 채널** → 수주 후 취소가 많음. 고객 품질 문제
- **접수율이 낮은 채널** → 신청 후 이탈이 많음. 서비스 UX 문제 또는 저품질 리드
- **수익률이 높은 채널** → 환급액이 큰 고객 위주로 유입됨 (단가 높음)
""",
    },
    "trend": {
        "title": "📖 분기별 트렌드 읽는 법",
        "body": """
## 왜 트렌드를 봐야 하나요?
채널의 수익률이 시간에 따라 변합니다. 한 시점만 보면 "지금 좋은 채널"인지 "원래부터 좋았던 채널"인지 모릅니다.

## 주의사항
- **최근 분기(Q4 2025 이후)는 payment lag 때문에 수익률이 낮게 나옵니다.**
  세금 환급은 신청 후 3~6개월 지나야 결제가 완료되기 때문입니다.
- **하락 트렌드 채널** → 마케팅 피로도, 경쟁 심화, 타겟 소진 가능성
- **급등 채널** → 새로운 고효율 오디언스 발굴 가능성 (단, 샘플 작을 수 있음)

## 해석 예시
- toss.join: Q1 15.8% → Q4 11.4% → 지속 하락. 타겟 소진 or 경쟁 증가
- kbcardlms: Q1 13.1% → Q4 9.1% → 하락. LMS 피로도 검토 필요
""",
    },
    "self_conv": {
        "title": "📖 자력전환율 읽는 법",
        "body": """
## 자력전환율이란?
채널에서 유입된 고객이 CRM(알림톡·SMS) 재유입 **없이** 스스로 결제까지 완료한 비율입니다.

```
자력전환율 = CRM 없이 결제한 딜 ÷ 해당 채널 전체 딜
```

## 왜 중요한가요?
CRM(알림톡·SMS) 발송에는 비용이 듭니다.
자력전환율이 낮은 채널 = CRM 비용을 추가로 쏟아야 결제가 일어남
→ **표면 ROAS보다 실제 ROAS가 낮을 수 있습니다**

## 활용법
| 자력전환율 | 해석 | 액션 |
|-----------|------|------|
| **높음 (>80%)** | 고품질 채널. CRM 없이도 결제 | 예산 확대 우선 고려 |
| **중간 (50~80%)** | 보통. CRM 연계 필요 | CRM 자동화 연결 |
| **낮음 (<50%)** | CRM 의존도 높음 | 진짜 ROAS = ROAS × 자력전환율로 재계산 |
""",
    },
    "roas": {
        "title": "📖 ROAS / CPL 읽는 법",
        "body": """
## ROAS (Return On Ad Spend)
```
ROAS = 예상결제액 ÷ 광고비
```
- ROAS 2.0 = 광고비 1원당 2원 회수 예상
- **주의**: 마케팅팀 예상결제액 기반. Pipedrive 실제 결제와 약 +6.5% 괴리

### 기준선
| ROAS | 의미 |
|------|------|
| < 1.0 | 손실 (광고비보다 회수가 적음) |
| 1.0 ~ 2.0 | 운영비 감안 시 위험 구간 |
| > 2.0 | 수익 구간 |

## CPL (Cost Per Lead)
```
CPL = 광고비 ÷ 신청건수
```
- CPL이 낮을수록 저렴하게 리드를 확보하는 채널
- **CPL만 보면 안 됩니다**: CPL 낮아도 수익률 낮으면 의미 없음
- CPL × (1/수익률) = 결제 1건당 실질 광고비용
""",
    },
    "attribution": {
        "title": "📖 어트리뷰션 읽는 법",
        "body": """
## 어트리뷰션(Attribution)이란?
한 고객이 결제하기까지 여러 채널을 거칩니다.
어느 채널에 공을 줄지 결정하는 방식이 **어트리뷰션 모델**입니다.

## 3가지 모델

| 모델 | 공식 | 특징 |
|------|------|------|
| **First Touch** | 최초 유입 채널에 100% | 어디서 처음 알게 됐나 (Awareness) |
| **Last Touch** | 마지막 채널에 100% | 누가 최종 결제를 끌어냈나 (Conversion) |
| **Linear** | 첫/마지막에 50:50 | 균등 배분 |

## 우리 데이터 한계
- `utm_source_query` = 신청 URL의 최초 쿼리 (First Touch 근사)
- `utm_source` = Pipedrive에 저장된 마지막 채널 (Last Touch)
- **조회(browse) 단계 UTM은 없음** — GA4 연동 필요

## CRM Lift 해석
같은 acquisition 채널에서 온 고객도
alrimtalk/SMS 재유입이 있을 때 수익률이 더 높다면
→ CRM이 전환에 실질 기여하고 있다는 의미
""",
    },
}


@st.dialog("📖 이 차트 이해하기", width="large")
def show_guide(key: str):
    g = GUIDES.get(key, {})
    st.markdown(f"## {g.get('title','')}")
    st.markdown(g.get("body", ""))


# 지표 정의 (호버 툴팁용)
METRIC_DEFS = {
    "수주율":   "수주율 = 성사됨(won) 딜 ÷ 전체 신청 딜\n= 최종 환급 결정까지 완료한 비율",
    "결제율":   "결제율 = 실제 결제가 발생한 딜 ÷ 전체 신청 딜\n= 돈이 실제로 들어온 비율 (수주 후 취소 제외)",
    "수익률":   "수익률 = 실제 결제액 ÷ 신청 환급액\n= 신청한 금액 중 실제로 받은 비율 (cohort 기준)",
    "접수율":   "접수율 = 세무서 신고 완료 딜 ÷ 전체 신청 딜\n= 신청 후 실제 신고 진행된 비율",
    "결정율":   "결정율 = 국세청 결정 완료 딜 ÷ 전체 신청 딜\n= 신청 후 환급 금액이 확정된 비율",
    "ROAS":    "ROAS = 예상결제액 ÷ 광고비\n= 1원 투자 시 예상 회수액 (마케팅팀 예상결제 기반, 실제와 ~6% 괴리)",
    "CPL":     "CPL (Cost Per Lead) = 광고비 ÷ 신청건수\n= 신청 1건 유치에 든 광고비",
    "자력전환율": "자력전환율 = CRM(alrimtalk·SMS) 재유입 없이 직접 결제 완료한 딜 비율\n= 높을수록 채널 자체 품질이 좋음",
}


# ─────────────────────────────────────────────
# 기간 / 필터 상수
# ─────────────────────────────────────────────
PERIOD_OPTIONS = ["12M", "6M", "3M", "1M", "4W", "3W", "2W", "1W"]
PERIOD_LABELS  = ["12개월(코호트)", "6개월", "3개월", "1개월", "4주", "3주", "2주", "1주"]

UTM_KEY = {
    "12M": "12M_cohort", "6M": "6M_recent", "3M": "3M_recent", "1M": "1M_recent",
    "4W": "4W_recent", "3W": "3W_recent", "2W": "2W_recent", "1W": "1W_recent",
}
DEEP_KEY = {p: p for p in PERIOD_OPTIONS}

tabs = st.tabs(["종합 리포트", "예측 현황", "채널 분석", "ROAS", "어트리뷰션", "마케팅 예측 공식", "시스템 상태"])

# ─────────────────────────────────────────────
# Tab 0 — 종합 리포트 (마케터용)
# ─────────────────────────────────────────────
with tabs[0]:
    st.markdown("### 📊 기간별 종합 성과 리포트")
    st.caption("채널 성과 · 광고 효율 · 퍼널 전환율을 한눈에 정리한 요약 리포트입니다.")

    rp_label = st.pills("분석 기간", PERIOD_LABELS[:4], default="12개월(코호트)", key="report_period")
    rp_period = PERIOD_OPTIONS[PERIOD_LABELS.index(rp_label)] if rp_label else "12M"
    rp_key    = UTM_KEY[rp_period]

    utm_d    = get_utm()
    funnel_d = get_funnel()
    roas_d   = get_roas()

    by_source       = (utm_d or {}).get("by_dimension", {}).get("utm_source", {}).get(rp_key, [])
    roas_window     = (roas_d or {}).get("by_window", {}).get(rp_key, {})
    by_channel_roas = roas_window.get("by_channel", [])
    funnel_rows     = (funnel_d or {}).get("funnel", []) if rp_period == "12M" else []

    if rp_period in ("3M", "1M"):
        st.warning(f"⚠️ {rp_label} 기간은 결제 완료까지 시간이 필요해 수익률이 실제보다 낮게 보입니다. 신청 건수·신청액 위주로 참고하세요.")

    # ── 1. KPI 카드 ─────────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("#### 📌 핵심 지표")
    if by_source:
        df_src = pd.DataFrame(by_source)
        total_deals = int(df_src["deals"].sum()) if "deals" in df_src.columns else 0
        total_apply = df_src["apply_oku"].sum() if "apply_oku" in df_src.columns else 0
        total_pay   = df_src["payment_oku"].sum() if "payment_oku" in df_src.columns else 0
        avg_yield   = round(100 * total_pay / total_apply, 1) if total_apply > 0 else 0
        n_ch        = len(df_src)

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("총 신청 건수",  f"{total_deals:,}건",    help="해당 기간 신청된 전체 딜 수")
        k2.metric("총 신청액",     f"{total_apply:.1f}억원", help="신청 환급액 합계")
        k3.metric("실제 결제액",   f"{total_pay:.1f}억원",   help="실제로 입금된 금액 합계")
        k4.metric("전체 수익률",   f"{avg_yield:.1f}%",      help="결제액 ÷ 신청액")
    else:
        st.info("UTM 채널 데이터를 찾을 수 없습니다. (`utm_channel_analysis.json`)")

    # ── 2. 자동 인사이트 ─────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("#### 💡 이 기간 핵심 인사이트")

    insights = []
    if by_source:
        df_src = pd.DataFrame(by_source)
        df_big = df_src[df_src["apply_oku"] >= 3].copy() if "apply_oku" in df_src.columns else df_src.copy()
        if not df_big.empty and "yield_pct" in df_big.columns:
            df_big_valid = df_big.dropna(subset=["yield_pct"])
            if not df_big_valid.empty:
                best  = df_big_valid.loc[df_big_valid["yield_pct"].idxmax()]
                worst = df_big_valid.loc[df_big_valid["yield_pct"].idxmin()]
                insights.append(
                    f"🏆 **가장 효율적인 채널**: `{best['utm_source']}` — "
                    f"수익률 **{best['yield_pct']:.1f}%** "
                    f"(신청 {best['apply_oku']:.1f}억 → 결제 {best.get('payment_oku',0):.1f}억)"
                )
                insights.append(
                    f"⚠️ **개선이 필요한 채널**: `{worst['utm_source']}` — "
                    f"수익률 **{worst['yield_pct']:.1f}%** "
                    f"(신청액 대비 결제 전환이 낮습니다)"
                )
        if "deals" in df_src.columns:
            top_vol = df_src.loc[df_src["deals"].idxmax()]
            insights.append(
                f"📈 **신청 건수 1위**: `{top_vol['utm_source']}` — {int(top_vol['deals']):,}건 유입"
            )

    if by_channel_roas:
        df_r = pd.DataFrame(by_channel_roas)
        if "ROAS_expected" in df_r.columns and "광고비" in df_r.columns:
            df_r_big = df_r[df_r["광고비"] >= 1_000_000]
            if not df_r_big.empty:
                best_roas = df_r_big.loc[df_r_big["ROAS_expected"].idxmax()]
                insights.append(
                    f"💰 **광고 효율 1위**: `{best_roas['채널']}` — "
                    f"ROAS **{best_roas['ROAS_expected']:.2f}x** "
                    f"(광고비 {best_roas['광고비']/10000:.0f}만원)"
                )
                below1 = df_r_big[df_r_big["ROAS_expected"] < 1.0]
                if not below1.empty:
                    names = ", ".join(f"`{c}`" for c in below1["채널"].tolist()[:3])
                    insights.append(
                        f"🔴 **ROAS 1.0 미만 채널 {len(below1)}개** — {names} 등 → "
                        f"광고비보다 회수액이 적은 구간, 예산 재검토 필요"
                    )

    if funnel_rows:
        df_fn = pd.DataFrame(funnel_rows)
        avg_pay_rate = df_fn["payment_rate"].mean()
        insights.append(
            f"🔄 **전체 결제 전환율 평균**: {avg_pay_rate:.1f}% — "
            f"신청 100건 중 약 {avg_pay_rate:.0f}건이 실제 결제까지 완료"
        )

    if insights:
        for ins in insights:
            st.markdown(f"- {ins}")
    else:
        st.info("분석에 필요한 데이터가 부족합니다.")

    # ── 3. 채널별 성과 표 ────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown(f"#### 📋 채널별 성과 요약 ({rp_label})")

    if by_source:
        df_src = pd.DataFrame(by_source).sort_values("apply_oku", ascending=False)

        def _yield_badge(y):
            try:
                v = float(y)
                if v >= 15: return "🟢 우수"
                elif v >= 8: return "🟡 보통"
                return "🔴 저조"
            except: return "—"

        df_src["상태"] = df_src["yield_pct"].apply(_yield_badge) if "yield_pct" in df_src.columns else "—"

        col_map = [
            ("utm_source", "채널"), ("deals", "신청건수"),
            ("apply_oku", "신청액(억)"), ("payment_oku", "결제액(억)"),
            ("yield_pct", "수익률(%)"), ("상태", "상태"),
        ]
        keep = [c for c, _ in col_map if c in df_src.columns]
        rename = {c: k for c, k in col_map if c in df_src.columns}
        st.dataframe(
            df_src[keep].rename(columns=rename).reset_index(drop=True),
            use_container_width=True, hide_index=True,
        )
        st.caption("수익률 기준 | 🟢 15% 이상: 우수 채널 / 🟡 8~15%: 평균 / 🔴 8% 미만: 개선 필요")
    else:
        st.info("채널 성과 데이터를 찾을 수 없습니다.")

    # ── 4. 퍼널 요약 (12M 코호트만) ─────────────────────────────────────────
    if funnel_rows:
        st.markdown("---")
        st.markdown("#### 🔄 신청 → 결제 퍼널 요약 (12개월 코호트)")
        st.caption("신청부터 실제 결제까지 각 단계에서 얼마나 이탈하는지 보여줍니다.")

        df_fn = pd.DataFrame(funnel_rows)
        avg_f = df_fn["filing_rate"].mean()
        avg_d = df_fn["decision_rate"].mean()
        avg_w = df_fn["won_rate"].mean()
        avg_p = df_fn["payment_rate"].mean()

        fc1, fc2, fc3, fc4 = st.columns(4)
        fc1.metric("① 접수율",  f"{avg_f:.1f}%", help="신청 후 세무서 신고까지 완료된 비율")
        fc2.metric("② 결정율",  f"{avg_d:.1f}%", help="국세청이 환급 금액을 확정한 비율")
        fc3.metric("③ 수주율",  f"{avg_w:.1f}%", help="최종 서비스 성사 비율")
        fc4.metric("④ 결제율",  f"{avg_p:.1f}%", help="실제로 돈이 들어온 비율")

        drops = [
            ("신청 → 접수",  100  - avg_f),
            ("접수 → 결정",  avg_f - avg_d),
            ("결정 → 수주",  avg_d - avg_w),
            ("수주 → 결제",  avg_w - avg_p),
        ]
        biggest = max(drops, key=lambda x: x[1])
        st.info(f"💡 가장 큰 이탈 구간: **{biggest[0]}** — 평균 **{biggest[1]:.1f}%p** 이탈. 이 단계 개선이 전체 전환율 향상에 가장 효과적입니다.")

    # ── 5. ROAS 요약 ─────────────────────────────────────────────────────────
    if by_channel_roas:
        st.markdown("---")
        st.markdown(f"#### 💰 광고 채널 효율 요약 ({rp_label})")
        st.caption("ROAS = 예상결제액 ÷ 광고비. 2.0x 이상이면 수익 구간입니다.")

        df_r = pd.DataFrame(by_channel_roas).copy()
        if "ROAS_expected" in df_r.columns:
            def _roas_badge(v):
                try:
                    fv = float(v)
                    if fv >= 2: return "🟢 수익"
                    elif fv >= 1: return "🟡 주의"
                    return "🔴 손실"
                except: return "—"

            df_r["상태"] = df_r["ROAS_expected"].apply(_roas_badge)
            if "광고비" in df_r.columns:
                df_r["광고비(만원)"] = df_r["광고비"].apply(
                    lambda v: f"{int(v/10000):,}" if pd.notna(v) else "—"
                )
            show_r = [c for c in ["채널", "광고비(만원)", "신청건수", "ROAS_expected", "상태"] if c in df_r.columns]
            df_r_show = df_r[show_r].rename(columns={"ROAS_expected": "ROAS"})
            st.dataframe(df_r_show, use_container_width=True, hide_index=True)

            n_g = len(df_r[df_r["ROAS_expected"] >= 2])
            n_y = len(df_r[(df_r["ROAS_expected"] >= 1) & (df_r["ROAS_expected"] < 2)])
            n_r = len(df_r[df_r["ROAS_expected"] < 1])
            st.caption(
                f"전체 {len(df_r)}개 채널 | "
                f"🟢 수익 {n_g}개 / 🟡 주의 {n_y}개 / 🔴 손실 {n_r}개 | "
                f"ROAS 기준: 🟢 ≥2.0x / 🟡 1.0~2.0x / 🔴 <1.0x"
            )


# ─────────────────────────────────────────────
# Tab 1 — 예측 현황
# ─────────────────────────────────────────────
with tabs[1]:
    data = get_forecast()
    if data is None:
        st.warning("forecast.json 파일을 찾을 수 없습니다.")
    else:
        mape = data.get("mape")
        pool = data.get("collection_pool", {})
        balance = pool.get("balance")
        delta = pool.get("monthly_delta")

        c1, c2, c3 = st.columns(3)
        c1.metric("MAPE (백테스트)", f"{mape:.2f}%" if mape is not None else "—")
        c2.metric(
            "추심 채권풀 잔액",
            f"{balance:.1f}억원" if balance is not None else "—",
        )
        c3.metric(
            "채권풀 월간 순변동",
            f"{delta:+.1f}억원" if delta is not None else "—",
            delta_color="inverse",
        )

        st.subheader("향후 5개월 결제 예측")
        forecast_rows = data.get("forecast", [])
        if forecast_rows:
            df_fc = pd.DataFrame(
                [
                    {
                        "월": r["month"],
                        "개인": r["individual"]["adjusted"],
                        "법인": r["corporate"]["total"],
                        "합계": r["grand_total"],
                    }
                    for r in forecast_rows
                ]
            )
            fig_fc = go.Figure()
            fig_fc.add_bar(x=df_fc["월"], y=df_fc["개인"], name="개인")
            fig_fc.add_bar(x=df_fc["월"], y=df_fc["법인"], name="법인")
            fig_fc.update_layout(
                barmode="stack",
                yaxis_title="억원",
                xaxis_title="",
                legend_title="구분",
                height=380,
            )
            st.plotly_chart(fig_fc, use_container_width=True)

        st.subheader("12개월 백테스트 — 실측 vs 예측")
        backtest_rows = data.get("backtest", [])
        if backtest_rows:
            df_bt = pd.DataFrame(backtest_rows)
            fig_bt = go.Figure()
            fig_bt.add_scatter(
                x=df_bt["month"],
                y=df_bt["actual"],
                mode="lines+markers",
                name="실측",
            )
            fig_bt.add_scatter(
                x=df_bt["month"],
                y=df_bt["predicted"],
                mode="lines+markers",
                name="예측",
                line=dict(dash="dash"),
            )
            fig_bt.update_layout(
                yaxis_title="억원",
                xaxis_title="",
                legend_title="",
                height=380,
            )
            st.plotly_chart(fig_bt, use_container_width=True)

# ─────────────────────────────────────────────
# Tab 2 — 채널 분석
# ─────────────────────────────────────────────
with tabs[2]:
    deep = get_channel_deep()
    utm = get_utm()

    funnel_data = get_funnel()

    ch_tabs = st.tabs(["퍼널 전환율", "분기별 트렌드", "자력전환율", "캠페인 상세"])

    # ── 채널 분석 서브탭 1: 퍼널 전환율 ──────────────────────────────────────
    with ch_tabs[0]:
        if st.button("📖 이 차트 이해하기", key="guide_funnel"):
            show_guide("funnel")
        if funnel_data is None:
            st.warning("channel_funnel.json 파일을 찾을 수 없습니다.")
        else:
            st.caption(
                f"12M 코호트(2024-11~2025-10) | 최소 {funnel_data['min_deals']}건 이상 채널만 표시\n"
                "**수주율**: 성사됨÷신청  **결제율**: 실결제÷신청  **수익률**: 결제액÷신청액"
            )
            df_fn = pd.DataFrame(funnel_data["funnel"])
            top_n = st.slider("표시 채널 수 (신청액 순)", 5, 31, 15, key="fn_topn")
            df_fn = df_fn.head(top_n)

            # 그룹 막대 (접수율·수주율·결제율)
            fig_fn = go.Figure()
            for col, label, color, n_col in [
                ("filing_rate",   "접수율",  "#4C9BE8", "filing_n"),
                ("decision_rate", "결정율",  "#7BC67E", "decision_n"),
                ("won_rate",      "수주율",  "#F9A825", "won_n"),
                ("payment_rate",  "결제율",  "#EF5350", "payment_n"),
            ]:
                n_series = df_fn[n_col] if n_col in df_fn.columns else df_fn["deals"]
                fig_fn.add_bar(
                    x=df_fn["channel"], y=df_fn[col], name=label,
                    marker_color=color,
                    customdata=list(zip(n_series, df_fn["deals"])),
                    hovertemplate=(
                        f"<b>%{{x}}</b><br>{label}: %{{y:.1f}}%"
                        f"<br><i>{METRIC_DEFS.get(label,'')}</i>"
                        "<br>해당 단계: %{customdata[0]:,}건 / 전체 신청: %{customdata[1]:,}건<extra></extra>"
                    ),
                )
            fig_fn.update_layout(
                barmode="group", height=440,
                yaxis_title="전환율 (%)", xaxis_tickangle=-30,
                legend_title="단계",
            )
            st.plotly_chart(fig_fn, use_container_width=True)

            # 신청액 · 결제액 · 수익률 복합 차트
            fig_y = go.Figure()
            fig_y.add_bar(
                x=df_fn["channel"], y=df_fn["apply_oku"],
                name="신청액(억)", marker_color="#90CAF9", opacity=0.8,
                yaxis="y2",
                hovertemplate="<b>%{x}</b><br>신청액: %{y:.1f}억<extra></extra>",
            )
            fig_y.add_bar(
                x=df_fn["channel"],
                y=df_fn["payment_oku"] if "payment_oku" in df_fn.columns else [],
                name="결제액(억)", marker_color="#A5D6A7", opacity=0.8,
                yaxis="y2",
                hovertemplate="<b>%{x}</b><br>결제액: %{y:.1f}억<extra></extra>",
            )
            fig_y.add_scatter(
                x=df_fn["channel"], y=df_fn["yield_pct"],
                name="수익률(%)", mode="lines+markers",
                marker=dict(size=8, color="crimson"), line=dict(color="crimson"),
                hovertemplate=(
                    "<b>%{x}</b><br>수익률: %{y:.2f}%"
                    f"<br><i>{METRIC_DEFS['수익률']}</i><extra></extra>"
                ),
            )
            fig_y.update_layout(
                barmode="group",
                height=400, yaxis_title="수익률 (%)",
                yaxis2=dict(title="금액(억)", overlaying="y", side="right", showgrid=False),
                xaxis_tickangle=-30, legend_title="",
            )
            st.plotly_chart(fig_y, use_container_width=True)

    # ── 채널 분석 서브탭 2: 분기별 트렌드 ────────────────────────────────────
    with ch_tabs[1]:
        if st.button("📖 이 차트 이해하기", key="guide_trend"):
            show_guide("trend")
        if funnel_data is None:
            st.warning("channel_funnel.json 파일을 찾을 수 없습니다.")
        else:
            trend = funnel_data["quarterly_trend"]
            quarters_list = funnel_data["quarters"]
            top_channels_trend = list(trend.keys())

            selected_chs = st.multiselect(
                "채널 선택",
                top_channels_trend,
                default=top_channels_trend[:6],
                key="trend_ch"
            )
            fig_trend = go.Figure()
            for ch in selected_chs:
                pts = trend.get(ch, [])
                xs = [p["quarter"] for p in pts if p["yield_pct"] is not None]
                ys = [p["yield_pct"] for p in pts if p["yield_pct"] is not None]
                ds = [p["deals"] for p in pts if p["yield_pct"] is not None]
                if xs:
                    fig_trend.add_scatter(
                        x=xs, y=ys, mode="lines+markers", name=ch,
                        customdata=ds,
                        hovertemplate=(
                            f"<b>{ch}</b><br>수익률: %{{y:.1f}}%"
                            f"<br><i>{METRIC_DEFS['수익률']}</i>"
                            "<br>딜수: %{customdata:,}건<extra></extra>"
                        ),
                    )
            fig_trend.update_layout(
                height=440, yaxis_title="수익률 (%)",
                legend_title="채널",
                hovermode="x unified",
            )
            st.plotly_chart(fig_trend, use_container_width=True)
            st.caption("수익률 = 결제액 ÷ 신청액 (payment lag 성숙 기준 — Q4 2025 이후는 아직 성숙 중)")

    # ── 채널 분석 서브탭 3: 자력전환율 ───────────────────────────────────────
    with ch_tabs[2]:
        if st.button("📖 이 차트 이해하기", key="guide_self_conv"):
            show_guide("self_conv")
        if funnel_data is None:
            st.warning("channel_funnel.json 파일을 찾을 수 없습니다.")
        else:
            df_sc = pd.DataFrame(funnel_data["self_conversion"])
            sc_min = st.number_input("최소 신청액(억)", 0.5, 50.0, 2.0, 0.5, key="sc_min")
            df_sc = df_sc[df_sc["apply_oku"] >= sc_min].copy()

            fig_sc = go.Figure()
            fig_sc.add_bar(
                x=df_sc["channel"], y=df_sc["self_pct"],
                name="자력전환",
                marker_color="#4C9BE8",
                customdata=df_sc[["self_deals", "self_yield"]].values,
                hovertemplate=(
                    "<b>%{x}</b><br>자력전환율: %{y:.1f}%"
                    f"<br><i>{METRIC_DEFS['자력전환율']}</i>"
                    "<br>자력딜: %{customdata[0]:,}건 | 자력수익률: %{customdata[1]:.1f}%<extra></extra>"
                ),
            )
            fig_sc.add_bar(
                x=df_sc["channel"], y=df_sc["crm_pct"],
                name="CRM 재유입 후 전환",
                marker_color="#EF9A9A",
                customdata=df_sc[["crm_deals", "crm_yield"]].values,
                hovertemplate=(
                    "<b>%{x}</b><br>CRM재유입율: %{y:.1f}%"
                    "<br>CRM딜: %{customdata[0]:,}건 | CRM수익률: %{customdata[1]:.1f}%<extra></extra>"
                ),
            )
            fig_sc.update_layout(
                barmode="stack", height=420,
                yaxis_title="비율 (%)", xaxis_tickangle=-30,
                legend_title="",
            )
            st.plotly_chart(fig_sc, use_container_width=True)
            st.caption("자력전환 = CRM(alrimtalk·SMS·kakaochannel) 재유입 없이 직접 결제 완료 | 스택 합=100%")

            # 요약 테이블
            df_sc_show = df_sc[[
                "channel", "total_deals", "apply_oku",
                "self_pct", "self_yield", "crm_pct", "crm_yield"
            ]].rename(columns={
                "channel": "채널", "total_deals": "전체딜", "apply_oku": "신청액(억)",
                "self_pct": "자력전환율(%)", "self_yield": "자력수익률(%)",
                "crm_pct": "CRM의존율(%)", "crm_yield": "CRM수익률(%)",
            })
            st.dataframe(df_sc_show, use_container_width=True, hide_index=True)

    # ── 채널 분석 서브탭 4: 캠페인 상세 ──────────────────────────────────────
    with ch_tabs[3]:
        fc1, fc2, fc3 = st.columns([3, 1, 1])
        with fc1:
            selected_label = st.pills("기간", PERIOD_LABELS, default="12개월(코호트)", key="ch_period")
        selected_period = PERIOD_OPTIONS[PERIOD_LABELS.index(selected_label)] if selected_label else "12M"
        with fc2:
            min_deals = st.number_input("최소 딜 수", 1, 200, 10, 5, key="ch_min_deals")
        with fc3:
            min_apply = st.number_input("최소 신청액(억)", 0.0, 50.0, 0.5, 0.5, key="ch_min_apply")

        maturity_warn = selected_period in ("1W", "2W", "3W", "4W", "1M", "3M")
        if maturity_warn:
            st.warning(f"⚠️ {selected_label} 기간은 payment lag 미성숙 — 수익률은 신뢰 불가. 신청액/딜수 기준으로만 판단하세요.")

        if deep is not None:
            ab = deep.get("campaign_ab", [])
            if ab:
                df_ab = pd.DataFrame(ab)
                df_ab = df_ab[df_ab["deals"] >= min_deals].head(30)
                avail_cols = [c for c in ["channel_type","utm_source","utm_medium","utm_campaign",
                                           "deals","apply_oku","payment_oku","yield_pct","won_rate","paid_rate"]
                              if c in df_ab.columns]
                df_ab = df_ab[avail_cols]
                df_ab.columns = ["유형","소스","미디움","캠페인","딜수","신청액(억)","결제액(억)","수익률(%)","수주율(%)","결제율(%)"][:len(avail_cols)]
                st.dataframe(df_ab, use_container_width=True, hide_index=True)

# ─────────────────────────────────────────────
# Tab 3 — ROAS
# ─────────────────────────────────────────────
with tabs[3]:
    roas = get_roas()

    # ── 상단 필터 ──
    rf1, rf2 = st.columns([3, 1])
    with rf1:
        roas_label = st.pills(
            "기간", PERIOD_LABELS, default="12개월(코호트)", key="roas_period"
        )
    roas_period = PERIOD_OPTIONS[PERIOD_LABELS.index(roas_label)] if roas_label else "12M"
    with rf2:
        roas_min_spend = st.number_input("최소 광고비(만원)", min_value=0, max_value=10000, value=100, step=100, key="roas_min_spend")

    if st.button("📖 이 차트 이해하기", key="guide_roas"):
        show_guide("roas")

    if roas is None:
        st.warning("roas_marketing.json 파일을 찾을 수 없습니다.")
    else:
        cohort = roas.get("by_window", {}).get(UTM_KEY[roas_period], {})
        by_channel = cohort.get("by_channel", [])

        if by_channel:
            st.subheader(f"채널별 ROAS ({roas_label})")
            df_roas = pd.DataFrame(by_channel).copy()
            if "광고비" in df_roas.columns:
                df_roas = df_roas[df_roas["광고비"] >= roas_min_spend * 10000]

            def roas_color(val):
                if val < 1:    return "red"
                elif val <= 2: return "orange"
                return "green"

            if df_roas.empty:
                st.info(f"광고비 ≥ {roas_min_spend:,}만원 조건을 만족하는 채널이 없습니다.")
            else:
                colors = [roas_color(r) for r in df_roas["ROAS_expected"]]
                # 예상결제율(%) = 예상결제/광고비 → 별도 axis
                df_roas["예상결제율(%)"] = (
                    df_roas["예상결제"] / df_roas["광고비"] * 100
                ).where(df_roas["광고비"] > 0).round(1) if "예상결제" in df_roas.columns else None

                fig_roas = go.Figure()
                hover_roas = (
                    "<b>%{x}</b>"
                    "<br>ROAS: %{y:.2f}"
                    f"<br><i>{METRIC_DEFS['ROAS']}</i>"
                    "<br>광고비: %{customdata[0]}<br>예상결제: %{customdata[1]}"
                    "<br>신청건수: %{customdata[2]:,}건<extra></extra>"
                )
                df_roas["광고비_fmt"]  = df_roas["광고비"].apply(lambda v: fmt_krw(v) if pd.notna(v) else "—")
                df_roas["예상결제_fmt"] = df_roas["예상결제"].apply(lambda v: fmt_krw(v) if pd.notna(v) else "—") if "예상결제" in df_roas.columns else "—"
                fig_roas.add_bar(
                    x=df_roas["채널"],
                    y=df_roas["ROAS_expected"],
                    marker_color=colors,
                    text=[f"{v:.2f}x" for v in df_roas["ROAS_expected"]],
                    textposition="outside",
                    customdata=df_roas[["광고비_fmt", "예상결제_fmt", "신청건수"]].values if "신청건수" in df_roas.columns else df_roas[["광고비_fmt", "예상결제_fmt"]].assign(n=0).values,
                    hovertemplate=hover_roas,
                )
                fig_roas.add_hline(y=1, line_dash="dash", line_color="red",   annotation_text="손익분기(1.0x)")
                fig_roas.add_hline(y=2, line_dash="dot",  line_color="orange", annotation_text="2.0x")
                fig_roas.update_layout(yaxis_title="ROAS", height=420, xaxis_tickangle=-30)
                st.plotly_chart(fig_roas, use_container_width=True)
                st.caption("ROAS = 예상결제액 ÷ 광고비 | 마케팅팀 예상결제 기반 (+6.5% 과대 가능성) | 빨강 <1 · 주황 1~2 · 초록 >2")

        by_media = cohort.get("by_media", [])
        if by_media:
            st.subheader(f"매체별 CPL 표 ({roas_label})")
            mc1, mc2, mc3 = st.columns([2, 1, 1])
            with mc1:
                media_search = st.text_input("매체명 검색", placeholder="예: toss, kakao, naver...", key="media_search")
            with mc2:
                media_min_spend = st.number_input("최소 광고비(만원)", min_value=0, max_value=10000, value=0, step=10, key="media_min_spend")
            with mc3:
                media_sort = st.selectbox("정렬 기준", ["광고비↓", "CPL↑", "ROAS↓", "신청건수↓"], key="media_sort")

            df_media = pd.DataFrame(by_media)
            total_count = len(df_media)
            if "광고비" in df_media.columns and media_min_spend > 0:
                df_media = df_media[df_media["광고비"] >= media_min_spend * 10000]
            if media_search.strip() and "채널" in df_media.columns:
                df_media = df_media[df_media["채널"].str.contains(media_search.strip(), case=False, na=False)]

            sort_map = {"광고비↓": ("광고비", False), "CPL↑": ("CPL_krw", True), "ROAS↓": ("ROAS_expected", False), "신청건수↓": ("신청건수", False)}
            sort_col, sort_asc = sort_map.get(media_sort, ("광고비", False))
            if sort_col in df_media.columns:
                df_media = df_media.sort_values(sort_col, ascending=sort_asc)

            st.caption(f"전체 {total_count}개 매체 중 {len(df_media)}개 표시")
            cols_show = [c for c in ["채널", "광고비", "CPL_krw", "ROAS_expected", "신청건수"] if c in df_media.columns]
            df_show = df_media[cols_show].copy()

            # 천단위 콤마 포맷
            for col in ["광고비", "CPL_krw"]:
                if col in df_show.columns:
                    df_show[col] = df_show[col].apply(lambda v: f"{int(round(v)):,}" if pd.notna(v) else "—")

            df_show = df_show.rename(columns={
                "채널": "매체", "광고비": "광고비(원)", "CPL_krw": "CPL(원)",
                "ROAS_expected": "ROAS", "신청건수": "신청건수",
            })

            def highlight_roas(val):
                try:    v = float(val)
                except: return ""
                if v < 1:   return "color: red; font-weight: bold"
                elif v <= 2: return "color: orange"
                return "color: green; font-weight: bold"

            styled = df_show.style.map(highlight_roas, subset=["ROAS"])
            st.dataframe(styled, use_container_width=True, hide_index=True)
            st.caption(f"CPL = {METRIC_DEFS['CPL']} | ROAS = {METRIC_DEFS['ROAS']}")

# ─────────────────────────────────────────────
# Tab 4 — 어트리뷰션
# ─────────────────────────────────────────────
with tabs[4]:
    attr_data = get_attribution()
    if st.button("📖 이 차트 이해하기", key="guide_attribution"):
        show_guide("attribution")

    if attr_data is None:
        st.warning("attribution_analysis.json 파일을 찾을 수 없습니다.")
    else:
        # ── 상단 필터 ──
        af1, af2, af3 = st.columns([2, 2, 2])
        with af1:
            attr_win = st.pills("기간", ["12M","6M","3M","1M","4W","1W"], default="12M", key="attr_win")
        with af2:
            attr_model = st.pills("귀속 모델", ["first","last","linear"],
                                  default="last", key="attr_model",
                                  help="first=최초유입 기여, last=최종전환 기여, linear=균등분배")
        with af3:
            attr_min_apply = st.number_input("최소 신청액(억)", 0.5, 50.0, 1.0, 0.5, key="attr_min")

        win_key  = attr_win  or "12M"
        model    = attr_model or "last"
        win_data = attr_data.get("windows", {}).get(win_key, {})
        ch_rows  = win_data.get("attribution", {}).get(model, [])

        model_label = {"first": "First Touch (최초 유입)", "last": "Last Touch (최종 전환)", "linear": "Linear (균등 배분)"}[model]
        st.caption(f"⚠️ 조회(browse) UTM은 Pipedrive 미보유 — First Touch는 신청 URL 쿼리스트링 기준 | {attr_data.get('as_of','')} 기준")

        # ── Bubble Chart: 볼륨 × 수익률 ──
        st.subheader(f"채널 포지셔닝 — {model_label} ({win_key})")
        if ch_rows:
            df_b = pd.DataFrame(ch_rows)
            df_b = df_b[
                (df_b["apply_oku"] >= attr_min_apply) &
                (df_b["yield_pct"].notna())
            ].copy()
            if not df_b.empty:
                # 사분면 참조선
                med_apply = df_b["apply_oku"].median()
                med_yield = df_b["yield_pct"].median()

                fig_bubble = px.scatter(
                    df_b,
                    x="apply_oku",
                    y="yield_pct",
                    size="apply_oku",
                    size_max=60,
                    text="channel",
                    color="yield_pct",
                    color_continuous_scale="RdYlGn",
                    hover_data={"deals": True, "pay_oku": True, "apply_oku": True},
                    labels={"apply_oku": "신청액(억)", "yield_pct": "수익률(%)", "channel": "채널"},
                    height=520,
                )
                fig_bubble.add_vline(x=med_apply, line_dash="dot", line_color="gray",
                                     annotation_text=f"중앙값 {med_apply:.1f}억")
                fig_bubble.add_hline(y=med_yield, line_dash="dot", line_color="gray",
                                     annotation_text=f"중앙값 {med_yield:.1f}%")
                fig_bubble.update_traces(textposition="top center", textfont_size=10)
                fig_bubble.update_layout(coloraxis_showscale=False)
                st.plotly_chart(fig_bubble, use_container_width=True)
                st.caption("우상단 = 볼륨 크고 수익률 높음 (핵심 채널) | 우하단 = 볼륨 크지만 수익률 낮음 (비효율) | 좌상단 = 수익률 높지만 소규모")
            else:
                st.info("필터 조건을 만족하는 채널이 없습니다.")

        # ── 모델 비교 테이블 ──
        st.subheader("모델별 채널 기여도 비교 (Top 20, 신청액 기준)")
        win_attr = win_data.get("attribution", {})
        dfs = {}
        for m in ("first", "last", "linear"):
            rows_m = win_attr.get(m, [])
            if rows_m:
                df_m = pd.DataFrame(rows_m)
                df_m = df_m[df_m["apply_oku"] >= attr_min_apply].head(20)
                dfs[m] = df_m.set_index("channel")[["apply_oku", "pay_oku", "yield_pct", "deals"]]

        if dfs:
            # 세 모델 yield_pct 비교
            all_ch = sorted(set().union(*[set(d.index) for d in dfs.values()]))
            compare_rows = []
            for ch in all_ch:
                row = {"채널": ch}
                for m in ("first", "last", "linear"):
                    if ch in dfs.get(m, {}):
                        r = dfs[m].loc[ch]
                        row[f"{m}_yield"] = r["yield_pct"]
                        row[f"{m}_apply"] = r["apply_oku"]
                    else:
                        row[f"{m}_yield"] = None
                        row[f"{m}_apply"] = None
                compare_rows.append(row)

            df_cmp = (
                pd.DataFrame(compare_rows)
                .dropna(subset=["last_apply"])
                .sort_values("last_apply", ascending=False)
                .head(20)
                .reset_index(drop=True)
            )
            df_cmp.columns = ["채널", "First수익률", "Last수익률", "Linear수익률", "First신청액", "Last신청액", "Linear신청액"]
            st.dataframe(
                df_cmp[["채널", "Last신청액", "First수익률", "Last수익률", "Linear수익률"]],
                use_container_width=True, hide_index=True
            )

        # ── CRM Lift ──
        crm_rows = win_data.get("crm_lift", [])
        if crm_rows:
            st.subheader("CRM 재유입 Lift 분석 — 채널별 alrimtalk/SMS 효과")
            df_lift = pd.DataFrame(crm_rows)
            fig_lift = go.Figure()
            fig_lift.add_bar(
                x=df_lift["acquisition"],
                y=df_lift["organic_yield"],
                name="CRM 없음 (organic)",
                marker_color="steelblue",
            )
            fig_lift.add_bar(
                x=df_lift["acquisition"],
                y=df_lift["crm_yield"],
                name="CRM 재유입",
                marker_color="coral",
            )
            fig_lift.update_layout(
                barmode="group",
                yaxis_title="수익률 (%)",
                height=420,
                xaxis_tickangle=-30,
                legend_title="",
            )
            st.plotly_chart(fig_lift, use_container_width=True)

            df_lift_show = df_lift[[
                "acquisition", "organic_deals", "organic_apply", "organic_yield",
                "crm_deals", "crm_apply", "crm_yield", "crm_lift_ppt"
            ]].copy()
            df_lift_show.columns = [
                "Acquisition 채널", "Organic딜", "Organic신청액", "Organic수익률",
                "CRM딜", "CRM신청액", "CRM수익률", "Lift(ppt)"
            ]
            st.dataframe(df_lift_show, use_container_width=True, hide_index=True)

        # ── Journey Matrix ──
        st.subheader("First × Last Touch Journey Matrix")
        matrix_data = win_data.get("journey_matrix", {})
        cells = matrix_data.get("cells", [])
        if cells:
            df_j = pd.DataFrame(cells)

            jc1, jc2 = st.columns([3, 1])
            with jc1:
                show_multi_only = st.toggle("멀티터치만 표시 (대각선 제외)", value=True, key="journey_multi")
            with jc2:
                heat_metric = st.selectbox("지표", ["신청액(억)", "딜수", "수익률(%)"], key="heat_metric")

            df_j_view = df_j[df_j["first"] != df_j["last"]].copy() if show_multi_only else df_j.copy()

            metric_col = {"신청액(억)": "apply_oku", "딜수": "deals", "수익률(%)": "yield_pct"}[heat_metric]

            if df_j_view.empty:
                st.info("멀티터치 경로가 없습니다.")
            else:
                # 멀티터치만 볼 때: 최소 딜 3건 이상만
                if show_multi_only:
                    df_j_view = df_j_view[df_j_view["deals"] >= 3]

                pivot = df_j_view.pivot_table(
                    index="first", columns="last", values=metric_col,
                    aggfunc="sum" if metric_col != "yield_pct" else "mean",
                    fill_value=0 if metric_col != "yield_pct" else None,
                )

                # 행/열 모두 합계 기준으로 정렬 (가장 큰 채널이 위/왼쪽)
                pivot = pivot.loc[
                    pivot.sum(axis=1).sort_values(ascending=False).index,
                    pivot.sum(axis=0).sort_values(ascending=False).index,
                ]

                color_scale = "Blues" if metric_col != "yield_pct" else "RdYlGn"
                fig_heat = px.imshow(
                    pivot,
                    color_continuous_scale=color_scale,
                    text_auto=".1f",
                    labels={"x": "Last Touch (최종전환)", "y": "First Touch (최초유입)", "color": heat_metric},
                    aspect="auto",
                    height=max(400, len(pivot) * 32),
                )
                fig_heat.update_xaxes(tickangle=-40)
                fig_heat.update_traces(textfont_size=9)
                st.plotly_chart(fig_heat, use_container_width=True)

                if show_multi_only:
                    n_multi = len(df_j_view)
                    total_apply = df_j_view["apply_oku"].sum()
                    st.caption(f"멀티터치 경로 {n_multi}개 | 신청액 합계 {total_apply:.1f}억 | 딜 3건 미만 제외")
                else:
                    st.caption("대각선 = 싱글터치 | 대각선 외 = 멀티터치")

                # 주요 멀티터치 경로 Top 10 테이블
                if show_multi_only:
                    st.write("**주요 멀티터치 경로 Top 10**")
                    top_paths = (
                        df_j_view
                        .sort_values("apply_oku", ascending=False)
                        .head(10)[["first", "last", "deals", "apply_oku", "yield_pct"]]
                        .rename(columns={"first": "최초유입", "last": "최종전환",
                                         "deals": "딜수", "apply_oku": "신청액(억)", "yield_pct": "수익률(%)"})
                        .reset_index(drop=True)
                    )
                    st.dataframe(top_paths, use_container_width=True, hide_index=True)

# ─────────────────────────────────────────────
# Tab 5 — 마케팅 예측 공식
# ─────────────────────────────────────────────
with tabs[5]:
    st.markdown("### 📐 마케팅 예측 공식 현황")
    st.caption("현재 공식(6.5%)의 문제와 개선 제안(4.7%), Pipeline별 실적, 월별 정확도, 로우 데이터 다운로드")

    # ── 기간 설정 ────────────────────────────────────────────────────────────
    mf1, mf2 = st.columns(2)
    with mf1:
        mf_from = st.date_input("신청일 시작", value=__import__("datetime").date(2024, 10, 20), key="mf_from")
    with mf2:
        mf_to   = st.date_input("신청일 종료", value=__import__("datetime").date(2025, 10, 20), key="mf_to")
    mf_from_s, mf_to_s = str(mf_from), str(mf_to)

    if not DB_PATH.exists():
        st.warning("DB 파일을 찾을 수 없습니다. (/tmp/history.sqlite)")
    else:
        mf_con  = sqlite3.connect(DB_PATH)
        mf_asof = mf_con.execute("SELECT MAX(as_of_date) FROM deal_history").fetchone()[0]
        st.caption(f"데이터 기준일: {mf_asof}")

        # ── 0. 4.7% 도출 근거 ──────────────────────────────────────────────────
        st.markdown("---")
        st.markdown("#### 🧮 4.7% 계수 도출 근거")
        st.caption("마케팅팀이 쓰는 '신청환급금' 전체 base에 적용하는 계수가 왜 4.7%인지 단계별로 보여줍니다.")

        # Step A: 유효 pipeline 실제 수익률
        eff_row = mf_con.execute("""
            SELECT SUM(apply_amount), SUM(payment_amount)
            FROM deal_history
            WHERE as_of_date=? AND apply_date BETWEEN ? AND ?
              AND pipeline IN ('B(젠트)-환급','C(젠트)-추심','법인-환급','법인-추심')
        """, (mf_asof, mf_from_s, mf_to_s)).fetchone()
        eff_apply, eff_pay = (eff_row[0] or 0), (eff_row[1] or 0)
        eff_yield = eff_pay / eff_apply * 100 if eff_apply > 0 else 0

        # Step B: 전체 base (마케팅 기준, A(지수) 포함)
        total_apply_all = mf_con.execute("""
            SELECT SUM(apply_amount) FROM deal_history
            WHERE as_of_date=? AND apply_date BETWEEN ? AND ?
        """, (mf_asof, mf_from_s, mf_to_s)).fetchone()[0] or 0

        # Step C: scale ratio & 환산 계수
        scale_ratio = total_apply_all / eff_apply if eff_apply > 0 else 0
        derived_pct = eff_yield / scale_ratio if scale_ratio > 0 else 0

        # 단계별 표시
        st.markdown(f"""
| 단계 | 계산 | 값 |
|------|------|----|
| **① 유효 pipeline 신청액** | B환급 + C추심 + 법인 합산 | **{eff_apply/1e8:.1f}억** |
| **② 유효 pipeline 결제액** | 동일 pipeline 실측 결제 | **{eff_pay/1e8:.1f}억** |
| **③ 실제 수익률** | ② ÷ ① | **{eff_yield:.2f}%** |
| **④ 전체 신청액 (마케팅 base)** | A(지수) 포함 전체 | **{total_apply_all/1e8:.1f}억** |
| **⑤ Base 배율** | ④ ÷ ① (마케팅 base가 유효 base의 몇 배) | **{scale_ratio:.2f}배** |
| **⑥ 환산 계수** | ③ ÷ ⑤ (마케팅 base 기준 등가 계수) | **{derived_pct:.2f}%** |
""")
        st.info(
            f"📌 결론: 실제 수익률 **{eff_yield:.1f}%** 는 유효 pipeline 기준 — "
            f"마케팅팀이 보는 전체 신청액은 유효 base의 **{scale_ratio:.1f}배**이므로 "
            f"전체 base에 적용할 등가 계수 = {eff_yield:.1f}% ÷ {scale_ratio:.1f} = **{derived_pct:.2f}% ≈ 4.7%**"
        )
        st.caption("※ A(지수) pipeline은 신청은 잡히지만 결제로 거의 이어지지 않아 분모만 키우는 구조. 이를 포함한 전체 base에 적용하려면 유효 수익률을 배율로 나눠야 함.")

        # ── 1. 공식 비교 카드 ─────────────────────────────────────────────────
        st.markdown("---")
        st.markdown("#### 🔢 예측 공식 비교")

        # 실제 신청액 (필터 전) 기간 합
        raw_apply = mf_con.execute("""
            SELECT SUM(apply_amount) FROM deal_history
            WHERE as_of_date=? AND apply_date BETWEEN ? AND ?
              AND pipeline NOT IN ('A(지수)')
        """, (mf_asof, mf_from_s, mf_to_s)).fetchone()[0] or 0

        fc1, fc2, fc3 = st.columns(3)
        fc1.metric("기간 내 총 신청액 (유효 base)", f"{raw_apply/1e8:.1f}억",
                   help="A(지수) 제외 유효 pipeline 합산")
        fc2.metric("현재 공식 예상결제 (×6.5%)", f"{raw_apply * 0.065 / 1e8:.1f}억",
                   help="마케팅팀 현재 사용 공식")
        fc3.metric("개선 공식 예상결제 (×4.7%)", f"{raw_apply * 0.047 / 1e8:.1f}억",
                   help="Pipedrive 실측 코호트 기반 제안 계수")

        st.info(
            "💡 현재 6.5% 공식은 **A(지수) pipeline 포함 전체 신청액** 기준으로 설계되어 약 38% 과대추정. "
            "개선 공식 4.7%는 유효 pipeline(B·C·법인) 실측 전환율 20.3% ÷ base 비율 4.77배로 환산한 값."
        )

        # ── 2. Pipeline별 전환율 ─────────────────────────────────────────────
        st.markdown("---")
        st.markdown("#### 🏗️ Pipeline별 신청 → 결제 전환율")

        pipe_rows = mf_con.execute("""
            SELECT
                pipeline,
                COUNT(*) deals,
                SUM(apply_amount) apply_sum,
                SUM(payment_amount) pay_sum,
                SUM(CASE WHEN payment_date IS NOT NULL THEN 1 ELSE 0 END) paid_n,
                SUM(CASE WHEN status='won' THEN 1 ELSE 0 END) won_n,
                SUM(CASE WHEN status='lost' THEN 1 ELSE 0 END) lost_n
            FROM deal_history
            WHERE as_of_date=? AND apply_date BETWEEN ? AND ?
            GROUP BY pipeline
            ORDER BY apply_sum DESC
        """, (mf_asof, mf_from_s, mf_to_s)).fetchall()

        df_pipe = pd.DataFrame(pipe_rows, columns=["Pipeline","딜수","신청액","결제액","결제완료","수주","취소"])
        df_pipe["신청액(억)"]  = (df_pipe["신청액"] / 1e8).round(1)
        df_pipe["결제액(억)"]  = (df_pipe["결제액"] / 1e8).round(1)
        df_pipe["수익률(%)"]   = (df_pipe["결제액"] / df_pipe["신청액"] * 100).where(df_pipe["신청액"] > 0).round(2)
        df_pipe["결제율(건수)"] = (df_pipe["결제완료"] / df_pipe["딜수"] * 100).round(1)
        df_pipe["비중(%)"]    = (df_pipe["신청액"] / df_pipe["신청액"].sum() * 100).round(1)

        def _pipe_note(p):
            if "A(지수)" in p:  return "🚨 분모 희석 (전환율 0.01%)"
            if "취소" in p:     return "⛔ 취소/환불 전용"
            if "환급" in p:     return "✅ 핵심 매출"
            if "추심" in p:     return "💰 추심 회수"
            return ""
        df_pipe["비고"] = df_pipe["Pipeline"].apply(_pipe_note)

        show_pipe = df_pipe[["Pipeline","비고","딜수","신청액(억)","결제액(억)","수익률(%)","결제율(건수)","비중(%)"]]
        st.dataframe(show_pipe, use_container_width=True, hide_index=True)

        # 파이 차트: pipeline 신청액 비중
        fig_pie = px.pie(
            df_pipe, values="신청액(억)", names="Pipeline",
            title="Pipeline별 신청액 비중",
            color_discrete_sequence=px.colors.qualitative.Set2,
            height=320,
        )
        fig_pie.update_traces(textinfo="label+percent")
        st.plotly_chart(fig_pie, use_container_width=True)
        st.caption("A(지수) pipeline이 신청액 절반 이상을 차지하며 전체 전환율을 희석 — 이 pipeline 제외 시 수익률이 2배로 올라감")

        # ── 3. 월별 신청 코호트 정확도 ────────────────────────────────────────
        st.markdown("---")
        st.markdown("#### 📅 월별 신청 코호트 — 예측 vs 실측")

        monthly_rows = mf_con.execute("""
            SELECT
                substr(apply_date,1,7) ym,
                SUM(apply_amount) apply_sum,
                SUM(payment_amount) pay_sum,
                COUNT(*) deals
            FROM deal_history
            WHERE as_of_date=? AND apply_date BETWEEN ? AND ?
              AND pipeline IN ('B(젠트)-환급','C(젠트)-추심','법인-환급','법인-추심')
            GROUP BY ym ORDER BY ym
        """, (mf_asof, mf_from_s, mf_to_s)).fetchall()

        df_mo = pd.DataFrame(monthly_rows, columns=["월","신청액","결제액(실측)","딜수"])
        df_mo["신청액(억)"]     = (df_mo["신청액"] / 1e8).round(2)
        df_mo["실측 결제액(억)"] = (df_mo["결제액(실측)"] / 1e8).round(2)
        df_mo["예측_4.7%(억)"]  = (df_mo["신청액"] * 0.047 / 1e8).round(2)
        df_mo["예측_6.5%(억)"]  = (df_mo["신청액"] * 0.065 / 1e8).round(2)
        df_mo["오차_4.7%"]      = ((df_mo["예측_4.7%(억)"] - df_mo["실측 결제액(억)"]) / df_mo["실측 결제액(억)"] * 100).round(1)
        df_mo["수익률(실측%)"]   = (df_mo["결제액(실측)"] / df_mo["신청액"] * 100).round(2)

        fig_mo = go.Figure()
        fig_mo.add_scatter(x=df_mo["월"], y=df_mo["실측 결제액(억)"], name="실측 결제액",
                           mode="lines+markers", line=dict(color="steelblue", width=2))
        fig_mo.add_scatter(x=df_mo["월"], y=df_mo["예측_4.7%(억)"], name="예측(4.7%)",
                           mode="lines+markers", line=dict(dash="dash", color="green"))
        fig_mo.add_scatter(x=df_mo["월"], y=df_mo["예측_6.5%(억)"], name="예측(6.5%·현재)",
                           mode="lines+markers", line=dict(dash="dot", color="red"))
        fig_mo.update_layout(height=380, yaxis_title="억원", xaxis_title="", legend_title="")
        st.plotly_chart(fig_mo, use_container_width=True)

        mape_47 = df_mo["오차_4.7%"].abs().mean()
        st.caption(f"4.7% 공식 MAPE: **{mape_47:.1f}%** | 유효 pipeline(B·C·법인) 기준")
        st.dataframe(
            df_mo[["월","딜수","신청액(억)","실측 결제액(억)","예측_4.7%(억)","오차_4.7%","수익률(실측%)"]],
            use_container_width=True, hide_index=True,
        )

        mf_con.close()

        # ── 4. Excel 다운로드 ─────────────────────────────────────────────────
        st.markdown("---")
        st.markdown("#### 📥 로우 데이터 다운로드 (Excel)")

        dl_pipe_filter = st.multiselect(
            "포함할 Pipeline", options=[r[0] for r in pipe_rows],
            default=[r[0] for r in pipe_rows if "A(지수)" not in r[0] and "테스트" not in r[0]],
            key="dl_pipe",
        )

        if st.button("📊 Excel 파일 생성", key="gen_excel"):
            with st.spinner("Excel 생성 중..."):
                dl_con = sqlite3.connect(DB_PATH)
                dl_asof = dl_con.execute("SELECT MAX(as_of_date) FROM deal_history").fetchone()[0]

                placeholders = ",".join("?" * len(dl_pipe_filter))
                df_raw = pd.read_sql_query(f"""
                    SELECT
                        deal_id, pipeline, status, source,
                        apply_date, apply_amount,
                        filing_date, filing_amount,
                        decision_date, decision_amount,
                        payment_date, payment_amount,
                        lost_reason, cancel_reason, hold_reason,
                        customer_type, utm_source, utm_medium, utm_campaign,
                        utm_source_query, update_time
                    FROM deal_history
                    WHERE as_of_date=? AND apply_date BETWEEN ? AND ?
                      AND pipeline IN ({placeholders})
                    ORDER BY apply_date DESC
                """, dl_con, params=[dl_asof, mf_from_s, mf_to_s] + list(dl_pipe_filter))
                dl_con.close()

                buf = io.BytesIO()
                with pd.ExcelWriter(buf, engine="openpyxl") as writer:
                    # Sheet 1: 로우 딜 데이터
                    df_raw.to_excel(writer, sheet_name="딜 로우데이터", index=False)
                    # Sheet 2: Pipeline 요약
                    show_pipe.to_excel(writer, sheet_name="Pipeline 요약", index=False)
                    # Sheet 3: 월별 코호트
                    df_mo[["월","딜수","신청액(억)","실측 결제액(억)","예측_4.7%(억)","오차_4.7%","수익률(실측%)"]].to_excel(
                        writer, sheet_name="월별 코호트 정확도", index=False
                    )

                buf.seek(0)
                fname = f"bznav_forecast_raw_{mf_from_s}_{mf_to_s}.xlsx"
                st.download_button(
                    label="⬇️ Excel 다운로드",
                    data=buf,
                    file_name=fname,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="dl_excel_btn",
                )
                st.success(f"✅ {len(df_raw):,}건 딜 데이터 + Pipeline 요약 + 월별 코호트 — 3개 시트")


# ─────────────────────────────────────────────
# Tab 6 — 시스템 상태
# ─────────────────────────────────────────────
with tabs[6]:
    vr = get_verification()
    pl = get_pipeline()

    if vr is None:
        st.warning("verification_report.json 파일을 찾을 수 없습니다.")
    else:
        severity = vr.get("severity", "unknown")
        passed = vr.get("passed", 0)
        total = vr.get("total_checks", 0)

        severity_color = {"info": "🟢", "warn": "🟡", "critical": "🔴"}.get(severity, "⚪")
        st.markdown(f"### 검증 결과  {severity_color} `{severity.upper()}`  —  {passed}/{total} 통과")

        results = vr.get("results", [])
        for r in results:
            icon = "✅" if r.get("ok") else "❌"
            check_name = r.get("check", "")
            detail = r.get("detail", "")
            value = r.get("value")
            value_str = f"  |  값: `{value}`" if value is not None else ""
            st.markdown(f"{icon} **{check_name}** — {detail}{value_str}")

    st.divider()

    if pl is None:
        st.warning("pipeline_state.json 파일을 찾을 수 없습니다.")
    else:
        st.subheader("파이프라인 실행 상태")
        run_id = pl.get("run_id", "—")
        status = pl.get("status", "—")
        started = pl.get("started_at", "—")
        finished = pl.get("finished_at", "—")
        dry_run = pl.get("dry_run", False)

        c1, c2, c3 = st.columns(3)
        c1.metric("Run ID", run_id)
        c2.metric("상태", status)
        c3.metric("Dry Run", "예" if dry_run else "아니오")
        st.caption(f"시작: {started}  |  종료: {finished}")

        steps = pl.get("steps", [])
        if steps:
            df_steps = pd.DataFrame(steps)
            cols = [c for c in ["name", "status", "duration_sec", "detail", "error"] if c in df_steps.columns]
            df_steps_show = df_steps[cols].copy()
            rename_steps = {
                "name": "단계",
                "status": "상태",
                "duration_sec": "소요(초)",
                "detail": "상세",
                "error": "오류",
            }
            df_steps_show = df_steps_show.rename(columns=rename_steps)
            st.dataframe(df_steps_show, use_container_width=True, hide_index=True)
