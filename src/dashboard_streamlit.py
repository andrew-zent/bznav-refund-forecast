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

tabs = st.tabs(["예측 현황", "채널 분석", "ROAS", "어트리뷰션", "시스템 상태"])

# ─────────────────────────────────────────────
# Tab 1 — 예측 현황
# ─────────────────────────────────────────────
with tabs[0]:
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
with tabs[1]:
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
with tabs[2]:
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
with tabs[3]:
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
# Tab 5 — 시스템 상태
# ─────────────────────────────────────────────
with tabs[4]:
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
