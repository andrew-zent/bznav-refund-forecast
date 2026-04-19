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

    # ── 상단 필터 ──
    fc1, fc2, fc3 = st.columns([3, 1, 1])
    with fc1:
        selected_label = st.pills(
            "기간", PERIOD_LABELS, default="12개월(코호트)", key="ch_period"
        )
    selected_period = PERIOD_OPTIONS[PERIOD_LABELS.index(selected_label)] if selected_label else "12M"
    with fc2:
        min_deals = st.number_input("최소 딜 수", min_value=1, max_value=200, value=10, step=5, key="ch_min_deals")
    with fc3:
        min_apply = st.number_input("최소 신청액(억)", min_value=0.0, max_value=50.0, value=0.5, step=0.5, key="ch_min_apply")

    deep_key = DEEP_KEY[selected_period]
    utm_key  = UTM_KEY[selected_period]

    if deep is None:
        st.warning("channel_deep_analysis.json 파일을 찾을 수 없습니다.")
    else:
        nv = deep.get("new_vs_remind", {}).get(deep_key, {})
        new_data = nv.get("new", {})
        remind_data = nv.get("remind", {})
        if new_data and remind_data:
            st.subheader(f"신규 vs 리마인드 수익률 비교 ({selected_label})")
            df_nr = pd.DataFrame(
                [
                    {
                        "구분": "신규",
                        "수익률(%)": new_data.get("yield_pct", 0),
                        "건수": new_data.get("total_deals", 0),
                    },
                    {
                        "구분": "리마인드",
                        "수익률(%)": remind_data.get("yield_pct", 0),
                        "건수": remind_data.get("total_deals", 0),
                    },
                ]
            )
            fig_nr = px.bar(
                df_nr,
                x="구분",
                y="수익률(%)",
                text="수익률(%)",
                color="구분",
                height=320,
            )
            fig_nr.update_traces(texttemplate="%{text:.2f}%", textposition="outside")
            fig_nr.update_layout(showlegend=False, yaxis_title="수익률 (%)")
            st.plotly_chart(fig_nr, use_container_width=True)
        elif not new_data and not remind_data:
            st.info(f"{selected_label} 기간에 데이터가 없습니다.")

    if utm is None:
        st.warning("utm_channel_analysis.json 파일을 찾을 수 없습니다.")
    else:
        src_data = utm.get("by_dimension", {}).get("utm_source", {}).get(utm_key, [])
        if src_data:
            st.subheader(f"UTM Source 수익률 Top 15 ({selected_label})")
            df_utm = pd.DataFrame(src_data)
            # 최소 딜 수 & 최소 신청액 필터 적용 후 수익률 순 정렬
            df_utm = (
                df_utm[
                    (df_utm["deals"] >= min_deals) &
                    (df_utm.get("apply_oku", df_utm.get("apply_amount", 0)) >= min_apply)
                ]
                .sort_values("yield_pct", ascending=False)
                .head(15)
            )
            if df_utm.empty:
                st.info(f"필터 조건(딜 ≥{min_deals}, 신청액 ≥{min_apply}억)을 만족하는 채널이 없습니다.")
            else:
                fig_utm = px.bar(
                    df_utm,
                    x="utm_source",
                    y="yield_pct",
                    text="yield_pct",
                    height=400,
                    labels={"utm_source": "채널", "yield_pct": "수익률 (%)"},
                    custom_data=["deals"],
                )
                fig_utm.update_traces(
                    texttemplate="%{text:.1f}%",
                    textposition="outside",
                    hovertemplate="<b>%{x}</b><br>수익률: %{y:.1f}%<br>딜 수: %{customdata[0]}건<extra></extra>",
                )
                fig_utm.update_layout(xaxis_tickangle=-35)
                st.plotly_chart(fig_utm, use_container_width=True)
                st.caption(f"필터: 딜 수 ≥ {min_deals}건, 신청액 ≥ {min_apply}억 | 표시: {len(df_utm)}개 채널")
        else:
            st.info(f"{selected_label} 기간의 UTM 데이터가 없습니다.")

    if deep is not None:
        ab = deep.get("campaign_ab", [])
        if ab:
            st.subheader(f"캠페인 상세 — {selected_label} (상위 30개)")
            df_ab = pd.DataFrame(ab)
            # 최소 딜 수 필터 적용
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

    if roas is None:
        st.warning("roas_marketing.json 파일을 찾을 수 없습니다.")
    else:
        cohort = roas.get("by_window", {}).get(UTM_KEY[roas_period], {})
        by_channel = cohort.get("by_channel", [])

        if by_channel:
            st.subheader(f"채널별 ROAS ({roas_label})")
            df_roas = pd.DataFrame(by_channel).copy()
            # 최소 광고비 필터
            if "광고비" in df_roas.columns:
                df_roas = df_roas[df_roas["광고비"] >= roas_min_spend * 10000]

            def roas_color(val):
                if val < 1:
                    return "red"
                elif val <= 2:
                    return "orange"
                return "green"

            if df_roas.empty:
                st.info(f"광고비 ≥ {roas_min_spend}만원 조건을 만족하는 채널이 없습니다.")
            else:
                colors = [roas_color(r) for r in df_roas["ROAS_expected"]]

                fig_roas = go.Figure()
                fig_roas.add_bar(
                    x=df_roas["채널"],
                    y=df_roas["ROAS_expected"],
                    marker_color=colors,
                    text=[f"{v:.2f}" for v in df_roas["ROAS_expected"]],
                    textposition="outside",
                )
                fig_roas.add_hline(
                    y=1, line_dash="dash", line_color="red", annotation_text="손익분기(1.0)"
                )
                fig_roas.add_hline(
                    y=2, line_dash="dot", line_color="orange", annotation_text="2.0"
                )
                fig_roas.update_layout(
                    yaxis_title="ROAS",
                    xaxis_title="",
                    height=420,
                    xaxis_tickangle=-30,
                )
                st.plotly_chart(fig_roas, use_container_width=True)
                st.caption("색상 기준: 빨간색 ROAS < 1, 주황색 1 ≤ ROAS ≤ 2, 초록색 ROAS > 2")

        by_media = cohort.get("by_media", [])
        if by_media:
            st.subheader(f"매체별 CPL 표 ({roas_label})")
            df_media = pd.DataFrame(by_media)
            # 최소 광고비 필터
            if "광고비" in df_media.columns:
                df_media = df_media[df_media["광고비"] >= roas_min_spend * 10000]
            cols_show = [c for c in ["채널", "광고비", "CPL_krw", "ROAS_expected", "신청건수"] if c in df_media.columns]
            df_show = df_media[cols_show].copy()
            rename_map = {
                "채널": "매체",
                "광고비": "광고비(원)",
                "CPL_krw": "CPL(원)",
                "ROAS_expected": "ROAS",
                "신청건수": "신청건수",
            }
            df_show = df_show.rename(columns=rename_map)

            def highlight_roas(val):
                try:
                    v = float(val)
                except (TypeError, ValueError):
                    return ""
                if v < 1:
                    return "color: red; font-weight: bold"
                elif v <= 2:
                    return "color: orange"
                return "color: green; font-weight: bold"

            styled = df_show.style.map(highlight_roas, subset=["ROAS"])
            st.dataframe(styled, use_container_width=True, hide_index=True)

# ─────────────────────────────────────────────
# Tab 4 — 어트리뷰션
# ─────────────────────────────────────────────
with tabs[3]:
    attr_data = get_attribution()
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
