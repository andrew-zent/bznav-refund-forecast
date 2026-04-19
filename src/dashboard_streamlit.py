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


tabs = st.tabs(["예측 현황", "채널 분석", "ROAS", "시스템 상태"])

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

    if deep is None:
        st.warning("channel_deep_analysis.json 파일을 찾을 수 없습니다.")
    else:
        nv = deep.get("new_vs_remind", {}).get("12M", {})
        new_data = nv.get("new", {})
        remind_data = nv.get("remind", {})
        if new_data and remind_data:
            st.subheader("신규 vs 리마인드 수익률 비교 (12M)")
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

    if utm is None:
        st.warning("utm_channel_analysis.json 파일을 찾을 수 없습니다.")
    else:
        src_data = utm.get("by_dimension", {}).get("utm_source", {}).get("12M_cohort", [])
        if src_data:
            st.subheader("UTM Source 수익률 Top 15 (12M)")
            df_utm = (
                pd.DataFrame(src_data)
                .sort_values("yield_pct", ascending=False)
                .head(15)
            )
            fig_utm = px.bar(
                df_utm,
                x="utm_source",
                y="yield_pct",
                text="yield_pct",
                height=400,
                labels={"utm_source": "채널", "yield_pct": "수익률 (%)"},
            )
            fig_utm.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
            fig_utm.update_layout(xaxis_tickangle=-35)
            st.plotly_chart(fig_utm, use_container_width=True)

    if deep is not None:
        ab = deep.get("campaign_ab", [])
        if ab:
            st.subheader("캠페인 상세 (상위 30개)")
            df_ab = pd.DataFrame(ab).head(30)[
                [
                    "channel_type",
                    "utm_source",
                    "utm_medium",
                    "utm_campaign",
                    "deals",
                    "apply_oku",
                    "payment_oku",
                    "yield_pct",
                    "won_rate",
                    "paid_rate",
                ]
            ]
            df_ab.columns = [
                "유형",
                "소스",
                "미디움",
                "캠페인",
                "딜수",
                "신청액(억)",
                "결제액(억)",
                "수익률(%)",
                "수주율(%)",
                "결제율(%)",
            ]
            st.dataframe(df_ab, use_container_width=True, hide_index=True)

# ─────────────────────────────────────────────
# Tab 3 — ROAS
# ─────────────────────────────────────────────
with tabs[2]:
    roas = get_roas()
    if roas is None:
        st.warning("roas_marketing.json 파일을 찾을 수 없습니다.")
    else:
        cohort = roas.get("by_window", {}).get("12M_cohort", {})
        by_channel = cohort.get("by_channel", [])

        if by_channel:
            st.subheader("채널별 ROAS (12M Cohort)")
            df_roas = pd.DataFrame(by_channel).copy()

            def roas_color(val):
                if val < 1:
                    return "red"
                elif val <= 2:
                    return "orange"
                return "green"

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
            st.subheader("매체별 CPL 표 (12M Cohort)")
            df_media = pd.DataFrame(by_media)
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
# Tab 4 — 시스템 상태
# ─────────────────────────────────────────────
with tabs[3]:
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
