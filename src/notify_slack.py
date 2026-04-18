"""Slack Webhook으로 주간 예측 + 진단 KPI 요약 발송.

입력:
- output/forecast.json (예측/backtest/cohort)
- output/timeline.json (WoW 비교용)
- output/snapshots/{date}.json (Top N 사유)

환경변수:
- SLACK_WEBHOOK_URL
"""
import os
import json
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = ROOT / "output"


def _load(path):
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except Exception:
        return None


def _block(text):
    return {"type": "section", "text": {"type": "mrkdwn", "text": text}}


def _divider():
    return {"type": "divider"}


def _format_forecast(forecast_data):
    fc = forecast_data["forecast"]
    mape = forecast_data.get("mape", "?")
    has_corp = bool(fc and "individual" in fc[0])

    lines = ["```"]
    if has_corp:
        lines.append(f"{'월':>8} | {'개인':>6} | {'법인':>6} | {'통합':>6}")
        lines.append("-" * 38)
        for f in fc[:5]:
            lines.append(
                f"{f['month']:>8} | {f['individual']['adjusted']:>5.1f}억 | "
                f"{f['corporate']['total']:>5.1f}억 | {f['grand_total']:>5.1f}억"
            )
    else:
        lines.append(f"{'월':>8} | {'권장':>6}")
        lines.append("-" * 22)
        for f in fc[:5]:
            lines.append(f"{f['month']:>8} | {f['adjusted']:>5.1f}억")
    lines.append("```")
    return "\n".join(lines), mape


def _wow_delta(timeline):
    """최신 2주차 diff 반환 (없으면 None)."""
    if not timeline or len(timeline.get("entries", [])) < 2:
        return None
    a = timeline["entries"][-2]
    b = timeline["entries"][-1]
    return {
        "date_prev": a.get("date"),
        "date_curr": b.get("date"),
        "conv_unf_delta": round(
            b.get("conversion_unfiltered_pct", 0) - a.get("conversion_unfiltered_pct", 0), 2),
        "conv_filt_delta": round(
            b.get("conversion_filtered_pct", 0) - a.get("conversion_filtered_pct", 0), 2),
        "a_share_delta": round(
            b.get("pipeline_shares", {}).get("A(지수)", 0)
            - a.get("pipeline_shares", {}).get("A(지수)", 0), 2),
        "pool_delta": round(b.get("pool_balance", 0) - a.get("pool_balance", 0), 2),
    }


def _emoji_trend(delta, inverse=False):
    """delta → 이모지 ('inverse=True'면 증가가 나쁜 지표)."""
    if abs(delta) < 0.01:
        return "→"
    if inverse:
        return "🔴" if delta > 0 else "🟢"
    return "🟢" if delta > 0 else "🔴"


def build_payload(forecast_data, timeline, latest_snapshot):
    """Slack Block Kit payload 구성."""
    gen = forecast_data.get("generated_at", "")[:10]
    fc_text, mape = _format_forecast(forecast_data)
    wow = _wow_delta(timeline)

    snap = latest_snapshot or {}
    conv = snap.get("conversion", {}).get("unfiltered", {})
    pool = snap.get("collection_pool", {})
    pipe_dist = snap.get("pipeline_distribution", {})
    a_share = 0
    b_share = 0
    total_apply = sum(v.get("apply", 0) for v in pipe_dist.values())
    if total_apply > 0:
        a_share = round(pipe_dist.get("A(지수)", {}).get("apply", 0) / total_apply * 100, 1)
        b_share = round(pipe_dist.get("B(젠트)-환급", {}).get("apply", 0) / total_apply * 100, 1)

    blocks = [
        {"type": "header",
         "text": {"type": "plain_text", "text": f"📊 비즈넵 주간 예측 & 진단 ({gen})"}},
    ]

    # 1. Forecast
    blocks.append(_block(f"*🎯 Forecast* (MAPE {mape}%)\n{fc_text}"))

    # 2. 진단 KPI + WoW
    kpi_lines = []
    if conv:
        conv_pct = conv.get("conversion_pct", 0)
        wow_tag = ""
        if wow:
            d = wow.get("conv_unf_delta", 0)
            if abs(d) > 0.001:
                wow_tag = f" ({_emoji_trend(d)} {d:+.2f}%p vs 전주)"
        kpi_lines.append(f"• Unfiltered 전환율: *{conv_pct:.2f}%*{wow_tag}")
    if a_share > 0:
        wow_tag = ""
        if wow:
            d = wow.get("a_share_delta", 0)
            if abs(d) > 0.001:
                wow_tag = f" ({_emoji_trend(d, inverse=True)} {d:+.2f}%p)"
        kpi_lines.append(f"• A(지수) 비중: *{a_share}%*{wow_tag} (저효율)")
    if b_share > 0:
        kpi_lines.append(f"• B(환급) 비중: *{b_share}%* (주 매출처)")
    if pool:
        bal = pool.get("balance", 0)
        harvest = pool.get("recent_3mo_avg_paid", 0)
        wow_tag = ""
        if wow:
            d = wow.get("pool_delta", 0)
            if abs(d) > 0.1:
                wow_tag = f" ({_emoji_trend(d)} {d:+.1f}억)"
        kpi_lines.append(f"• 추심 풀: *{bal:.1f}억*{wow_tag}, 월 회수 *{harvest:.2f}억*")

    if kpi_lines:
        blocks.append(_divider())
        blocks.append(_block("*📈 진단 KPI*\n" + "\n".join(kpi_lines)))

    # 3. Top 사유
    top_lost = snap.get("top_lost_reasons", {}).get("top", [])[:3]
    top_cancel = snap.get("top_cancel_reasons", {}).get("top", [])[:3]
    if top_lost or top_cancel:
        blocks.append(_divider())
        reason_lines = []
        if top_lost:
            reason_lines.append("*🚫 실패 Top 3*")
            for r in top_lost:
                reason_lines.append(
                    f"• {r['label']}: {r['apply_amount']:.0f}억 ({r['share_pct']:.1f}%)"
                )
        if top_cancel:
            reason_lines.append("\n*❌ 취소 Top 3*")
            for r in top_cancel:
                reason_lines.append(
                    f"• {r['label']}: {r['apply_amount']:.0f}억 ({r['share_pct']:.1f}%)"
                )
        blocks.append(_block("\n".join(reason_lines)))

    # 4. 링크
    blocks.append(_divider())
    blocks.append(_block(
        "🔗 <https://andrew-zent.github.io/bznav-refund-forecast/|대시보드> · "
        "<https://github.com/andrew-zent/bznav-refund-forecast/blob/main/docs/marketing_forecast_proposal.md|제안서>"
    ))

    return {
        "text": f"비즈넵 주간 예측 & 진단 ({gen}) — MAPE {mape}%",  # fallback
        "blocks": blocks,
    }


def main():
    webhook = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook:
        print("SLACK_WEBHOOK_URL not set. Skipping notification.")
        return

    forecast_data = _load(OUTPUT_DIR / "forecast.json")
    if not forecast_data:
        print("No forecast.json found.")
        return

    timeline = _load(OUTPUT_DIR / "timeline.json")

    gen = forecast_data.get("generated_at", "")[:10]
    latest_snapshot = _load(OUTPUT_DIR / "snapshots" / f"{gen}.json")
    if not latest_snapshot:
        snap_dir = OUTPUT_DIR / "snapshots"
        if snap_dir.exists():
            files = sorted(snap_dir.glob("*.json"))
            if files:
                latest_snapshot = _load(files[-1])

    payload = build_payload(forecast_data, timeline, latest_snapshot)

    req = urllib.request.Request(
        webhook,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req) as resp:
            print(f"Slack notification sent ({resp.status})")
    except Exception as e:
        print(f"Slack notification failed: {e}")


if __name__ == "__main__":
    main()
