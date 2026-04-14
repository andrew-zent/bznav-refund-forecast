"""
Slack Webhook으로 예측 결과 요약 발송.

환경변수:
  SLACK_WEBHOOK_URL: Slack Incoming Webhook URL
"""
import os
import json
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = ROOT / "output"


def main():
    webhook = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook:
        print("SLACK_WEBHOOK_URL not set. Skipping notification.")
        return

    forecast_path = OUTPUT_DIR / "forecast.json"
    if not forecast_path.exists():
        print("No forecast.json found.")
        return

    data = json.loads(forecast_path.read_text())
    fc = data["forecast"]
    mape = data["mape"]
    gen = data["generated_at"]

    lines = [f"*비즈넵 결제 예측 ({gen[:10]})*"]
    lines.append(f"MAPE: {mape}%  |  데이터: {data['total_claims']:,}건")
    lines.append("```")
    lines.append(f"{'월':>8} | {'모델':>6} | {'시즌보정':>7} | {'권장':>6}")
    lines.append("-" * 40)
    for f in fc:
        adj_tag = f" ({f['season_adj']:+.0%})" if f["season_adj"] != 0 else ""
        lines.append(f"{f['month']:>8} | {f['total']:>5.1f}억 | {f['adjusted']:>6.1f}억{adj_tag}")
    lines.append("```")

    payload = {"text": "\n".join(lines)}
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
