"""
Slack Webhook으로 예측 결과 요약 발송 (#only-결제 채널).

환경변수:
  SLACK_WEBHOOK_URL: Slack Incoming Webhook URL
"""
import os
import json
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = ROOT / "output"

DASHBOARD_URL = "https://andrew-zent.github.io/bznav-refund-forecast/"


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
    total_claims = data["total_claims"]
    total_corp = data.get("total_corp_claims", 0)

    lines = [f"*환급 수수료 결제 예측 — 주간 업데이트 ({gen[:10]})*"]
    lines.append("")
    lines.append(f"개인 MAPE: *{mape}%* | 데이터: {total_claims:,}건 (개인) / {total_corp:,}건 (법인)")
    lines.append("")

    month_names = {"01": "1월", "02": "2월", "03": "3월", "04": "4월",
                   "05": "5월", "06": "6월", "07": "7월", "08": "8월",
                   "09": "9월", "10": "10월", "11": "11월", "12": "12월"}

    for f in fc:
        m = month_names.get(f["month"][-2:], f["month"])
        ind = f["individual"]["adjusted"]
        corp = f.get("corporate", {}).get("total", 0)
        grand = f["grand_total"]
        lines.append(f"> *{m}*  개인 {ind:.2f}억 + 법인 {corp:.2f}억 = *{grand:.2f}억*")

    lines.append("")
    lines.append(f"📊 대시보드: {DASHBOARD_URL}")

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
