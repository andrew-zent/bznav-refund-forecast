"""
Severity 기반 Slack 알림 라우터.

모든 Agent가 공유하는 알림 인프라.
severity: info / warn / critical
"""
import json
import os
import urllib.request
from datetime import datetime, timezone


SEVERITY_EMOJI = {"info": "ℹ️", "warn": "⚠️", "critical": "🚨"}


def send_slack(message: str, severity: str = "info", webhook_url=None):
    """Slack Webhook으로 메시지 전송."""
    url = webhook_url or os.environ.get("SLACK_WEBHOOK_URL")
    if not url:
        print(f"[SLACK:{severity}] {message}")
        return False

    emoji = SEVERITY_EMOJI.get(severity, "ℹ️")
    payload = {"text": f"{emoji} *[{severity.upper()}]* {message}"}
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status == 200
    except Exception as e:
        print(f"Slack send failed: {e}")
        return False


def format_report(title: str, items: list[dict], severity: str = "info") -> str:
    """검증 결과를 Slack 메시지로 포맷팅."""
    emoji = SEVERITY_EMOJI.get(severity, "ℹ️")
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [f"{emoji} *{title}* ({ts})"]
    for item in items:
        status = "✅" if item.get("ok") else "❌"
        lines.append(f"  {status} {item['check']}: {item['detail']}")
    return "\n".join(lines)
