"""
Confluence 주간 리포트 자동 게시.

입력:
  output/forecast.json
  output/verification_report.json

동작:
  1. verification_report + forecast 읽기
  2. Markdown 리포트 생성
  3. Confluence 페이지 업데이트 (없으면 신규 생성)

환경변수 (필수):
  CONFLUENCE_EMAIL         — Atlassian 계정 이메일
  CONFLUENCE_API_TOKEN     — Atlassian API 토큰
                             https://id.atlassian.com/manage-profile/security/api-tokens

환경변수 (선택):
  CONFLUENCE_BASE_URL      — 기본값: https://zenterprise.atlassian.net
  CONFLUENCE_REPORT_PAGE_ID — 업데이트할 페이지 ID (없으면 신규 생성)
  CONFLUENCE_SPACE_ID      — 신규 생성 시 space ID (기본: 470614018)
  CONFLUENCE_PARENT_ID     — 신규 생성 시 부모 페이지 ID (기본: 5000527914)
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import urllib.request
import urllib.error
import base64

ROOT = Path(__file__).resolve().parent.parent.parent
FORECAST_PATH = ROOT / "output" / "forecast.json"
VERIFY_PATH   = ROOT / "output" / "verification_report.json"

CONFLUENCE_BASE_URL = os.getenv("CONFLUENCE_BASE_URL", "https://zenterprise.atlassian.net")
CONFLUENCE_SPACE_ID = os.getenv("CONFLUENCE_SPACE_ID", "470614018")
CONFLUENCE_PARENT_ID = os.getenv("CONFLUENCE_PARENT_ID", "5000527914")
REPORT_TITLE = "📊 주간 결제 예측 리포트 (자동 갱신)"


# ── Confluence REST API 클라이언트 ─────────────────────────────────────────
class ConfluenceClient:
    def __init__(self, base_url: str, email: str, token: str):
        self.base_url = base_url.rstrip("/")
        credentials = base64.b64encode(f"{email}:{token}".encode()).decode()
        self.auth_header = f"Basic {credentials}"

    def _request(self, method: str, path: str, body: dict | None = None) -> dict:
        url = f"{self.base_url}/wiki/api/v2{path}"
        data = json.dumps(body).encode() if body else None
        req = urllib.request.Request(
            url, data=data, method=method,
            headers={
                "Authorization": self.auth_header,
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(req) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as e:
            err_body = e.read().decode()
            raise RuntimeError(f"Confluence API {method} {path} → {e.code}: {err_body[:300]}")

    def get_page(self, page_id: str) -> dict:
        return self._request("GET", f"/pages/{page_id}?body-format=storage")

    def update_page(self, page_id: str, title: str, body_md: str, current_version: int) -> dict:
        return self._request("PUT", f"/pages/{page_id}", {
            "id": page_id,
            "status": "current",
            "title": title,
            "version": {"number": current_version + 1, "message": "자동 갱신"},
            "body": {"representation": "wiki", "value": body_md},
        })

    def create_page(self, space_id: str, parent_id: str, title: str, body_md: str) -> dict:
        return self._request("POST", "/pages", {
            "spaceId": space_id,
            "parentId": parent_id,
            "status": "current",
            "title": title,
            "body": {"representation": "wiki", "value": body_md},
        })

    def find_child_page(self, parent_id: str, title: str) -> str | None:
        result = self._request("GET", f"/pages/{parent_id}/children?limit=50")
        for item in result.get("results", []):
            if item.get("title") == title:
                return item["id"]
        return None


# ── 리포트 콘텐츠 생성 ──────────────────────────────────────────────────────
def _severity_badge(severity: str) -> str:
    return {"info": "✅ INFO", "warn": "⚠️ WARN", "critical": "🔴 CRITICAL"}.get(severity, severity)


def build_report(forecast: dict, verify: dict) -> str:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    severity = verify.get("severity", "?")
    passed = verify.get("passed", 0)
    total  = verify.get("total_checks", 0)
    gen_at = forecast.get("generated_at", "?")[:16]

    lines: list[str] = []

    # 헤더
    lines += [
        f"*자동 갱신: {now}* | 예측 생성: {gen_at}",
        "",
        f"== 검증 결과: {_severity_badge(severity)} ({passed}/{total} passed) ==",
        "",
        "|| 항목 || 결과 || 상세 ||",
    ]
    for r in verify.get("results", []):
        icon = "(/)" if r["ok"] else "(x)"
        lines.append(f"| {r['check']} | {icon} | {r.get('detail', '')} |")

    # 예측 테이블
    lines += ["", "== 향후 5개월 예측 ==", "", "|| 월 || 합계 (억) || 개인 정기 || 개인 추심 || 법인 ||"]
    for f in forecast.get("forecast", []):
        month   = f.get("month", "?")
        total_f = f.get("grand_total", 0)
        ind     = f.get("individual", {})
        corp    = f.get("corporate", {})
        regular = ind.get("regular", 0)
        collect = ind.get("collection", 0)
        corp_t  = corp.get("total", 0)
        lines.append(f"| {month} | *{total_f:.1f}* | {regular:.1f} | {collect:.1f} | {corp_t:.2f} |")

    # 모델 지표
    mape = forecast.get("mape", None)
    if mape is not None:
        lines += ["", f"MAPE (12M 백테스트): *{mape:.1f}%*"]

    # 채권풀
    pool = forecast.get("collection_pool", {})
    if pool:
        balance = pool.get("balance", 0)
        delta   = pool.get("monthly_delta", 0)
        rate    = pool.get("utilization_rate", 0)
        lines += [
            "",
            "== 채권풀 건강도 ==",
            f"* 잔액: {balance:.1f}억",
            f"* 순변동: {delta:+.1f}억/월",
            f"* 월 회수율: {rate:.3f}%",
        ]

    # 링크
    lines += [
        "",
        "----",
        "* [대시보드|https://andrew-zent.github.io/bznav-refund-forecast/]",
        "* [GitHub|https://github.com/andrew-zent/bznav-refund-forecast]",
        "* [제안서|https://zenterprise.atlassian.net/wiki/spaces/~892994477/pages/5250416646]",
    ]

    return "\n".join(lines)


# ── 메인 ───────────────────────────────────────────────────────────────────
def run() -> dict:
    email = os.getenv("CONFLUENCE_EMAIL")
    token = os.getenv("CONFLUENCE_API_TOKEN")
    if not email or not token:
        print("SKIP: CONFLUENCE_EMAIL / CONFLUENCE_API_TOKEN 미설정")
        return {"skipped": True}

    if not FORECAST_PATH.exists() or not VERIFY_PATH.exists():
        print(f"SKIP: forecast.json 또는 verification_report.json 없음")
        return {"skipped": True}

    forecast = json.loads(FORECAST_PATH.read_text())
    verify   = json.loads(VERIFY_PATH.read_text())

    body_wiki = build_report(forecast, verify)
    client = ConfluenceClient(CONFLUENCE_BASE_URL, email, token)

    # 페이지 ID: 환경변수 우선 → 제목으로 검색 → 신규 생성
    page_id = os.getenv("CONFLUENCE_REPORT_PAGE_ID") or client.find_child_page(CONFLUENCE_PARENT_ID, REPORT_TITLE)

    if page_id:
        page = client.get_page(page_id)
        version = page["version"]["number"]
        client.update_page(page_id, REPORT_TITLE, body_wiki, version)
        action = f"updated (v{version+1})"
    else:
        result = client.create_page(CONFLUENCE_SPACE_ID, CONFLUENCE_PARENT_ID, REPORT_TITLE, body_wiki)
        page_id = result["id"]
        action = "created"

    url = f"{CONFLUENCE_BASE_URL}/wiki/spaces/~892994477/pages/{page_id}"
    print(f"Confluence {action}: {url}")
    return {"page_id": page_id, "action": action, "url": url}


if __name__ == "__main__":
    result = run()
    if result.get("skipped"):
        sys.exit(0)
    print(f"→ {result.get('url')}")
