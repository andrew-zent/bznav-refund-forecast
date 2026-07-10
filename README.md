# bznav Refund 수수료 결제 예측 & 마케팅 채널 분석

Pipedrive deals → Phase 2 v2 코호트 모델 → 5개월 예측 자동화 + UTM 채널 효율 / ROAS / 채널 정밀 분석.

**대시보드**: [https://andrew-zent.github.io/bznav-refund-forecast/dashboard.html](https://andrew-zent.github.io/bznav-refund-forecast/dashboard.html)

---

## 빠른 시작

```bash
cp .env.example .env
# .env에 PIPEDRIVE_API_TOKEN, PIPEDRIVE_DOMAIN 입력

pip install -r requirements.txt
source .env && bash run_local.sh
```

GitHub Actions Secrets (3개):

```bash
gh secret set PIPEDRIVE_API_TOKEN
gh secret set PIPEDRIVE_DOMAIN
gh secret set SLACK_WEBHOOK_URL   # optional
```

매주 월요일 09:00 KST 자동 실행.

---

## 프로젝트 구조

```
bznav-refund-forecast/
├── .github/workflows/
│   └── weekly_update.yml         # 자동 실행 + GitHub Pages 배포
│
├── src/
│   ├── config.py                 # 필드 매핑, 모델 설정, 시즌 보정
│   ├── extract_pipedrive.py      # Pipedrive API → SQLite history archive
│   ├── model.py                  # Phase 2 v2 코호트 예측 모델
│   ├── generate_dashboard.py     # forecast.json → dashboard.html
│   ├── notify_slack.py           # Slack 알림
│   │
│   ├── utm_channel_analysis.py   # [분석1] UTM 채널 효율 (yield/건수/금액, 3개 윈도우)
│   ├── roas_from_marketing_sheet.py  # [분석2] ROAS — 마케팅팀 Daily Report 기반
│   ├── channel_deep_analysis.py  # [분석3] 신규/리마인드, 캠페인 A/B, 멀티터치
│   │
│   └── agents/
│       └── filing_rate_monitor.py  # [신고율 관리] 신청완료→신고완료 전환/취소율 + 인용확인 현황 (일일)
│
├── data/
│   └── 비즈넵환급_Daily Report_*.xlsx  # 마케팅팀 Daily Report (수동 갱신)
│
├── output/
│   ├── forecast.json             # 예측 결과
│   ├── dashboard.html            # GitHub Pages 대시보드
│   ├── utm_channel_analysis.json # UTM 채널 효율 결과
│   ├── roas_marketing.json       # ROAS 분석 결과
│   └── channel_deep_analysis.json # 채널 정밀 분석 결과
│
└── docs/
    ├── utm_channel_analysis.md   # UTM 채널 분석 리포트
    ├── roas_marketing.md         # ROAS 분석 리포트
    └── channel_deep_analysis.md  # 채널 정밀 분석 리포트
```

---

## 파이프라인

```
매주 월요일 09:00 KST
  ├── 1. Pipedrive 증분 추출 → SQLite history archive
  ├── 2. Phase 2 v2 모델 재학습 → 5개월 예측 + 백테스트
  ├── 3. 대시보드 HTML 갱신
  ├── 4. Slack 알림
  └── 5. Git commit + push → GitHub Pages 배포
```

마케팅 분석은 수동 실행 (마케팅팀 Excel 업데이트 후):

```bash
python src/utm_channel_analysis.py       # UTM 채널 효율
python src/roas_from_marketing_sheet.py  # ROAS (data/에 Excel 필요)
python src/channel_deep_analysis.py      # 신규/리마인드, A/B, 멀티터치
```

---

## 신고율 관리 (일일)

매일 09:00 KST 자동 실행 (`.github/workflows/daily_filing_rate.yml`, 개인 파이프라인만 대상):

- **일일 흐름**: 어제 신청완료 → 신고완료 전환 건수, 취소 건수
- **백로그 에이징**: 신청완료 후 미해결 건을 0-7/8-14/15-30/31-60/60일+ 구간별 집계, 보류 사유 브레이크다운
- **성숙 코호트 전환율**: 신청 후 45~75일 지난 건 기준 신고완료율/취소율/취소 사유 top
- **인용확인 현황**: 완료 건수(당일/누적), 기한(6주) 경과 미확인 건, 상태별(세무서 비협조 등) 브레이크다운

결과는 Slack으로 매일 발송되고 `output/filing_rate_report.json` + `output/filing_rate_snapshots/YYYY-MM-DD.json`에 저장된다. 임계값(`src/agents/filing_rate_monitor.py` 상단)은 실측 데이터 없이 잡은 초기값이므로 운영하면서 조정 필요.

수동 실행:

```bash
python src/dump_fields.py             # 사유/상태 필드 id→label 매핑 갱신 (선택, output/field_catalog.json)
python src/agents/filing_rate_monitor.py
```

---

## 예측 모델

- **Phase 2 v2**: 4단계 코호트 분산 (신청→신고→결정→결제)
- **MAPE**: 5.9% (12개월 백테스트)
- **시즌 보정**: 종소세(6~7월 -10%), 1Q(3월 +20%), 연말(12월 +15%)
- **완성 코호트**: 2024-11 ~ 2025-10 (결제 lag 평균 73일, 성숙 완료)

---

## 마케팅 채널 분석 요약 (2026-04-18 기준)

| 분석 | 스크립트 | 리포트 | 핵심 발견 |
|---|---|---|---|
| UTM 채널 효율 | `utm_channel_analysis.py` | [docs](docs/utm_channel_analysis.md) | toss.join yield 14.84%, google_pmax 9.70% |
| ROAS | `roas_from_marketing_sheet.py` | [docs](docs/roas_marketing.md) | 12M ROAS 2.83, 🔴 정리 대상 7개 매체 |
| 채널 정밀 | `channel_deep_analysis.py` | [docs](docs/channel_deep_analysis.md) | 멀티터치 yield +2.8%p 프리미엄, sena_feedbanner 15.18% 최고 |

**즉시 액션 Top 3**

1. `google_pmax` 캠페인 중단 — yield 9.7%, ROAS 0.47 (적자)
2. `shinhancard_franchise` 타깃 필터 재설정 — enterprise(14.6%) vs franchise(9.3%), -5.4%p
3. `toss.join` 광고비 삭감 전 멀티터치 경로 분석 선행 — toss→alrimtalk 712건, yield 15.78%
