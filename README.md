# bznav Refund 수수료 결제 예측

Pipedrive deals 데이터 → Phase 2 v2 코호트 분산 모델 → 5개월 미래 예측 자동화.

**대시보드**: [https://andrew-zent.github.io/bznav-refund-forecast/dashboard.html](https://andrew-zent.github.io/bznav-refund-forecast/dashboard.html)

## 빠른 시작 (5분)

### 1. Pipedrive API 키 발급

1. Pipedrive → 설정 → Personal Preferences → API
2. Personal API Token 복사

### 2. 로컬 테스트

```bash
cp .env.example .env
# .env 파일에 실제 값 입력

pip install -r requirements.txt
source .env && bash run_local.sh
```

### 3. GitHub Actions 설정

```bash
# Secrets 등록 (3개)
gh secret set PIPEDRIVE_API_TOKEN
gh secret set PIPEDRIVE_DOMAIN
gh secret set SLACK_WEBHOOK_URL  # optional
```

매주 월요일 09:00 KST 자동 실행. 수동 실행: Actions → Run workflow.

## 프로젝트 구조

```
bznav-refund-forecast/
├── .github/workflows/
│   └── weekly_update.yml    # 매주 자동 실행 + GitHub Pages 배포
├── src/
│   ├── config.py            # 필드 매핑, 모델 설정, 시즌 보정
│   ├── extract_pipedrive.py # Pipedrive API → data/deals_raw.json
│   ├── model.py             # Phase 2 v2 모델 (학습/예측/백테스트)
│   ├── generate_dashboard.py# forecast.json → dashboard.html
│   └── notify_slack.py      # Slack 알림
├── data/                    # API 추출 데이터 (gitignore)
├── output/
│   ├── forecast.json        # 예측 결과 (기계 읽기용)
│   └── dashboard.html       # 대시보드 (GitHub Pages로 공개)
├── .env.example
├── requirements.txt
└── run_local.sh
```

## 파이프라인 흐름

```
매주 월요일 09:00 KST
  ├── 1. Pipedrive API 증분 추출 (~1분)
  ├── 2. Phase 2 v2 모델 재학습 (~10초)
  │     ├── 코호트 분산 비율 갱신 (rolling 6m)
  │     ├── 향후 5개월 예측
  │     └── 12개월 백테스트 MAPE 산출
  ├── 3. 대시보드 HTML 갱신
  ├── 4. Slack 알림
  ├── 5. Git commit + push (이력 보존)
  └── 6. GitHub Pages 자동 배포
```

## 모델 요약

- **Phase 2 v2**: 4단계 코호트 분산 (신청→신고→결정→결제)
- **MAPE**: 5.9% (12개월 백테스트)
- **시즌 보정**: 종소세(6~7월 -10%), 1Q(3월 +20%), 연말(12월 +15%)

## 설정 변경

`src/config.py`에서:
- `ROLLING_WINDOW`: 분산 비율 학습 윈도우 (기본 6개월)
- `SEASON_ADJUSTMENT`: 월별 시즌 보정 비율
- `FIELD_MAP_BY_NAME`: Pipedrive 컬럼 ↔ 모델 변수 매핑
