# 비즈넵 결제 예측 — Agent 시스템 로드맵

## 목표

현재 파이프라인(추출→모델→대시보드→알림)을 **자율 감시·검증·복구 가능한 Agent 시스템**으로 발전.
사람이 주간 결과를 확인하지 않아도 이상이 감지되면 알아서 원인 분석 + 알림.

## 현재 상태

```
[GitHub Actions: 월요일 09:00 KST]
  extract_pipedrive.py → extract_corp.py → model.py → dashboard → slack → push
```

- 에러 핸들링: API 429 재시도, 파일 미존재 fallback 정도
- 모니터링: 없음 (MAPE는 계산하지만 알림 없음)
- 검증: 백테스트 MAPE 계산만 (임계값 초과 알림 없음)

## 목표 상태

```
[Orchestrator Agent]
  ├─ extract → [Watcher] 데이터 품질 검증
  │                  ├─ pass → model 실행
  │                  └─ fail → 원인 분석 + Slack 알림 + 중단/재시도
  ├─ model   → [Verifier] 예측 결과 검증
  │                  ├─ pass → dashboard + slack
  │                  └─ warn → 상세 리포트 + 경고 알림
  └─ 상태 관리 + 로그 + 스케줄링
```

---

## Phase 1: Watcher Agent (데이터 품질 감시)

**마일스톤**: 데이터 이상 시 자동 감지 + Slack 알림

### 토픽

1. **데이터 완결성 검증** — 추출 후 deals 건수, 필수 필드 null율, 금액 이상치 탐지
2. **분포 이상 감지** — 월별 신청/결정/결제 금액의 Z-score 기반 이상치 (과거 12개월 대비)
3. **Pipedrive API 헬스 체크** — 응답 지연, 에러율, 변경된 필드 스키마 감지
4. **알림 라우터** — severity(info/warn/critical) 기반 Slack 채널 분기

### 의존성

- 없음 (독립적으로 시작 가능)

### 산출물

- `src/agents/watcher.py` — 데이터 검증 함수 모음
- `src/agents/alerts.py` — severity 기반 Slack 알림 라우터

---

## Phase 2: Verifier Agent (예측 결과 검증)

**마일스톤**: 매주 예측 결과를 자동 검증하고 이상 시 상세 리포트 + 알림

### 토픽

1. **Forecast-vs-Actual 자동 비교** — 직전 월 예측과 실제값 비교, 오차 임계값 초과 시 알림
2. **모델 드리프트 탐지** — MAPE 추이 모니터링, 연속 3개월 오차 방향 편향 감지
3. **분산 비율 안정성** — a2f/f2d/d2p 분산이 과거 대비 급변하면 경고
4. **채권풀 건강도** — 순변동 추이, 회수율 변화, 에이징 분포 이동 모니터링
5. **검증 리포트 생성** — 검증 결과를 구조화된 JSON + Slack 요약으로 출력

### 의존성

- Phase 1 alerts.py (알림 라우터)

### 산출물

- `src/agents/verifier.py` — 예측 검증 함수 모음
- `output/verification_report.json` — 주간 검증 리포트

---

## Phase 3: Orchestrator (오케스트레이션)

**마일스톤**: 전체 파이프라인을 Agent가 자율 관리, 이상 시 재시도/중단/에스컬레이션

### 토픽

1. **파이프라인 스테이트 머신** — extract→validate→model→verify→notify 단계별 상태 관리
2. **에러 복구 전략** — 단계별 retry/skip/abort 정책 (예: API 장애 시 재시도, 데이터 이상 시 이전 데이터로 fallback)
3. **실행 로그** — 각 단계 소요시간, 성공/실패, 데이터 건수 등 구조화 로그
4. **GitHub Actions 통합** — 기존 weekly_update.yml을 orchestrator 호출로 교체
5. **수동 트리거 + CLI** — `python -m agents.orchestrator run` / `--dry-run` / `--force-rerun`

### 의존성

- Phase 1 (Watcher), Phase 2 (Verifier)

### 산출물

- `src/agents/orchestrator.py` — 메인 오케스트레이터
- `src/agents/state.py` — 파이프라인 상태 관리
- `.github/workflows/weekly_update.yml` — 오케스트레이터 호출로 교체

---

## Phase 4 (향후): 고도화

- **자동 하이퍼파라미터 튜닝** — ROLLING_WINDOW, 시즌 보정 계수 자동 최적화
- **멀티 모델 앙상블** — 코호트 분산 + ARIMA + Prophet 비교 선택
- **Confluence 자동 리포트** — 주간 검증 결과를 Confluence에 자동 게시
- **대시보드 실시간화** — GitHub Pages → Streamlit/Gradio 전환

---

## 원칙

- **점진적 통합**: Phase 1~2는 기존 파이프라인에 병렬 추가 (기존 동작 깨뜨리지 않음)
- **단순함 우선**: 외부 의존성 최소화 (numpy만, 별도 프레임워크 없음)
- **Slack 중심 알림**: 모든 Agent의 출력은 Slack으로 수렴
- **테스트 가능**: 각 Agent는 독립 실행 가능 (`python src/agents/watcher.py`)
