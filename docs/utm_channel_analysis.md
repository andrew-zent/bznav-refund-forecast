# UTM 채널 효율 분석 — 전체기간 / 최근 3개월 / 최근 1개월

- 생성: 2026-04-18 기준 (history.sqlite as_of_date)
- 분석 코드: [src/utm_channel_analysis.py](../src/utm_channel_analysis.py)
- Raw 결과: [output/utm_channel_analysis.json](../output/utm_channel_analysis.json), [output/utm_channel_analysis.csv](../output/utm_channel_analysis.csv)
- 분모: A(지수) pipeline 제외 (1차 심사 탈락 전용 → 분모 희석 방지)

---

## 0. 읽는 법 — 지표 정의와 신뢰 구간

**% 단위 기준 — 모든 비율은 두 종류 중 하나입니다**
- **건수 기준 (count-based)**: 분자/분모 모두 deals 건수. 비율 = 해당 건수 ÷ 전체 건수
- **금액 기준 (amount-based)**: 분자/분모 모두 환급금(원). 비율 = 해당 금액 ÷ 전체 금액

| 지표 | 정의 (분자 ÷ 분모) | 단위 기준 |
|---|---|---|
| `deals` | 신청 건수 (절대값) | — |
| `apply_oku` | 신청 환급금 합 (억원, 절대값) | — |
| **건수%** (`deals_share_pct`) | 채널 deals ÷ 전체 deals | **건수** |
| **금액%** (`apply_share_pct`) | 채널 신청금 ÷ 전체 신청금 | **금액** |
| `filing_rate_pct` (신고%) | 신고완료 deals ÷ 전체 deals | **건수** |
| `payment_rate_pct` (결제%) | 결제완료 deals ÷ 전체 deals | **건수** |
| `won_rate_pct` (won%) | won deals ÷ 전체 deals | **건수** |
| **`yield_pct`** | **회수금액 ÷ 신청금액** | **금액** |
| `avg_apply_manwon` | 신청금 합 ÷ deals (만원) | — |
| `avg_lag_days` | 신청→결제 평균 일수 (결제완료 건만) | — |

**핵심 차이**: payment_rate_pct(건수)와 yield_pct(금액)는 다른 정보다.
- 결제%가 높아도 yield가 낮으면 → 작은 건만 결제됨 (실패 건이 큰 케이스)
- 결제%가 낮아도 yield가 높으면 → 결제된 건들의 회수율이 평균 이상

**기간 선택 기준**
- `12M_cohort` (2024-11~2025-10): payment lag ~73일 성숙 완료. **모든 yield/payment 지표 신뢰 가능.**
- `3M_recent` (~90일): 신청량/신고전환은 의미 있음. **payment_rate/yield는 미성숙(보통 1/3 수준)이므로 절대값보다 채널 간 상대 비교에 사용.**
- `1M_recent` (~30일): **신청량과 신고전환만 신뢰.** payment ~1% 수준은 미성숙 잔류.

---

## 1. 채널 카테고리별 효율 (12M 완성 코호트, A 제외)

`utm_medium` 기준 — 어디에 돈/시간을 써야 하는지의 1차 판단.

| medium | deals | 건수% | 신청억 | 금액% | 신고%(건수) | 결제%(건수) | **yield%(금액)** | 평균신청(만원) | lag(일) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| partner | 18,246 | 35.8 | 493.6 | 31.0 | 91.4 | 72.1 | **14.4** | 270 | 80 |
| crm (알림톡) | 12,237 | 24.0 | 354.9 | 22.3 | 89.5 | 73.2 | **14.0** | 290 | 85 |
| paid_msg (카드사 LMS) | 6,123 | 12.0 | 199.8 | 12.6 | 85.4 | 70.5 | 11.5 | 326 | 78 |
| paid_display | 6,206 | 12.2 | 173.6 | 10.9 | 87.3 | 68.7 | 11.8 | 280 | 86 |
| (UTM 없음) | 3,161 | 6.2 | 122.7 | 7.7 | 85.4 | 69.3 | 12.3 | 388 | 82 |
| sms | 1,455 | 2.9 | 107.2 | 6.7 | 84.1 | 67.1 | **10.8** | **737** | 87 |
| paid_search (naver/google) | 1,084 | 2.1 | 46.9 | 2.9 | 80.8 | 67.7 | 11.7 | 433 | 81 |
| organic | 568 | 1.1 | 26.8 | 1.7 | 82.7 | 68.3 | 11.0 | 473 | 81 |
| affillates | 722 | 1.4 | 25.2 | 1.6 | 87.8 | 71.7 | 11.8 | 349 | 84 |
| ig (페이스북/인스타) | 731 | 1.4 | 22.1 | 1.4 | 88.0 | 73.1 | 10.5 | 302 | 84 |

**해석**
- 🟢 **partner / crm 두 카테고리가 매출 풀의 53%(금액 기준)를 책임지고 yield도 가장 높다 (14%, 1원 신청당 14원 회수).** 다만 crm은 리마인드 성격(자체 신규 유입은 약함) — 신규 채널 평가에서 분리 필요.
- 🟡 **paid_search yield 11.7%(금액)** — 신청량은 작지만 안정적. 확대 시 효율 유지 여부 확인 필요.
- 🟡 **sms는 deal size가 가장 크지만(평균 737만원) yield 10.8%로 평균 이하.** 큰 건이 결제까지 가지 못함 — 고객 자격 사전 필터 부재 가능성.
- 🔴 **email/youtuber yield <8%** — 소량이라 통계적 유의성 낮지만 투입 시 손실 가능.

---

## 2. UTM 소스별 — 12M 완성 코호트 Top 15 (A 제외)

| utm_source | deals | apply억 | 신고%(건수) | 결제%(건수) | **yield%(금액)** | 평균(만원) | lag |
|---|---:|---:|---:|---:|---:|---:|---:|
| **toss.join** | 16,338 | **429.9** | 91.9 | 72.3 | **14.84** | 263 | 80 |
| **alrimtalk** (CRM) | 11,576 | 332.3 | 89.4 | 73.2 | **14.24** | 287 | 85 |
| sms | 1,455 | 107.2 | 84.1 | 67.1 | 10.76 | 737 | 87 |
| referer | 1,857 | 82.5 | 84.9 | 68.9 | 12.07 | 444 | 82 |
| kbcardlms | 1,805 | 72.0 | 83.5 | 68.8 | 12.12 | 399 | 78 |
| **kakaobank** | 2,681 | 64.2 | 91.8 | 74.9 | **14.05** | 240 | 83 |
| google.adwords | 1,558 | 48.7 | 80.7 | 62.6 | **10.13** | 312 | 87 |
| naver.searchad | 1,003 | 44.2 | 81.9 | 68.5 | 11.66 | 440 | 81 |
| hanacardlms | 1,222 | 41.1 | 84.7 | 68.3 | 11.30 | 336 | 84 |
| **kakaochannel** | 1,230 | 37.4 | 87.5 | 70.7 | **13.14** | 304 | 82 |
| **shinhancardlms** | 1,470 | 33.5 | 91.2 | 80.1 | 12.43 | 228 | **72** |
| partner | 873 | 32.1 | 86.5 | 71.0 | 11.51 | 367 | 84 |
| **cashnote** | 858 | 29.9 | 83.6 | **59.7** | **9.73** | 348 | 84 |
| facebook.business | 854 | 26.0 | 87.6 | 72.1 | 10.98 | 304 | 85 |
| banksalad | 405 | 22.4 | 82.7 | 65.4 | 10.93 | 554 | 85 |

**핵심 발견**
- **toss.join 단일 채널이 전체 신청풀의 27%, 매출의 30%+ 기여.** 비중·yield 모두 1위.
- **kakaobank, kakaochannel, alrimtalk 등 카카오/메신저 계열 yield 13~14%로 평균 상회.**
- **cashnote yield 9.7%** — 신청량 대비 결제 전환이 낮음 (60%). 카드매출 채널 특성상 자영업자 자격 미충족 비율 추정.
- **shinhancardlms는 lag 72일로 최단** + 결제율 80% — 회수 속도 강점. CTV/회수효율 최우선 채널.
- **google.adwords yield 10.1%** — 같은 paid_search인 naver(11.7%) 대비 1.5%p 낮음. 키워드/소재 점검 가치 있음.

---

## 3. 채널 추세 — 12M → 3M → 1M 비중 변화

**모든 수치는 금액% (apply_oku 기준 신청 풀 점유율).** 같은 채널이 3구간에서 어떻게 움직이는지 추적.

| utm_source | 12M(금액%) | 3M(금액%) | 1M(금액%) | 추세 | 코멘트 |
|---|---:|---:|---:|:---:|---|
| toss.join | **27.0** | 8.7 | 7.6 | 🔻 급감 | 채널 피로도 명확. 12M 1위 → 1M 5위 |
| alrimtalk | 20.9 | 11.9 | 9.0 | 🔻 감소 | 자동 CRM 효과 둔화 |
| **alrimtalk_manual** | 4.9 | **15.4** | **20.9** | 🚀 폭증 | 수동 알림톡 캠페인이 전체 1위로 부상 |
| **kakao** (display) | 1.6 | 4.9 | **12.0** | 🚀 급증 | 신규 또는 확대 — 지난 1개월에 집중 |
| sms | 6.7 | 5.8 | 8.3 | 🟡 회복 | 등락 있으나 yield 낮음 유지 |
| kbcardlms | 4.5 | **6.5** | 8.0 | 🟢 증가 | 카드사 LMS 중 점유 확대 |
| naver.searchad | 2.8 | 4.6 | 4.1 | 🟢 안정 증가 | 효율 안정 + 점유 확대 |
| facebook.business | 1.6 | **5.4** | 3.6 | 🟢 증가 | IG 광고 효과 가시화 |
| google.adwords | 3.1 | 3.8 | 3.7 | 🟡 유지 | yield 낮으나 점유 안정 |
| shinhancardlms | 2.1 | 2.4 | **4.4** | 🟢 회복 | 1M 점유 회복 — lag 짧아 단기 회수 기여 |
| kakaochannel | 2.3 | 1.9 | 1.9 | 🟡 미세 감소 | 안정 |
| cashnote | 1.9 | 1.3 | 1.0 | 🔻 감소 | yield 낮음 + 비중 축소 — 자연 정리 중 |
| kakaobank | 4.0 | 3.1 | 1.4 | 🔻 감소 | 점유 절반 이하 — 점검 필요 |
| okpos | 1.7 | 4.7 | 2.4 | 🟡 변동 | 단기 캠페인 추정 |

---

## 4. 의사결정 매트릭스 — 어디에 마케팅을 더 해야 하는가

12M yield(금액%, 성숙)와 1M 점유율(금액%) 추세를 교차해 4분면화.

### 🟢 확대 권장 (yield 높음 + 점유 증가)
- **kakao (display)** — 12M yield 데이터 부족하지만 1M 금액 점유 12% 폭증. 단가/소재 확인 후 예산 재배분 검토.
- **shinhancardlms** — yield 12.4%, 결제 lag 72일 최단, 1M 점유 회복. **단기 매출 부스터로 최적.**
- **alrimtalk_manual** — 1M 점유 1위. 다만 이미 CRM 자산 활용이라 신규 비용 X. 운영 리소스 확대.
- **naver.searchad** — yield 11.7%, 점유 꾸준한 증가. 키워드 입찰가 확대 여지.
- **facebook.business / IG** — yield 11%, 1M 점유 확대 중. CPC 효율 모니터링 후 예산 추가.

### 🟡 유지 (high volume, average yield)
- **kbcardlms / hanacardlms / wooricardlms** — paid_msg 평균 yield 11~12%, 점유 안정. 현 수준 유지.
- **partner medium 전체** — yield 14% 우수, 풀의 31%. 핵심 매출 백본. **단가 인상 협상 가치 있음.**

### 🔴 점검 / 축소 검토
- **toss.join** — 12M 27% → 1M 8% 채널 피로도. 효율은 여전히 14.8%로 좋지만 마케팅 가능 여지가 줄고 있음. **신규 placement/소재 테스트 또는 단가 협상.**
- **google.adwords** — 12M yield 10.1% (네이버 11.7% 대비 -1.5%p). **키워드/매칭 타입/QS 점검** 후 결정.
- **sms** — deal size 큰데 yield 10.8%. **자격 필터 강화** 후 재평가.
- **cashnote** — yield 9.7% + 점유 감소. **자영업자 segment 자격 검증 강화하거나 정리.**
- **kakaobank** — 12M yield 14% 우수했으나 1M 점유 1.4%. **노출 위치/단가 변화 원인 확인 필요.**

### ⚪ 데이터 부족 (소규모 — 결정 보류)
- sena, youtuber, blog, naver(generic), bznav_app — yield 변동 크고 표본 적음. 소액 테스트 후 재평가.

---

## 5. 운영 가이드

**매주 점검 (지표 모니터링)**
1. 1M_recent 신청량 점유율 변동 ±5%p 채널 → 원인 파악
2. 12M_cohort yield < 10% 채널 → 자격 필터 / 단가 점검
3. shinhancardlms lag (현재 72일) 유지 여부 → 회수 속도 KPI

**분기 점검 (전략 재배분)**
- 12M → 신규 분기 코호트 yield 차이 ±2%p 채널은 예산 재배분 트리거
- A(지수) 비중 추적 — 자격 필터 부재의 직접 지표 (현재 137k/241k = 57%)

**스크립트 재실행**
```bash
gh release download history-archive -R andrew-zent/bznav-refund-forecast \
  -p history.sqlite -O /tmp/history.sqlite --clobber
python3 src/utm_channel_analysis.py
```

**제외/포함 조정**
- A(지수) 포함 비교가 필요하면 [src/utm_channel_analysis.py](../src/utm_channel_analysis.py)의 `EXCLUDE_PIPELINES = ()` 로 변경.

---

## 6. 한계와 다음 단계

**현재 분석의 한계**
- 비용 데이터 미반영 — yield는 매출 효율, ROAS 아님. 채널별 광고비 결합하면 진짜 ROAS 산출 가능.
- utm_medium=`(none)` 6.2% — 추적 누락. 7.7% 신청금 풀이 출처 불명.
- 멀티터치(조회 UTM ≠ 신청 UTM 12.6%) 미분리 — 마지막 클릭 기준 집계.
- A(지수) 제외만 적용. 법인/개인 분리 미적용.

**제안하는 후속 분석**
1. **광고비 결합 ROAS** — Pipedrive 외부의 광고비 데이터 (GA4/Meta Ads/Naver SA) 결합
2. **멀티터치 기여도** — utm_source_query × utm_source 조합으로 first-touch vs last-touch 분리
3. **신규 vs 리마인드 분리** — alrimtalk/sms/kakaochannel 계열을 "리마인드"로 라벨링한 신규 유입 전용 매트릭스
4. **utm_campaign 단위** — campaign_id별 효율 (소재/타깃별 A/B 시그널)
5. **고객 LTV** — 1회 환급 후 추심/세무대리 등 cross-sell 기여도

필요한 분석부터 우선 정해주시면 스크립트 확장하겠습니다.
