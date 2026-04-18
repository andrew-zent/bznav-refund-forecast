# [2~4/4] 채널 정밀 분석 — 신규/리마인드, 캠페인 A/B, 멀티터치

- 코드: [src/channel_deep_analysis.py](../src/channel_deep_analysis.py)
- 산출: [output/channel_deep_analysis.json](../output/channel_deep_analysis.json), [output/channel_deep_analysis.csv](../output/channel_deep_analysis.csv)
- 기간: 12M 완성 코호트 (2024-11~2025-10), A(지수) 제외

---

## [2/4] 신규 vs 리마인드 채널 분리

**리마인드 정의**: alrimtalk 계열·sms·kakaochannel·kakaobrandmsg·friendtalk (utm_medium=crm/sms 포함)

| 유형 | deals | 신청억(금액%) | yield%(금액) | won%(건수) |
|---|---:|---:|---:|---:|
| **신규** | 35,795 | **1,086억 (68%)** | 12.91 | 71.2 |
| **리마인드** | 15,116 | **503억 (32%)** | **13.22** | **72.8** |
| unknown | 45 | 1.6억 | 12.78 | 46.7 |

**핵심 발견**
- 리마인드 yield가 신규보다 **+0.31%p 높음** — 재접촉 고객이 자격 통과율·결제율 모두 우월
- **신규 볼륨이 2.4배 크지만 yield 차이는 작음** → 신규 채널 품질 향상 여지 (자격 필터 강화)
- ROAS 분석에서 alrimtalk ROAS 33.86이 나온 이유 = 낮은 발송비 + 리마인드 yield 프리미엄
- **의사결정**: 신규 yield 12.91%를 리마인드 수준(13.22%)으로 끌어올리면 → 1,086억 × +0.31%p = **+3.4억 추가 회수**. 방법: 신규 채널 자격 사전 스크리닝 강화.

### 신규 채널 Top 10 (12M, apply 기준)

| utm_source | apply억 | yield%(금액) | won%(건수) |
|---|---:|---:|---:|
| toss.join | 429.9 | 14.84 | 72.3 |
| referer | 82.5 | 12.07 | 68.9 |
| kbcardlms | 72.0 | 12.12 | 68.8 |
| kakaobank | 64.2 | **14.05** | 74.9 |
| google.adwords | 48.7 | 10.13 | 62.6 |
| naver.searchad | 44.2 | 11.66 | 68.5 |
| hanacardlms | 41.1 | 11.30 | 68.3 |
| kakaochannel* | 37.4 | 13.14 | 70.7 |
| partner | 32.1 | 11.51 | 71.0 |
| cashnote | 29.9 | 9.73 | 59.7 |

\* kakaochannel은 리마인드/신규 혼재 — 추후 세분화 가능

---

## [3/4] utm_campaign 단위 A/B 효율 (12M, 신규 채널, 10건 이상)

| utm_source | utm_campaign | apply억 | yield%(금액) | 평가 |
|---|---|---:|---:|---|
| toss.join | toss.join_inapppage_btn | 429.9 | 14.84 | 🟢 기준선 |
| kakaobank | kakaobank_hometop_big | 50.4 | 14.24 | 🟢 |
| **sena** | **sena_feedbanner** | 6.0 | **15.18** | 🟢 단일 최고 yield |
| shinhancardlms | shinhancard_enterprisecard | 19.7 | **14.63** | 🟢 카드사 LMS 최고 |
| zeropay | zeropay_lms_applydetarget | 11.1 | 14.05 | 🟢 |
| kbcardlms | kbcard_enterprisecard | 29.7 | 12.63 | 🟡 |
| wooricardlms | wooricard_franchise | 22.2 | 11.93 | 🟡 |
| facebook.business | meta_asc_completeregistration | 9.0 | 11.87 | 🟡 |
| kbcardlms | kbcard_franchise | 42.4 | 11.77 | 🟡 |
| banksalad | banksalad_cpa_bridgepage | 22.4 | 10.92 | 🟡 |
| app_bznav | bznav_app_to_refund | 9.4 | 10.78 | 🟡 |
| okpos | okpos_inappbanner | 15.3 | 10.63 | 🟡 |
| hanacardlms | hanacard_entrepreneur | 17.0 | 10.26 | 🟡 |
| hanacardlms | hanacard_franchise | 23.9 | 10.26 | 🟡 |
| google.adwords | **google_demandgen_250701** | 9.2 | **10.35** | 🔴 |
| google.adwords | **google_pmax_250401** | 32.2 | **9.70** | 🔴 |
| shinhancardlms | **shinhancard_franchise** | 13.8 | **9.27** | 🔴 같은 채널, -5.4%p |

**캠페인 단위 발견**
- **같은 채널인데 캠페인별 yield 격차 최대 5.4%p** (shinhancardlms: enterprise 14.63% vs franchise 9.27%)
  - enterprise 캠페인은 법인/사업자 타깃 → 환급 자격률 높음
  - franchise 캠페인은 프랜차이즈 가맹점 → 자격 탈락 비율 높음
- **google_pmax yield 9.7%** — ROAS 적자 일치. 캠페인 자체를 정리하거나 타깃 재설정.
- **sena_feedbanner yield 15.18%** (전체 최고) — 소규모지만 최고 품질. 예산 확대 테스트 후보.

---

## [4/4] 멀티터치 기여도 — first vs last touch

| 구분 | deals | 비율 |
|---|---:|---:|
| 단일 터치 | 41,546 | **89.9%** |
| 멀티터치 | 4,657 | **10.1%** |

### 주요 멀티터치 경로 (first → last, 건수 5개 이상)

| 경로 | deals | apply억 | yield%(금액) | 해석 |
|---|---:|---:|---:|---|
| **(none) → alrimtalk** | 1,302 | 23.0 | **15.62** | UTM 없이 유입 후 알림톡으로 전환 |
| **toss.join → alrimtalk** | 712 | 16.8 | **15.78** | 토스 조회 → 알림톡 최종 신청 |
| alrimtalk → sms | 347 | 25.7 | 12.33 | 알림톡 조회 후 SMS 리마인드로 전환 |
| toss.join → alrimtalk.toss.join | 580 | 18.6 | 10.93 | 토스 재유입 경로 |

**핵심 발견**

1. **멀티터치 고객 yield 15.6~15.8% vs 단일터치 평균 12.9%** — **+2.7~2.9%p 프리미엄**
   - 멀티터치 = 관심도 높은 고객 → 자격 검증·결제까지 완주율 높음
   - 이전 분석의 "멀티터치 deal size +48%" 발견과 일치

2. **toss.join → alrimtalk 경로 yield 15.78% (최고)**
   - 토스에서 조회만 하고 나갔다가 alrimtalk 리마인드에서 신청 완료
   - last-touch(alrimtalk) 기준으로만 보면 alrimtalk의 공헌 과대평가됨
   - **toss.join이 진짜 1차 기여자** — toss.join 예산 삭감 시 이 경로 단절

3. **(none) → alrimtalk 1,302건, 23억**
   - 최초 유입 UTM 없음 → 오가닉/다이렉트 접속 후 알림톡 리마인드로 신청
   - alrimtalk ROAS 33배는 이 건들이 크게 기여 — 실제 alrimtalk의 "신규 유입 효과"는 0

4. **귀속 보정 제안**
   - Last-touch만 쓰면 알림톡·sms를 과대평가, toss.join·partner를 과소평가
   - 단순 first-touch라도 `utm_source_query` 를 기준 채널로 쓰면 toss.join 기여도 복원됨

---

## 종합 의사결정 요약 (2~4/4 결합)

| 우선순위 | 채널/캠페인 | 근거 | 액션 |
|---|---|---|---|
| 1 | google_pmax 캠페인 정리 | yield 9.7% + ROAS 0.47 | 즉시 중단 or 타깃 재설정 |
| 2 | shinhancardlms franchise 캠페인 점검 | enterprise(14.6%) vs franchise(9.3%), -5.4%p | 프랜차이즈 타깃 자격 필터 추가 |
| 3 | toss.join → alrimtalk 경로 보호 | 712건, yield 15.78% — 삭제 시 경로 단절 | toss.join 광고비 삭감 전 경로 분석 선행 |
| 4 | sena_feedbanner 확대 테스트 | yield 15.18% 최고 — 단 볼륨 소규모 | 예산 2배 → 효율 유지 확인 |
| 5 | 신규 채널 자격 필터 강화 | 신규 yield 12.91% vs 리마인드 13.22%, +0.31%p 갭 | 조회 시점 업종/규모 사전 스크리닝 |
