"""
필드 매핑 및 모델 설정.
Pipedrive 커스텀 필드 key → 우리 모델 변수명.
"""

# Pipedrive 컬럼명 → 내부 변수명 매핑 (xlsx 컬럼 헤더 기준)
FIELD_MAP_BY_NAME = {
    "거래 - ✔ 신청일자": "apply_date",
    "거래 - 상태": "status",
    "거래 - 📍 결제금액-알림톡발송": "payment_amount",
    "거래 - ✍ 결정 환급액-알림톡발송": "decision_amount",
    "거래 - ✔ 조회 환급액": "apply_amount",
    "거래 - ✔ 신고일자": "filing_date",
    "거래 - ✍ 신고 환급액-알림톡발송": "filing_amount",
    "거래 - ✍ 결정일자": "decision_date",
    "거래 - 💸 결제일자": "payment_date",
    "거래 - 파이프라인": "pipeline",
    "거래 - 감면only 여부": "is_only_gam",
}

# 파이프라인 분류 — 개인
PIPELINE_REGULAR = "B(젠트)-환급"
PIPELINE_COLLECTION = ("C(젠트)-추심", "E(가은)-미수채권")
STATUS_EXCLUDE = "실패"

# 파이프라인 분류 — 법인
CORP_PIPELINE_REGULAR = "법인-환급"
CORP_PIPELINE_COLLECTION = ("법인-추심",)

# 모델 하이퍼파라미터
CHAIN_DIST_MAX_OFF = {
    "a2f": 4,  # 신청→신고
    "f2d": 4,  # 신고→결정
    "d2p": 3,  # 결정→결제
}
ROLLING_WINDOW = 5  # 6→5: 5개월 연속 과소추정 편향 보정 (최근 코호트 반영 강화)
APP_FALLBACK_WINDOW = 3  # 신청금액 fallback 윈도우
COLLECTION_MA_WINDOW = 2  # 추심 MA 윈도우 (tuner 최적화: 3→2, MAPE 4.22%→3.94%)

# 시즌 보정 (24개월 백테스트 최적화 + 조사관 도메인 지식 반영)
# 종소세 시즌(6~7월): 결정 지연 → 결제 이월 효과, d2p 분포가 이미 반영하므로 중복 차감 제거
# 원칙: 보정 없음 MAPE 5.89% > 기존 보정 6.27%이므로 최소 개입
SEASON_ADJUSTMENT = {
    1: 0.00,    # 연초 — 종소세 결정지연 이월 가능, 기존+5%는 과보정
    2: 0.00,    # 설 연휴 — -5%→0%: 연속 과소추정 기간(11~3월) 하방압력 제거 (기존 -0.05)
    3: +0.10,   # 1Q 폭증 — 기존+20%는 과보정 (raw 오차 -1%)
    4: 0.00,    # 평월
    5: 0.00,    # 종소세 신고기
    6: 0.00,    # 종소세 결정지연 — 결제 감소 아닌 이월, -10% 제거
    7: 0.00,    # 종소세 결정지연 — 동일, -10% 제거
    8: 0.00,    # 평월
    9: 0.00,    # 평월
    10: 0.00,   # 평월
    11: 0.00,   # 평월
    12: +0.05,  # 연말 결제집중 — 기존+15%에서 축소
}
