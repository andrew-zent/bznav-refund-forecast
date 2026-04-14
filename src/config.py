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

# 파이프라인 분류
PIPELINE_REGULAR = "B(젠트)-환급"
PIPELINE_COLLECTION = ("C(젠트)-추심", "E(가은)-미수채권")
STATUS_EXCLUDE = "실패"

# 모델 하이퍼파라미터
CHAIN_DIST_MAX_OFF = {
    "a2f": 4,  # 신청→신고
    "f2d": 4,  # 신고→결정
    "d2p": 3,  # 결정→결제
}
ROLLING_WINDOW = 6  # 최적 윈도우 (Phase 3 실험 1c 결과)
APP_FALLBACK_WINDOW = 3  # 신청금액 fallback 윈도우
COLLECTION_MA_WINDOW = 3  # 추심 MA 윈도우

# 시즌 보정 (Phase 3 분석 결과)
SEASON_ADJUSTMENT = {
    1: +0.05,   # 1Q
    2: -0.10,   # 설 연휴
    3: +0.20,   # 1Q 폭증
    4: 0.00,    # 평월
    5: 0.00,    # 종소세 진입 (±불확실, 시나리오)
    6: -0.10,   # 종소세 결제기
    7: -0.10,   # 종소세 결제기
    8: 0.00,    # 평월
    9: 0.00,    # 평월
    10: 0.00,   # 평월
    11: 0.00,   # 평월
    12: +0.15,  # 연말 결제집중
}
