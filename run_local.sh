#!/bin/bash
# 로컬 실행 스크립트 (테스트용)
# 사용법: source .env && bash run_local.sh

set -e
cd "$(dirname "$0")"

echo "=== 1. Pipedrive 데이터 추출 ==="
python3 src/extract_pipedrive.py

echo ""
echo "=== 2. 모델 학습 & 예측 ==="
cd src && python3 model.py && cd ..

echo ""
echo "=== 3. 대시보드 생성 ==="
cd src && python3 generate_dashboard.py && cd ..

echo ""
echo "=== 4. Slack 알림 (optional) ==="
python3 src/notify_slack.py

echo ""
echo "✅ 완료! output/dashboard.html 열기:"
open output/dashboard.html
