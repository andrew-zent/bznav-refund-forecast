#!/bin/bash
# 개발 환경 git hooks 설치
# 실행: bash scripts/setup_hooks.sh

set -e
ROOT="$(git rev-parse --show-toplevel)"
HOOKS_DIR="$ROOT/.git/hooks"

cat > "$HOOKS_DIR/pre-commit" << 'HOOK'
#!/bin/bash
# dashboard_streamlit.py 변경 시 커밋 전 기동 검증

if git diff --cached --name-only | grep -q "dashboard_streamlit.py"; then
    echo "🔍 dashboard_streamlit.py 변경 감지 — 기동 검증 실행..."
    python src/agents/dashboard_validator.py
    if [ $? -ne 0 ]; then
        echo ""
        echo "❌ 커밋 차단: 대시보드 기동 검증 실패"
        echo "   에러를 수정한 뒤 다시 커밋하세요."
        exit 1
    fi
fi
exit 0
HOOK

chmod +x "$HOOKS_DIR/pre-commit"
echo "✅ pre-commit hook 설치 완료 ($HOOKS_DIR/pre-commit)"
