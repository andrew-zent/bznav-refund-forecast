"""
대시보드 기동 검증 — Streamlit 앱이 오류 없이 시작되는지 확인.

  python src/agents/dashboard_validator.py

exit 0: 정상
exit 1: 기동 실패 (에러 내용 출력)
"""
from __future__ import annotations

import subprocess
import sys
import time
import urllib.request
from pathlib import Path

ROOT      = Path(__file__).resolve().parent.parent.parent
DASHBOARD = ROOT / "src" / "dashboard_streamlit.py"
PORT      = 18765   # 충돌 없는 임시 포트
TIMEOUT   = 25      # 초


def validate() -> bool:
    # ── 1. 구문 검사 ──────────────────────────────────────────────────────────
    r = subprocess.run(
        [sys.executable, "-m", "py_compile", str(DASHBOARD)],
        capture_output=True, text=True,
    )
    if r.returncode != 0:
        print(f"❌ Syntax error in dashboard_streamlit.py:\n{r.stderr}")
        return False
    print("✅ syntax OK")

    # ── 2. Streamlit 기동 ────────────────────────────────────────────────────
    proc = subprocess.Popen(
        [sys.executable, "-m", "streamlit", "run", str(DASHBOARD),
         "--server.headless", "true",
         "--server.port", str(PORT),
         "--logger.level", "error"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        cwd=str(ROOT),
    )

    # ── 3. health 엔드포인트 폴링 ────────────────────────────────────────────
    deadline = time.time() + TIMEOUT
    started  = False
    while time.time() < deadline:
        try:
            resp = urllib.request.urlopen(
                f"http://localhost:{PORT}/healthz", timeout=2
            )
            if resp.status == 200:
                started = True
                break
        except Exception:
            pass
        time.sleep(1)

    # ── 4. 프로세스 정리 ──────────────────────────────────────────────────────
    proc.terminate()
    try:
        stderr_out = proc.stderr.read().decode(errors="replace") if proc.stderr else ""
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        stderr_out = ""

    if started:
        print("✅ dashboard starts without error")
        return True

    print(f"❌ dashboard failed to start within {TIMEOUT}s")
    # StreamlitAPIException 같은 핵심 에러만 추출해서 출력
    for line in stderr_out.splitlines():
        if any(kw in line for kw in ("Error", "Exception", "Traceback", "error")):
            print(f"   {line}")
    return False


if __name__ == "__main__":
    sys.exit(0 if validate() else 1)
