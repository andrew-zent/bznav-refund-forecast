"""
Phase 3: Orchestrator — 파이프라인 오케스트레이션.

전체 파이프라인을 스테이트 머신으로 관리하고,
각 단계별 에러 복구 전략을 적용.

독립 실행: python src/agents/orchestrator.py [--dry-run]
"""
import json
import subprocess
import sys
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent.parent
SRC = ROOT / "src"


class StepStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


class ErrorPolicy(str, Enum):
    RETRY = "retry"       # 재시도 후 실패 시 abort
    SKIP = "skip"         # 실패해도 다음 단계로
    ABORT = "abort"       # 즉시 중단


@dataclass
class StepResult:
    name: str
    status: StepStatus = StepStatus.PENDING
    started_at: str = ""
    finished_at: str = ""
    duration_sec: float = 0
    detail: str = ""
    error: str = ""


@dataclass
class PipelineState:
    run_id: str = ""
    started_at: str = ""
    finished_at: str = ""
    status: str = "pending"
    steps: list[StepResult] = field(default_factory=list)
    dry_run: bool = False

    def to_dict(self):
        d = asdict(self)
        d["steps"] = [asdict(s) for s in self.steps]
        return d


# 파이프라인 단계 정의
PIPELINE_STEPS = [
    {
        "name": "extract_individual",
        "script": "extract_pipedrive.py",
        "description": "개인 Pipedrive 추출",
        "error_policy": ErrorPolicy.RETRY,
        "max_retries": 2,
        "env_required": ["PIPEDRIVE_API_TOKEN", "PIPEDRIVE_DOMAIN"],
    },
    {
        "name": "extract_corp",
        "script": "extract_corp.py",
        "description": "법인 Pipedrive 추출",
        "error_policy": ErrorPolicy.SKIP,  # 법인 실패해도 개인만으로 진행
        "max_retries": 1,
        "env_required": ["CORP_PIPEDRIVE_API_TOKEN"],
    },
    {
        "name": "watch_data",
        "script": "agents/watcher.py",
        "description": "데이터 품질 검증",
        "error_policy": ErrorPolicy.ABORT,  # 품질 실패 시 중단
        "max_retries": 0,
    },
    {
        "name": "run_model",
        "script": "model.py",
        "description": "예측 모델 실행",
        "error_policy": ErrorPolicy.ABORT,
        "max_retries": 0,
        "env_required": ["PIPEDRIVE_API_TOKEN", "PIPEDRIVE_DOMAIN"],
    },
    {
        "name": "verify_forecast",
        "script": "agents/verifier.py",
        "description": "예측 결과 검증",
        "error_policy": ErrorPolicy.SKIP,  # 검증 실패해도 결과는 배포
        "max_retries": 0,
    },
    {
        "name": "generate_dashboard",
        "script": "generate_dashboard.py",
        "description": "대시보드 생성",
        "error_policy": ErrorPolicy.ABORT,
        "max_retries": 0,
    },
    {
        "name": "notify_slack",
        "script": "notify_slack.py",
        "description": "Slack 알림",
        "error_policy": ErrorPolicy.SKIP,
        "max_retries": 0,
    },
    {
        "name": "ensemble_forecast",
        "script": "agents/ensemble.py",
        "description": "멀티 모델 앙상블 예측 (cohort+ARIMA+ETS)",
        "error_policy": ErrorPolicy.SKIP,
        "max_retries": 0,
    },
    {
        "name": "confluence_report",
        "script": "agents/confluence_reporter.py",
        "description": "Confluence 주간 리포트 자동 게시",
        "error_policy": ErrorPolicy.SKIP,
        "max_retries": 0,
    },
]


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _run_step(step_def: dict, dry_run: bool = False) -> StepResult:
    """단일 스텝 실행."""
    result = StepResult(name=step_def["name"])
    result.started_at = _now()

    script_path = SRC / step_def["script"]
    if not script_path.exists():
        result.status = StepStatus.FAILED
        result.error = f"Script not found: {script_path}"
        result.finished_at = _now()
        return result

    if dry_run:
        result.status = StepStatus.SKIPPED
        result.detail = "dry-run"
        result.finished_at = _now()
        return result

    max_retries = step_def.get("max_retries", 0)
    last_error = ""

    for attempt in range(max_retries + 1):
        start = time.time()
        try:
            proc = subprocess.run(
                [sys.executable, str(script_path)],
                cwd=str(ROOT),
                capture_output=True,
                text=True,
                timeout=600,  # 10분 타임아웃
            )
            elapsed = time.time() - start
            result.duration_sec = round(elapsed, 1)

            if proc.returncode == 0:
                result.status = StepStatus.SUCCESS
                # 마지막 5줄 요약
                lines = proc.stdout.strip().split("\n")
                result.detail = "\n".join(lines[-5:]) if lines else "(no output)"
                result.finished_at = _now()
                return result
            else:
                last_error = proc.stderr.strip()[-500:] or proc.stdout.strip()[-500:]
                if attempt < max_retries:
                    print(f"  retry {attempt + 1}/{max_retries}...")
                    time.sleep(5 * (attempt + 1))

        except subprocess.TimeoutExpired:
            last_error = "Timeout (600s)"
        except Exception as e:
            last_error = str(e)

    result.status = StepStatus.FAILED
    result.error = last_error
    result.finished_at = _now()
    return result


def run_pipeline(dry_run: bool = False) -> PipelineState:
    """전체 파이프라인 실행."""
    state = PipelineState(
        run_id=datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S"),
        started_at=_now(),
        status="running",
        dry_run=dry_run,
    )

    print("=" * 60)
    print(f"Orchestrator: pipeline run {state.run_id}" + (" [DRY-RUN]" if dry_run else ""))
    print("=" * 60)

    for step_def in PIPELINE_STEPS:
        name = step_def["name"]
        desc = step_def["description"]
        policy = step_def["error_policy"]

        print(f"\n▶ [{name}] {desc}...")
        result = _run_step(step_def, dry_run=dry_run)
        state.steps.append(result)

        if result.status == StepStatus.SUCCESS:
            print(f"  ✅ success ({result.duration_sec}s)")
        elif result.status == StepStatus.SKIPPED:
            print(f"  ⏭️  skipped ({result.detail})")
        elif result.status == StepStatus.FAILED:
            print(f"  ❌ failed: {result.error[:200]}")

            if policy == ErrorPolicy.ABORT:
                print(f"  🛑 ABORT — error policy = abort")
                state.status = "failed"
                state.finished_at = _now()
                _save_state(state)
                _notify_failure(state, step_def, result)
                return state
            elif policy == ErrorPolicy.SKIP:
                print(f"  ⏭️  continuing — error policy = skip")
            # RETRY는 _run_step 내부에서 처리됨

    state.status = "success"
    state.finished_at = _now()
    _save_state(state)
    print(f"\n{'=' * 60}")
    print(f"Pipeline {state.status}: {_count_status(state)}")
    return state


def _count_status(state: PipelineState) -> str:
    counts = {}
    for s in state.steps:
        counts[s.status] = counts.get(s.status, 0) + 1
    return ", ".join(f"{k}={v}" for k, v in counts.items())


def _save_state(state: PipelineState):
    """실행 상태를 JSON으로 저장."""
    path = ROOT / "output" / "pipeline_state.json"
    path.parent.mkdir(exist_ok=True)
    path.write_text(json.dumps(state.to_dict(), ensure_ascii=False, indent=2))
    print(f"→ {path}")


def _notify_failure(state: PipelineState, step_def: dict, result: StepResult):
    """실패 시 Slack 알림."""
    try:
        from agents.alerts import send_slack
        msg = (
            f"*Pipeline FAILED* at `{step_def['name']}` ({step_def['description']})\n"
            f"Error: {result.error[:300]}\n"
            f"Run: {state.run_id}"
        )
        send_slack(msg, severity="critical")
    except Exception:
        pass  # 알림 실패는 무시


def main():
    dry_run = "--dry-run" in sys.argv
    state = run_pipeline(dry_run=dry_run)
    sys.exit(0 if state.status == "success" else 1)


if __name__ == "__main__":
    main()
