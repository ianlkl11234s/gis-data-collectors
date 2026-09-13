"""日本醫療 analytics pipeline 的低頻 orchestration adapter。

這不是資料 parser，也不將全國資料寫入 Supabase；pipeline 的原始/processed
artifact 是唯一大檔載體，collector 只回傳可被 scheduler 記錄的小型摘要。
"""
from __future__ import annotations

import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import ClassVar

import config
from collectors.base import BaseCollector


class JapanMedicalPipelineCollector(BaseCollector):
    """以固定 allowlist 執行一個 analytics pipeline，禁止 shell 轉譯。"""

    name: ClassVar[str] = "jp_medical"
    interval_minutes: ClassVar[int] = 1440
    script_relative: ClassVar[str]
    command_args: ClassVar[tuple[str, ...]] = ()
    COLLECT_TIMEOUT = 60 * 60

    @staticmethod
    def _build_frontend_enabled() -> bool:
        """Keep publication preparation opt-in without adding shared config state."""
        return os.getenv("JP_MEDICAL_BUILD_FRONTEND", "").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }

    def should_persist_local(self) -> bool:
        return False

    def _command(self) -> list[str]:
        if not config.ANALYTICS_ROOT:
            raise RuntimeError("ANALYTICS_ROOT is required for Japan medical pipelines")
        root = Path(config.ANALYTICS_ROOT).expanduser().resolve()
        script = root / self.script_relative
        if not Path(config.PIPELINE_PYTHON).is_file() or not os.access(config.PIPELINE_PYTHON, os.X_OK):
            raise RuntimeError("PIPELINE_PYTHON must name an existing executable")
        if not script.is_file() or root not in script.parents:
            raise RuntimeError(f"approved pipeline script is unavailable: {self.script_relative}")
        return [config.PIPELINE_PYTHON, str(script), *self.command_args]

    def _commands(self) -> tuple[list[str], ...]:
        return (self._command(),)

    def _frontend_command(self) -> list[str]:
        if not config.ANALYTICS_ROOT:
            raise RuntimeError("ANALYTICS_ROOT is required for Japan medical pipelines")
        root = Path(config.ANALYTICS_ROOT).expanduser().resolve()
        script = root / "pipelines/world/jp_medical_frontend/build.py"
        if not script.is_file() or root not in script.parents:
            raise RuntimeError("Japan medical frontend build pipeline is unavailable")
        return [config.PIPELINE_PYTHON, str(script)]

    def collect(self) -> dict:
        source_commands = self._commands()
        started = datetime.now(timezone.utc)
        for command in source_commands:
            try:
                completed = subprocess.run(command, cwd=config.ANALYTICS_ROOT, check=False, shell=False,
                    timeout=config.JP_MEDICAL_PIPELINE_TIMEOUT_SECONDS, capture_output=True, text=True)
            except subprocess.TimeoutExpired as exc:
                raise RuntimeError(f"pipeline timed out after {config.JP_MEDICAL_PIPELINE_TIMEOUT_SECONDS}s") from exc
            if completed.returncode:
                # stdout/stderr might contain source content; keep scheduler result bounded.
                tail = (completed.stderr or completed.stdout or "").strip().replace("\n", " ")[-500:]
                raise RuntimeError(f"pipeline exited {completed.returncode}: {tail}")
        frontend_command = self._frontend_command() if self._build_frontend_enabled() else None
        if frontend_command:
            try:
                completed = subprocess.run(frontend_command, cwd=config.ANALYTICS_ROOT, check=False, shell=False,
                    timeout=config.JP_MEDICAL_PIPELINE_TIMEOUT_SECONDS, capture_output=True, text=True)
            except subprocess.TimeoutExpired as exc:
                raise RuntimeError(f"frontend build timed out after {config.JP_MEDICAL_PIPELINE_TIMEOUT_SECONDS}s") from exc
            if completed.returncode:
                tail = (completed.stderr or completed.stdout or "").strip().replace("\n", " ")[-500:]
                # build.py atomically replaces its current pointer only after validation;
                # a failed run must remain a collector error and cannot be reported ready.
                raise RuntimeError(f"frontend build exited {completed.returncode}: {tail}")
        artifact_plan = None
        if config.JP_MEDICAL_ARTIFACTS_ENABLED:
            # This deliberately runs only the read-only planner. S3 write mode has
            # no collector path until exact payload/destination authorization.
            from tasks.jp_medical_artifacts import build_plan, execute_plan
            artifact_plan = execute_plan(build_plan(self.name, Path(config.ANALYTICS_ROOT)))
        return {
            "pipeline": self.name,
            "status": "completed",
            "started_at": started.isoformat(),
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "commands": [[Path(part).name if i < 2 else part for i, part in enumerate(command)] for command in source_commands],
            "frontend_bundle_built": bool(frontend_command),
            "artifact_plan": {"raw_files": len(artifact_plan["raw"]), "bundle_files": len(artifact_plan["bundle"])} if artifact_plan else None,
        }


class JapanMedicalNaviiCollector(JapanMedicalPipelineCollector):
    name = "jp_medical_navii"
    interval_minutes = config.JP_MEDICAL_NAVII_INTERVAL
    script_relative = "pipelines/world/jp_medical_navii/jp_medical_navii.py"
    command_args = ("run",)


class JapanMedicalIdwrCollector(JapanMedicalPipelineCollector):
    name = "jp_medical_idwr"
    interval_minutes = config.JP_MEDICAL_IDWR_INTERVAL
    script_relative = "pipelines/world/jp_medical_idwr/pipeline.py"

    def _commands(self) -> tuple[list[str], ...]:
        # Daily rechecks current plus previous year for the ISO-week boundary.
        # At month start both receive a full annual integrity pass.
        now = datetime.now(timezone.utc)
        extra = ("--all-weeks",) if now.day <= 3 else ("--recheck-weeks", "4")
        base = super()._command()
        return tuple(base + ["--year", str(year), *extra] for year in (now.year, now.year - 1))


class JapanMedicalReportsCollector(JapanMedicalPipelineCollector):
    name = "jp_medical_reports"
    interval_minutes = config.JP_MEDICAL_REPORTS_INTERVAL
    script_relative = "pipelines/world/jp_medical_reports/jp_medical_reports.py"

    def _commands(self) -> tuple[list[str], ...]:
        primary = super()._command()
        supplemental = Path(config.ANALYTICS_ROOT).resolve() / "pipelines/world/jp_medical_supplements/collect.py"
        if not supplemental.is_file():
            raise RuntimeError("Japan medical supplements pipeline is unavailable")
        return (primary, [config.PIPELINE_PYTHON, str(supplemental)])
