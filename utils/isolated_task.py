"""低頻重任務的子行程執行機制（記憶體隔離＋OOM 優先犧牲＋逾時強制結束）。

為什麼：collector 主程序長駐 40 多條執行緒，glibc 每條執行緒的 arena 在尖峰
（數百 MB 的 COPY、Arrow、tar）之後不會把記憶體還給 OS，RSS 每遇一次尖峰就往上
墊一階。重任務放進子行程跑，結束時整個行程的記憶體一次還給 OS。

子行程和主程序在同一個容器 cgroup，尖峰仍算在 mem_limit 裡；所以子行程啟動時
會把自己的 /proc/self/oom_score_adj 設成 1000，cgroup OOM 時先砍它，而不是 RSS
最大的主程序（主程序被砍＝全部 collector 一起掛）。行程可以自己「調高」這個值，
不需要特權；寫不進去（非 Linux、唯讀 /proc）只記 log，不影響任務。

**重任務怎麼接**（只給低頻、尖峰大的任務：每日匯出、備份、發布；高頻 collector
不要用，每輪啟動 Python＋import 的成本約數秒與上百 MB）：

    from utils.isolated_task import run_isolated

    def my_job():
        # target 是 "module:function"，回傳值必須可 JSON 序列化（非 JSON 型別用 str()）
        return run_isolated("my_job", "tasks.my_heavy:run_once",
                            kwargs={"day": "2026-11-06"}, timeout=3600)

    schedule.every().day.at("04:00").do(
        sched.submit, _as_task("my_job", my_job, timeout=1200, heavy=True))

- 子行程的例外會變成主程序的 IsolatedTaskError（訊息已脫敏），沿用呼叫端既有的
  try/except → notify 路徑。
- 逾時（timeout 秒）會對整個 process group 送 SIGKILL（含孫行程），丟 IsolatedTaskError。
- 被訊號殺掉（多半是 OOM）回傳碼為負，錯誤訊息會標明 likely OOM。
- target 函式要在子行程內自己建立 DB／S3 連線，不能依賴主程序的物件。
"""

from __future__ import annotations

import importlib
import json
import logging
import os
import signal
import subprocess
import sys
import tempfile
import time
import traceback
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[1]
OOM_SCORE_ADJ_PATH = "/proc/self/oom_score_adj"


class IsolatedTaskError(RuntimeError):
    """子行程失敗、逾時或被訊號結束。"""


def set_oom_score_adj(value: int = 1000, path: str | None = None) -> bool:
    """把目前行程設成 OOM 優先犧牲對象；失敗時記 log 並回傳 False（不丟例外）。"""
    path = path or os.getenv("ISOLATED_TASK_OOM_ADJ_PATH") or OOM_SCORE_ADJ_PATH
    try:
        with open(path, "w") as fh:
            fh.write(str(int(value)))
        return True
    except OSError as exc:
        logger.info("oom_score_adj 無法設定（%s）：%s", path, exc)
        return False


def _redact(text: str) -> str:
    try:
        from utils.redact import redact_secrets
        return redact_secrets(text)
    except Exception:  # pragma: no cover - redact 本身不應失敗
        return text


def run_isolated(
    name: str,
    target: str,
    *,
    kwargs: dict | None = None,
    timeout: float | None = None,
    python: str | None = None,
) -> Any:
    """在子行程執行 target（"module:function"），回傳其結果（經 JSON 往返）。

    Raises:
        IsolatedTaskError: 子行程丟例外、非零結束、被訊號殺掉或逾時。
    """
    started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix=f"isolated-{name}-") as tmp:
        result_path = Path(tmp) / "result.json"
        kwargs_path = Path(tmp) / "kwargs.json"
        kwargs_path.write_text(json.dumps(kwargs or {}, default=str), encoding="utf-8")
        cmd = [
            python or sys.executable, "-m", "utils.isolated_task",
            "--name", name, "--target", target,
            "--kwargs-file", str(kwargs_path), "--result-file", str(result_path),
        ]
        proc = subprocess.Popen(cmd, cwd=REPO_ROOT, start_new_session=True)
        logger.info("[%s] 子行程啟動 pid=%s target=%s", name, proc.pid, target)
        try:
            returncode = proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                proc.kill()
            proc.wait()
            raise IsolatedTaskError(f"{name} 子行程逾時 {timeout:.0f}s，已強制結束")

        elapsed = time.monotonic() - started
        payload = None
        if result_path.exists():
            try:
                payload = json.loads(result_path.read_text(encoding="utf-8"))
            except ValueError:
                payload = None

        if returncode == 0 and payload and payload.get("ok"):
            logger.info("[%s] 子行程完成（%.1fs）", name, elapsed)
            return payload.get("result")

        if payload and not payload.get("ok") and payload.get("error"):
            logger.error("[%s] 子行程例外：\n%s", name, payload.get("traceback", ""))
            raise IsolatedTaskError(f"{name} 子行程失敗: {payload['error']}")
        detail = f"{name} 子行程結束碼 {returncode}"
        if returncode < 0:
            detail += f"（被訊號 {-returncode} 結束；likely OOM）"
        elif returncode == 0:
            detail += "（沒有寫出結果檔）"
        raise IsolatedTaskError(detail)


def _write_result(path: Path, payload: dict) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, default=str, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, path)


def _child_main(argv: list[str]) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Run one heavy task in an isolated child process.")
    parser.add_argument("--name", required=True)
    parser.add_argument("--target", required=True)
    parser.add_argument("--kwargs-file", required=True, type=Path)
    parser.add_argument("--result-file", required=True, type=Path)
    args = parser.parse_args(argv)

    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, level_name, logging.INFO),
        format=f"%(asctime)s [{args.name}-child] %(levelname)s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    # 先設 OOM 優先順序，再 import 重量級模組。
    set_oom_score_adj(1000)

    try:
        module_name, _, func_name = args.target.partition(":")
        if not module_name or not func_name:
            raise ValueError(f"target 格式需為 module:function，收到 {args.target!r}")
        func = getattr(importlib.import_module(module_name), func_name)
        kwargs = json.loads(args.kwargs_file.read_text(encoding="utf-8"))
        result = func(**kwargs)
    except BaseException as exc:  # noqa: BLE001 - 一律回報給主程序
        _write_result(args.result_file, {
            "ok": False,
            "error": _redact(f"{type(exc).__name__}: {exc}"),
            "traceback": _redact(traceback.format_exc()),
        })
        return 1
    _write_result(args.result_file, {"ok": True, "result": result})
    return 0


if __name__ == "__main__":
    sys.exit(_child_main(sys.argv[1:]))
