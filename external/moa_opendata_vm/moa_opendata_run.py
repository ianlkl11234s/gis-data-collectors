#!/usr/bin/env python3
"""農業部開放資料（data.moa.gov.tw）collector 的 HiCloud VM runner。

2026-10-23 起 data.moa.gov.tw 擋 Zeabur（JP IP → TCP ConnectTimeout），台灣 VM 可連。
本 runner **不複製邏輯**：直接 import 主 repo 的 collector class，呼叫同一個
``BaseCollector.run()``，所以 LocalStorage、SupabaseWriter、collector_status 心跳、
run ledger 與 S3 歸檔都跟 Zeabur 上完全相同。

用法（VM 上，cron 先 ``set -a; . ./.env; set +a``）::

    python3 moa_opendata_run.py --all-due        # 依本地 state 只跑到期的（cron 用）
    python3 moa_opendata_run.py food_prices      # 指定名稱：不看 due，直接跑一次
    python3 moa_opendata_run.py --archive        # tasks/archive.py：tar.gz → S3 + 清本地
    python3 moa_opendata_run.py --dry-check      # 只 import／列排程，不連 DB、不寫 S3

* ``*_ENABLED`` toggle 不影響本 runner（toggle 只有 Zeabur 的 registry/main.py 在看）；
  Zeabur 端要設 false 防雙跑。
* 每個 collector 在獨立子行程跑（1 vCPU／~380MB 可用 RAM：大型 JSON 用完即還給 OS）。
* 單一 app 級 flock：搶不到 lock 直接 exit 3，不排隊。
* 任何一個 collector 回傳 error → exit 1（cron log 可見），且不更新 state，下一輪再試。
"""

from __future__ import annotations

import argparse
import fcntl
import json
import logging
import os
import subprocess
import sys
import types
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

TAIPEI_TZ = timezone(timedelta(hours=8))
HERE = Path(__file__).resolve().parent

# VM 上 runner 與 config.py 同層（/opt/moa-opendata/）；在 repo 內則是 external/moa_opendata_vm/。
APP_DIR = Path(os.environ.get("MOA_APP_DIR") or (HERE if (HERE / "config.py").exists() else HERE.parents[1]))

# 名稱 → (module, class)。順序即 --all-due 的執行順序。
COLLECTORS: dict[str, tuple[str, str]] = {
    "animal_adoption": ("collectors.animal_adoption", "AnimalAdoptionCollector"),
    "food_prices": ("collectors.food_prices", "FoodPricesCollector"),
    "animal_shelter_outcomes": ("collectors.animal_shelter_outcomes", "AnimalShelterOutcomesCollector"),
    "animal_shelter_pressure": ("collectors.animal_shelter_outcomes", "AnimalShelterPressureCollector"),
    "animal_veterinary_clinics": ("collectors.animal_welfare_points", "AnimalVeterinaryClinicsCollector"),
    "animal_licensed_pet_businesses": ("collectors.animal_welfare_points", "AnimalLicensedPetBusinessesCollector"),
    "animal_protection_offices": ("collectors.animal_welfare_points", "AnimalProtectionOfficesCollector"),
}

# 允許 cron 漂移／前一輪執行時間：elapsed ≥ interval × 0.9 即視為到期。
DUE_FRACTION = 0.9
CHILD_TIMEOUT_SEC = int(os.environ.get("MOA_CHILD_TIMEOUT_SEC", "1500"))

EXIT_OK, EXIT_FAILED, EXIT_USAGE, EXIT_LOCKED = 0, 1, 2, 3


def bootstrap_import_path(app_dir: Path = APP_DIR) -> None:
    """讓 ``collectors``／``tasks`` 以「空殼」package 載入，跳過它們重量級的 ``__init__``。

    ``collectors/__init__.py`` 會 import registry → 全部 collector（numpy、xarray…）；
    ``tasks/__init__.py`` 會 import gfw_hourly_publish、backup_supabase（yaml、scripts.*）。
    VM 只需要這 7 個 collector 與 archive/mini_taipei_publish（supabase_writer 頂層 import）。
    """
    if str(app_dir) not in sys.path:
        sys.path.insert(0, str(app_dir))
    for name in ("collectors", "tasks"):
        existing = sys.modules.get(name)
        if existing is not None and getattr(existing, "__file__", None):
            continue  # 已經以完整 package 載入（例如在 repo 內跑 pytest），不覆蓋
        pkg = types.ModuleType(name)
        pkg.__path__ = [str(app_dir / name)]
        sys.modules[name] = pkg


def state_dir() -> Path:
    base = os.environ.get("MOA_STATE_DIR")
    if base:
        return Path(base)
    data_dir = os.environ.get("DATA_DIR")
    # state 放在 DATA_DIR 的上一層（/var/lib/moa-opendata/state），不讓 archive.py 掃到
    return (Path(data_dir).parent if data_dir else APP_DIR) / "state"


def load_state(path: Path) -> dict[str, str]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return {k: v for k, v in data.items() if isinstance(v, str)} if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def save_state(path: Path, state: dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def is_due(last_success_iso: str | None, interval_minutes: int, now: datetime) -> bool:
    """無紀錄（或紀錄壞掉）→ 到期；否則 elapsed ≥ interval × DUE_FRACTION 才到期。"""
    if not last_success_iso:
        return True
    try:
        last = datetime.fromisoformat(last_success_iso)
    except ValueError:
        return True
    if last.tzinfo is None:
        last = last.replace(tzinfo=TAIPEI_TZ)
    return (now - last) >= timedelta(minutes=interval_minutes * DUE_FRACTION)


@contextmanager
def app_lock(path: Path):
    """非阻塞 flock；拿不到 yield False。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    fh = open(path, "a+")
    try:
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield False
            return
        try:
            yield True
        finally:
            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
    finally:
        fh.close()


def interval_of(name: str) -> int:
    import config  # noqa: PLC0415 - config 讀環境變數，延後到 bootstrap 之後
    return int(getattr(config, f"{name.upper()}_INTERVAL"))


def load_collector_class(name: str):
    import importlib  # noqa: PLC0415
    module_name, class_name = COLLECTORS[name]
    return getattr(importlib.import_module(module_name), class_name)


# ── 子行程：真正跑 collector ──────────────────────────────────────────────

def child_run(name: str) -> int:
    cls = load_collector_class(name)
    result = cls().run()
    if result.get("error"):
        print(f"[moa_opendata_run] ✗ {name}: {result['error']}", flush=True)
        return EXIT_FAILED
    print(f"[moa_opendata_run] ✓ {name}", flush=True)
    return EXIT_OK


def child_flush_buffer() -> int:
    """Zeabur main.py 每 5 分鐘 flush buffer；VM 一次性 runner 改在每輪開頭補寫。"""
    from collectors.base import get_supabase_writer  # noqa: PLC0415
    writer = get_supabase_writer()
    if writer is None:
        print("[moa_opendata_run] ⚠ Supabase writer 未啟用（SUPABASE_ENABLED／SUPABASE_DB_URL），跳過 buffer flush")
        return EXIT_OK
    writer.flush_buffer()
    return EXIT_OK


def child_archive() -> int:
    from tasks.archive import ArchiveTask  # noqa: PLC0415
    result = ArchiveTask().run()
    if result is None:  # S3 未設定或 ARCHIVE_ENABLED=false
        return EXIT_FAILED
    return EXIT_FAILED if result["archive"]["failed"] else EXIT_OK


def spawn(args: list[str]) -> int:
    cmd = [sys.executable, str(Path(__file__).resolve()), *args]
    try:
        return subprocess.run(cmd, timeout=CHILD_TIMEOUT_SEC, check=False).returncode
    except subprocess.TimeoutExpired:
        print(f"[moa_opendata_run] ✗ {' '.join(args)} 超過 {CHILD_TIMEOUT_SEC}s，已中止", flush=True)
        return EXIT_FAILED


def buffer_has_files() -> bool:
    data_dir = os.environ.get("DATA_DIR")
    return bool(data_dir) and any((Path(data_dir) / "buffer").glob("*.json"))


# ── 父行程：排程、lock、state ─────────────────────────────────────────────

def plan(names: list[str], all_due: bool, now: datetime, state: dict[str, str]) -> list[str]:
    if names:
        return names
    if not all_due:
        return []
    return [n for n in COLLECTORS if is_due(state.get(n), interval_of(n), now)]


def run_collectors(selected: list[str], state_path: Path, runner=spawn) -> int:
    if buffer_has_files():
        runner(["--_child-flush-buffer"])
    exit_code = EXIT_OK
    for name in selected:
        rc = runner(["--_child-run", name])
        if rc == EXIT_OK:
            state = load_state(state_path)
            state[name] = datetime.now(TAIPEI_TZ).isoformat(timespec="seconds")
            save_state(state_path, state)
        else:
            exit_code = EXIT_FAILED
    return exit_code


def dry_check(state_path: Path) -> int:
    """只 import + 實例化 + 印排程；DATA_DIR 會被 mkdir，不連 DB、不碰 S3。"""
    import config  # noqa: PLC0415
    now = datetime.now(TAIPEI_TZ)
    state = load_state(state_path)
    print(f"APP_DIR={APP_DIR}  DATA_DIR={config.LOCAL_DATA_DIR}  state={state_path}")
    print(f"SUPABASE_ENABLED={config.SUPABASE_ENABLED}  SUPABASE_DB_URL set={bool(config.SUPABASE_DB_URL)}  "
          f"S3_BUCKET set={bool(config.S3_BUCKET)}")
    # psycopg2 ThreadedConnectionPool 會在建構時就連滿 minconn；dry-check 不建 writer。
    config.SUPABASE_ENABLED = False
    for name in COLLECTORS:
        cls = load_collector_class(name)
        collector = cls()
        assert collector.name == name, (collector.name, name)
        due = is_due(state.get(name), collector.interval_minutes, now)
        print(f"  {name:32s} interval={collector.interval_minutes:>6}m  "
              f"last_success={state.get(name, '-'):25s} due={due}")
    return EXIT_OK


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("names", nargs="*", help=f"collector 名稱：{', '.join(COLLECTORS)}")
    parser.add_argument("--all-due", action="store_true", help="只跑到期的 collector（cron 用）")
    parser.add_argument("--archive", action="store_true", help="執行 tasks/archive.py 歸檔與本地清理")
    parser.add_argument("--dry-check", action="store_true", help="只 import／列排程")
    parser.add_argument("--_child-run", dest="child_run", help=argparse.SUPPRESS)
    parser.add_argument("--_child-flush-buffer", dest="child_flush", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--_child-archive", dest="child_archive", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    bootstrap_import_path()

    if args.child_run:
        return child_run(args.child_run)
    if args.child_flush:
        return child_flush_buffer()
    if args.child_archive:
        return child_archive()

    unknown = [n for n in args.names if n not in COLLECTORS]
    if unknown:
        parser.error(f"未知 collector：{unknown}")
    state_path = state_dir() / "last_success.json"
    if args.dry_check:
        return dry_check(state_path)
    if not (args.names or args.all_due or args.archive):
        parser.print_help()
        return EXIT_USAGE

    with app_lock(state_dir() / "run.lock") as acquired:
        if not acquired:
            print("[moa_opendata_run] 另一個 run 仍持有 lock，本輪不執行", flush=True)
            return EXIT_LOCKED
        if args.archive:
            return spawn(["--_child-archive"])
        selected = plan(args.names, args.all_due, datetime.now(TAIPEI_TZ), load_state(state_path))
        print(f"[moa_opendata_run] {datetime.now(TAIPEI_TZ):%F %T} 本輪：{selected or '（無到期）'}", flush=True)
        return run_collectors(selected, state_path)


if __name__ == "__main__":
    sys.exit(main())
