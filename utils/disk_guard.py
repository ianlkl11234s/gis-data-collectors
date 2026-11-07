"""本機磁碟護欄：量 LOCAL_DATA_DIR 用量與磁碟剩餘空間，分「警告／嚴重」兩級告警。

為什麼：原始檔只有在 S3 驗證成功（.archive-receipts）後才會被 archive 刪掉；S3 斷線
或歸檔一直失敗時本機會持續長大，直到磁碟滿、所有 collector 一起寫入失敗。

**只會自動清「本來就可丟」或「已驗證歸檔」的東西**（clean_safe）：
- LOCAL_DATA_DIR/tmp/** 超過 DISK_GUARD_TMP_MAX_AGE_HOURS 的暫存檔（備份／下載暫存；
  正在使用的暫存檔一定比這個新）
- LOCAL_DATA_DIR 根目錄 `.tmp_*.tar.gz`（archive 打包暫存）超過同一時限
- LOCAL_DATA_DIR/.memwatch/** 超過 DISK_GUARD_MEMWATCH_KEEP_DAYS 的觀測紀錄
- .archive-receipts/<collector>/<YYYY-MM-DD>.json：對應的日期目錄已經不存在、且日期早於
  max(DISK_GUARD_RECEIPT_KEEP_DAYS, 該 collector 保留天數 + 7) 天。
  依據：receipt 只在 tasks/archive.py 處理「仍存在的日期目錄」時讀取（決定跳過上傳、
  決定可否刪除）；目錄被刪之後沒有任何程式再讀它。即使日後同日期目錄重新出現，
  archive 也會因 members 不符而重新向 S3 驗證，與 receipt 不存在時的路徑相同。

**絕不自動刪除尚未有 receipt 驗證的原始資料**。超過嚴重門檻時只告警（附最大的目錄），
後續處置（暫停寫原始檔等）由人決定。
"""

from __future__ import annotations

import logging
import os
import shutil
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path

import config

logger = logging.getLogger(__name__)

LEVELS = ("ok", "warn", "critical")
_GB = 1024 ** 3


@dataclass
class GuardConfig:
    data_warn_gb: float = 0.0       # 0＝不檢查本機資料總量
    data_crit_gb: float = 0.0
    free_warn_pct: float = 15.0     # 磁碟剩餘 < 此百分比＝警告
    free_crit_pct: float = 5.0
    tmp_max_age_hours: float = 24.0
    memwatch_keep_days: int = 14
    receipt_keep_days: int = 30
    realert_hours: float = 6.0

    @classmethod
    def from_config(cls) -> "GuardConfig":
        return cls(
            data_warn_gb=config.DISK_GUARD_DATA_WARN_GB,
            data_crit_gb=config.DISK_GUARD_DATA_CRIT_GB,
            free_warn_pct=config.DISK_GUARD_FREE_WARN_PCT,
            free_crit_pct=config.DISK_GUARD_FREE_CRIT_PCT,
            tmp_max_age_hours=config.DISK_GUARD_TMP_MAX_AGE_HOURS,
            memwatch_keep_days=config.DISK_GUARD_MEMWATCH_KEEP_DAYS,
            receipt_keep_days=config.DISK_GUARD_RECEIPT_KEEP_DAYS,
            realert_hours=config.DISK_GUARD_REALERT_HOURS,
        )


@dataclass
class DiskStatus:
    level: str
    data_bytes: int
    free_pct: float
    free_bytes: int
    reasons: list[str] = field(default_factory=list)
    top_dirs: list[tuple[str, int]] = field(default_factory=list)
    cleaned: dict = field(default_factory=dict)


def dir_sizes(data_dir: Path) -> tuple[int, dict[str, int]]:
    """回傳 (總 bytes, {頂層名稱: bytes})；不跟 symlink，讀不到的檔略過。"""
    total = 0
    per_top: dict[str, int] = {}
    if not data_dir.exists():
        return 0, {}
    for entry in os.scandir(data_dir):
        size = _tree_size(Path(entry.path)) if entry.is_dir(follow_symlinks=False) else _file_size(entry)
        per_top[entry.name] = size
        total += size
    return total, per_top


def _file_size(entry) -> int:
    try:
        return entry.stat(follow_symlinks=False).st_size
    except OSError:
        return 0


def _tree_size(root: Path) -> int:
    total = 0
    stack = [root]
    while stack:
        try:
            it = os.scandir(stack.pop())
        except OSError:
            continue
        with it:
            for entry in it:
                if entry.is_dir(follow_symlinks=False):
                    stack.append(Path(entry.path))
                else:
                    total += _file_size(entry)
    return total


def evaluate(data_bytes: int, free_pct: float, cfg: GuardConfig) -> tuple[str, list[str]]:
    level, reasons = "ok", []

    def bump(new: str, why: str):
        nonlocal level
        reasons.append(why)
        if LEVELS.index(new) > LEVELS.index(level):
            level = new

    gb = data_bytes / _GB
    if cfg.data_crit_gb > 0 and gb >= cfg.data_crit_gb:
        bump("critical", f"本機資料 {gb:.1f} GB ≥ 嚴重門檻 {cfg.data_crit_gb:g} GB")
    elif cfg.data_warn_gb > 0 and gb >= cfg.data_warn_gb:
        bump("warn", f"本機資料 {gb:.1f} GB ≥ 警告門檻 {cfg.data_warn_gb:g} GB")
    if free_pct < cfg.free_crit_pct:
        bump("critical", f"磁碟剩餘 {free_pct:.1f}% < 嚴重門檻 {cfg.free_crit_pct:g}%")
    elif free_pct < cfg.free_warn_pct:
        bump("warn", f"磁碟剩餘 {free_pct:.1f}% < 警告門檻 {cfg.free_warn_pct:g}%")
    return level, reasons


def _unlink_older_than(root: Path, cutoff_ts: float, pattern: str = "*") -> int:
    n = 0
    if not root.exists():
        return 0
    for p in root.rglob(pattern):
        try:
            if p.is_file() and not p.is_symlink() and p.stat().st_mtime < cutoff_ts:
                p.unlink()
                n += 1
        except OSError:
            continue
    return n


def prune_archive_receipts(data_dir: Path, keep_days: int, today: date | None = None) -> int:
    """刪除「日期目錄已不存在且夠舊」的 archive receipt（判斷依據見模組 docstring）。"""
    today = today or datetime.now().date()
    root = data_dir / ".archive-receipts"
    if not root.exists():
        return 0
    removed = 0
    for collector_dir in root.iterdir():
        if not collector_dir.is_dir():
            continue
        try:
            retention = int(config.get_retention_days(collector_dir.name))
        except Exception:
            retention = int(getattr(config, "ARCHIVE_RETENTION_DAYS", 7))
        min_age = max(int(keep_days), retention + 7)
        for receipt in collector_dir.glob("*.json"):
            try:
                d = datetime.strptime(receipt.stem, "%Y-%m-%d").date()
            except ValueError:
                continue
            if (today - d).days <= min_age:
                continue
            date_dir = data_dir / collector_dir.name / f"{d:%Y}" / f"{d:%m}" / f"{d:%d}"
            if date_dir.exists():
                continue  # 原始檔還在 → receipt 仍有用（決定能否刪原始檔）
            try:
                receipt.unlink()
                removed += 1
            except OSError:
                continue
    return removed


def clean_safe(data_dir: Path, cfg: GuardConfig, now_ts: float | None = None) -> dict:
    """只清可丟／已驗證的輔助檔；回傳各類刪除數。"""
    now_ts = time.time() if now_ts is None else now_ts
    tmp_cutoff = now_ts - cfg.tmp_max_age_hours * 3600
    return {
        "tmp": _unlink_older_than(data_dir / "tmp", tmp_cutoff)
        + (_unlink_older_than_flat(data_dir, tmp_cutoff, ".tmp_*.tar.gz")),
        "memwatch": _unlink_older_than(data_dir / ".memwatch", now_ts - cfg.memwatch_keep_days * 86400),
        "receipts": prune_archive_receipts(
            data_dir, cfg.receipt_keep_days, datetime.fromtimestamp(now_ts).date()),
    }


def _unlink_older_than_flat(root: Path, cutoff_ts: float, pattern: str) -> int:
    n = 0
    if not root.exists():
        return 0
    for p in root.glob(pattern):
        try:
            if p.is_file() and p.stat().st_mtime < cutoff_ts:
                p.unlink()
                n += 1
        except OSError:
            continue
    return n


def check(data_dir: Path, cfg: GuardConfig, now_ts: float | None = None) -> DiskStatus:
    cleaned = clean_safe(data_dir, cfg, now_ts)
    data_bytes, per_top = dir_sizes(data_dir)
    usage = shutil.disk_usage(data_dir if data_dir.exists() else data_dir.parent)
    free_pct = usage.free / usage.total * 100 if usage.total else 100.0
    level, reasons = evaluate(data_bytes, free_pct, cfg)
    top = sorted(per_top.items(), key=lambda kv: kv[1], reverse=True)[:5]
    return DiskStatus(level=level, data_bytes=data_bytes, free_pct=free_pct,
                      free_bytes=usage.free, reasons=reasons, top_dirs=top, cleaned=cleaned)


class DiskGuard:
    """定期執行（排程 task）；只在等級升高、嚴重持續超過 realert_hours、或恢復時發通知。"""

    def __init__(self, data_dir: Path | None = None, cfg: GuardConfig | None = None, notify=None):
        self.data_dir = Path(data_dir or config.LOCAL_DATA_DIR)
        self.cfg = cfg or GuardConfig.from_config()
        self._notify = notify
        self.last_level = "ok"
        self._last_alert_ts = 0.0
        self.last_status: DiskStatus | None = None

    def _send(self, text: str) -> None:
        if self._notify is not None:
            self._notify(text)
            return
        try:
            from utils.notify import send_telegram, _instance_tag
            send_telegram(text.replace("{tag}", _instance_tag()))
        except Exception as exc:  # 通知失敗不影響 collector
            logger.warning("disk guard 通知失敗: %s", exc)

    def run(self, now_ts: float | None = None) -> dict:
        now_ts = time.time() if now_ts is None else now_ts
        st = check(self.data_dir, self.cfg, now_ts)
        self.last_status = st
        if any(st.cleaned.values()):
            logger.info("disk guard 清理：%s", st.cleaned)
        prev = self.last_level
        rank, prev_rank = LEVELS.index(st.level), LEVELS.index(prev)
        should = (
            rank > prev_rank
            or (st.level == "critical"
                and now_ts - self._last_alert_ts >= self.cfg.realert_hours * 3600)
            or (st.level == "ok" and prev != "ok")
        )
        if should:
            self._send(self._format(st))
            self._last_alert_ts = now_ts
        self.last_level = st.level
        return {
            "level": st.level, "data_gb": round(st.data_bytes / _GB, 2),
            "free_pct": round(st.free_pct, 1), "cleaned": st.cleaned, "alerted": should,
        }

    @staticmethod
    def _format(st: DiskStatus) -> str:
        icon = {"ok": "✅", "warn": "⚠️", "critical": "🚨"}[st.level]
        title = {"ok": "本機磁碟恢復正常", "warn": "本機磁碟警告", "critical": "本機磁碟嚴重"}[st.level]
        lines = [f"{icon} *{title}*{{tag}}\n",
                 f"本機資料: {st.data_bytes / _GB:.1f} GB",
                 f"磁碟剩餘: {st.free_bytes / _GB:.1f} GB（{st.free_pct:.1f}%）"]
        lines += [f"• {r}" for r in st.reasons]
        if st.level != "ok" and st.top_dirs:
            lines.append("\n最大的目錄:")
            lines += [f"• {name.replace('_', chr(92) + '_')}: {size / _GB:.2f} GB"
                      for name, size in st.top_dirs]
            lines.append("\n未歸檔的原始檔不會自動刪除；請檢查 S3 歸檔是否失敗。")
        return "\n".join(lines)
