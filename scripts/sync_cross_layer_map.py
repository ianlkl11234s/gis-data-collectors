#!/usr/bin/env python3
"""同步 config/cross_layer_map.yaml ↔ config.py 的 collector 清冊。

背景（AR-05）
--------------
`config.py` 的 ``_COLLECTOR_TOGGLES`` 是「有哪些 collector」的單一真相（SSOT）。
`config/cross_layer_map.yaml` 是 ``tasks/daily_report.py`` 跨層健康檢查掃描的清冊。
兩者一旦 drift（yaml 少收 collector），漏掉的 collector 就會 **silent fail** —
daily_report 完全不會掃它、壞了也沒人知道。本腳本負責消滅這種 drift。

SSOT 與各欄位怎麼推導
----------------------
- 「有哪些 collector」 → ``config._COLLECTOR_TOGGLES``（prefix + repo 預設 enabled / interval）
- prefix → yaml key（collector ``name``）與 required_env → ``collectors/registry.py``
- ``supabase_tables`` → ``storage/supabase_tables.py`` 的 ``TABLE_MAP``（history + current）
- ``deployment`` → 已知 3 個跑在 HiCloud VM 的例外；其餘 enabled→zeabur / disabled→disabled
  （這條規則可 100% 重現既有 41 筆 entry 的 deployment 值，故 sync 不會與人工值打架）

enabled 語意（重要）
---------------------
- **新補回**的 entry：``enabled`` 取自 ``config.py`` 的 **repo 預設**。
  Zeabur / VM 的環境變數（``{PREFIX}_ENABLED``）可在 runtime override，此處只記錄 repo 預設。
- **既有** entry 的 ``enabled`` 是人工維護的「production 應在跑」語意
  （例如 ship_ais / waste_positions repo 預設 false、但 production 開著 → yaml 記 true）。
  **本腳本不覆蓋既有 entry 的任何欄位**，只負責把「缺的 collector」補回來。

用法
----
    python3 scripts/sync_cross_layer_map.py            # 回填缺少的 collector（不動既有 entry）
    python3 scripts/sync_cross_layer_map.py --check    # 只檢查 drift：有缺 → 印出並 exit 1（給 CI / 測試用）
    python3 scripts/sync_cross_layer_map.py --dry-run  # 印出「會補哪些」但不寫檔

設計取捨
--------
- 只 **append** 缺的 entry 到檔尾自動管理區塊，逐字保留既有內容（含註解 / 分區）。
  既有 entry 的欄位（人工調過的 interval / enabled / critical / notes）一律不動。
- 無法機器推導的欄位（multi_table / reference / 無 SB 表的 supabase_tables、
  s3_prefixes 的 expected_daily）填保守預設並標 ``# TODO(sync): 人工確認``。
- 冪等：跑第二次時缺集合為空 → 什麼都不寫。
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# 允許從 repo 根目錄 import config / collectors / storage
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import yaml  # noqa: E402

import config  # noqa: E402
from collectors.registry import COLLECTOR_REGISTRY  # noqa: E402
from storage.supabase_tables import TABLE_MAP  # noqa: E402

CROSS_LAYER_YAML = _REPO_ROOT / "config" / "cross_layer_map.yaml"

# 已知跑在 HiCloud VM 的 collector（name）— 需 Taiwan IP，Zeabur 端強制關閉
HICLOUD_VM_COLLECTORS = frozenset({"ship_ais", "waste_positions", "cdc_public_health_weekly"})

# 自動管理區塊的標記（用來判斷是否已寫過區塊標頭，維持冪等）
_AUTO_SECTION_MARKER = "# 自動同步補回（scripts/sync_cross_layer_map.py）"

_AUTO_SECTION_HEADER = f"""
# ─────────────────────────────────────────────────────────
{_AUTO_SECTION_MARKER}
# 以下 entry 由 config._COLLECTOR_TOGGLES + collectors/registry.py
# + storage/supabase_tables.py TABLE_MAP 推導自動補回。
# enabled 為 config.py 的 repo 預設（Zeabur/VM env 可 override）。
# 標 TODO(sync) 的欄位無法機器推導，請人工複核後移除註解。
# 複核後可把 entry 搬進上方對應主題分區並手動微調 —
# sync 只補「缺的 collector」，不會再覆蓋既有 entry。
# ─────────────────────────────────────────────────────────
"""


# ────────────────────────────────────────────────────────────────────
# SSOT 讀取 + drift 計算
# ────────────────────────────────────────────────────────────────────
def load_config_collectors() -> dict[str, dict]:
    """回傳 {collector_name: {prefix, enabled, interval, required_env}}。

    以 config._COLLECTOR_TOGGLES 為 SSOT，透過 registry 把 prefix 對到 collector name。
    """
    by_prefix = {e.config_prefix: e for e in COLLECTOR_REGISTRY}
    out: dict[str, dict] = {}
    for prefix, enabled_default, interval_default in config._COLLECTOR_TOGGLES:
        entry = by_prefix.get(prefix)
        if entry is None:
            # toggle 沒有對應 registry entry → 無法推導 name/tables，用 prefix 小寫兜底並警告
            name = prefix.lower()
            print(
                f"⚠️  toggle {prefix} 在 collectors/registry.py 找不到對應 entry，"
                f"退回用 name={name}（tables 無法推導）",
                file=sys.stderr,
            )
            required_env: tuple[str, ...] = ()
        else:
            name = entry.cls.name
            required_env = entry.required_env
        out[name] = {
            "prefix": prefix,
            "enabled": bool(enabled_default),
            "interval": int(interval_default),
            "required_env": required_env,
        }
    return out


def load_yaml_keys() -> set[str]:
    """讀出 cross_layer_map.yaml 目前收了哪些 collector key。"""
    if not CROSS_LAYER_YAML.exists():
        return set()
    with open(CROSS_LAYER_YAML, encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return set(data.keys())


def compute_drift() -> tuple[list[str], list[str]]:
    """回傳 (missing, orphan)。

    - missing：config 有、yaml 沒有 → 會被 daily_report silent 略過的 collector（**要修**）
    - orphan ：yaml 有、config 沒有 → 可能是已從 config 移除但 yaml 忘了清（僅警告）
    """
    config_names = set(load_config_collectors().keys())
    yaml_keys = load_yaml_keys()
    missing = sorted(config_names - yaml_keys)
    orphan = sorted(yaml_keys - config_names)
    return missing, orphan


# ────────────────────────────────────────────────────────────────────
# 欄位推導
# ────────────────────────────────────────────────────────────────────
def derive_deployment(name: str, enabled: bool) -> str:
    """hicloud_vm（3 例外）/ zeabur（enabled）/ disabled（未 enabled）。

    此規則可 100% 重現既有 41 筆 entry 的 deployment 值。
    """
    if name in HICLOUD_VM_COLLECTORS:
        return "hicloud_vm"
    return "zeabur" if enabled else "disabled"


def derive_tables(name: str) -> list[str] | None:
    """從 TABLE_MAP 推導 supabase_tables；無法推導回傳 None。

    無法推導的情況：
      - name 不在 TABLE_MAP（例如 bake job / daily 彙總類無 SB 表）
      - is_multi_table / is_reference（表清單不在 TABLE_MAP，須人工列）
    """
    cfg = TABLE_MAP.get(name)
    if not cfg:
        return None
    if cfg.get("is_multi_table") or cfg.get("is_reference"):
        return None
    tables: list[str] = []
    if cfg.get("history"):
        tables.append(cfg["history"])
    if cfg.get("current"):
        tables.append(cfg["current"])
    return tables or None


def render_entry(name: str, meta: dict) -> str:
    """把單一 collector 推導成 cross_layer_map.yaml 的 entry 文字（2-space 縮排，比照既有風格）。"""
    enabled = meta["enabled"]
    deployment = derive_deployment(name, enabled)
    interval = meta["interval"]
    tables = derive_tables(name)

    lines = [f"{name}:"]
    lines.append(f"  enabled: {str(enabled).lower()}")
    lines.append(f"  deployment: {deployment}")
    lines.append(f"  expected_interval_min: {interval}")
    if tables:
        lines.append(f"  supabase_tables: [{', '.join(tables)}]")
    else:
        lines.append(
            "  supabase_tables: []  "
            "# TODO(sync): 人工確認 — 此 collector 無 TABLE_MAP 對應"
            "（multi_table / reference / 無 SB 表）"
        )
    lines.append("  s3_prefixes:")
    lines.append(
        f"    - {{prefix: {name}/archives/, expected_daily: false}}  "
        "# TODO(sync): 人工確認 expected_daily 與 prefix"
    )
    lines.append("  critical: false  # TODO(sync): 人工確認 criticality")
    lines.append("  notes: 自動補回（scripts/sync_cross_layer_map.py）— 欄位請人工複核")
    return "\n".join(lines)


# ────────────────────────────────────────────────────────────────────
# 回填
# ────────────────────────────────────────────────────────────────────
def backfill(dry_run: bool = False) -> list[str]:
    """把缺少的 collector 補進 yaml（append 到檔尾自動管理區塊）。回傳實際補的 name list。"""
    missing, _ = compute_drift()
    if not missing:
        return []

    config_collectors = load_config_collectors()
    blocks = [render_entry(name, config_collectors[name]) for name in missing]
    new_text = "\n\n".join(blocks) + "\n"

    if dry_run:
        return missing

    existing = CROSS_LAYER_YAML.read_text(encoding="utf-8")
    if not existing.endswith("\n"):
        existing += "\n"

    parts = [existing]
    if _AUTO_SECTION_MARKER not in existing:
        parts.append(_AUTO_SECTION_HEADER)
    parts.append("\n")
    parts.append(new_text)

    merged = "".join(parts)

    # 寫入前先確認整份仍是合法 YAML，避免補壞
    yaml.safe_load(merged)

    CROSS_LAYER_YAML.write_text(merged, encoding="utf-8")
    return missing


# ────────────────────────────────────────────────────────────────────
# CLI
# ────────────────────────────────────────────────────────────────────
def _print_drift(missing: list[str], orphan: list[str]) -> None:
    if missing:
        print(f"❌ cross_layer_map.yaml 缺 {len(missing)} 個 collector（daily_report 會 silent 略過）：")
        for name in missing:
            print(f"   - {name}")
    if orphan:
        print(f"⚠️  yaml 有 {len(orphan)} 個 collector 已不在 config._COLLECTOR_TOGGLES（orphan，建議人工確認）：")
        for name in orphan:
            print(f"   - {name}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="同步 cross_layer_map.yaml ↔ config.py collector 清冊")
    parser.add_argument(
        "--check", action="store_true",
        help="只檢查 drift，有缺 collector 就 exit 1（CI / 測試用），不寫檔",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="印出會補哪些 collector 但不寫檔",
    )
    args = parser.parse_args(argv)

    missing, orphan = compute_drift()

    if args.check:
        if missing:
            _print_drift(missing, orphan)
            print("\n→ 跑 `python3 scripts/sync_cross_layer_map.py` 自動回填。")
            return 1
        if orphan:
            _print_drift(missing, orphan)
        print("✅ cross_layer_map.yaml 與 config._COLLECTOR_TOGGLES 一致（無缺漏）。")
        return 0

    if not missing:
        if orphan:
            _print_drift(missing, orphan)
        print("✅ 無 drift，cross_layer_map.yaml 已收齊所有 collector。")
        return 0

    added = backfill(dry_run=args.dry_run)
    if args.dry_run:
        print(f"[dry-run] 會補回 {len(added)} 個 collector：")
        for name in added:
            print(f"   + {name}")
    else:
        print(f"✅ 已補回 {len(added)} 個 collector 到 {CROSS_LAYER_YAML.name}：")
        for name in added:
            print(f"   + {name}")
        print("\n⚠️  請人工複核標 TODO(sync) 的欄位（supabase_tables / s3_prefixes / critical）。")
    if orphan:
        _print_drift([], orphan)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
