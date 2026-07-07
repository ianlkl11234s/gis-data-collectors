"""cross_layer_map.yaml ↔ config.py collector 清冊一致性測試（AR-05）。

守門目標：config._COLLECTOR_TOGGLES 新增 collector 卻忘了同步
config/cross_layer_map.yaml 時，這條測試要變紅 —— 否則漏掉的 collector
會被 daily_report silent 略過、壞了沒人知道。

修法：跑 `python3 scripts/sync_cross_layer_map.py` 自動回填缺的 entry。
"""

from pathlib import Path

import pytest
import yaml

from scripts.sync_cross_layer_map import (
    compute_drift,
    derive_deployment,
    derive_tables,
    load_config_collectors,
    load_yaml_keys,
)


def test_no_missing_collectors():
    """每個 config._COLLECTOR_TOGGLES 的 collector 都要在 cross_layer_map.yaml 有 entry。

    有缺 → daily_report 會 silent 略過 → 補 yaml（或跑 sync 腳本）。
    """
    missing, _orphan = compute_drift()
    assert not missing, (
        f"cross_layer_map.yaml 缺 {len(missing)} 個 collector：{missing}\n"
        "→ 跑 `python3 scripts/sync_cross_layer_map.py` 自動回填。"
    )


def test_no_orphan_entries():
    """yaml 不該有 config._COLLECTOR_TOGGLES 已移除的 collector（orphan）。

    orphan 代表 collector 從 config 拿掉但 yaml 忘了清，monitoring 會查一個不存在的 collector。
    """
    _missing, orphan = compute_drift()
    assert not orphan, (
        f"cross_layer_map.yaml 有 {len(orphan)} 個 orphan（config 已無此 collector）：{orphan}\n"
        "→ 人工確認後從 yaml 移除。"
    )


def test_config_collectors_map_to_registry_names():
    """每個 toggle 都能對到 registry 的 collector name（sync 靠這個推導 yaml key）。"""
    collectors = load_config_collectors()
    # load 過程若有 toggle 對不到 registry 會走 prefix 小寫兜底；這裡確保沒有那種情況
    yaml_keys = load_yaml_keys()
    # 所有 config collector name 都應在 yaml（已 sync 過），順帶驗證 name 對得起來
    assert set(collectors) <= yaml_keys


def test_derive_deployment_reproduces_convention():
    """deployment 推導規則要能重現既有 41 筆 entry 的慣例。"""
    # HiCloud VM 3 例外
    assert derive_deployment("ship_ais", True) == "hicloud_vm"
    assert derive_deployment("waste_positions", True) == "hicloud_vm"
    assert derive_deployment("cdc_public_health_weekly", True) == "hicloud_vm"
    # enabled → zeabur，未 enabled → disabled
    assert derive_deployment("youbike", True) == "zeabur"
    assert derive_deployment("flight_fr24", False) == "disabled"


def test_derive_tables_from_table_map():
    """supabase_tables 從 TABLE_MAP 推導：一般表回 list、multi_table/無對應回 None。"""
    # 一般 history+current
    assert derive_tables("road_congestion") == [
        "realtime.road_sections_live",
        "realtime.road_sections_current",
    ]
    # is_multi_table → 無法推導
    assert derive_tables("power_taipower") is None
    # 不在 TABLE_MAP → None
    assert derive_tables("global_climate_bake") is None


# ─────────────────────────────────────────────────────────
# cross_layer_map.yaml ↔ realtime_tables.yaml 同步守門（2026-07-07 稽核補上）
# ─────────────────────────────────────────────────────────

# 刻意不列入 realtime_tables.yaml 的表（每張都有 current / 心跳表代為監控，
# 或本質恆空）。新增豁免前先確認：該表壞掉時，誰會發現？
_REALTIME_TABLES_EXEMPT = {
    "realtime.market_index_tick":              # tick 表大、MAX() 貴；由 market_index_current 代監控
        "market_index_current 代監控",
    "realtime.tourist_shuttle_positions":      # 由 tourist_shuttle_current 代監控
        "tourist_shuttle_current 代監控",
    "realtime.parking_lots_availability":      # 由 parking_lots_current 代監控
        "parking_lots_current 代監控",
    "realtime.parking_segments_availability":  # 由 parking_segments_current 代監控
        "parking_segments_current 代監控",
    "public.drought_alert_history":            # 與 current 同批寫入（hash 去重、事件驅動），單獨列只會 DEAD 噪音
        "drought_alert_current 代監控",
    "realtime.road_sections_live":             # MAX() 會 timeout；只監控 road_sections_current
        "road_sections_current 代監控",
    "realtime.yt_live_history":                # 由 yt_live_current 代監控
        "yt_live_current 代監控",
    "realtime.nuclear_radiation_measurements": # 由 nuclear_radiation_stations（updated_at touch 心跳）代監控
        "nuclear_radiation_stations 代監控",
    "realtime.flight_trails":                  # partitioned parent 恆空，NEVER 是預期，已刻意移除
        "partitioned parent 恆空，刻意移除",
}

_CONFIG_DIR = Path(__file__).resolve().parent.parent / "config"


def _load_yaml(name: str):
    with open(_CONFIG_DIR / name, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def test_supabase_tables_covered_by_realtime_tables():
    """enabled collector 的每張 supabase 表都要在 realtime_tables.yaml（或豁免名單）。

    漏列 → daily_report 的 RPC 新鮮度檢查（_section_supabase_realtime）撈不到
    該表 → 壞了沒人知道。新增 collector 忘了步驟 8（補 realtime_tables.yaml）
    時，這條測試要變紅。

    只檢查 enabled collector：disabled 的表本來就刻意從 realtime_tables 移除，
    避免天天顯示 DEAD 干擾（見 realtime_tables.yaml 內 drought_alert 註解）。
    重新啟用 collector 時本測試會轉紅，逼你把表加回清冊 —— 這正是守門目的。
    """
    cmap = _load_yaml("cross_layer_map.yaml")
    rt = _load_yaml("realtime_tables.yaml").get("tables", [])
    rt_set = {f"{t['schema']}.{t['table']}" for t in rt}

    missing = []
    for name, cfg in cmap.items():
        if not cfg.get("enabled"):
            continue
        for table in cfg.get("supabase_tables") or []:
            if table not in rt_set and table not in _REALTIME_TABLES_EXEMPT:
                missing.append(f"{name}: {table}")

    assert not missing, (
        f"以下 {len(missing)} 張表在 cross_layer_map.yaml 有列但 realtime_tables.yaml 沒有：\n  "
        + "\n  ".join(missing)
        + "\n→ 補進 config/realtime_tables.yaml（daily_report 新鮮度檢查靠它），"
        "或確認有 current/心跳表代監控後加入本檔 _REALTIME_TABLES_EXEMPT 並附理由。"
    )


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
