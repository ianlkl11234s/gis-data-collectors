"""cross_layer_map.yaml ↔ config.py collector 清冊一致性測試（AR-05）。

守門目標：config._COLLECTOR_TOGGLES 新增 collector 卻忘了同步
config/cross_layer_map.yaml 時，這條測試要變紅 —— 否則漏掉的 collector
會被 daily_report silent 略過、壞了沒人知道。

修法：跑 `python3 scripts/sync_cross_layer_map.py` 自動回填缺的 entry。
"""

import pytest

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


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
