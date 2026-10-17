#!/usr/bin/env python3
"""產生 docs/handbook/collectors.md（collector 清單）。

資料來源（都是 repo 內機器可讀的權威來源，本腳本不手寫任何 collector 事實）：
- collectors/registry.py           COLLECTOR_REGISTRY：有哪些 collector、prefix、required_env、是否長駐
- config.py                         _COLLECTOR_TOGGLES：repo 預設 enabled／interval（不讀環境變數覆寫）
- config/cross_layer_map.yaml       production 實況：enabled、deployment、監控期望間隔、寫入表、notes
- storage/supabase_tables.py        TABLE_MAP：資料層（history／current／多表／reference）
- config/realtime_tables.yaml       表級新鮮度監控清單（判斷「無表級監控」）

唯一手寫的是 SOURCES_DOC：collector → docs/handbook/sources/<分類>.md。新增 collector
沒有登記分類時，本腳本會直接失敗並列出名字。

設計限制：
- 純靜態解析（ast / yaml），不 import config.py 或 collectors（避免 load_dotenv 讀 .env、
  避免依賴環境變數與網路），CI 可直接跑。
- 輸出不含時間戳，同一份程式碼永遠產出相同內容，--check 才有意義。

用法：
    python3 scripts/gen_handbook_catalog.py           # 重產 docs/handbook/collectors.md
    python3 scripts/gen_handbook_catalog.py --check   # 只比對；與現有檔不同 → exit 1
"""
from __future__ import annotations

import argparse
import ast
import importlib.util
import sys
from collections import defaultdict
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
REGISTRY_PY = REPO_ROOT / "collectors" / "registry.py"
CONFIG_PY = REPO_ROOT / "config.py"
CLM_YAML = REPO_ROOT / "config" / "cross_layer_map.yaml"
REALTIME_YAML = REPO_ROOT / "config" / "realtime_tables.yaml"
TABLES_PY = REPO_ROOT / "storage" / "supabase_tables.py"
OUTPUT_MD = REPO_ROOT / "docs" / "handbook" / "collectors.md"

# ────────────────────────────────────────────────────────────────────
# 唯一手寫的對照：collector → sources 文件（分類）
# 新增 collector 時在這裡加一行，並在對應 sources/*.md 加一段 `### <name>`。
# ────────────────────────────────────────────────────────────────────
SOURCES_LABEL = {
    "tdx": "TDX 交通",
    "cwa-ncdr": "氣象署／NCDR",
    "water-env": "水利／環境",
    "marine-air-space": "海空太空",
    "jma-global": "日本／全球氣候",
    "internet-health": "網路健康",
    "gov-misc": "政府雜項",
}

SOURCES_DOC = {
    # TDX 交通
    "youbike": "tdx", "vd": "tdx", "freeway_vd": "tdx", "road_congestion": "tdx",
    "parking": "tdx", "parking_offstreet": "tdx", "parking_ref": "tdx",
    "bus": "tdx", "bus_intercity": "tdx", "tourist_shuttle": "tdx",
    "tra_train": "tdx", "tra_static": "tdx", "rail_timetable": "tdx",
    "road_event_live": "tdx", "road_event_planned": "tdx",
    # 氣象署／NCDR（含共用海象表的 ISOHE、共用落雷表的台電落雷）
    "weather": "cwa-ncdr", "temperature": "cwa-ncdr", "rain_gauge_realtime": "cwa-ncdr",
    "earthquake": "cwa-ncdr", "earthquake_catalog": "cwa-ncdr",
    "earthquake_town_intensity": "cwa-ncdr", "earthquake_shakemap_grid": "cwa-ncdr",
    "earthquake_moment_tensor": "cwa-ncdr", "cwa_satellite": "cwa-ncdr",
    "cwa_marine_observation": "cwa-ncdr", "isohe_port_marine": "cwa-ncdr",
    "ncdr_alerts": "cwa-ncdr", "lightning_cwa": "cwa-ncdr", "lightning_events": "cwa-ncdr",
    "cwa_uv_daily": "cwa-ncdr",
    # 水利／環境
    "water_reservoir": "water-env", "river_water_level": "water-env",
    "groundwater_level": "water-env", "water_reservoir_daily_ops": "water-env",
    "wra_drought_alert": "water-env", "iot_wra": "water-env", "uswg": "water-env",
    "precipitation_raster": "water-env", "wic_sewer": "water-env",
    "wic_evacuate": "water-env", "wic_pumb": "water-env",
    "air_quality": "water-env", "air_quality_imagery": "water-env",
    "air_quality_microsensors": "water-env", "water_effluent_monitoring": "water-env",
    "cems_stack_monitoring": "water-env", "nuclear_radiation": "water-env",
    "nusc_gamma_radiation": "water-env",
    # 海空太空
    "ship_ais": "marine-air-space", "ship_tdx": "marine-air-space",
    "aisstream": "marine-air-space", "gfw_vessel_presence": "marine-air-space",
    "flight_fr24": "marine-air-space", "flight_fr24_zone": "marine-air-space",
    "flight_opensky": "marine-air-space", "satellite": "marine-air-space",
    "satellite_passes_daily": "marine-air-space", "launch": "marine-air-space",
    # 日本／全球氣候
    "global_climate_usgs_earthquake": "jma-global", "global_climate_jma_typhoon": "jma-global",
    "global_climate_jtwc": "jma-global", "global_climate_cmems": "jma-global",
    "global_climate_cwa_typhoon": "jma-global", "global_climate_ncdr_typhoon_routes": "jma-global",
    "global_climate_cams": "jma-global", "global_climate_noaa_gfs": "jma-global",
    "global_climate_bake": "jma-global", "jma_amedas": "jma-global",
    "jma_warnings": "jma-global", "jma_quake": "jma-global", "jma_raster": "jma-global",
    "jma_seasonal": "jma-global", "jma_lightning": "jma-global",
    "jma_forecast": "jma-global", "env_jp_wbgt": "jma-global", "jma_ocean_env": "jma-global",
    # 網路健康
    "cloudflare_radar": "internet-health", "ioda_internet_health": "internet-health",
    "ripe_atlas_internet_health": "internet-health", "ripe_ris_live": "internet-health",
    # 政府雜項
    "er_hospital_realtime": "gov-misc", "power_taipower": "gov-misc",
    "news_events": "gov-misc", "global_events": "gov-misc",
    "twse_market_index": "gov-misc", "food_prices": "gov-misc",
    "pla_activity_daily": "gov-misc", "pla_tracks_vectorize": "gov-misc",
    "cdc_public_health_weekly": "gov-misc", "yt_live_video_resolver": "gov-misc",
    "correctional_daily_snapshot": "gov-misc", "animal_adoption": "gov-misc",
    "animal_shelter_outcomes": "gov-misc", "animal_shelter_pressure": "gov-misc",
    "animal_veterinary_clinics": "gov-misc", "animal_licensed_pet_businesses": "gov-misc",
    "animal_protection_offices": "gov-misc", "immigration_apis_airport": "gov-misc",
    "npa_traffic_accident_a1": "gov-misc", "tpml_seat": "gov-misc",
    "foursquare_poi": "gov-misc", "waste_positions": "gov-misc", "waste_match": "gov-misc",
    # 非 collector 的排程 task（cross_layer_map 有、registry 沒有）
    "gfw_hourly_publish": "marine-air-space",
}

DEPLOYMENT_LABEL = {"zeabur": "Zeabur", "hicloud_vm": "VM", "disabled": "停用"}

# clm 監控門檻與 repo 預設間隔差到這個倍數以上，就在備註標出來（兩者語意不同，不默選一個）
INTERVAL_MISMATCH_FACTOR = 3

NOTE_MAX_CHARS = 90

# cross_layer_map notes 裡屬於「稽核紀錄」而非 collector 特性的句子開頭，不放進備註
AUDIT_NOTE_PREFIXES = ("production 實測", "破案", "2026-07-08 依主站", "2026-07-03 用戶設")


# ────────────────────────────────────────────────────────────────────
# 靜態讀取
# ────────────────────────────────────────────────────────────────────
def _literal(node: ast.AST):
    return ast.literal_eval(node)


def _find_assign(tree: ast.Module, target: str) -> ast.AST:
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for t in node.targets:
                if isinstance(t, ast.Name) and t.id == target:
                    return node.value
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            if node.target.id == target and node.value is not None:
                return node.value
    raise RuntimeError(f"找不到 {target} 的賦值")


def _class_name_attr(module_path: Path, class_name: str) -> str:
    """回傳 collector class 的 `name` 類屬性（同模組內沿 base class 往上找）。"""
    tree = ast.parse(module_path.read_text(encoding="utf-8"))
    classes = {n.name: n for n in tree.body if isinstance(n, ast.ClassDef)}
    seen = set()
    current = class_name
    while current in classes and current not in seen:
        seen.add(current)
        cls = classes[current]
        for stmt in cls.body:
            if isinstance(stmt, ast.Assign):
                for t in stmt.targets:
                    if isinstance(t, ast.Name) and t.id == "name":
                        return _literal(stmt.value)
            if isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name):
                if stmt.target.id == "name" and stmt.value is not None:
                    return _literal(stmt.value)
        bases = [b.id for b in cls.bases if isinstance(b, ast.Name)]
        current = bases[0] if bases else ""
    raise RuntimeError(f"{module_path}: 找不到 {class_name}.name")


def load_registry() -> list[dict]:
    """依 registry 順序回傳 [{name, display, prefix, required_env, persistent}]。"""
    tree = ast.parse(REGISTRY_PY.read_text(encoding="utf-8"))
    class_module: dict[str, Path] = {}
    for node in tree.body:
        if isinstance(node, ast.ImportFrom) and node.level == 1 and node.module:
            mod_path = REGISTRY_PY.parent.joinpath(*node.module.split(".")).with_suffix(".py")
            for alias in node.names:
                class_module[alias.asname or alias.name] = mod_path
    entries = []
    for call in _find_assign(tree, "COLLECTOR_REGISTRY").elts:
        args = list(call.args)
        kwargs = {k.arg: k.value for k in call.keywords}
        cls_name = args[0].id
        display = _literal(args[1])
        prefix = _literal(args[2])
        req = args[3] if len(args) > 3 else kwargs.get("required_env")
        pers = args[4] if len(args) > 4 else kwargs.get("persistent")
        entries.append({
            "name": _class_name_attr(class_module[cls_name], cls_name),
            "display": display,
            "prefix": prefix,
            "required_env": tuple(_literal(req)) if req is not None else (),
            "persistent": bool(_literal(pers)) if pers is not None else False,
        })
    return entries


def load_toggles() -> dict[str, tuple[bool, int]]:
    """config._COLLECTOR_TOGGLES 的 repo 預設值：{prefix: (enabled, interval_min)}。"""
    tree = ast.parse(CONFIG_PY.read_text(encoding="utf-8"))
    return {p: (bool(e), int(i)) for p, e, i in _literal(_find_assign(tree, "_COLLECTOR_TOGGLES"))}


def load_table_map() -> dict:
    """storage/supabase_tables.py 是純資料模組（無 import），直接以檔案路徑載入。"""
    spec = importlib.util.spec_from_file_location("_handbook_supabase_tables", TABLES_PY)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module.TABLE_MAP


def load_yaml(path: Path):
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


# ────────────────────────────────────────────────────────────────────
# 欄位推導
# ────────────────────────────────────────────────────────────────────
def fmt_interval(minutes: int | None) -> str:
    if minutes is None:
        return "—"
    if minutes % 1440 == 0:
        return f"{minutes // 1440} 日"
    if minutes % 60 == 0:
        return f"{minutes // 60} 小時"
    return f"{minutes} 分"


def table_layer(table: str) -> str:
    """依表名推資料層（只用於說明，不是契約）。"""
    short = table.split(".")[-1]
    schema = table.split(".")[0] if "." in table else ""
    if short.endswith("_current"):
        return "current"
    if "trails" in short:
        return "trails"
    if short.endswith(("_runs", "_receipts", "_manifests", "_batches", "_ingest_health")):
        return "ledger"
    if schema == "reference" or short.endswith("_ref") or short.endswith("_stations"):
        return "reference"
    if short.endswith("_daily") and schema == "analytics":
        return "彙總"
    return "history"


def data_layers(name: str, tables: list[str], table_map: dict, has_s3: bool) -> str:
    cfg = table_map.get(name, {})
    if not tables:
        return "不寫 DB（本地／S3）" if has_s3 else "不寫 DB"
    layers = []
    for t in tables:
        lay = table_layer(t)
        if lay not in layers:
            layers.append(lay)
    order = ["current", "history", "trails", "reference", "ledger", "彙總"]
    layers.sort(key=order.index)
    text = "＋".join(layers)
    if cfg.get("is_multi_table"):
        text += "（多表）"
    if cfg.get("is_reference"):
        text += "（reference 寫入）"
    return text


def short_note(note: str | None) -> str:
    if not note:
        return ""
    text = " ".join(str(note).split())
    # 取第一個「有內容」的句子；跳過清冊稽核紀錄類的句子（實測時間、校正紀錄）
    parts = [p.strip() for p in text.replace("；", "。").split("。") if p.strip()]
    parts = [p for p in parts if not p.startswith(AUDIT_NOTE_PREFIXES)]
    if not parts:
        return ""
    text = parts[0]
    if len(text) > NOTE_MAX_CHARS:
        text = text[:NOTE_MAX_CHARS].rstrip() + "…"
    return text


def esc(text: str) -> str:
    return text.replace("|", "\\|")


# ────────────────────────────────────────────────────────────────────
# 產生
# ────────────────────────────────────────────────────────────────────
def build_rows() -> tuple[list[dict], list[str]]:
    registry = load_registry()
    toggles = load_toggles()
    clm = load_yaml(CLM_YAML)
    realtime = load_yaml(REALTIME_YAML)
    table_map = load_table_map()

    monitored = {f"{t['schema']}.{t['table']}" for t in realtime.get("tables", [])}

    names = [e["name"] for e in registry]
    task_names = [k for k in clm if k not in names]
    unmapped = [n for n in names + task_names if n not in SOURCES_DOC]
    if unmapped:
        raise SystemExit(
            "以下 collector 沒有登記 sources 分類，請在 scripts/gen_handbook_catalog.py "
            f"的 SOURCES_DOC 補一行，並在對應 docs/handbook/sources/*.md 加一段：{unmapped}"
        )
    missing_clm = [n for n in names if n not in clm]
    if missing_clm:
        raise SystemExit(f"cross_layer_map.yaml 缺 collector：{missing_clm}（先跑 scripts/sync_cross_layer_map.py）")

    # 共用表（只算 production 應在跑的 collector）
    table_owners: dict[str, list[str]] = defaultdict(list)
    for key, entry in clm.items():
        if entry.get("enabled"):
            for t in entry.get("supabase_tables") or []:
                table_owners[t].append(key)

    rows = []
    for e in registry + [{"name": t, "task": True} for t in task_names]:
        name = e["name"]
        c = clm[name]
        tables = list(c.get("supabase_tables") or [])
        has_s3 = bool(c.get("s3_prefixes"))
        enabled = bool(c.get("enabled"))
        deployment = c.get("deployment", "")
        if not enabled:
            where = "停用" if deployment in ("disabled", "zeabur") else f"停用（{DEPLOYMENT_LABEL.get(deployment, deployment)}）"
        else:
            where = DEPLOYMENT_LABEL.get(deployment, deployment)
        flags = []
        if e.get("task"):
            flags.append("排程 task（非 registry）")
            interval = c.get("expected_interval_min")
        else:
            _, interval = toggles.get(e["prefix"], (None, None))
            if e.get("persistent"):
                where += "（長駐 worker）" if enabled else ""
                flags.append("長駐 WebSocket worker")
            if e.get("required_env"):
                flags.append("需 " + "、".join(e["required_env"]))
        expected = c.get("expected_interval_min")
        if interval and expected and (
            expected >= interval * INTERVAL_MISMATCH_FACTOR or interval >= expected * INTERVAL_MISMATCH_FACTOR
        ):
            flags.append(f"監控門檻 {fmt_interval(expected)}")
        if c.get("critical"):
            flags.append("critical")
        if enabled:
            shared = sorted({o for t in tables for o in table_owners.get(t, []) if o != name})
            if shared:
                flags.append("與 " + "、".join(shared) + " 共用表（一源壞掉可能被他源新鮮度蓋過）")
            if not (set(tables) & monitored) and not e.get("task"):
                flags.append("無表級監控")
        note = short_note(c.get("notes"))
        if note:
            flags.append(note)
        doc = SOURCES_DOC[name]
        rows.append({
            "name": name,
            "category": SOURCES_LABEL[doc],
            "doc": doc,
            "interval": fmt_interval(interval),
            "where": where,
            "tables": "<br>".join(tables) if tables else "—",
            "layers": data_layers(name, tables, table_map, has_s3),
            "notes": "；".join(flags),
        })

    footnotes = []
    orphan_owners = sorted({t["owner_collector"] for t in realtime.get("tables", [])} - set(clm))
    if orphan_owners:
        footnotes.append(
            "realtime_tables.yaml 有 owner 在 repo 內找不到 collector："
            + "、".join(f"`{o}`" for o in orphan_owners)
        )
    return rows, footnotes


def render(rows: list[dict], footnotes: list[str]) -> str:
    order = list(SOURCES_LABEL)
    rows = sorted(rows, key=lambda r: order.index(r["doc"]))  # 穩定排序：分類內維持 registry 順序
    enabled = sum(1 for r in rows if not r["where"].startswith("停用"))
    lines = [
        "<!-- 自動產生，勿手改；跑 python3 scripts/gen_handbook_catalog.py -->",
        "# collector 清單",
        "",
        "> **自動產生，勿手改**；跑 `python3 scripts/gen_handbook_catalog.py` 重產，"
        "`--check` 檢查是否最新（CI：`tests/test_handbook_catalog.py`）。",
        "> 來源：`collectors/registry.py`、`config.py` 的 `_COLLECTOR_TOGGLES`、"
        "`config/cross_layer_map.yaml`、`storage/supabase_tables.py` 的 `TABLE_MAP`、"
        "`config/realtime_tables.yaml`。要改內容就改這些來源（備註取自 cross_layer_map 的 notes）。",
        "",
        f"共 {len(rows)} 筆（含排程 task），production 應在跑 {enabled} 筆。",
        "",
        "欄位說明：",
        "",
        "- **頻率**：`config.py` 的 repo 預設間隔。production 可用 `<PREFIX>_INTERVAL` 環境變數覆寫，"
        "實際值以 Zeabur／VM 設定為準；監控門檻（cross_layer_map 的 `expected_interval_min`）和預設差 3 倍以上時寫在備註。",
        "- **執行位置**：取自 cross_layer_map 的 `enabled`／`deployment`（反映 production，不是 repo 預設）。"
        "VM 的部署方式見 [`docs/EXTERNAL_COLLECTORS.md`](../EXTERNAL_COLLECTORS.md)。",
        "- **資料層**：依表名推測（current＝最新狀態表、history＝歷史表、ledger＝執行紀錄表），僅供導覽。",
        "- **保留期**：一律見 gis-platform 的 `metadata.retention_policies` 與對應 migration，本表不抄天數"
        "（理由見 [data-lifecycle.md](data-lifecycle.md)）。",
        "- **無表級監控**：production 在跑，但寫入的表都不在 `realtime_tables.yaml`，表停寫不會出現在日報。",
        "",
        "| 名稱 | 類別 | 頻率 | 執行位置 | 寫入表 | 資料層 | 保留期 | 怎麼收 | 備註 |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for r in rows:
        link = f"[sources/{r['doc']}](sources/{r['doc']}.md#{r['name']})"
        lines.append(
            "| " + " | ".join([
                f"`{r['name']}`", r["category"], r["interval"], r["where"], esc(r["tables"]),
                r["layers"], "見 retention_policies", link, esc(r["notes"]),
            ]) + " |"
        )
    if footnotes:
        lines += ["", "## 清冊不一致（產生時偵測）", ""]
        lines += [f"- {f}" for f in footnotes]
    return "\n".join(lines) + "\n"


def generate() -> str:
    rows, footnotes = build_rows()
    return render(rows, footnotes)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--check", action="store_true", help="只比對，不寫檔；不同則 exit 1")
    args = parser.parse_args(argv)
    content = generate()
    current = OUTPUT_MD.read_text(encoding="utf-8") if OUTPUT_MD.exists() else None
    if args.check:
        if current != content:
            print(
                f"{OUTPUT_MD.relative_to(REPO_ROOT)} 不是最新 → 跑 python3 scripts/gen_handbook_catalog.py",
                file=sys.stderr,
            )
            return 1
        print("collectors.md 已是最新")
        return 0
    OUTPUT_MD.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_MD.write_text(content, encoding="utf-8")
    print(f"已寫入 {OUTPUT_MD.relative_to(REPO_ROOT)}（{content.count(chr(10))} 行）")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
