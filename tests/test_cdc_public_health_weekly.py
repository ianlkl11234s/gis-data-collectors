"""cdc_public_health_weekly（Zeabur 版 + VM 版）fail-loud 契約：抓不到要報錯，不寫 0。"""
from __future__ import annotations

import importlib
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import requests

import config

ROOT = Path(__file__).resolve().parents[1]
VM_DIR = ROOT / "external" / "cdc_public_health_weekly_vm"

RODS_OK = "年,週,年齡別,縣市,縣市別代碼,類流感急診就診人次\n2026,40,0-6,臺北市,63,12\n2026,41,0-6,臺北市,63,15\n"
EV_OK = "年,週,年齡別,縣市,縣市別代碼,腸病毒急診就診人次\n2026,40,0-6,臺北市,63,3\n"
DENGUE_OK = "發病年份,發病週別,縣市,鄉鎮,性別,是否為境外移入,年齡層,確定病例數\n2026,40,高雄市,前鎮區,男,否,30-34,2\n"
RODS_RENAMED = "Y,W,年齡別,縣市,縣市別代碼,人次\n2026,40,0-6,臺北市,63,12\n"  # 欄位改名


def _resp(text="", status=200):
    r = MagicMock()
    r.content = text.encode("utf-8")
    if status >= 400:
        err = requests.HTTPError(f"{status} Client Error")
        r.raise_for_status.side_effect = err
    else:
        r.raise_for_status.return_value = None
    return r


def _router(mapping):
    """filename -> 回應文字 / (text,status) / Exception"""
    def get(url, **_kw):
        name = url.rsplit("/", 1)[-1]
        v = mapping[name]
        if isinstance(v, Exception):
            raise v
        if isinstance(v, tuple):
            return _resp(*v)
        return _resp(v)
    return get


FILES = {
    "influenza": "RODS_Influenza_like_illness.csv",
    "dengue": "Weekly_Age_County_Gender_061.csv",
    "enterovirus": "RODS_EnteroviralInfection.csv",
}


def _mapping(**over):
    m = {FILES["influenza"]: RODS_OK, FILES["dengue"]: DENGUE_OK, FILES["enterovirus"]: EV_OK}
    for k, v in over.items():
        m[FILES[k]] = v
    return m


# ───────────────────────── Zeabur 版 ─────────────────────────
@pytest.fixture
def zeabur(monkeypatch):
    monkeypatch.setattr(config, "CDC_PUBLIC_HEALTH_WEEKLY_DISABLED", "", raising=False)
    from collectors.cdc_public_health_weekly import CdcPublicHealthWeeklyCollector
    c = CdcPublicHealthWeeklyCollector.__new__(CdcPublicHealthWeeklyCollector)
    c._session = MagicMock()
    c.name = "cdc_public_health_weekly"

    def use(mapping):
        c._session.get.side_effect = _router(mapping)
        return c
    return use


def test_zeabur_all_ok_no_error(zeabur):
    r = zeabur(_mapping()).collect()
    assert "_collector_error" not in r
    assert r["dataset_stats"] == {"influenza": 2, "dengue": 1, "enterovirus": 1}
    assert r["total_rows"] == 4


def test_zeabur_404_marks_error_keeps_good_data(zeabur):
    r = zeabur(_mapping(dengue=("not found", 404))).collect()
    assert r["dataset_stats"]["dengue"].startswith("error:")
    assert r["dataset_stats"]["dengue"] != 0
    assert "dengue" in r["_collector_error"]
    assert {d["disease_code"] for d in r["data"]} == {"influenza", "enterovirus"}


def test_zeabur_200_empty_file_is_error(zeabur):
    r = zeabur(_mapping(dengue="")).collect()
    assert r["dataset_stats"]["dengue"].startswith("error:")
    assert "dengue" in r["_collector_error"]


def test_zeabur_header_only_is_error(zeabur):
    r = zeabur(_mapping(influenza="年,週,類流感急診就診人次\n")).collect()
    assert "influenza" in r["_collector_error"]


def test_zeabur_renamed_columns_is_error_and_names_missing(zeabur):
    r = zeabur(_mapping(influenza=RODS_RENAMED)).collect()
    assert r["dataset_stats"]["influenza"].startswith("error:")
    assert "缺必要欄位" in r["dataset_stats"]["influenza"] or "缺必要欄位" in r["_collector_error"]
    assert "就診人次" in r["_collector_error"]


def test_zeabur_connection_error_is_error(zeabur):
    r = zeabur(_mapping(enterovirus=requests.ConnectionError("timeout"))).collect()
    assert "enterovirus" in r["_collector_error"]
    assert r["total_rows"] == 3


def test_zeabur_disabled_dengue_skips_without_error(zeabur, monkeypatch):
    monkeypatch.setattr(config, "CDC_PUBLIC_HEALTH_WEEKLY_DISABLED", "dengue")
    m = _mapping(dengue=("gone", 404))
    c = zeabur(m)
    r = c.collect()
    assert "_collector_error" not in r
    assert r["dataset_stats"]["dengue"] == "disabled"
    fetched = [call.args[0] for call in c._session.get.call_args_list]
    assert not any(FILES["dengue"] in u for u in fetched)


def test_zeabur_base_run_notifies_on_failure(zeabur, monkeypatch):
    """_collector_error 經 BaseCollector.run → notify_error（既有失敗通知路徑）"""
    import collectors.base as base
    notified = []
    monkeypatch.setattr(base, "notify_error", lambda *a, **k: notified.append(a))
    c = zeabur(_mapping(dengue=("x", 404)))
    c.storage = MagicMock()
    c.supabase_writer = None
    c.should_persist_local = lambda: False
    c.require_db_write = lambda: False
    c.run_count = c.error_count = c.consecutive_errors = 0
    out = c.run()
    assert "error" in out and notified


# ───────────────────────── VM 版 ─────────────────────────
@pytest.fixture
def vm(monkeypatch, tmp_path):
    monkeypatch.setenv("SUPABASE_DB_URL", "postgresql://u:p@localhost/db")  # dummy
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.delenv("CDC_PUBLIC_HEALTH_WEEKLY_DISABLED", raising=False)
    monkeypatch.syspath_prepend(str(VM_DIR))
    sys.modules.pop("cdc_public_health_weekly_collect", None)
    mod = importlib.import_module("cdc_public_health_weekly_collect")
    written = []
    monkeypatch.setattr(mod, "connect_with_retry", lambda *a, **k: MagicMock())
    monkeypatch.setattr(mod, "flush_pending", lambda *a, **k: None)
    monkeypatch.setattr(mod, "has_pending", lambda *a, **k: False)
    monkeypatch.setattr(mod, "save_batch", lambda *a, **k: None)
    monkeypatch.setattr(mod, "write_to_supabase",
                        lambda conn, recs, ts: written.append(list(recs)) or len(recs))
    mod._written = written

    def use(mapping):
        monkeypatch.setattr(mod.SESSION, "get", _router(mapping))
        return mod
    yield use
    sys.modules.pop("cdc_public_health_weekly_collect", None)


def _snapshot(tmp_path):
    import json
    fps = list(Path(tmp_path).rglob("cdc_public_health_weekly_*.json"))
    assert len(fps) == 1
    return json.loads(fps[0].read_text(encoding="utf-8"))


def test_vm_all_ok_exit0(vm, tmp_path):
    mod = vm(_mapping())
    assert mod.main() == 0
    snap = _snapshot(tmp_path)
    assert snap["per_disease"]["dengue"] == {"status": "ok", "rows": 1}
    assert snap["failed"] == []


def test_vm_404_exit1_good_data_written_status_error(vm, tmp_path):
    mod = vm(_mapping(dengue=("nf", 404)))
    assert mod.main() == 1
    assert len(mod._written[0]) == 3          # 好資料照寫
    snap = _snapshot(tmp_path)
    assert snap["per_disease"]["dengue"]["status"] == "error"
    assert snap["failed"] == ["dengue"]


def test_vm_200_empty_exit1(vm):
    mod = vm(_mapping(dengue=""))
    assert mod.main() == 1


def test_vm_renamed_columns_exit1_and_reports_missing(vm, tmp_path):
    mod = vm(_mapping(influenza=RODS_RENAMED))
    assert mod.main() == 1
    err = _snapshot(tmp_path)["per_disease"]["influenza"]["error"]
    assert "缺必要欄位" in err and "就診人次" in err


def test_vm_all_fail_exit1(vm):
    boom = requests.ConnectionError("x")
    mod = vm({FILES["influenza"]: boom, FILES["dengue"]: boom, FILES["enterovirus"]: boom})
    assert mod.main() == 1


def test_vm_disabled_dengue_exit0_no_fetch(vm, monkeypatch, tmp_path):
    monkeypatch.setenv("CDC_PUBLIC_HEALTH_WEEKLY_DISABLED", "dengue")
    mod = vm(_mapping(dengue=("gone", 404)))
    assert mod.main() == 0
    assert _snapshot(tmp_path)["per_disease"]["dengue"] == {"status": "disabled"}


def test_vm_no_retained_rows_after_cutoff_exit1(vm, monkeypatch, tmp_path):
    mod = vm(_mapping())
    monkeypatch.setattr(mod, "KEEP_YEARS", 0)   # cutoff 在未來 → 啟用來源皆 0 筆保留列
    assert mod.main() == 1
    snap = _snapshot(tmp_path)
    assert snap["failed"] == ["dengue", "enterovirus", "influenza"]
