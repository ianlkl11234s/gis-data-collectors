"""docs/handbook/collectors.md 必須和程式中的清冊一致。

守門目標：改了 collectors/registry.py、config.py、config/cross_layer_map.yaml、
storage/supabase_tables.py 或 config/realtime_tables.yaml，卻沒重產手冊的 collector
清單時，這條測試要變紅；新增 collector 卻沒登記 sources 分類時也要變紅。

修法：跑 `python3 scripts/gen_handbook_catalog.py`，並 commit docs/handbook/collectors.md。
"""

import re
from pathlib import Path

from scripts import gen_handbook_catalog as catalog


def test_collectors_md_is_up_to_date():
    assert catalog.main(["--check"]) == 0, (
        "docs/handbook/collectors.md 不是最新 → 跑 `python3 scripts/gen_handbook_catalog.py`"
    )


def test_every_collector_has_sources_doc():
    """registry 每個 collector 都登記了 sources 分類，且對應的 sources 檔存在。"""
    names = [e["name"] for e in catalog.load_registry()]
    assert set(names) <= set(catalog.SOURCES_DOC)
    sources_dir = catalog.REPO_ROOT / "docs" / "handbook" / "sources"
    for doc in set(catalog.SOURCES_DOC.values()):
        assert (sources_dir / f"{doc}.md").exists(), f"缺 docs/handbook/sources/{doc}.md"


def test_sources_docs_have_section_per_collector():
    """collectors.md 的「怎麼收」連結 sources/<doc>.md#<name>，對應檔要有 `### <name>` 段落。"""
    sources_dir = catalog.REPO_ROOT / "docs" / "handbook" / "sources"
    missing = []
    for name, doc in catalog.SOURCES_DOC.items():
        text = (sources_dir / f"{doc}.md").read_text(encoding="utf-8")
        if not re.search(rf"^### {re.escape(name)}\s*$", text, flags=re.M):
            missing.append(f"{doc}.md#{name}")
    assert not missing, f"sources 文件缺段落：{missing}"


def test_generation_is_deterministic():
    assert catalog.generate() == catalog.generate()


def test_no_retention_days_in_catalog():
    """保留期只寫「見 retention_policies」，不抄天數（正本在 gis-platform）。"""
    text = Path(catalog.OUTPUT_MD).read_text(encoding="utf-8")
    rows = [line for line in text.splitlines() if line.startswith("| `")]
    assert rows
    assert all("| 見 retention_policies |" in line for line in rows)
