"""航跡圖左上表格 OCR → 逐項次機型／架次／時段（live.pla_activity_items）。

通報原文完全不含機型，機型只在航跡圖左上表格裡 —— 這是唯一來源。

⚠️ 多機型項次是**合併計數**：「(3 sorties of fighter / UAV)」是兩種合計 3 架次，
   拆不開。`kinds` 長度 > 1 的列，`sorties` 不可分攤到各機型。

搬自 taipei-gis-analytics `scripts/pla_tracks/load_items.py`（原為 CLI 批次腳本，
此處只取解析邏輯，寫入交給 supabase_writer）。
"""
from __future__ import annotations

import os
import re

from .table_items import read_items

# OCR 出來的英文機型字樣 → 正規化 key
KIND_MAP = [
    ('bomber', 'bomber'),
    ('fighter', 'fighter'),
    ('helicopter', 'helicopter'),
    ('uav', 'uav'),
    ('drone', 'uav'),
    ('support', 'support'),
]
# 「(20 sorties of fighter / support aircraft / UAV)」
RE_SORTIE = re.compile(r'\((\d+)\s+sorties?\s+of\s+([^)]+)\)', re.I)
# 「空飄氣球2顆(Balloon x 2)」——中文段 OCR 會亂碼，只吃英文
RE_BALLOON = re.compile(r'ballo{0,2}n\s*[x×]\s*(\d+)', re.I)
# 時段「0600-1945時」——中文亂碼但數字會留下
RE_TIME = re.compile(r'\b(\d{4})\s*[-–]\s*(\d{4})\b')


def parse_item(text: str, kind_flag: str):
    """→ (sorties, kinds[], balloon_count, time_window)"""
    tw = None
    m = RE_TIME.search(text)
    if m:
        tw = f'{m.group(1)}-{m.group(2)}'

    if kind_flag == 'balloon':
        b = RE_BALLOON.search(text)
        return None, ['balloon'], (int(b.group(1)) if b else 1), tw

    m = RE_SORTIE.search(text)
    if not m:
        return None, [], None, tw
    n = int(m.group(1))
    blob = m.group(2).lower()
    kinds = []
    for needle, key in KIND_MAP:
        if needle in blob and key not in kinds:
            kinds.append(key)
    return n, kinds, None, tw


def read_day_items(img_dir: str, report_date: str) -> list[dict]:
    """單日圖 → live.pla_activity_items 的 row dict 清單（表格讀不到時回空）。"""
    path = os.path.join(img_dir, f'{report_date}.jpg')
    items, _ = read_items(path, tmp_png=os.path.join(img_dir, '_ocr_tmp.png'))
    rows = []
    for it in items:
        sorties, kinds, balloon, tw = parse_item(it['text'], it['kind'])
        rows.append({
            'report_date': report_date,
            'item_no': it['no'],
            'sorties': sorties,
            'kinds': kinds,
            'is_balloon': it['kind'] == 'balloon',
            'balloon_count': balloon,
            'time_window': tw,
            'ocr_text': it['text'][:600],
        })
    return rows
