"""水情燈號 daily collector

對應 topic-research/water-overview/kpi-data-status.md 行動 C1。

來源：https://www.wra.gov.tw/EarlyWarning.aspx?n=18804&sms=9114
  - 無 API / 無 CSV，只能 HTML scrape
  - 不定期更新（抗旱會議公告後），實測 18 天未變屬常態
  - 4 級燈號：type-2=綠（水情提醒）/ type-3=黃 / type-4=橙 / type-5=紅
  - 藍燈 = 未發布（頁面未列的地區，前端 fallback 顯示）

寫入：
  - public.drought_alert_current     UPSERT by region_name（最新狀態）
  - public.drought_alert_history     INSERT ON CONFLICT (region_name, published_date) DO NOTHING

去重策略：
  - collector 算整頁 HTML SHA256
  - 與資料庫中最新一筆 source_hash 比對；相同 → no-op skip 整批
  - 不同 → 寫 current + history

不能標 LIVE：DataAge UI 顯示「採樣 X 天前」橘色。
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
from datetime import date, datetime
from typing import Optional

import requests
from bs4 import BeautifulSoup

import config
from collectors.base import BaseCollector, TAIPEI_TZ

logger = logging.getLogger(__name__)

WRA_DROUGHT_URL = "https://www.wra.gov.tw/EarlyWarning.aspx?n=18804&sms=9114"

# type-N → (alert_level, alert_color)
TYPE_TO_LEVEL = {
    'type-2': ('綠燈', '#33A02C'),
    'type-3': ('黃燈', '#FDB813'),
    'type-4': ('橙燈', '#F58220'),
    'type-5': ('紅燈', '#E32636'),
}


def _parse_roc_date(text: str) -> Optional[date]:
    """民國年文字 → date。'115年4月27日' → date(2026, 4, 27)"""
    if not text:
        return None
    m = re.search(r'(\d+)\s*年\s*(\d+)\s*月\s*(\d+)\s*日', text)
    if not m:
        return None
    try:
        return date(int(m.group(1)) + 1911, int(m.group(2)), int(m.group(3)))
    except ValueError:
        return None


def _parse_drought_html(html: str) -> dict:
    """解析 wra EarlyWarning HTML

    Returns:
        {
            'published_date': date | None,
            'alerts': [
                {'region_name', 'alert_level', 'alert_label', 'alert_color'},
                ...
            ],
            'source_hash': str  (canonical-JSON SHA256，只 hash 結構化內容；
                                 頁面 ViewState 等動態欄位不影響 hash)
        }
    """
    soup = BeautifulSoup(html, 'html.parser')

    # 發布日期
    published_date = None
    update_block = soup.find('div', class_='updatetime')
    if update_block:
        date_div = update_block.find('div', class_='date')
        if date_div:
            published_date = _parse_roc_date(date_div.get_text(strip=True))

    # 各區燈號
    alerts = []
    info_list = soup.find('div', class_='info-list')
    if info_list:
        for li in info_list.find_all('li'):
            locate = li.find('div', class_='locate')
            current = li.find('div', class_='current')
            if not locate or not current:
                continue

            region_name = locate.get_text(strip=True)
            alert_label = current.get_text(strip=True)
            type_class = next(
                (c for c in current.get('class', []) if c.startswith('type-')),
                None,
            )
            if type_class and type_class in TYPE_TO_LEVEL:
                alert_level, alert_color = TYPE_TO_LEVEL[type_class]
            else:
                alert_level, alert_color = '?', None

            alerts.append({
                'region_name': region_name,
                'alert_level': alert_level,
                'alert_label': alert_label,
                'alert_color': alert_color,
            })

    # canonical hash：只看結構化內容（published_date + alerts），
    # 不 hash 整頁 HTML，避免 ViewState/timestamp 噪音讓 hash 每次都變
    canonical = {
        'published_date': published_date.isoformat() if published_date else None,
        'alerts': sorted(
            [{k: a[k] for k in ('region_name', 'alert_level', 'alert_label', 'alert_color')}
             for a in alerts],
            key=lambda a: a['region_name'],
        ),
    }
    canonical_json = json.dumps(canonical, ensure_ascii=False, sort_keys=True)
    source_hash = hashlib.sha256(canonical_json.encode('utf-8')).hexdigest()

    return {
        'published_date': published_date,
        'alerts': alerts,
        'source_hash': source_hash,
    }


class WraDroughtAlertCollector(BaseCollector):
    """水情燈號 daily collector（不能標 LIVE，上游不定期更新）"""

    name = "wra_drought_alert"
    interval_minutes = config.WRA_DROUGHT_ALERT_INTERVAL

    def __init__(self):
        super().__init__()
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "GIS-DataCollectors/1.0 (wra-drought-alert; +https://www.wra.gov.tw/)",
            "Referer": "https://www.wra.gov.tw/",
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
        })

    def _fetch_html(self) -> str:
        resp = self._session.get(WRA_DROUGHT_URL, timeout=config.REQUEST_TIMEOUT)
        resp.raise_for_status()
        # 上游回 UTF-8
        resp.encoding = 'utf-8'
        return resp.text

    def _check_existing_hash(self) -> Optional[str]:
        """從 Supabase 取最新 source_hash（去重用）

        若無 supabase_writer 或表為空 → None（會寫入）
        """
        if not self.supabase_writer:
            return None
        try:
            with self.supabase_writer.with_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT source_hash FROM public.drought_alert_current "
                        "ORDER BY fetched_at DESC LIMIT 1"
                    )
                    row = cur.fetchone()
                    return row[0] if row else None
        except Exception as e:
            logger.warning(f"[{self.name}] 讀取既有 hash 失敗（將正常寫入）: {e}")
            return None

    def collect(self) -> dict:
        html = self._fetch_html()
        parsed = _parse_drought_html(html)

        existing_hash = self._check_existing_hash()
        unchanged = existing_hash and existing_hash == parsed['source_hash']

        # 把 published_date 轉 str（給 supabase_writer 寫入 DATE 欄）
        published_date_str = (
            parsed['published_date'].isoformat() if parsed['published_date'] else None
        )
        fetched_at = datetime.now(TAIPEI_TZ)

        records = []
        if not unchanged:
            for a in parsed['alerts']:
                records.append({
                    'region_name': a['region_name'],
                    'alert_level': a['alert_level'],
                    'alert_label': a['alert_label'],
                    'alert_color': a['alert_color'],
                    'published_date': published_date_str,
                    'source_hash': parsed['source_hash'],
                    'source_url': WRA_DROUGHT_URL,
                    'fetched_at': fetched_at,
                })

        n_alerts = len(parsed['alerts'])
        if unchanged:
            print(
                f"[{self.name}] 上游 HTML 未變動 (hash={parsed['source_hash'][:12]}) → skip"
            )
        else:
            print(
                f"[{self.name}] 上游有變動，{n_alerts} 筆燈號（"
                f"發布日期={published_date_str}, hash={parsed['source_hash'][:12]}）"
            )

        return {
            'data': records,
            'source_hash': parsed['source_hash'],
            'published_date': published_date_str,
            'n_alerts': n_alerts,
            'unchanged': unchanged,
        }
