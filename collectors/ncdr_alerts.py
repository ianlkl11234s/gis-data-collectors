"""
NCDR 災害示警收集器

從國家災害防救科技中心 (NCDR) 災害示警公開資料平台取得目前生效中的
CAP (Common Alerting Protocol) 示警，每 15 分鐘執行一次。

資料來源：
    Feed 列表 (JSON):  https://alerts.ncdr.nat.gov.tw/JSONAtomFeeds.ashx
    單筆 CAP 檔:       https://alerts.ncdr.nat.gov.tw/Capstorage/{機關}/{年}/{類型}/xxx.cap

特性：
    - feed 只列「目前有效」示警，過期自動消失
    - 以 identifier UPSERT，重複收集不會產生重複資料
    - 自動累積歷史（過期示警仍保留於 DB）
    - 解析 CAP <polygon> → MULTIPOLYGON (WGS84)
    - 完全獨立於其他 collector，例外被 BaseCollector.run 隔離
"""

import json
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Optional

import requests

import config
from .base import BaseCollector, TAIPEI_TZ


CAP_NS = {'cap': 'urn:oasis:names:tc:emergency:cap:1.2'}
FEED_URL = "https://alerts.ncdr.nat.gov.tw/JSONAtomFeeds.ashx"


class NCDRAlertsCollector(BaseCollector):
    """NCDR 災害示警收集器（每 15 分鐘）"""

    name = "ncdr_alerts"
    interval_minutes = getattr(config, 'NCDR_ALERTS_INTERVAL', 15)

    def __init__(self):
        super().__init__()
        self._session = requests.Session()
        self._session.headers.update({
            'User-Agent': 'taipei-gis-analytics/ncdr-collector',
            'Accept': '*/*',
        })

    # ------------------------------------------------------------
    # Fetch
    # ------------------------------------------------------------

    def _fetch_feed(self) -> list:
        """抓取目前生效中的示警 feed (JSON)"""
        r = self._session.get(FEED_URL, timeout=config.REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        entries = data.get('entry', [])
        # feed 對單筆會回傳 dict 而非 list
        if isinstance(entries, dict):
            entries = [entries]
        return entries

    def _fetch_cap(self, url: str) -> Optional[str]:
        """下載單一 CAP XML"""
        try:
            r = self._session.get(url, timeout=config.REQUEST_TIMEOUT)
            r.raise_for_status()
            return r.text
        except Exception as e:
            print(f"   ⚠ CAP 下載失敗 {url}: {e}")
            return None

    # ------------------------------------------------------------
    # Parse
    # ------------------------------------------------------------

    @staticmethod
    def _text(elem, path: str) -> str:
        if elem is None:
            return ''
        node = elem.find(path, CAP_NS)
        return (node.text or '').strip() if node is not None and node.text else ''

    @staticmethod
    def _polygon_to_wkt(polygon_str: str) -> Optional[str]:
        """CAP polygon「lat,lon lat,lon ...」→ WKT POLYGON((lon lat, ...))"""
        coords = []
        for pair in polygon_str.strip().split():
            try:
                lat_s, lon_s = pair.split(',')
                lat, lon = float(lat_s), float(lon_s)
                coords.append(f"{lon} {lat}")
            except (ValueError, IndexError):
                continue
        if len(coords) < 3:
            return None
        # 確保 polygon 閉合
        if coords[0] != coords[-1]:
            coords.append(coords[0])
        return f"(({', '.join(coords)}))"

    def _parse_cap(self, xml_text: str) -> Optional[dict]:
        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError as e:
            print(f"   ⚠ CAP XML 解析失敗: {e}")
            return None

        identifier = self._text(root, 'cap:identifier')
        if not identifier:
            return None

        sender = self._text(root, 'cap:sender')
        sent = self._text(root, 'cap:sent')
        status = self._text(root, 'cap:status')
        msg_type = self._text(root, 'cap:msgType')
        scope = self._text(root, 'cap:scope')

        info = root.find('cap:info', CAP_NS)
        category = event = urgency = severity = certainty = ''
        headline = description = instruction = sender_name = ''
        effective = onset = expires = ''
        if info is not None:
            category = self._text(info, 'cap:category')
            event = self._text(info, 'cap:event')
            urgency = self._text(info, 'cap:urgency')
            severity = self._text(info, 'cap:severity')
            certainty = self._text(info, 'cap:certainty')
            sender_name = self._text(info, 'cap:senderName')
            headline = self._text(info, 'cap:headline')
            description = self._text(info, 'cap:description')
            instruction = self._text(info, 'cap:instruction')
            effective = self._text(info, 'cap:effective')
            onset = self._text(info, 'cap:onset')
            expires = self._text(info, 'cap:expires')

        # area: 收集所有 polygon、geocode、areaDesc
        polygons_wkt = []
        geocodes = []
        area_descs = []
        if info is not None:
            for area in info.findall('cap:area', CAP_NS):
                ad = self._text(area, 'cap:areaDesc')
                if ad:
                    area_descs.append(ad)
                for poly in area.findall('cap:polygon', CAP_NS):
                    if poly.text:
                        wkt = self._polygon_to_wkt(poly.text)
                        if wkt:
                            polygons_wkt.append(wkt)
                for gc in area.findall('cap:geocode', CAP_NS):
                    name = self._text(gc, 'cap:valueName')
                    value = self._text(gc, 'cap:value')
                    if value:
                        geocodes.append({'name': name, 'value': value})

        geom = None
        if polygons_wkt:
            geom = f"SRID=4326;MULTIPOLYGON({','.join(polygons_wkt)})"

        return {
            'identifier': identifier,
            'sender': sender,
            'sender_name': sender_name,
            'category': category,
            'event': event,
            'urgency': urgency,
            'severity': severity,
            'certainty': certainty,
            'status': status,
            'msg_type': msg_type,
            'scope': scope,
            'headline': headline,
            'description': description,
            'instruction': instruction,
            'area_desc': ' / '.join(area_descs),
            'geocodes': json.dumps(geocodes, ensure_ascii=False) if geocodes else None,
            'sent': sent or None,
            'effective': effective or None,
            'onset': onset or None,
            'expires': expires or None,
            'geom': geom,
        }

    # ------------------------------------------------------------
    # Collect
    # ------------------------------------------------------------

    def collect(self) -> dict:
        fetch_time = datetime.now(TAIPEI_TZ)
        print(f"   抓取 NCDR feed...")

        entries = self._fetch_feed()
        print(f"   feed 內 {len(entries)} 筆生效中示警")

        alerts = []
        category_counts = {}
        for entry in entries:
            link = entry.get('link', {})
            cap_url = link.get('@href') if isinstance(link, dict) else None
            if not cap_url:
                continue

            xml_text = self._fetch_cap(cap_url)
            if not xml_text:
                continue

            parsed = self._parse_cap(xml_text)
            if not parsed:
                continue

            parsed['cap_url'] = cap_url
            parsed['event_term'] = (entry.get('category') or {}).get('@term', '') if isinstance(entry.get('category'), dict) else ''
            parsed['feed_title'] = entry.get('title', '')
            parsed['feed_summary'] = (entry.get('summary') or {}).get('#text', '') if isinstance(entry.get('summary'), dict) else ''
            parsed['author'] = (entry.get('author') or {}).get('name', '') if isinstance(entry.get('author'), dict) else ''
            parsed['collected_at'] = fetch_time.isoformat()

            alerts.append(parsed)
            term = parsed['event_term'] or parsed['event'] or 'unknown'
            category_counts[term] = category_counts.get(term, 0) + 1

        # 以 identifier 去重（保留第一筆）
        seen = set()
        unique = []
        for a in alerts:
            if a['identifier'] in seen:
                continue
            seen.add(a['identifier'])
            unique.append(a)

        with_geom = sum(1 for a in unique if a.get('geom'))
        print(f"   解析成功 {len(unique)} 筆 (含 polygon: {with_geom})")
        print(f"   類型分布: {category_counts}")

        return {
            'fetch_time': fetch_time.isoformat(),
            'total_alerts': len(unique),
            'with_geom': with_geom,
            'category_counts': category_counts,
            'data': unique,
        }
