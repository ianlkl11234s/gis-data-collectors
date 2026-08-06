"""共機航跡示意圖向量化 — 每日自動化收集器

`pla_activity_daily` 只把 `track_chart_url` 寫進 DB，圖本身不留、也不向量化。
本 collector 補上後半段：

    live.pla_activity_daily.track_chart_url  （上游 collector 產出）
      → 下載航跡圖 → S3 `pla/track_charts/YYYY/MM/{date}.jpg`（保全原料）
      → CV 向量化（collectors/pla_tracks/）
      → spatial.pla_tracks（活動區多邊形）
      + live.pla_activity_items（表格 OCR：機型／架次／時段）
      + spatial.pla_tracks_runs（ledger：跑過沒、結果如何）

## 為什麼要 ledger 表

「共機 0 架次」的日子是**合法的 0 形狀**，在 pla_tracks 裡沒有任何 row —— 光看
pla_tracks 分不出「那天沒共機」與「那天沒跑」。ledger 讓兩者可區分，同時當作
真正的心跳：每天必有一列，斷了就是壞了。

2026-08-06 立此 collector 之前，向量化是 taipei-gis-analytics 的手動批次腳本，
斷了 5 天（07-31 ~ 08-05）沒有任何告警。

## 冪等

ledger 有成功列的日子不重跑；失敗列（error 非空）每天重試一次。
形狀寫入前先 DELETE 該日 —— 重跑抽出較少形狀時，舊的多餘形狀不會殘留。
"""
from __future__ import annotations

import os
import tempfile
from datetime import datetime

import requests
import urllib3

import config
from collectors.base import BaseCollector, TAIPEI_TZ
from storage.db import connect_supabase
from utils.notify import notify_error

# ⚠ collectors.pla_tracks 只在 _process_day() 內 import（lazy）。
#   它依賴 scipy / scikit-image，而 registry.py 會 import 每一支 collector ——
#   在頂層 import 等於「這兩個套件沒裝好就全部 collector 起不來」。
#   比照 global_climate 對 xarray / cfgrib 的作法，把重依賴推遲到真的要用時。

# mnd.gov.tw 憑證缺 SKI（同 NHI / CDC），verify=False
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

S3_PREFIX = "pla/track_charts"

# 待處理日期：有航跡圖、在回看窗內、且 ledger 沒有成功列
PENDING_SQL = """
SELECT d.report_date::text, d.track_chart_url
FROM live.pla_activity_daily d
WHERE d.track_chart_url IS NOT NULL
  AND d.report_date >= (now() AT TIME ZONE 'Asia/Taipei')::date - %s::int
  AND NOT EXISTS (
      SELECT 1 FROM spatial.pla_tracks_runs r
      WHERE r.report_date = d.report_date AND r.error IS NULL
  )
ORDER BY d.report_date DESC
LIMIT %s::int
"""


class PlaTracksVectorizeCollector(BaseCollector):
    """共機航跡圖向量化 — 每日補齊尚未處理的通報日。"""

    name = "pla_tracks_vectorize"
    interval_minutes = config.PLA_TRACKS_VECTORIZE_INTERVAL

    # 單張約 5s（含 OCR），MAX_DAYS_PER_RUN 張 + 下載，抓寬裕值
    COLLECT_TIMEOUT = 1200

    # 只回補最近 N 天 —— 更早的缺口屬歷史回填，走一次性腳本而非每日流程
    LOOKBACK_DAYS = 30
    # 單輪上限：擋住「第一次上線把 30 天全跑掉」拖垮這一輪
    MAX_DAYS_PER_RUN = 10

    def __init__(self):
        super().__init__()
        # ⚠ 不能用 self.storage —— get_storage() 永遠回傳 LocalStorage（S3 歸檔另由
        #   ArchiveTask 批次上傳 tar.gz）。原圖要進 pla/track_charts/ 供日後重跑
        #   向量化，必須自己開一條 S3，比照 scripts/backfill_pla_activity.py。
        self._s3 = None
        if config.S3_BUCKET:
            try:
                from storage.s3 import S3Storage
                self._s3 = S3Storage()
            except Exception as e:
                print(f"[{self.name}] ⚠ S3 初始化失敗，原圖不備份: {e}")

        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; GIS-DataCollectors/1.0)",
            # ⚠ 抓 JPG 必須是 image/* —— mnd 對 text/html 的 Accept 回 406
            "Accept": "image/*,*/*",
        })
        self._session.verify = False

    # ── 待處理日期 ──────────────────────────────────────────
    def _find_pending(self) -> list[tuple[str, str]]:
        conn = connect_supabase(autocommit=True, statement_timeout_ms=60_000)
        try:
            with conn.cursor() as cur:
                cur.execute(PENDING_SQL, (self.LOOKBACK_DAYS, self.MAX_DAYS_PER_RUN))
                return [(r[0], r[1]) for r in cur.fetchall()]
        finally:
            conn.close()

    # ── 原料保全 ────────────────────────────────────────────
    def _fetch_chart(self, url: str, report_date: str, img_dir: str) -> str | None:
        """下載航跡圖到 img_dir/{date}.jpg，順手備份 S3。回傳 S3 key（沒 S3 則 None）。"""
        try:
            resp = self._session.get(url, timeout=config.REQUEST_TIMEOUT)
            resp.raise_for_status()
        except requests.RequestException as e:
            raise RuntimeError(f"下載航跡圖失敗: {e}") from e

        with open(os.path.join(img_dir, f"{report_date}.jpg"), "wb") as f:
            f.write(resp.content)

        if self._s3 is None:
            return None
        key = f"{S3_PREFIX}/{report_date[:4]}/{report_date[5:7]}/{report_date}.jpg"
        try:
            self._s3.s3.put_object(
                Bucket=self._s3.bucket, Key=key, Body=resp.content,
                ContentType=resp.headers.get("Content-Type", "image/jpeg"))
            return key
        except Exception as e:
            # 原料備份失敗不該擋下向量化（圖已在本地，這一輪照樣算得出來）
            print(f"[{self.name}] ⚠ {report_date} S3 備份失敗: {e}")
            return None

    # ── 單日處理 ────────────────────────────────────────────
    def _process_day(self, report_date: str, url: str, img_dir: str) -> list[dict]:
        from collectors.pla_tracks.georef import vectorize_day
        from collectors.pla_tracks.items import read_day_items

        s3_key = self._fetch_chart(url, report_date, img_dir)
        result = vectorize_day(img_dir, report_date)

        records: list[dict] = [{
            '_type': 'run',
            'report_date': report_date,
            'plate_ok': result['plate_ok'],
            'plate_size': f"{result['plate_size'][0]}x{result['plate_size'][1]}",
            'georef_dev': result['georef_dev'],
            'expected': result['expected'],
            'extracted': len(result['shapes']),
            'balloon_items': result['balloon_items'],
            'guided': result['guided'],
            'ok': result['ok'],
            'edge_precision': result['edge_precision'],
            'red_recall': result['red_recall'],
            'chart_s3_key': s3_key,
            'error': None,
        }]
        if not result['plate_ok']:
            # 版型不認得 —— 不產形狀也不讀表格，交給告警處理
            print(f"[{self.name}] ⚠ {report_date} 未知版型 {result['plate_size']}，跳過向量化")
            return records

        for s in result['shapes']:
            ring = ','.join(f"{lon} {lat}" for lon, lat in s['ring'])
            records.append({
                '_type': 'track',
                'report_date': report_date,
                'shape_no': s['shape_no'],
                'geom': f"SRID=4326;POLYGON(({ring}))",
                'shape_kind': s['shape_kind'],
                'vertices': s['vertices'],
                'table_items': result['expected'],
                'balloon_items': result['balloon_items'],
                # 守門沒過 → 標待審，兩支 RPC 預設排除，不靜默採用
                'needs_review': not result['ok'],
                'guided': result['guided'],
                'edge_precision': result['edge_precision'],
                'red_recall': result['red_recall'],
            })

        try:
            for row in read_day_items(img_dir, report_date):
                records.append({'_type': 'item', **row})
        except Exception as e:
            # 表格 OCR 失敗只損失機型維度，形狀照樣入庫
            print(f"[{self.name}] ⚠ {report_date} 表格 OCR 失敗: {type(e).__name__}: {e}")

        return records

    # ── 主流程 ──────────────────────────────────────────────
    def collect(self) -> dict:
        now = datetime.now(tz=TAIPEI_TZ)
        pending = self._find_pending()
        if not pending:
            return {'data': [], 'pending': 0, 'processed': 0, 'collected_at': now.isoformat()}

        records: list[dict] = []
        processed = failed = passed = bad_plate = 0
        with tempfile.TemporaryDirectory(prefix='pla_tracks_') as img_dir:
            for report_date, url in pending:
                try:
                    day_records = self._process_day(report_date, url, img_dir)
                except Exception as e:
                    msg = f"{type(e).__name__}: {e}"
                    print(f"[{self.name}] ✗ {report_date} 處理失敗: {msg}")
                    failed += 1
                    # 失敗也寫 ledger（error 非空 → 明天會重試），才看得出連續失敗
                    records.append({
                        '_type': 'run', 'report_date': report_date,
                        'plate_ok': False, 'plate_size': None, 'georef_dev': None,
                        'expected': None, 'extracted': 0, 'balloon_items': 0,
                        'guided': False, 'ok': False, 'edge_precision': None,
                        'red_recall': None, 'chart_s3_key': None, 'error': msg[:500],
                    })
                    continue
                records.extend(day_records)
                processed += 1
                run = day_records[0]
                passed += bool(run['ok'])
                bad_plate += not run['plate_ok']
                print(f"[{self.name}] {report_date}: 期望 {run['expected']} / "
                      f"抽出 {run['extracted']} → {'✓' if run['ok'] else '待審'}")

        # 版型不認得是**靜默失敗**：ledger 照樣有列（心跳正常）但一個形狀都產不出來，
        # 光靠 realtime_tables.yaml 的新鮮度監控看不出來 → 主動告警。
        # 不 raise：raise 會讓這一輪的 ledger 也寫不進去，反而失去「哪天壞了」的紀錄。
        if bad_plate:
            notify_error(
                self.name,
                f"{bad_plate}/{processed} 天的航跡圖版型不認得（已知只支援 720×1040）—— "
                f"國防部可能改版，需更新 collectors/pla_tracks/georef.py 的 PLATE_SIZE 與 GEOREF")

        return {
            'data': records,
            'pending': len(pending),
            'processed': processed,
            'passed': passed,
            'failed': failed,
            'bad_plate': bad_plate,
            'shapes': sum(1 for r in records if r['_type'] == 'track'),
            'items': sum(1 for r in records if r['_type'] == 'item'),
            'collected_at': now.isoformat(),
        }
