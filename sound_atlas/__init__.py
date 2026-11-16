"""City Sound Atlas（聲景）來源檔管線：即時 10 分分片＋隔日每日檔。

設計正本：docs/sound-atlas-live/DESIGN.md。

- 每日建檔器（`daily.py`）是唯一正本；即時分片（`live.py`）用同一批純函式（`radar`、`freeway_vd`、
  `rain_gauge`、`natural`、`aviation`），只差在「看不到未來資料」時一律給「無法判斷」或暫定值。
- 演算法從 mini-taiwan-pulse `scripts/preprocess/build-sound-*.py`（W6-1～W6-3）搬來；
  網格、雨勢分級、雷達色表等共用常數放 `contract/`，與前端逐位元組相同（sha256 記在 contract/manifest.json）。
"""
