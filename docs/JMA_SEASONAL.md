# JMA 季節觀測 collector（jma_seasonal）

日本氣象廳的「季節觀測」：生物季節（櫻、楓、銀杏）、山頂初冠雪、梅雨入り／明け。只收資料，不含前端圖層。

- 程式：`collectors/global_climate/jma_seasonal.py`
- 開關：`JAPAN_JMA_SEASONAL_ENABLED`（預設 false）、`JAPAN_JMA_SEASONAL_INTERVAL`（預設 360 分）
- 表：gis-platform `migrations/439_jma_seasonal_observations.sql`（live schema，四張表，永久保留）
- 歷史回填：`scripts/backfill_jma_phenology.py`
- 測試：`tests/test_jma_seasonal.py`
- 授權：公共データ利用規約 1.0，前端顯示要標「出典：気象庁」並註明加工

## 來源

| 來源 | URL | 更新 | 新鮮度判斷 |
|---|---|---|---|
| 生物季節 當年表（楓銀杏） | `sakura/data/phn_012〜015.html` | 每日 08:35 | HTTP Last-Modified（頁面沒有時間戳） |
| 生物季節 當年表（櫻） | `sakura/data/sakura_kaika.html`、`sakura_mankai.html` | 12–6 月每日 3 次 | HTTP Last-Modified |
| 生物季節 XML 電文 | `developer/xml/feed/other_l.xml` 中的 `VGSK55` | 觀測當天 | 電文 ReportDateTime |
| 山頂積雪狀況／初冠雪 | `stats/data/mdrr/fsn_cap/index.php` | 每小時 25 分 | 頁面「YYYY年M月D日H時 時点」 |
| 梅雨速報 | `cpd/baiu/sokuhou_baiu.html` | 發表時 | 頁面「更新日」＋Last-Modified |

每輪的來源時間戳放在回傳的 `source_times`。超過門檻的來源列在 `stale_sources`（初冠雪 6 小時、楓銀杏頁 3 天），只回報、不擋寫入，因為季節外頁面本來就不會更新。

## 對照與口徑（踩過的坑）

- **頁面檔名編號不等於現象碼**：`phn_012` 是銀杏黃葉，現象碼 013；`phn_013`→014、`phn_014`→015（楓紅葉）、`phn_015`→016。現象碼與累年 CSV 檔名、XML `Kind/Code` 是同一套（XML 的 11＝すすき 011）。
- **楓銀杏頁兩年並列**（例如 2025、2026 欄），每欄各成一列。`-`＝尚未觀測或欠測，不寫列；`///`＝沒有差值，存 NULL。這類頁面沒有平年日欄，`normal_date` 用 `obs_date − 平年差` 推得；2025 年全部 193 筆與累年 CSV 的日期、平年日逐筆比對一致。
- **season_year 是累年表的年欄**：冬春種目在 1/1 前觀測記在隔年（沖繩櫻 12/25 記在隔年欄），秋種目在 12/31 後觀測記在前一年（楓落葉 1/5 記在前一年欄）。`obs_date` 一律是真實日曆日。
- **代替種目**：HTML 給樹種名（やまもみじ、ひかんざくら）；累年 CSV 只有 rm 碼（6 代替・構內、7 代替・附近、8 正規・構內、9 正規・附近），rm 6/7 的 `alt_species` 寫「代替種目」。櫻頁的種類欄空白表示そめいよしの，存 NULL。
- **站點**：HTML 沒有站號。站號取累年 CSV「番号」（國際地點番號下三碼，加上 47），座標取 `bosai/amedas/const/amedastable.json`。站表同名多站時取 type A（気象官署，例如山形、佐賀、下関、高松）；南大東島在站表叫「南大東」。
- **初冠雪手法分段**：2027 寒候年（2026-08～2027-07）起改用解析積雪深判定（`judgement_method='analysis'`），2026 寒候年以前是目視（`'visual'`）。JMA 明講兩者不可單純比較，時序分析要依 judgement_method 分開。初冠雪表沒有山的座標，mountain_code 用 JMA `records/{code}.html` 的代碼。
- **梅雨速報季節外**仍是最近一季（例如 10 月時是令和 8 年＝2026），year 取頁面標題的令和年；「－」＝無法特定，`period_date` 存 NULL。

## 只記變動與冷儲存

- 每列的 `record_key` 與 DB 唯一鍵一致。collector 只送 state 裡沒看過的列，state 只在 DB 寫入成功後前進（`require_db_write`，作法同 jma_quake）。DB 端再用 `ON CONFLICT DO NOTHING` 擋一次，重送不會產生重複。
- XML 電文用 URL 記錄是否看過，看過的不再下載。
- 山頂積雪狀況只在狀態改變時寫一列到 `jma_snowcap_status`。解析度等於收集間隔，預設 6 小時。
- 原始頁面只有在該來源產出新列時，才放進回傳的 `raw_payload`，存在本地 JSON，再由 ArchiveTask 每日歸檔到 S3 `jma_seasonal/archives/`。沒有變動的輪次不留原文。
- 缺值一律 NULL，不補 0；未觀測不寫列。

## 表（migration 439）

| 表 | 唯一鍵 | 內容 |
|---|---|---|
| `live.jma_phenology_observations` | NULLS NOT DISTINCT (source, station_name, phenomenon_code, season_year, obs_date, diff_normal_days, diff_last_year_days, alt_species, info_type) | 生物季節；source＝current_html／xml／ruinenchi_csv |
| `live.jma_snowcap` | (mountain_code, season_year, judgement_method, first_snowcap_date) | 初冠雪日 |
| `live.jma_snowcap_status` | PK (mountain_code, source_time) | 積雪狀況變動紀錄 |
| `live.jma_rainy_season` | NULLS NOT DISTINCT (region, year, event, period_text, class_text) | 梅雨入り（onset）／明け（end）速報 |

Views：`public.jma_phenology_latest`（每站×現象×季年取一列，來源優先 current_html > ruinenchi_csv > xml）、`public.jma_phenology_current_season`、`public.jma_snowcap_latest`、`public.jma_rainy_season_latest`。

## 歷史回填

```bash
# 預演：只抓資料、算筆數，不連 DB
python3 scripts/backfill_jma_phenology.py --raw-dir /tmp/jma_seasonal_raw
# 正式寫入（migration 439 apply 之後）
python3 scripts/backfill_jma_phenology.py --apply --env-file ../gis-platform/.env --dsn-key DATABASE_POOL_URL
```

預設範圍：生物季節 2021–2025（含平年値），初冠雪目視 2021–2026 寒候年。2026-10-12 預演結果：phenology 1,534 列（6 現象 × 5 年；代替種目 149 列、跨年列 15 列；站號與座標全數對上），snowcap 261 列（44 山）。

## 2026-10-12 實打結果（第一輪）

| 來源 | 列數 |
|---|---|
| 生物季節 HTML | 307（櫻開花 2026 年 58、滿開 56；銀杏黃葉 2025 年 47、落葉 48；楓紅葉 2025 年 50、落葉 48） |
| XML VGSK55 | 6（全部是 2026-10 的すすき開花） |
| 初冠雪 | 1（旭岳 2026-10-02，analysis）；積雪狀況 44 |
| 梅雨速報 | 24（令和 8 年＝2026，12 地方 × 入／明） |

截至 10-12，2026 年的楓紅葉、銀杏黃葉都還沒有任何觀測（10 月 0 筆）；上一季（2025 年）最早的楓紅葉是旭川 10-22。

## 上線步驟（主 agent）

1. gis-platform apply migration 439
2. 執行歷史回填 `--apply`
3. S3 lifecycle 補 `jma_seasonal/archives/` 前綴規則（archive-deep-270d）
4. data-collectors merge 後，在 Zeabur 設定 `JAPAN_JMA_SEASONAL_ENABLED=true` 並 restart
5. `config/cross_layer_map.yaml` 的 `enabled` 改成 true
