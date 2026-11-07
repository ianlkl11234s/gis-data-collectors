# 影像改走 R2：每日 manifest 契約（2026-11-07）

> 目的：網站的氣象／空品／雨量影像不再經 Supabase RPC 回 base64，改讀 R2 上的「影像檔＋每日幀清單」，
> 之後 DB 影像表可以退役。原則：R2 給瀏覽器讀、S3 是永久正本、Supabase 只放需要伺服器端查詢的即時資料。

## 1. 涉及的來源

| source | collector | DB 表（過渡期照寫） | 影像 key（R2 bucket `mini-tw-pulse`） | product |
|---|---|---|---|---|
| `cwa` | `collectors/cwa_satellite.py` | `live.cwa_imagery_frames`（有 `image_key`） | `imagery/cwa/<dataset_id>/<YYYYMMDD>/<HHMMSS>.<png\|jpg>`（AR-11 起已寫） | CWA dataset id，如 `O-A0058-005` |
| `aqi` | `collectors/air_quality_imagery.py` | `live.aqi_imagery_frames`（有 `image_key`） | `imagery/aqi/<product>/<YYYYMMDD>/<HHMMSS>.png`（已寫） | `AQI`／`PM25`／`PM10`／`O3`／`NO2` |
| `precip` | `collectors/precipitation_raster.py` | `live.precipitation_raster_frames`（**沒有** `image_key` 欄） | `imagery/precip/ch<N>/<YYYYMMDD>/<HHMMSS>.png`（**本次新增**） | `ch1`／`ch3`／`ch6`／`ch24` |

JMA 發布幀（`imagery/jma/`，`collectors/weather_raster/publish.py`）本來就是 R2＋DB 小清單表，不在本次範圍。

時間一律 UTC。影像物件 `Cache-Control: public, max-age=31536000, immutable`。

## 2. manifest

- `imagery/<source>/manifest-<YYYY-MM-DD>.json`：該 **UTC 日** 的全部幀。
- `imagery/<source>/latest.json`：每個 product 最新一幀（監控／除錯用，前端不依賴）。
- 兩者 `Cache-Control: public, max-age=60`（當天的會一直被改寫）。Cloudflare 對 JSON 目前是 `DYNAMIC`（不在 edge 快取）。

```json
{
  "version": 1,
  "source": "precip",
  "date": "2026-11-07",
  "date_basis": "UTC",
  "updated_at": "2026-11-07T12:00:05Z",
  "frames": [
    {
      "product": "ch1",
      "observed_at": "2026-11-07T08:00:00Z",
      "key": "imagery/precip/ch1/20261107/080000.png",
      "mime_type": "image/png",
      "image_size": 7802,
      "bbox": [119.98, 21.86, 122.04, 25.34]
    }
  ]
}
```

- `bbox` = `[lon_min, lat_min, lon_max, lat_max]`（WGS84）。cwa 取 CWA metadata；aqi 固定 `[120.0, 21.9, 122.0, 25.3]`（與 RPC 硬編一致）；precip 由 ul/br 角點換算。
- `frames` 依 `(product, observed_at)` 排序、去重。空圖（precip `is_empty`）不進 manifest。
- 前端 URL＝`https://data.itsmigu.com/<key>`（R2 公開網域，GET 回 `access-control-allow-origin: *`）。
- 前端讀「台北某日」或「最近 24 小時」時，抓涵蓋該時間窗的 1–2 份 UTC manifest 再依 `observed_at` 過濾。

寫入（`storage/imagery_manifest.py`）：collector 上傳影像成功後 read-merge-write 當日 manifest，內容沒變就不寫；
每支 collector 只有單一實例，不處理並發。全部 best-effort：R2 失敗只印警告，DB 寫入與 local 歸檔不受影響。

## 3. 保存期限與正本

| 位置 | 期限 | 角色 |
|---|---|---|
| R2 `imagery/`（含 manifest） | lifecycle 180 天 | 給瀏覽器讀 |
| S3 `migu-gis-data-collector/<collector>/archives/<YYYY-MM-DD>.tar.gz` | 永久 | 正本：local JSON 內含 `image_b64`（cwa／aqi）或 `image_bytes_b64`（precip），每日 `tasks/archive.py` 打包上傳 |
| DB 影像表 | cwa／aqi 14 天（cron 只刪 `image_key IS NOT NULL`）；**precip 無 cleanup，自 2026-06 累積** | 過渡期雙寫，待退役 |

## 4. 上線順序

1. 部署本 collector 版本（新主機已有 `R2_*` 四個變數；cwa／aqi 已在用）。看 log 沒有「manifest 更新失敗」，
   並 `curl https://data.itsmigu.com/imagery/precip/latest.json` 有內容。
2. 跑 bootstrap 補過去幾天：
   `python3 scripts/bootstrap_imagery_manifest.py --source all --days 14 --dry-run` 看數字 → 去掉 `--dry-run` 正式跑。
   （aqi 若要連 `image_key IS NULL` 的舊列一起補，先跑 `scripts/backfill_imagery_r2.py --table aqi`。）
3. 前端（mini-taiwan-pulse `feat/imagery-r2-manifest`）才可發布到 master。前端讀不到 manifest 時會退回 RPC，所以順序錯了不會全黑，只是還在打 DB。

## 5. DB 退役前置條件（另案拍板，不在本 PR）

1. 前端發布後，正式站一週內 `get_aqi_imagery_frames_batch`、`get_precipitation_raster_frames`、`get_latest_precipitation_raster`、
   `get_cwa_imagery_frames_batch`、`get_cwa_imagery_manifest` 呼叫量趨近 0（`pg_stat_statements`）。
2. 確認 S3 每日 tar 持續含影像（抽一天解開看 JSON 內 base64 長度）。
3. 前端拿掉 RPC 退路 → collector 停寫 DB（`supabase_writer` 的三個 transform 改不寫或改只寫小清單）→ migration drop 表／RPC／cleanup cron。
4. 若 DB 要保留一段過渡清單，precip 需另補 `image_key` 欄＋比照 cwa／aqi 的 cleanup cron。
