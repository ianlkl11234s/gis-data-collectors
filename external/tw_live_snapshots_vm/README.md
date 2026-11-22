# external/tw_live_snapshots_vm — 登革熱聚集區＋航船布告（HiCloud VM）

> 2026-11-21 B8 草稿；**2026-11-22 已部署**到 HiCloud VM（R2 金鑰方案 A，cron 已啟用）。兩個來源都擋境外 IP（美國雲端 45 秒逾時），所以放台灣 IP 的 VM。
> 主 repo schema 正本：`collectors/cdc_dengue_cluster.py`、`collectors/motcmpb_nav_notices.py`（pulse-host 上
> `CDC_DENGUE_CLUSTER_ENABLED`／`MOTCMPB_NAV_NOTICES_ENABLED` 一律維持 false，避免雙跑）。
> 解析邏輯：`utils/tw_live_snapshots.py`（部署時 scp 到本目錄，與主 repo 同一份）。
> 規格與實測：taipei-gis-analytics `docs/topic-research/tw_layer_ext_20261120/b8/layer-plan.md`。

## 流程

```
cron：nav 每小時 :27、dengue 每日 15:13
  → 抓來源 → tw_live_snapshots 正規化
  → 本機 /var/lib/tw-live-snapshots/data/<collector>/YYYY/MM/DD/*.json（nav 只在內容變動時存）
  → （有 R2 金鑰時）R2 live/tw/{dengue_cluster,nav_notices}/latest.geojson＋status.json
每日 03:27：archive_tw_snapshots.py → s3://<bucket>/{cdc_dengue_cluster,motcmpb_nav_notices}/archives/YYYY-MM-DD.tar.gz
```

不寫 Supabase（每個人看都一樣的最新快照，ADR-0026 §5.1 Q2＝否）。

## 待站主決定：VM 的快照怎麼到 R2

VM 目前只有 S3 與 Supabase 金鑰，`pulse-secret-run vm-env-sync` 白名單也不含 R2_*。

| 方案 | 做法 | 優缺點 |
|---|---|---|
| A（建議） | VM 環境檔加 4 個 R2_* key，collect 腳本直接寫 R2 | 延遲最短；要把 R2 寫入金鑰放上 VM（範圍可限 `live/tw/` 用 R2 API token 的 bucket 權限） |
| B | VM 只寫 S3；pulse-host 另加一個小任務把 S3 最新檔鏡射到 R2 | VM 不碰 R2；多一段排程、延遲多一輪 |

選 A 時：擴充 vm-env-sync 白名單（要站主同意）或由站主手動填環境檔。

## 部署步驟（站主同意後）

1. 本機：`scp collect_tw_snapshots.py archive_tw_snapshots.py crontab.example ../../utils/tw_live_snapshots.py <VM_HOST>:/opt/tw-live-snapshots/`
2. VM：`mkdir -p /var/lib/tw-live-snapshots/{data,state} /var/log/tw-live-snapshots`；python 套件 `requests python-dotenv boto3`（其他 VM app 已裝）。
3. 環境檔：`pulse-secret-run vm-env-sync --src <既有 app> --dst tw-live-snapshots --keys S3_ACCESS_KEY,S3_SECRET_KEY,S3_BUCKET,S3_REGION`；R2 依上表。
4. 手動跑一次：`python3 collect_tw_snapshots.py dengue`、`python3 collect_tw_snapshots.py nav`、`python3 archive_tw_snapshots.py`（應印「無到期日期」）。
5. cron：先 `crontab -l > /root/crontab.bak.<日期>`，再把 `crontab.example` 的 BEGIN／END 區塊加進 root crontab。
6. logrotate：`/var/log/tw-live-snapshots/*.log` 加進 `/etc/logrotate.d/pulse-collectors`。
7. health_report：`external/_shared/health_report.py` 的 `ARCHIVE_TARGETS` 加兩個 collector；`DEFAULT_TARGETS` 加 nav（dengue 每日一次另設門檻）。
8. S3 lifecycle：`pulse-secret-run s3-lifecycle-add --names cdc_dengue_cluster,motcmpb_nav_notices --backup <data-collectors worktree>/docs/aws-lifecycle-backups/<日期>.json`。
9. `config/cross_layer_map.yaml` 兩筆改 `enabled: true`、`deployment: hicloud_vm`。

## 已知坑

- 登革熱：非流行季 23 檔都是空 FeatureCollection，0 筆是正常的；健康看 23 檔都 200、`_all` 與縣市聯集一致。
- 航船布告：來源約 9 MB、每次即時產生；同一圖形重複多列、時段兩欄對調、Circle／Sector 只給中心點，解析細節在 `utils/tw_live_snapshots.py`。
- TLS：只放寬 `VERIFY_X509_STRICT`，不關驗證（2026-11-21 本機 Python 驗證通過）。
