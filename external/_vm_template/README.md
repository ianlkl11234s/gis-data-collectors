# external/_vm_template — HiCloud VM 收集器範本

> 需要台灣 IP 的收集器放 HiCloud VM（其他放 pulse-host）。新增一支時複製本目錄成
> `external/<name>_vm/`，把 `<name>`、`<app>` 換掉。VM 位址與帳號見私人筆記，不寫進 repo。
> 2026-11-08 整理 VM 時定下的標準（起因：舊歸檔只回掃 30 天，卡住約 1.8GB 舊檔；日誌沒輪替）。

## 標準流程

```
收集（cron）→ 本機留 N 天 → 每日歸檔 S3＋核對 → 日誌輪替 30 天 → 健康報告告警
```

| 環節 | 規則 | 範本 |
|---|---|---|
| 程式位置 | `/opt/<app>/`（自帶 `.env`：`S3_BUCKET`、`S3_ACCESS_KEY`、`S3_SECRET_KEY`、`S3_REGION`、`ARCHIVE_RETENTION_DAYS`） | — |
| 收集 | 每輪寫 `/var/lib/<app>/data/<name>/YYYY/MM/DD/<name>_HHMM.json`（台北時間） | `collect_template.py` |
| 本機保留 | `ARCHIVE_RETENTION_DAYS=N`：最近 N 天（含今天）留在本機 | — |
| 歸檔 | 每天一次，掃**本機所有**早於保留期的日期目錄（不限回掃天數）；打包 `s3://<bucket>/<name>/archives/YYYY-MM-DD.tar.gz`；S3 已有同名物件**不覆寫**、本地保留；上傳後 `head_object` 大小一致才刪本地 | `archive_template.py` |
| 排程 | root crontab 用 `# ─── <name> cron BEGIN/END ───` 包起來；分鐘錯開既有工作（見 `crontab.example`），歸檔放 03:00–03:30 | `crontab.example` |
| 日誌 | `/var/log/<app>/{collect,archive}.log`；加進 `/etc/logrotate.d/pulse-collectors`（daily、rotate 30、compress、copytruncate） | `logrotate.example` |
| 告警 | `external/_shared/health_report.py` 每天 07:00 推 S3 snapshot，pulse-host 的 daily_report 轉 Telegram；新 app 要加進 `ARCHIVE_TARGETS`（滯留檢查）與 `DEFAULT_TARGETS`（24h 成功率） | — |

journald 全機上限 300M（`/etc/systemd/journald.conf.d/50-pulse-size.conf`），新 app 不需另設。

## 新增收集器檢查清單

1. 複製範本、改名，填 `.env`（只寫 key 名稱進文件，值不進 repo）。
2. `python3 archive_<name>.py` 在 VM 手動跑一次：應印出「無到期日期」或上傳結果。
3. 加 cron（先 `crontab -l > /root/crontab.bak.<日期>`）。
4. logrotate：把 `/var/log/<app>/*.log` 加進 `/etc/logrotate.d/pulse-collectors`，`logrotate -d` 驗證。
5. health_report：`ARCHIVE_TARGETS`、`DEFAULT_TARGETS` 各加一筆，部署後手動跑一次看 `alerts`。
6. 已在其他主機收集過的 collector 搬上 VM 時，搬遷當天兩邊都會寫同一天：VM 版另存
   `YYYY-MM-DD.vm.tar.gz`，不要覆蓋（前例：CDC 2026-06-16、moa 7 支 2026-10-23）。
