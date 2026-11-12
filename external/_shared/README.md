# external/_shared — HiCloud VM 共用工具

> VM 位址與登入帳號不放在公開 repo：文中的 `<VM_HOST>`、`<帳號>` 請見私人筆記。

放跨 collector 共用的 VM 端工具。

## 目前內容

| 檔案 | 用途 |
|---|---|
| `health_report.py` | 每天 07:00 跑一次，把 VM health snapshot 推 S3 給 Zeabur daily_report 撈 |
| `setup_health.sh` | 在 VM 上安裝 health_report.py + cron |

## 為什麼要這個

`tasks/daily_report.py` 已經自動涵蓋了「VM 上的 collector 是否有寫入 Supabase / S3」，
但**看不到 VM 本身的狀態**（VM 死了 / cron 掛了 / 磁碟滿了 / IP 又被擋了）。
這個 health_report 補上這塊。

## 架構

```
[每天 07:00] HiCloud VM cron
  → /opt/external-health/health_report.py
     ├─ tail 各 collector log 統計 24h runs/success/last_success/last_count
     ├─ 系統指標 (uptime / loadavg / disk%)
     ├─ Outbound 健檢（對每個 collector 來源各 ping 一次 — 提早發現 IP 又被擋）
     ├─ 列出 data dir 各子目錄大小
     ├─ 告警檢查（alerts）：磁碟 >80%、日期目錄超過保留天數+2 天未清（ARCHIVE_TARGETS）
     └─ 上傳 s3://migu-gis-data-collector/_external_vm_health/<HOST>/YYYY-MM-DD.json

[每天 08:00] Zeabur daily_report
  → tasks/monitoring.list_vm_health_snapshots() 撈 _external_vm_health/ 下所有 host 最新 JSON
  → _section_external_vm_health 把結果格式化進 Telegram 日報
  → snapshot age > 26h → 標 VM 失聯 🔴
  → snapshot.alerts 非空 → host 標 🔴 並逐條列出
```

## 部署（新加裝 / 災害復原）

```bash
# 1. 從本機 Mac scp 過去
scp external/_shared/health_report.py external/_shared/setup_health.sh \
    <帳號>@<VM_HOST>:/tmp/

# 2. 在 VM 上安裝
ssh <帳號>@<VM_HOST>
bash /tmp/setup_health.sh

# 3. 手動跑一次驗證
python3 /opt/external-health/health_report.py
# 預期 stdout 印出 snapshot JSON + "✓ uploaded → s3://..."
```

`.env` 共用 `/opt/ship-ais/.env`（已內建邏輯，找到就讀）。無需另外設定。

## 自訂監測 collector 清單

預設只監測 `ship_ais` 與 `waste_positions`。未來新增第三支 collector 時：

1. 編輯 `health_report.py` 加入 `DEFAULT_TARGETS`
2. 或者用 yaml 指定（環境變數 `EXTERNAL_VM_HEALTH_CONFIG=/path/to/targets.yaml`）

範例 `targets.yaml`：
```yaml
- name: new_collector
  log_path: /var/log/new-collector/collect.log
  data_dir: /var/lib/new-collector/data/new_collector
  outbound:
    - {label: source_api, host: api.example.gov.tw, path: /endpoint}
  run_pattern: '\[INFO\] snapshot → |\[ERROR\] FAILED: '   # 每輪恰好命中一行
  success_pattern: 'Supabase 寫入: (\d+) 筆'
  expected_interval_min: 5
```

`run_pattern` 要挑「每一輪恰好印一行」的 log（成功與抓取失敗各一種），不能用每個城市各印一行的訊息。

## 維運

```bash
tail -f /var/log/external-health/health.log     # 每天 07:00 一段
```

暫停：`crontab -l | sed '/external-health/s/^/#/' | crontab -`

## 已知限制

- snapshot 是「自己看自己」，VM 死透就不會 push。Zeabur 端用「snapshot age > 26h → 失聯」反推
- 真要 ultra-paranoid 應該再加外部 ping（例如 Cloudflare cron Worker），但目前過剩
- log 分析靠各 target 的 `run_pattern`／`success_pattern`，collector 改 log 文字要同步更新（`tests/test_vm_health_log_parse.py` 從收集器原始碼取樣本行，改了會紅）
- 有 logrotate 留下的 `collect.log.1`（或 `.1.gz`）時會一併讀，否則每日輪替後 07:00 只看得到午夜後 7 小時；snapshot 的 `earliest_log_at` 遠晚於 24 小時前代表 log 被截斷
