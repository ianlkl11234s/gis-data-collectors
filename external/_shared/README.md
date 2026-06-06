# external/_shared — HiCloud VM 共用工具

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
     └─ 上傳 s3://migu-gis-data-collector/_external_vm_health/<HOST>/YYYY-MM-DD.json

[每天 08:00] Zeabur daily_report
  → tasks/monitoring.list_vm_health_snapshots() 撈 _external_vm_health/ 下所有 host 最新 JSON
  → _section_external_vm_health 把結果格式化進 Telegram 日報
  → snapshot age > 26h → 標 VM 失聯 🔴
```

## 部署（新加裝 / 災害復原）

```bash
# 1. 從本機 Mac scp 過去
scp external/_shared/health_report.py external/_shared/setup_health.sh \
    root@210.61.15.74:/tmp/

# 2. 在 VM 上安裝
ssh root@210.61.15.74
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
  success_pattern: 'Supabase 寫入: (\d+) 筆'
  expected_interval_min: 5
```

## 維運

```bash
tail -f /var/log/external-health/health.log     # 每天 07:00 一段
```

暫停：`crontab -l | sed '/external-health/s/^/#/' | crontab -`

## 已知限制

- snapshot 是「自己看自己」，VM 死透就不會 push。Zeabur 端用「snapshot age > 26h → 失聯」反推
- 真要 ultra-paranoid 應該再加外部 ping（例如 Cloudflare cron Worker），但目前過剩
- log 分析寫死 `[INFO] Supabase 寫入: ...` regex，collector 改變 log 格式要同步更新
