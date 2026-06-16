# 外部 Collectors（台灣 IP 必需）

> 此文件記錄**因網路限制無法在 Zeabur 跑、必須在台灣 ISP 機房跑**的 collector 們，
> 以及未來新增此類 collector 的標準流程。

## TL;DR

部分台灣政府 API（例如航港局 AIS）對 firewall 白名單**只放台灣 ISP 的 AS**
（HiNet / SeedNet / TFN / 遠傳 / 台灣大 / 是方 / 亞太），擋掉所有國際雲商
（Linode/Zeabur、Google Cloud、AWS、Azure 等）。

解決方式：在中華電信 **HiCloud** 開最小規格 VM（NT$ 480/月），cron 跑單檔 collector，
寫入同一個 Supabase + 同一個 S3 bucket，與主 repo 的儲存規格完全對齊。

---

## 目前清單

| Collector | 來源 API | 部署位置 | 子目錄 |
|---|---|---|---|
| `ship_ais` | `mpbais.motcmpb.gov.tw` (航港局) | HiCloud VM `210.61.15.74` | [`external/ship_ais_vm/`](../external/ship_ais_vm/) |
| `waste_positions` | 高雄/新北/台南 GPS 三家 | 同上 VM | [`external/waste_positions_vm/`](../external/waste_positions_vm/) |
| `cdc_public_health_weekly` | 疾管署 `od.cdc.gov.tw` | 同上 VM（待部署） | [`external/cdc_public_health_weekly_vm/`](../external/cdc_public_health_weekly_vm/) |

主 repo 對應 collector 的 `*_ENABLED` 環境變數**必須在 Zeabur 上設為 `false`**（避免雙跑、避免無意義的 timeout 錯誤刷 log）。

---

## 怎麼判斷一個 collector 是否需要走外部？

### 症狀
- Collector 在 Zeabur 連續 timeout、但本機（HiNet）測 curl 正常 → **大概率 IP 封鎖**

### 確認步驟
1. **本機**測 `curl -I <API URL>`，看 HTTP status
2. **Zeabur 容器內**測連線：
   ```bash
   zeabur service exec --id <SVC_ID> -- python3 -c \
     "import socket,urllib.request; \
      print(socket.gethostbyname('<HOST>')); \
      print(urllib.request.urlopen('https://api.ipify.org',timeout=10).read())"
   ```
3. 若 DNS 通但 TCP 443 timeout → **IP 被擋**
4. 若 Zeabur 出口 IP 是 Linode/Akamai 段、本機是 HiNet → **AS 被擋**（最常見）

### 推測規則
| 政府網域 | 風險 |
|---|---|
| `*.motcmpb.gov.tw`（航港局）| 高 |
| `*.afa.gov.tw`（空中航管） | 推測高 |
| `od.cdc.gov.tw`（疾管署 CSV 下載）| **高**（2026-06-16 實證 Zeabur timeout，雖然 `data.cdc.gov.tw` CKAN API 仍開放）|
| `*.cwa.gov.tw`（氣象） | 中（部分 API 仍開放） |
| `data.gov.tw` | 低（國際 IP 多半 OK） |
| `tdx.transportdata.tw` | 低（明確開放 API 給開發者） |

**對策**：新增 collector 第一次部署到 Zeabur 後，先觀察 24 小時錯誤率，若是這類 timeout pattern 就切外部。

---

## 新增「外部 collector」的 SOP

### Step 1：在主 repo 照常實作
依照 `.claude/CLAUDE.md` 的 7 步流程把 collector 寫好（registry、config、table_map、transform、data-inventory…）。
**不要為了「反正會放外部」而簡化 — 主 repo 仍要有完整實作**，理由：
- transform 邏輯 + table schema 是 **唯一真相來源**，VM 的單檔版照抄它
- 如果哪天封鎖解除，主 repo 改回 `_ENABLED=true` 就能直接跑

### Step 2：Zeabur 設 `_ENABLED=false`
部署到 Zeabur 但**預設關閉**。
在 collector 的 docstring 標註 `⚠️ Taiwan IP required, runs on external VM`。

### Step 3：在 `external/<name>_vm/` 寫單檔版
參考 [`external/ship_ais_vm/`](../external/ship_ais_vm/) 的結構：

```
external/<name>_vm/
├── README.md                # 該 collector 在 VM 的部署文件
├── <name>_collect.py        # 主 collector（單檔，fetch + transform + DB write）
├── archive_<name>.py        # 每日 S3 歸檔（如需要）
├── setup_vm.sh              # 一鍵裝環境
├── setup_cron.sh            # 安裝 cron
├── test_<host>.py           # 驗證 VM 能連得到目標 API
├── test_s3.py               # 驗證 S3 key
└── .env.example             # env 範本
```

### Step 4：部署到 VM
- 共用同一台 HiCloud VM（資源充足前都堆同台、降低成本）
- App dir 分開：`/opt/<name>/`
- Cron 加新規則（用 `# ─── <name> cron BEGIN/END ───` 標記方便將來移除）
- log 分流：`/var/log/<name>/*.log`

### Step 5：更新文件
- 把該 collector 加進本檔的「目前清單」表格
- 更新 `gis-platform/docs/data-inventory.md`（標註 deployment = External VM）
- 在 `.claude/CLAUDE.md` 也提一下（已內建在外部 collector 章節）

---

## VM 維運常識

### 共用一台 VM 的容量規劃
HiCloud b.1c1g（1vCPU/1GB/30GB）能撐到大約：
- 5–10 個 collector（依資料量、interval）
- 磁碟主要花在 7 天 snapshot retention（每 collector ~50–500 MB/天）
- 滿了再升級 `b.2c4g` 或加掛 storage

### log 輪替
TODO：尚未設 logrotate。`/var/log/ship-ais/collect.log` 一年約 50MB，先不急，超過 100MB 再加：
```
/etc/logrotate.d/ship-ais
```

### 監控
TODO：目前依賴主 repo 的 `notify_error`。VM 沒有 notify hook，得想辦法把錯誤 push 出來（簡單做法：cron 失敗自動 mail，或寫個 healthcheck endpoint）。

### 帳號 / 帳單
- HiCloud 帳號：個人 Gmail `ianlk11234s@gmail.com`
- 月結，刷信用卡
- Console: https://hicloud.hinet.net/

### 災害復原
完整流程在每個 `external/<name>_vm/README.md` 都有「Deploy 流程」一節，照走 10 分鐘可在新 VM 復原。
重點：scp 6 個檔 → 跑 setup_vm.sh → 填 .env → 跑 setup_cron.sh。**不需要 git clone、不需要 GitHub 認證**。

---

## 為什麼不用其他方案？

| 方案 | 否決理由 |
|---|---|
| AWS / GCP / Azure 任一 region | 即使在台灣機房，出口 IP 仍是雲商自己的 AS，被白名單擋 |
| Cloudflare Workers | Workers egress 走 Cloudflare anycast IP，不是台灣 ISP |
| 家裡電腦 + Cloudflare Tunnel | 0 成本但 SLA 不可控（停電/搬家/換 ISP 就掛） |
| 跟主 repo 共用 Zeabur 容器 + 加 proxy | 還是要找台灣 proxy，問題回到原點 |
| 跟航港局申請白名單 | 可以平行嘗試，但通常流程慢、不保證過 |

驗證過程記錄於本檔 commit 2026-06-06。
