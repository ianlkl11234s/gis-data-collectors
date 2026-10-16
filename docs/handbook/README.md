# data-collectors 手冊

這是「**我們怎麼收資料**」的正本：架構、資料生命週期、各類來源怎麼收、新增 collector 的步驟、設計原則、監控與營運、踩過的雷和為什麼長成現在這樣。讀者是未來的自己、Claude／Codex agent，以及新加入的人。

上游 API 本身怎麼運作、資料庫端的調查全文、事故長文都不在這裡，手冊只放一句結論和連結（分工見文末）。

---

## 依你的情況從哪裡讀

| 你現在要… | 依序讀 |
|---|---|
| **第一次接觸這個 repo** | [architecture.md](architecture.md) → [data-lifecycle.md](data-lifecycle.md) → [principles.md](principles.md) → 瀏覽 [collectors.md](collectors.md) |
| **加一個新 collector** | [new-collector.md](new-collector.md)（設計 12 題＋上線步驟＋驗收）→ 對應的 `sources/*.md` → [principles.md](principles.md) |
| **某個 collector 出問題** | [operations.md](operations.md)（健康三件事、量測口徑、排障）→ [pitfalls.md](pitfalls.md) 找有沒有踩過 → 對應的 `sources/*.md` |
| **想知道某個設計為什麼這樣** | [history.md](history.md)（時間線＋事故轉折表）→ [pitfalls.md](pitfalls.md) 的長文連結 |
| **要改文件或手冊** | [maintenance.md](maintenance.md) |

---

## 每份檔一句話

| 檔案 | 內容 |
|---|---|
| [README.md](README.md) | 本頁：讀者路徑、檔案清單、分工 |
| [architecture.md](architecture.md) | 執行模型：registry → scheduler → BaseCollector → SupabaseWriter（TABLE_MAP、連線池、buffer、心跳、亂碼 allowlist）、長駐 worker、VM、內建 API |
| [data-lifecycle.md](data-lifecycle.md) | 熱溫冷層：Supabase → 本地 JSON → S3 tar → R2 發布；保留期「去哪查」、archive 驗證、備份、熱冷分離 |
| [collectors.md](collectors.md) | 所有 collector 的清單（頻率、執行位置、寫入表、熱冷層、特殊注意），由腳本從程式與設定檔產生 |
| [sources/tdx.md](sources/tdx.md) | TDX 系列（公車、台鐵、國道、路況、停車）：共用金鑰、全域節流、夜間暫停、401／429 |
| [sources/cwa-ncdr.md](sources/cwa-ncdr.md) | 中央氣象署與 NCDR：天氣、地震、落雷、雲圖、海象、UV、災害示警 |
| [sources/water-env.md](sources/water-env.md) | 水利署水文與 IoT、北市水利、環境部空品／放流水／煙道、輻射監測 |
| [sources/marine-air-space.md](sources/marine-air-space.md) | 船舶（AIS、AISStream、GFW）、航班（FR24、OpenSky）、衛星與發射 |
| [sources/jma-global.md](sources/jma-global.md) | 日本 JMA 全系列、環境省 WBGT、全球氣候（GFS、CMEMS、CAMS、USGS、颱風） |
| [sources/internet-health.md](sources/internet-health.md) | 網路健康：Cloudflare Radar、IODA、RIPE Atlas／RIS |
| [sources/gov-misc.md](sources/gov-misc.md) | 其他政府資料：急診、台電、新聞、全球事件、股市、共機、疾管署、移民署、矯正署、交通事故、動保、農產品價格、北市圖 |
| [new-collector.md](new-collector.md) | 設計前 12 題、跨 repo 上線步驟（唯一正本）、驗收「證明它真的在跑」、VM 分支 |
| [principles.md](principles.md) | 一行一條的設計原則，每條附為什麼與出處 |
| [operations.md](operations.md) | 監控與日報、健康三件事與三大盲點、量測口徑、Zeabur／VM 操作、事故第一步 |
| [pitfalls.md](pitfalls.md) | 事故索引：日期｜一句教訓｜長文連結（含其他 repo 的相關事故） |
| [history.md](history.md) | 迭代歷程時間線、Disk IO 三輪、事故轉折表 |
| [maintenance.md](maintenance.md) | 什麼事件更新什麼、守門機制、PUBLIC repo 守則、踩到新雷時的步驟 |

---

## 維護規則摘要

完整版見 [maintenance.md](maintenance.md)。

- **同一件事只寫一次**，其他地方用連結；保留期天數、表結構、上游 API 細節都不在手冊抄寫。
- 新增或改動 collector、事故、保留期政策變動時，**在同一個 PR** 更新對應檔案（PR template 有一題提醒）。
- [collectors.md](collectors.md) 由腳本產生，不手改。
- 本 repo 是 PUBLIC：不寫金鑰值、連線字串、VM 位址與帳號（用 `<VM_HOST>`）、個人 email。

---

## 和其他文件的分工

| 位置 | 放什麼 | 不放什麼 |
|---|---|---|
| 本手冊 `docs/handbook/` | 我們怎麼收資料：架構、生命週期、來源收法、上線 SOP、原則、營運、歷程 | 上游 API 細節、DB 調查全文、事故長文、保留期天數 |
| `.claude/CLAUDE.md` | agent 的硬規則與「做 X 先讀手冊哪一份」 | SOP 全文、架構說明 |
| `.claude/pitfalls/` | 本 repo 的事故長文（症狀、證據、根因、修正） | 原則本身（抽成一行放 [principles.md](principles.md)） |
| `docs/investigations/` | 時點性的調查與證據 | 長期有效的結論（要回寫手冊） |
| `AGENTS.md` | Codex review 規則與「已決定、不需再提」事項 | — |
| `docs/AWS_INVENTORY.md`、`docs/EXTERNAL_COLLECTORS.md` | S3 現況與 lifecycle 規則、VM 模式的正本 | — |
| taipei-gis-analytics `docs/api-platforms/` | 上游 API 怎麼運作：端點、配額、怪癖 | 我們的頻率、寫入表、守門機制 |
| taipei-gis-analytics `docs/DATA_LIFECYCLE.md` | 資料該走動態、半動態還是靜態的分流決策 | collector 上線細步（連到 [new-collector.md](new-collector.md)） |
| gis-platform migrations、`metadata.retention_policies`、`docs/data-inventory.md` | 表結構、保留期、cron、表清冊的正本 | — |
| gis-platform `docs/research/` | 資料庫端的調查（Disk IO 各輪） | — |
| `.gis-agent-system/decisions/` | 跨 repo 架構決策（ADR） | — |
| gis-wiki `topics/`（與 `inbox/` 草稿） | 跨專案通用的心智模型（例：資料工程盲點卡） | 本 repo 專屬操作 |

gis-platform、taipei-gis-analytics、.gis-agent-system 是 private repo，手冊裡只寫路徑。
