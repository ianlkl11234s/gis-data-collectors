# 手冊維護規則

一句話：**改了什麼，就在同一個 PR 更新對應的那一份，而且只寫一次——其他地方用連結**。手冊會過時的原因從來不是沒人寫，而是同一件事寫在三個地方、只改了一個（2026-10 盤點時，`.claude/CLAUDE.md` 和 analytics `DATA_LIFECYCLE.md` 都還在教 2026-07 就廢掉的保留期做法）。

---

## 1. 什麼事件要更新什麼

| 觸發事件 | 必須更新 | 可選 |
|---|---|---|
| 新增或改動 collector | registry、`config.py`、`TABLE_MAP`、`cross_layer_map.yaml`（含 notes）、`realtime_tables.yaml`、`backup_manifest.yaml`、gis-platform migration＋`metadata.retention_policies`、S3 規則 `archive-deep-270d:{name}`；跑 `python3 scripts/gen_handbook_catalog.py` 重產 [collectors.md](collectors.md)（新 collector 要先在腳本的 `SOURCES_DOC` 登記分類）；在對應的 `sources/*.md` 加一段。完整順序見 [new-collector.md](new-collector.md) | [history.md](history.md) 加一行 |
| 停用、或改跑 VM | `cross_layer_map.yaml` 的 `enabled`／`deployment`；[`docs/EXTERNAL_COLLECTORS.md`](../EXTERNAL_COLLECTORS.md) 清單；`sources/*.md` 該段的狀態 | — |
| 事故 | `.claude/pitfalls/<YYYY-MM-DD>-<主題>.md` 寫長文；[pitfalls.md](pitfalls.md) 加一行索引；產生新原則時在 [principles.md](principles.md) 加一行並回連 pitfall；[history.md](history.md) 事故轉折表加一列 | [operations.md](operations.md) 排障段 |
| 調查完成 | `docs/investigations/` 放證據；**長期有效的結論回寫手冊**（證據檔本身是時點性的，不會有人回頭讀） | — |
| 保留期或 S3／R2 政策變動 | gis-platform migration（`metadata.retention_policies`）、[`docs/AWS_INVENTORY.md`](../AWS_INVENTORY.md)；[data-lifecycle.md](data-lifecycle.md) 只更新「去哪查」，**不抄天數** | — |
| 跨 repo 架構決策 | `.gis-agent-system/decisions/` 寫 ADR；手冊加連結 | — |
| 上游 API 行為的新發現 | taipei-gis-analytics `docs/api-platforms/`；`sources/*.md` 只加一行連結 | — |

### 正本在哪（不要複製）

| 內容 | 正本 | 手冊怎麼寫 |
|---|---|---|
| 表結構、保留期天數、cron | gis-platform migrations、`metadata.retention_policies` | 寫「去哪查」與 migration 號 |
| 表清冊 | gis-platform `docs/data-inventory.md` | 連結，不抄數量 |
| 上游 API 怎麼運作 | analytics `docs/api-platforms/` | 一行連結 |
| 事故經過 | `.claude/pitfalls/`、gis-platform `docs/research/` | 一句教訓＋連結 |
| 架構決策 | `.gis-agent-system/decisions/`（ADR） | 連結 |
| collector 清單、頻率、寫入表 | `collectors/registry.py`、`config.py`、`config/*.yaml`、`storage/supabase_tables.py` | 由腳本產生 [collectors.md](collectors.md)，不手寫 |
| S3 bucket、lifecycle 規則 | [`docs/AWS_INVENTORY.md`](../AWS_INVENTORY.md) | 連結 |
| VM 模式與 SOP | [`docs/EXTERNAL_COLLECTORS.md`](../EXTERNAL_COLLECTORS.md) | 連結 |
| Codex review 規則與「已決定」事項 | 根目錄 `AGENTS.md` | 不重寫 |

---

## 2. 守門機制

| 機制 | 擋什麼 | 狀態 |
|---|---|---|
| CI 檢查 [collectors.md](collectors.md) 是否最新 | 改了 registry、`config.py`、`cross_layer_map.yaml`、`TABLE_MAP` 或 `realtime_tables.yaml` 卻沒重產清單；新增 collector 卻沒登記 `sources/` 分類 | `tests/test_handbook_catalog.py`（呼叫 `scripts/gen_handbook_catalog.py --check`）。修法：跑 `python3 scripts/gen_handbook_catalog.py` 並 commit 產出 |
| PR template 的一題「是否需要更新 `docs/handbook/`？」 | 改了行為卻沒改文件 | `.github/pull_request_template.md` 已加 |
| `tests/test_cross_layer_sync.py` | registry、`config.py`、`cross_layer_map.yaml`、`realtime_tables.yaml` 之間 drift | 已在 CI |
| 每季掃描一次 | 文件狀態寫「未部署」「planned」「待上線」，實際已啟用（或反過來） | 人工，見下 |

每季掃描（約 1、4、7、10 月初）：

```bash
# 狀態字眼
rg -n '未部署|not deployed|planned|待上線|待部署' docs/
# 寫死的數字（應改成「以 yaml 為準」）
rg -n '[0-9]+ 張表|[0-9]+ 個 collector' docs/ README.md .claude/
# PUBLIC 守則（見 §3）
git grep -nE '[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}' -- '*.md'
git grep -nE '(root|ubuntu)@[0-9]'
```

把結果和 `config/cross_layer_map.yaml` 的 `enabled`／`deployment` 對照，不一致的改文件（或改設定），並在 PR 說明是哪一邊錯。

---

## 3. PUBLIC repo 守則

本 repo（ianlkl11234s/gis-data-collectors）是公開的，任何人都看得到所有檔案與 git 歷史。

| 不能放 | 可以放 |
|---|---|
| 金鑰值、token、連線字串、密碼 | 環境變數的**名稱**（例如 `CWA_API_KEY`） |
| VM 的位址與登入帳號（一律寫 `<VM_HOST>`、`<帳號>`，實際值放私人筆記） | bucket 名稱（程式碼裡本來就有） |
| 個人 email | 公開資料平台上本來就公開的 dataKey（例如 data.taipei） |
| bucket 權限矩陣、「哪支金鑰能讀哪些 bucket」、哪個 `.env` 裡有哪些金鑰 | 「需要哪些環境變數」 |

- 從 HEAD 刪掉不等於撤回公開：git 歷史仍查得到。真的外洩的祕密要**換掉**（例：2026-10-05 OSRM token 曾寫死在程式裡，處理方式是換新 token 並改從環境變數讀）。
- `docs/investigations/evidence/` 的營運 log（Zeabur runtime log、deployment JSON）提交前要人工抽查一次。
- Zeabur CLI 的 `variable env`／`variable create` 會把祕密值印到終端，輸出要導向 `/dev/null`，不要貼進文件或 PR。
- 提交前可以掃一次：`git diff --cached | rg -n 'postgres(ql)?://|Bearer |AKIA|sk-|@gmail\.com'`。

---

## 4. 踩到新雷時要做什麼

1. **先止血**：照 [operations.md](operations.md) 的排障步驟處理（共用資源出事時先切斷最大來源，不要先自動重啟）。
2. **留證據**：查詢結果、log、時間點放 `docs/investigations/<YYYY-MM-DD>-<主題>.md`（evidence 依 §3 抽查）。
3. **寫長文**：根因確定後寫 `.claude/pitfalls/<YYYY-MM-DD>-<主題>.md`：症狀、根因、修正、給後人的教訓。DB 端為主的事故，長文放 gis-platform `docs/research/`，本 repo 只補一段並連過去。
4. **抽原則**：教訓能寫成一行「以後一律怎樣」的，加進 [principles.md](principles.md)，附「為什麼」與 pitfall 連結。
5. **更新索引與歷程**：[pitfalls.md](pitfalls.md) 加一行；[history.md](history.md) 事故轉折表加一列。
6. **回頭修 SOP**：如果是上線步驟漏了某件事，改 [new-collector.md](new-collector.md)；如果是設計題沒問到，加進 §1 的設計檢查。
7. **查別處有沒有寫錯**：用 `git grep`／`rg` 掃本 repo 和相關 repo 有沒有同一件事的舊說法，改成連結到新正本。

判斷要不要寫：一次性、不會再發生的（例如某天上游維護）寫在 investigations 就好；會再發生、或會讓人做錯決定的，才進 pitfalls 與 principles。
