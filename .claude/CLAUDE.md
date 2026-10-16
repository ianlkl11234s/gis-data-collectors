# Data Collectors - Claude 專案指引

GIS 基礎設施的**持續收集層**：Zeabur（主力）與 HiCloud VM 上 24/7 收集，寫入共用 Supabase（gis-platform）與 S3。上游是 taipei-gis-analytics（探索、分流決策），表結構與保留期在 gis-platform。

詳細知識都在 [docs/handbook/](../docs/handbook/README.md)；本檔只放硬規則與路由，不放 SOP 全文。

## 硬規則

1. **新增 collector 一律照 [handbook/new-collector.md](../docs/handbook/new-collector.md) 全部步驟走**：跨 repo 多步是刻意設計，不算過度流程；漏一步就斷鏈且多半默默漏報。
2. 新 collector 在 `_COLLECTOR_TOGGLES` **預設 `enabled=False`**；跑在 HiCloud VM 的 collector，Zeabur 上對應 `*_ENABLED` **一律 `false`**（雙跑會重複寫入）。
3. **保留期只在 `metadata.retention_policies` 登記一列**（gis-platform mig 282 起），不要改清理函式、不要找「硬編碼清單」。天數不寫進文件，去註冊表或 migration 查。
4. 新表建在 `live`，**同一支 migration 就 ENABLE RLS**＋anon／authenticated SELECT policy（範本 gis-platform `418_funeral_enable_rls.sql`）。
5. 走 Supavisor transaction pool（6543）**禁止 session 級 SET**（含 `set_session`），只用交易內 `SET LOCAL`。
6. 寫 DB 的時間一律 tz-aware；缺值存 NULL，不可當 0。
7. 要加唯一索引去重時，**先部署 collector 的冪等寫入，再建唯一索引**。
8. 動到 S3 storage class／lifecycle／prefix 後，**同步更新 [docs/AWS_INVENTORY.md](../docs/AWS_INVENTORY.md)**；新 collector 要補 `archive-deep-270d:{name}`（整份覆寫，不可只送新規則）。
9. 本 repo 是 **PUBLIC**：不寫金鑰值、連線字串、VM 位址與登入帳號（寫 `<VM_HOST>`）、個人 email；Zeabur CLI 輸出祕密值時導向 `/dev/null`。
10. 程式行為以程式碼為準（例：寫入層是連線池 `storage/db.py`；scheduler pool 大小見 `main.py` 的 `run_collectors()`），文件與程式不一致時改文件。

## 做 X 先讀

| 要做的事 | 先讀 |
|---|---|
| 新增或改動 collector | [handbook/new-collector.md](../docs/handbook/new-collector.md)、對應的 `handbook/sources/*.md` |
| 了解架構、寫入流程、TABLE_MAP 模式 | [handbook/architecture.md](../docs/handbook/architecture.md) |
| 保留期、S3／R2、歸檔、備份 | [handbook/data-lifecycle.md](../docs/handbook/data-lifecycle.md) |
| collector 出問題、監控、量測 Disk IO | [handbook/operations.md](../docs/handbook/operations.md)、[handbook/pitfalls.md](../docs/handbook/pitfalls.md) |
| 判斷某個寫法對不對 | [handbook/principles.md](../docs/handbook/principles.md) |
| 改 HiCloud VM 上的 collector | [docs/EXTERNAL_COLLECTORS.md](../docs/EXTERNAL_COLLECTORS.md)、`external/{name}_vm/README.md` |
| 想知道為什麼這樣設計 | [handbook/history.md](../docs/handbook/history.md) |
| 改文件、踩到新雷 | [handbook/maintenance.md](../docs/handbook/maintenance.md) |
| 本地測試、Git 慣例 | [.claude/principles.md](principles.md) |
