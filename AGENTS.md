# Data Collectors agent entry

本 repo 的詳細規則以 [`.claude/CLAUDE.md`](.claude/CLAUDE.md) 為準；只按需讀取與當前任務相關的記憶、文件與 skills，不要全文載入。

- collector、排程、資料庫與部署是同一條鏈；修改範圍、測試、部署及外部寫入授權沿用既有規則，不可自行擴大。
- 可獨立的盤點、搜尋、格式整理交由 Luna；有明確邊界的實作、測試或 review 交由 Terra。主 agent 負責跨 repo 整合、scope、測試與最終驗收。
- Claude commands/hooks 不代表 Codex 會自動執行；以可用工具與已驗證 runtime evidence 為準。

## Code Review Rules

- Collector 不得把 secret、token 或 credential 寫入 log、fixture、artifact 或 commit；外部 API 呼叫必須有合理的 timeout、bounded retry/backoff，並保留失敗與缺資料狀態。
- Supabase 寫入需檢查 TABLE_MAP、transformer、upsert/current key 與 schema 欄位一致；不得以錯誤 conflict key 靜默覆寫或製造重複資料。
- 新增或修改 collector 時，檢查 registry、toggle、cross-layer map、realtime tables、retention/backup manifest 與 gis-platform migration 契約是否同步，避免監控或保存鏈斷裂。

### 已決定、不需再提的事項

- 政府來源頁未標示授權的資料照常收集與寫入，下游來源卡標「授權待確認」即可；collector 不需為此另設 HOLD 或停收。
- Google 定位或 Google Places 推得的座標維持現狀，不要求額外標注或改授權欄。
- 正本在 gis-agent-system reports/code-review/POLICIES.md。
