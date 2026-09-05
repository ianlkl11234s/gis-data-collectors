# Data Collectors agent entry

本 repo 的詳細規則以 [`.claude/CLAUDE.md`](.claude/CLAUDE.md) 為準；只按需讀取與當前任務相關的記憶、文件與 skills，不要全文載入。

- collector、排程、資料庫與部署是同一條鏈；修改範圍、測試、部署及外部寫入授權沿用既有規則，不可自行擴大。
- 可獨立的盤點、搜尋、格式整理交由 Luna；有明確邊界的實作、測試或 review 交由 Terra。主 agent 負責跨 repo 整合、scope、測試與最終驗收。
- Claude commands/hooks 不代表 Codex 會自動執行；以可用工具與已驗證 runtime evidence 為準。
