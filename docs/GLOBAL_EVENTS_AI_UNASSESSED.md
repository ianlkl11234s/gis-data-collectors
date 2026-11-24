# 全球事件候選：「AI 未判讀」契約

2026-11-24 起。跨 repo：data-collectors（寫入）→ gis-platform（欄位，migration 566）→ mini-taiwan-pulse（顯示，`docs/features/global-events/handoff.md`）。

## 狀態定義

候選永遠不會再有 AI 判讀時，稱為「AI 未判讀」，有兩種原因：

| 代碼 | 什麼時候 | 給人看的理由（`reason_zh_tw`，逐字固定） |
|---|---|---|
| `provider_rejected` | AI 服務的內容審查擋下這筆。同一輪內把該批二分重送，單獨送出仍被擋才算 | AI 未判讀：這則新聞沒有通過 AI 服務的自動內容審查，所以沒有 AI 判讀，之後也不會再判；不代表不重要。 |
| `model_failed` | 同一筆連續 4 輪都拿不到有效的判讀（例如輸出截斷、格式錯誤），被放行出佇列 | AI 未判讀：多次嘗試都沒有取得有效的 AI 判讀，之後不會再判；不代表不重要。 |

兩種都寫成 `assessment_status = 'pending'`（資料庫只允許 assessed｜pending），`decision`、`assessed_at` 都是 NULL。之後移出佇列，不會每小時重送，也不會等 48 小時過期後無聲消失。

供應商名稱、錯誤代碼（例如 `data_inspection_failed`、`content_policy_violation`）不寫進給人看的文字，只留在 collector log 和私有 run manifest 的 `routing.ai_rejected`。

## 機器可讀標記

- **現在（不需 migration）**：`reason_zh_tw` 等於上表兩句之一，就是「AI 未判讀」。這兩句是常數（`collectors/global_events.py` 的 `AI_UNASSESSED_REASONS_ZH_TW`），改字要同步改前端的比對表。
- **gis-platform migration 566 套用後**：在 pulse-host 的 collector.env 設 `GLOBAL_EVENTS_ASSESSMENT_NOTE_ENABLED=true` 並重建容器，collector 會另外送 `assessment_note`（`provider_rejected`｜`model_failed`），window RPC 會帶到前端。前端優先看 `assessment_note`，沒有才比對文字。
- **順序**：566 沒套之前不可開這個旗標。舊版 ingest 有欄位白名單，多一個鍵會整批拒收。測試 `test_flag_off_sends_only_565_keys_flag_on_adds_assessment_note` 守住這一點。

## 舊資料

2026-11-24 13:09（台北）寫入的那一筆（`cand_f78816ff6d4876eaa1a836da`）用的是舊句子「AI 供應商內容審查拒絕判讀（Alibaba data_inspection_failed）；本筆未經 AI 判讀，不代表不相關或不重要。」。前端把這句也當成 `provider_rejected`。候選表有不可修改的 trigger，不能 UPDATE；不需要另外修正。

## 什麼情況仍然整輪中止

- 401、402、沒有內容審查標記的 403。
- 一輪裡沒有任何一批判讀成功，而且被擋下的筆數超過 max(5, 半數)：這種情況比較像帳號或供應商政策改變，不像幾則敏感新聞。
