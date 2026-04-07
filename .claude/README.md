# Claude 協作筆記

此資料夾收錄與 Claude Code 協作時累積的專案知識、踩過的坑、以及運作原則。
目的：避免重複犯錯、加速未來協作上下文。

## 結構

```
.claude/
├── README.md              # 本檔案，索引
├── principles.md          # 專案運作原則 / 慣例
└── pitfalls/              # 過往踩過的坑（按日期 + 主題命名）
    └── YYYY-MM-DD-<主題>.md
```

## 文件清單

### 原則 & 慣例
- [principles.md](principles.md) — 專案運作原則、開發 / 部署 / 資料處理慣例

### Pitfalls（踩坑紀錄）

| 日期 | 主題 | 摘要 |
|------|------|------|
| 2026-04-07 | [Supabase 寫入 +8h 時區偏移](pitfalls/2026-04-07-timezone-bug.md) | `datetime.now()` naive 導致全部時間戳偏移 8 小時，發現後從 S3 全量回補 30 天歷史資料 |

## 怎麼使用

- 新發現的 bug / 設計陷阱 → 在 `pitfalls/` 加新檔案，並更新本檔的清單
- 新確立的開發原則 → 補進 `principles.md`
- Claude Code 開啟此專案時會自動讀到 `CLAUDE.md`（在專案根目錄），可在 `CLAUDE.md` 引用此資料夾的文件
