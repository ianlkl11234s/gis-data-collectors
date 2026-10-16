# Claude 協作筆記

此資料夾收錄與 Claude Code 協作時累積的專案知識、踩過的坑、以及運作原則。
目的：避免重複犯錯、加速未來協作上下文。

## 結構

```
.claude/
├── README.md              # 本檔案，索引
├── principles.md          # 協作慣例（收集原則已搬到 docs/handbook/principles.md）
└── pitfalls/              # 過往踩過的坑（按日期 + 主題命名）
    └── YYYY-MM-DD-<主題>.md
```

## 文件清單

### 原則 & 慣例
- [docs/handbook/principles.md](../docs/handbook/principles.md) — 收集原則正本（固定編號、附出處）
- [principles.md](principles.md) — Git、開發環境、與 Claude 協作慣例

### Pitfalls（踩坑紀錄）

索引統一放在 [docs/handbook/pitfalls.md](../docs/handbook/pitfalls.md)（日期｜一句教訓｜連結，含外部 repo 的相關事故）。長文仍放在本資料夾 `pitfalls/`。

## 怎麼使用

- 新發現的 bug / 設計陷阱 → 在 `pitfalls/` 加新檔案，並在 [docs/handbook/pitfalls.md](../docs/handbook/pitfalls.md) 加一行
- 新確立的收集原則 → 補進 [docs/handbook/principles.md](../docs/handbook/principles.md)（接在該類最後，不重排編號）
- Claude Code 開啟此專案時會自動讀到 `.claude/CLAUDE.md`，可在其中引用此資料夾與手冊的文件
