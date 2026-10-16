# 專案運作原則（已搬移）

收集原則（寫入、冪等、負載、保留期、排程、量測、健康判讀、部署）的正本已搬到 [docs/handbook/principles.md](../docs/handbook/principles.md)，每條有固定編號與出處。
儲存分層見 [docs/handbook/data-lifecycle.md](../docs/handbook/data-lifecycle.md)，collector 執行與寫入流程見 [docs/handbook/architecture.md](../docs/handbook/architecture.md)。

以下保留不屬於手冊範圍的協作慣例。

## Git 慣例

- Commit message 用繁體中文 + conventional commits prefix (`fix:` / `feat:` / `refactor:` 等)
- 重大修正請在訊息中說明 **問題 → 修正 → 影響範圍**
- Push 到 `main` 即觸發 Zeabur 部署，**注意衝擊範圍**

## 開發環境

- **Python**: 3.11+
- **指令**: 統一使用 `python3` / `pip3`
- **本地測試 collector**: `python3 -m collectors.<name>` 或寫單檔測試腳本

## 與 Claude 協作

- **CLAUDE.md** 在 `.claude/`，記錄技術棧、表結構、常用指令等「always-on」上下文
- 重大決策或複雜 debug 完成後，在 `.claude/pitfalls/` 留長文，並在 [docs/handbook/pitfalls.md](../docs/handbook/pitfalls.md) 加一行索引
