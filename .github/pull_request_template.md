## 摘要

<!-- 這個 PR 做了什麼、為什麼 -->

## 驗證

<!-- 跑了哪些測試、怎麼確認在正式環境有效（有跑／解析成功／有新資料） -->

## 檢查

- [ ] 是否需要更新 `docs/handbook/`？（新增或改動 collector、停用或改跑 VM、事故、保留期或 S3／R2 政策變動 → 是；規則見 `docs/handbook/maintenance.md`）
- [ ] 新增或改動 collector 時，已照 `docs/handbook/new-collector.md` 同步 registry、config、cross_layer_map、realtime_tables、backup_manifest 與 gis-platform migration
- [ ] 沒有寫入金鑰值、連線字串、VM 位址與帳號、個人 email（本 repo 是 PUBLIC）
