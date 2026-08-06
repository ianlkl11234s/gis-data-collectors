"""共機航跡示意圖向量化（CV pipeline）。

原始出處：taipei-gis-analytics `scripts/pla_tracks/`（研究期產物，方法演進與失敗
紀錄見該 repo `docs/topic-research/defense_pla/shape-extraction-methodology.md`）。
2026-08-06 搬進 data-collectors 供每日自動化使用 —— 此處為 canonical home，
analytics 端僅保留研究／疊圖驗證用途。

模組分工：
  vectorize_pla_chart  底層影像處理（紅色遮罩、灰網格線、經緯網格偵測、線性擬合）
  batch_vectorize      solve_georef：單張圖的像素↔經緯配準
  shape_extract        形狀抽取（分群 → Hough 矩形驗證 → 殘差輪廓）
  table_items          圖左上表格 OCR（tesseract -l eng）判項次數與類型
  georef               固化配準常數 + 單日 pipeline 封裝（自動化入口）

⚠ shape_extract 以模組級全域 `S` 記影像目錄（set_image_dir）。同一 collector
  不會併發（CollectorScheduler 有 skip-if-running），但**不可跨 collector 共用**。

⚠ 各檔尾端的 `__main__` CLI 區塊在此**不可執行**（package 相對 import 擋住直接
  跑檔案）。刻意保留不刪 —— 演算法要兩邊同步改，diff 越小越不容易漏。
  要用 CLI 請走 analytics 版：`taipei-gis-analytics/scripts/pla_tracks/`。
"""
