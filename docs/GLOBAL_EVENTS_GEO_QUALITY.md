# Global Events：地理標記品質與「重要事情」可見性

調查日期：2026-09-05 · 母體：`intel.global_event_candidate_observations`（唯讀）＋ GDELT GKG 原始檔實測 join
相關：[ADR-0002](https://github.com/iCHEF/taipei-gis-analytics)（`docs/adr/0002-global-event-candidate-visibility.md`）、`gis-platform/migrations/397_`–`399_`

起因：使用者回報「一些國家的標記做得還不夠好」，INTEL 面板 526 件中 117 件「待定位」，且大量低價值條目與重大事件並列。

---

## 1. 基線

表是 append-only immutable，同一 `candidate_id` 會有 pending / assessed 兩列，**所有比率都必須先
`DISTINCT ON (candidate_id) ORDER BY candidate_id, available_at DESC` 去重**，否則會被重複列灌水。

### 1.1 最近 7 天（去重後）

| 指標 | 值 |
|---|---|
| 候選 | 558 |
| 待定位（`places = []`） | 129（23.1%） |
| keep_core / keep_watch / drop_noise | 163 / 162 / 233（29% / 29% / **42%**） |

### 1.2 關鍵切點：metadata fallback 上線前後

`50cc8df`（Show source-backed approximate places when Qwen has no location selection）在
**2026-09-03 09:00–10:00 UTC** 之間上線。

| 寫入時間 (UTC) | 寫入列數 | 含 `metadata_fallback` | 待定位 |
|---|---|---|---|
| 2026-09-03 09:00 | 150 | 0 | **105** |
| 2026-09-03 10:00 起（全部） | 408 | 全批皆有 | 74 |

→ **截圖裡 117 筆待定位有約 105 筆是上線前的舊資料**，會自然滾出 7 天視窗，不需要任何改動。

### 1.3 上線後基線（`available_at >= 2026-09-03 10:00 UTC`，去重後）— 評估改版就用這個

| 指標 | 值 |
|---|---|
| 候選 | **507** |
| 待定位 | **75（14.8%）** |
| 有「模型選出的」place（非 fallback） | 262（51.7%） |
| keep_core / keep_watch / drop_noise | 170 / 165 / 172 |
| 標題 fingerprint 跨輪重複 | 35 組、多出 55 列（10.8%） |

`decision × 待定位`：keep_core 7/170（4.1%）、keep_watch 20/165、**drop_noise 48/172（27.9%）**
→ 待定位高度集中在垃圾條目；真正重要的事件只有 4% 定不出來。

`decision × severity` 幾乎共線（keep_core 只落在 2–3、keep_watch 只落在 1–2、drop_noise 只落在 0–1），
`confidence` 也幾乎無鑑別力（0.9 這一格 keep_core 78 筆、drop_noise 72 筆）。
→ 前端拿 severity／confidence 排序等於拿 decision 排序，沒有第二個獨立維度可用。

`taiwan_relationship = 'none'` 佔 464/507 = **91.5%**；`taiwan_impact_zh_tw` 平均 19.4 字。
（`validate_stage1` 早就允許 `none` 時傳空字串並自動補字，但 prompt 沒告訴模型，等於白寫。）

---

## 2. 診斷：待定位的根因拆分

方法：取 8 個 GKG slot 下載原始檔（standard + translation 共 16 檔），用候選的**全部**代表文件 URL
反查 GKG `V1LOCATIONS`(col 9)。窗內去重候選 78 筆，其中 26 筆待定位。

| 桶 | 筆數 | 佔比 | 程式成因 |
|---|---|---|---|
| GKG `V1LOCATIONS` 完全空 | 18 | 69% | GDELT 端 geocoder 失敗；本 repo 原無 headline fallback |
| 只有 type 2（US State）/ 5（World ADM1） | 8 | 31% | `candidate_display_records` 的 kind map 只收 1/3/4，其餘 `continue` |
| **有 type 1/3/4 + 座標卻沒被用** | **0** | **0%** | fallback 已榨乾 GKG |

**國家標錯 0 筆，只有沒標。** 對照組 52 筆已定位全部落在「有 type 1/3/4」。

進一步人工判讀 26 筆待定位：約 10 筆「GKG 空但標題寫得很清楚」（`Egypt's South Sinai`、
`Wollongong`、`Mt Maunganui`、`Osoyoos`…），約 8 筆真的無地點（且其中 7 筆是 `drop_noise`，
本來就不該進 LLM）。

**為什麼只有 51.7% 有模型選點**：`validate_stage1` 要求 `basis` 是**該 evidence 所屬那一篇**標題的
逐字子字串，但 GKG `name` 是英文而 translation stream 的標題是外文，這個門檻對外文候選近乎不可能通過。
（此為程式＋樣本推導的機制，非量測值——rejection 只寫進 S3 run manifest，DB 看不到。）

### 使用者點名的例子，逐一歸因

| 使用者看到 | 原始標題 | decision | places | 根因 |
|---|---|---|---|---|
| 干邑熱潮：馬戲舞蹈與驚險體驗 | `Coup de chauffe à Cognac : trois jours de cirque…` | drop_noise | 1 | routing regex `coup` 誤命中法文 `coup de` |
| 高等教育界作弊風氣猖獗（待定位） | `Cheating epidemic in higher ed` | drop_noise | 0 | regex `epidemic` 誤命中隱喻 ＋ GKG 無 location |
| 「唯一可行的交通工具」：尼亞加拉瀑布市（待定位） | `'Only vehicle possible': Niagara Falls extends state of emergency` | drop_noise | 0 | fallback 上線前的舊列（GKG 其實有 type 3） |
| 夏威夷食物籃發放百萬磅援助物資 | `Hawaii Food Basket distributes nearly 1M pounds of aid…` | keep_watch | 3 | `hurricane` 命中，但這是災後行政／公益後續 |

---

## 3. 建議 A：確定性修正（不動 LLM 契約、不改 prompt）

### A-1a｜type 2/5 降級為國家代表點 ✅ 已實作

ADM1 mention 是「國家」的真實證據，但座標是州質心。做法：掃**整個 batch** 所有候選的 type 1 evidence
建 `country_code -> 國家代表點` anchor（同 cc 依 `evidence_id` 排序取最小，保證同批次結果一致），
ADM1-only evidence 用該 anchor 的名稱與座標發佈 `country_center`，`evidence_basis` 明寫
「來源僅提供一級行政區提及（…），降級為國家代表點；座標為 GDELT 在本輪其他文章中使用的該國固定質心，
非本文報導之位置。」

> ⚠️ anchor **必須是 batch 級**。候選在 fallback 之後還待定位，就代表它自己的 evidence 裡沒有可用的
> type 1；只掃單一候選的 anchor 永遠不會 fire。

不做的替代方案：新增 `admin1_center` kind（要動 migration 399 CHECK + RPC 398 + 前端 + workbench，
四個 repo）；把州質心直接標成 `country_center`（語意造假）。

### A-2｜Headline gazetteer ✅ 已實作

`migrations/399_global_event_headline_geography.sql` **早就把路鋪好了**：`source_kind` allowlist 已含
`headline_gazetteer`、lineage regex 已允許 `<kind>:<url>#<片段>` 形式、只禁止它搭 `event_point`。
但 collector 從來沒產出過這個 source_kind。**零 migration 可實作。**

做法（不引入任何外部資料集）：在 `parse_gkg_artifact` 掃全部列時，把 GDELT 自己的
`V1LOCATIONS` 地名累進成索引（`短名 -> 型別/國碼/座標`，同名取更具體者），
對 `places == []` 的候選用代表標題做最長地名比對。名稱與座標都來自 GDELT 自己的 geocoding，
只有「這個標題提到這個地名」是我們的判斷，而 `evidence_basis` 逐字引用標題讓人可覆核。

精確度與 recall 的實作要點：

- **只比對第一個 `|` / ` - ` / ` – ` / ` · ` 之前的片段**——GDELT 標題常帶 `| My Grande Prairie Now`
  這種發行商後綴，實測就是這一條造成假陽性。
- **masthead stop-list**（United / News / Post / Times / Herald / Independent / Guardian…）——
  這些字 GDELT 也會 geocode 成聚落。
- **NFKD + 折疊 okina 與連字號**（`Hawaiʻi` → `hawaii`），但**撇號當詞界**
  （`Egypt's South Sinai` → `egypt s`，否則折成 `egypts` 就再也對不上國家 `Egypt`）。
- 地名長度 ≥ 4；ADM1 型別的 gazetteer 條目**不發佈**（州質心且此路徑無 anchor）。
- **索引必須持久化**：`collect()` 在有 `routing_work` 時直接沿用上一輪的 batch，**不再跑
  `parse_gkg_artifact`**，這條 resume path 上 run-local 索引會是空的。

**實測 recall**：26 筆待定位原始命中 8 筆，扣掉已屬 A-1a 桶的 2 筆與發行商假陽性 1 筆，
**A-2 淨增量 5/26 ≈ 19%（下限）**；桶「GKG 空但標題可推」的天花板是 10/26 ≈ 38%。
跨輪字典成長後會往天花板靠。

### A-3｜放寬 `basis` 綁定（**本 PR 不含，下一支**）

`validate_stage1` 目前要求 `basis` 出自 evidence 所屬那一篇的標題，建議放寬為「該候選任一代表文件標題」
（仍然來源綁定、仍不可造座標）。這是唯一需要改 validator 的一項。

---

## 4. 建議 B：prompt 改版（**本 PR 不含，下一支**）

現行 stage1 prompt 的問題，逐條：

| # | 問題 | 證據 |
|---|---|---|
| B1 | **完全沒有重要性判準**——system prompt 只說「依人命、生活、社會或跨境影響判斷」，沒有 core/watch/drop 的門檻定義，也沒有例子。對照同 repo 的 `news_events.py` v2 prompt（`gis_relevance` / `severity` / `is_event` 三維度、每一級寫死定義），global_events 明顯落後。 | §2 表：使用者點名的 4 例有 3 例落在「準備／教學／回顧」「災後行政」「藝文活動」「比喻用法」四類 |
| B2 | severity 與 decision 共線，前端沒有第二個排序維度 | §1.3 |
| B3 | `taiwan_impact_zh_tw` 白寫（validator 早就允許 `none` 時留空） | §1.3 |
| B4 | 輸出長度撐滿：每筆 ~143 中文字，10 筆/chunk 撞 `content_length` 4.2k–8k 與偶發 `finish_reason=length` | §1.3 |
| B5 | 繁中標題保留 ASCII 引號 lead、報社後綴、未譯地名 | §2 尼亞加拉那筆 |
| B6 | 沒有 few-shot | — |

改版時**必須同步把 `STAGE1_PROMPT_VERSION` 從 `v3` 升到 `v4`**——它是 `stage1_cache` 的 cache key 之一，
不改的話舊快取會回放舊回應，改版等於沒上。

---

## 5. 建議 C：LLM 前 pre-filter

### C-1｜per-signal veto ✅ 已實作

不是候選層級砍掉，而是**讓誤命中的那個 signal 失效**；候選若還有其他 signal 就照樣進 LLM。
這保住了 `What to know about a reported US airstrike that hit a wedding in Iran`
（真 keep_core，會被候選層級的「what to know」規則誤殺）。

**實測（507 筆上線後候選，只用標題重跑 regex）**

| 版本 | 砍掉 | drop_noise | 誤砍 core | 誤砍 watch | 精確度 |
|---|---|---|---|---|---|
| **採用版（不含 cyclone 規則）** | **32 / 507 = 6.3%** | **28** | 2 | 2 | **88%** |
| 含 cyclone 規則 | 38 / 507 = 7.5% | 32 | 3 | 3 | 84% |

**刻意不放 `cyclone` 規則**：forward-only lookahead 分不出 `after Cyclone Narelle`（真災後）與
`Cyclone Opener`（球隊名），多砍 6 筆換來多誤砍 2 筆，不划算；體育場合交給 prompt 的 drop_noise 判準。

逐條精確度：`home invasion` 100%（6 筆）、法文 `coup de/du/d'` 84%（19 筆）、
`X epidemic` 隱喻 100%（2 筆）、英文 `major/scores coup` 71%（14 筆）。

### C-2｜評估過但不採用

| 方案 | 實測 | 結論 |
|---|---|---|
| GKG themes 硬門檻（要求含 NATURAL_DISASTER/CRISISLEX/KILL…） | 78 筆樣本只砍 6 筆（8%），含 2 筆 keep_watch | 效益太低，impact regex 已與 themes 高度重疊 |
| 「準備／教學／回顧」候選層級砍 | 命中 18 筆，8 筆是 keep_core | 精確度 39%，會誤殺重大事件 |
| 「災後行政」候選層級砍 | 命中 28 筆，精確度 54%，誤殺 4 筆 keep_core | 改放進 prompt 當 drop_noise 判準，別放進 router |
| 來源域名白名單 | 未實測 | 會系統性砍掉非英語小媒體，與「提高可見覆蓋」目標相衝 |

### C-3｜跨輪重複（**本 PR 不含**）

507 筆裡 35 組標題 fingerprint 重複、多出 55 列（10.8%）。`_candidate_id` 把 `first_slot`/`last_slot`
算進 identity，同一則新聞在下一輪窗口就是新 candidate_id，於是重新付一次 LLM 費、在面板重複出現。
建議另開「近 24h 已評估過的 title fingerprint → assessment」本地小 cache。

### C-4｜前端預設過濾（**mini-taiwan-pulse，本 PR 不含**）

`src/components/sidebar/GlobalEventsList.tsx` 目前沒有任何 decision 過濾，172 筆 drop_noise 與
170 筆 keep_core 並列。預設只顯示 keep_core + keep_watch（335/507），drop_noise 收進「顯示全部」開關。
符合 handoff「重要性是顯示篩選，而非資料門檻」——資料照存，只改預設視圖。
順帶：待定位計數會從 75 掉到 27。

---

## 6. 評測計畫（prompt v4 用）

輸入來源：S3 `global_events/handoff/batches/<batch_id>.json` 就是 `_request_stage1` 的輸入；
`handoff/runs/<run_id>.json` 有舊 prompt 的 `finish_reason` / `content_length` / usage / rejections
可直接當 baseline（**零呼叫**）。`stage1_cache/` 在 Zeabur volume，不需要。

步驟：抓最近 3 個 batch × 每個取前 10 筆 = 3 chunk → 舊/新 prompt 各跑一次（**6 次呼叫**，
預留 8 次上限）→ 兩組都過 `validate_stage1` 與 `candidate_display_records` → 人工抽查 30 筆。

| 指標 | baseline | 目標 |
|---|---|---|
| `location_evidence_ids` 通過 validator 的候選比例 | 51.7% | ≥ 70% |
| 最終 `places` 非空比例 | 85.2% | ≥ 92% |
| 人工抽 30 筆國家正確率 | 待測 | ≥ 95%，0 筆標錯 |
| `content_length` 中位數 | 4.2k–8k | 下降 ≥ 20%，0 次 `finish_reason != stop` |
| validator rejection 數 | 讀 run manifest | 不增加 |

---

## 7. 風險

| # | 風險 | 緩解 |
|---|---|---|
| R1 | 改 prompt 沒改 `STAGE1_PROMPT_VERSION` → `stage1_cache` 回放舊回應，改版靜默失效 | 同步升版號 |
| R2 | migration 399 的 `location_kind` enum 只允許 `event_point/city_center/country_center/unknown`，違反會 `RAISE EXCEPTION` 讓**整批** ingest 失敗 | A-1a 沿用 `country_center`；`admin1_center` 需先發 migration |
| R3 | lineage regex `^<kind>:(gdelt:[a-zA-Z0-9_-]+\|https?://[^\s#]+#.+)$` | gazetteer 用 `<kind>:<url>#<片段>`；**URL 本身含 `#` 時退回 `gdelt:headline_<sha>` 形式** |
| R4 | `evidence_url` 必須在 `source_urls` 內 | 只用 `representative_documents` 且已在 `source_urls` 的 URL |
| R5 | `evidence_basis` 不得為空、≤500 字 | 三條路徑都填固定句型 |
| R6 | `place_key` 重複 → 整批失敗 | 沿用既有 `place_identity` / `seen_places` 去重路徑，不另外 append |
| R10 | gazetteer 假陽性（發行商後綴、masthead） | 分隔符前片段 + stop-list + `evidence_basis` 明示「未確認為精確發生地」 |
| R8 | `pulse-intel-workbench` 若窮舉解析 `source_kind` | 399 已把 `headline_gazetteer` 放進 allowlist，代表設計時就預期它出現；上線前仍建議 workbench 側確認 |

---

## 8. 資料延遲：已由 PR #85 修復並驗證 ✅

> **狀態（2026-09-05）**：已修復。本節保留原始分析當歷史脈絡，因為「穩態吞吐沒問題、
> 落後全來自停機」這個判讀決定了 #85 採用的修法。

### 8.1 當初觀測到的現象（2026-09-04）

collector **有在跑**（最近 12 小時寫入 221 列），但處理到的 GKG slot 只到 2026-09-03 12:45 UTC。

| 寫入時間 (UTC) | 該批最新 `observed_at` | 落後 |
|---|---|---|
| 2026-09-03 10:00 | 2026-09-02 20:45 | 13.3h |
| 2026-09-04 04:00 | 2026-09-03 00:45 | 27.3h |
| 2026-09-04 23:00 | 2026-09-03 12:45 | **34.3h** |

**穩態吞吐約 1:1，本來就沒問題**（09-04 04:00 → 15:00：11 小時 wall-clock 推進 10 小時 GKG）。
落後幾乎全部來自兩個空窗：09-03 13:00 → 09-04 04:00（15h）、09-04 15:00 → 23:00（8h），
合理假設是 OpenRouter 429 讓整輪 collect 失敗、checkpoint 不推進。

### 8.2 #85 怎麼修的

因為根因是「停機」而不是「吞吐不足」，#85 沒有無條件放寬每輪檔數——那樣只會在 429 期間
更快撞限流、把空窗拉更長。它做的是兩件事：

1. **先讓一輪不再全有全無**：chunk 反覆失敗會被切小、最後以 `assessment_status=pending`
   釋出，一個中毒的 cohort 不會卡住整個 queue，checkpoint 因此能繼續推進。
2. **再讓落後時才加寬窗口**：`_max_files_per_stream()` 是**條件式**的——
   只有當該 stream 的 checkpoint 落後超過 `GLOBAL_EVENTS_CATCHUP_LAG_HOURS`（預設 6 小時）
   才放寬到 `GLOBAL_EVENTS_CATCHUP_FILES_PER_STREAM`（預設 24 檔／輪），追上後自動回到
   `GLOBAL_EVENTS_MAX_FILES_PER_STREAM`（8 檔）。

### 8.3 驗證（2026-09-05，#85 merged 後）

- 08:25、09:28 兩輪 receipt 皆 `accepted`，並帶上 `collector_version`。
- catch-up 生效：每 stream 24 檔／輪。
- 落後從 **1 天 11 小時降到 1 天 1.5 小時**（單輪推進遠大於 1:1，正在收斂）。

→ 剩下的是等它把 backlog 吃完；不需要再改參數。

---

## 9. 執行順序

| 順序 | 項目 | repo | 需 migration | 狀態 |
|---|---|---|---|---|
| 0 | §8 的資料延遲 | data-collectors / Zeabur | ❌ | ✅ PR #85（09-05 驗證，落後 1天11h → 1天1.5h） |
| 1 | C-4 前端預設過濾 drop_noise | mini-taiwan-pulse | ❌ | 待辦 |
| 2 | C-1 per-signal veto | data-collectors | ❌ | ✅ 本 PR |
| 3 | A-1a ADM1 降級（batch 級 anchor） | data-collectors | ❌ | ✅ 本 PR |
| 4 | A-2 headline gazetteer | data-collectors | ❌ | ✅ 本 PR |
| 5 | B prompt v4 + A-3 validator 放寬 | data-collectors | ❌ | 待辦（需先跑 §6 評測） |
| 6 | C-3 跨輪重複 cache | data-collectors | ❌ | backlog |
| 7 | `admin1_center` 精確州級定位 | 4 個 repo | ✅ | 之後再議 |
