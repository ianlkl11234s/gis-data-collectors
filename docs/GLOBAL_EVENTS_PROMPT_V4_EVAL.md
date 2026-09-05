# Stage1 prompt v4 replay 評測

日期：2026-09-05 · 模型：`qwen/qwen3.7-flash`（temperature 0、`reasoning.effort=none`、`max_tokens` 8192，兩臂完全相同）
輸入：S3 `global_events/handoff/batches/` 最近 3 個 batch，各取前 10 筆候選 = **30 個候選 / 3 個 chunk**
腳本：[`scripts/eval_global_event_prompt_ab.py`](../scripts/eval_global_event_prompt_ab.py)
背景與建議編號：[`GLOBAL_EVENTS_GEO_QUALITY.md`](./GLOBAL_EVENTS_GEO_QUALITY.md) §4（B1–B9）、§3 A-3、§6

**呼叫次數：9 次**（v3 基線 3 + v4 draft-1 3 + v4 final-of-eval 3），預算上限 10。
成本合計約 US$0.006。評測不寫回任何 run manifest／handoff／候選觀測／checkpoint。

---

## 0. 結論（先講）

**§6.3 的硬門檻沒過，所以這支 PR 開成 draft。**

1. **v4 的「重要性判準」是明確的勝利**——災後行政、人情趣味、預報階段全部被正確重新分類，
   而且同一則新聞的多家轉載終於判得一致（見 §3）。
2. **v4 的「location 段改寫」是明確的失敗**——改寫兩次，有效選點從 21/30 掉到 16/30 再掉到 4/30，
   被 validator 丟掉的選點從 1 筆暴增到 28 筆。**這一段已在本 branch 還原成 v3 逐字原文。**
3. **A-3（validator 放寬）實測效果為零**：三臂裡 `strict == relaxed`，
   模型產出的 basis 只要能過放寬版，就一定也能過 v3 嚴格版。
   → 原提案「51.7% 選點率主因是 basis 綁定太嚴」的假說**被推翻**（詳見 §5）。
4. **B3（`taiwan_impact_zh_tw` 留空）也是零效果**：v3 基線本來就有 29/30 回傳空字串，
   19.4 字的平均長度是 validator 自動補的 `模型判斷無臺灣關聯，未提供補充說明。`（正好 19 字），
   不是模型寫的。**原提案這一條的前提是錯的。**
5. 輸出縮減只有 −6.9%，未達 −20% 目標；`summary`／`reason` 確實各縮 32~33%，但 `title` 反而變長。

---

## 1. §6.3 指標對照

| 指標 | 門檻 | v3 基線 | v4 draft-1 | v4 final-of-eval | 判定 |
|---|---|---|---|---|---|
| 有有效 `location_evidence_ids` 的候選 | ≥ 70% | **21/30 = 70%** | 16/30 = 53% | 4/30 = 13% | ❌ **硬門檻不過** |
| ⤙ 同時也過 v3 嚴格 basis 規則 | — | 21 | 16 | 4 | A-3 效果 = **0** |
| 最終 `places` 非空 | ≥ 92% | 28/30 = 93% | 28/30 = 93% | 28/30 = 93% | ✅ 持平（由 #86 的確定性定位撐住） |
| 人工抽 30 筆國家正確率 | ≥ 95%、0 筆標錯 | 30/30 正確 | 27/30（3 筆多出錯誤國家） | 27/30 | ❌ **硬門檻不過**（成因見 §4） |
| `finish_reason != stop` | 0 | 0 | 0 | 0 | ✅ |
| validator rejection 數 | 不增加 | 0 | 0 | 0 | ✅ |
| 被丟棄的 location 選點（diagnostics） | 不增加 | **1** | 12 | 28 | ❌ |
| `content_length` 合計 | 中位數 −20% | 22,152 | 20,644（−6.8%） | 21,177（−4.4%） | ❌ 未達標 |
| `completion_tokens`（`output_units`） | −20% | 9,188 | 8,556（−6.9%） | 8,814（−4.1%） | ❌ 未達標 |
| `prompt_tokens`（`input_units`） | — | 25,343 | 28,496（+12%） | 28,631（+13%） | 新 prompt 較長的代價 |
| 成本 (USD) | — | 0.00195 | 0.00197 | 0.00200 | 持平 |

**各欄位中文字數合計（30 筆）**

| 欄位 | v3 | v4 draft-1 | v4 final-of-eval |
|---|---|---|---|
| `title_zh_tw` | 482 | 546（**+13%**） | 582（+21%） |
| `summary_zh_tw` | 1,849 | 1,244（**−33%**） | 1,341（−27%） |
| `reason_zh_tw` | 1,319 | 892（**−32%**） | 821（−38%） |
| `taiwan_impact_zh_tw` | 561 | 555 | 550 |

`taiwan_impact_zh_tw` 幾乎不動的原因見 §0.4：那 561 字絕大多數是 validator 補的固定句。

---

## 2. decision 分布

| decision | v3 | v4 draft-1 | v4 final-of-eval |
|---|---|---|---|
| keep_core | 11 | **4** | 4 |
| keep_watch | 7 | 14 | 9 |
| drop_noise | 12 | 12 | 17 |

v4 把 keep_core 從 11 收到 4。逐筆檢查（§3）顯示**這個收縮方向是對的**：
被降級的多是「尚未登陸的風暴預報」與「州級疫情」，v4 rubric 明文把這兩類放在 keep_watch。

---

## 3. 使用者點名的 4 例，以及 v4 修對了什麼

使用者原本點名的 4 則（干邑 / 作弊風氣 / 尼亞加拉 / 夏威夷食物籃）**都不在這 3 個 batch 的前 10 名裡**，
所以無法直接復測。但這 30 筆裡出現了同型別的案例，v4 全部改對：

| # | 標題 | v3 | v4 | 對應 rubric |
|---|---|---|---|---|
| 12 | `RDOS eases rules for Faulder residents rebuilding after wildfire` | keep_watch | **drop_noise** | b 災後行政後續 |
| 15 | `Wildfire SNAP benefits extended to Sept. 7` | keep_watch | **drop_noise** | b 災後行政後續（＝「夏威夷食物籃」同型） |
| 17 | `Hurricane Marie: Baby Born in Ambulance Amid Storm Traffic` | keep_watch | **drop_noise** | e 個人層級人情趣味 |
| 11 | `Nigeria Debunks Alleged Support for Niger Coup` | keep_core | **keep_watch** | f 純聲明／否認 |
| 25/27/28 | `Hurricane Lowell forecast to make sharp turn` 等 | keep_core | **keep_watch** | 尚未登陸的風暴預報 |
| 9 | `Adieu les baskets, ces ballerines Clarks sont le coup de coeur…` | drop_noise | drop_noise | 已由 #86 的 `coup de` veto 擋在 LLM 之前的同型 |

**一致性也變好**：`Hurricane Lowell Makes Sharp Turn Towards Hawaii` 有 4 家轉載（#13、18、19、20），
v3 給出 `keep_core/2`、`drop_noise/0`、`drop_noise/0`、`drop_noise/0` 四種不一致判斷；
v4 一律 `keep_watch/2`。

---

## 4. 「國家標錯」的真正成因

v4 有 3 筆多出 v3 沒有的國家碼：#2 Sydney 禽流感多了 `US`、#7 阿根廷／福克蘭多了 `IR,IS`、
#9 Clarks 芭蕾鞋多了 `UK`。

但這**不是模型把國家標錯**，而是**選點率下降的連帶結果**：
候選一旦沒有任何模型選點，`candidate_display_records` 的 metadata fallback 就會把該候選
**所有** GKG 地理提及都當概略點輸出（每個點的 `evidence_basis` 都已標明
「來源新聞的地理提及，僅供概略定位；未確認為精確發生地」）。
所以 §1 的「國家正確率」門檻沒過，根因與「選點率」門檻沒過是同一個。

→ 只要把 location 段還原成 v3（已做），這一項應同時回到基線。

---

## 5. 兩個被推翻的假說（本次評測最有價值的產出）

### 5.1 A-3：basis 綁定不是選點率的瓶頸

`GLOBAL_EVENTS_GEO_QUALITY.md` §2 推論「v3 要求 basis 出自 evidence 自己那一篇的標題，
對外文候選近乎不可能通過」，並預估放寬後選點率 51.7% → ≥70%。

實測：三臂的 `selection_strict` 與 `selection_relaxed` **完全相同**（21/21、16/16、4/4）。
模型從來沒有產出過「引自另一篇代表標題」的 basis，所以放寬根本沒有機會生效。
而且 v3 基線在這 30 筆上本來就有 70% 選點率——production 的 51.7% 更可能來自
**候選組成不同**（這裡取的是每批 routing rank 前 10 名，evidence 較豐富），而非 basis 規則。

→ A-3 仍然保留（它只放寬 validator 的接受條件，不可能變壞），但必須標記為
**實測 0 效果**，不能當成選點率的解方。原文件的 §2 說法應視為已被推翻。

### 5.2 B3：`taiwan_impact_zh_tw` 並沒有「白寫」

v3 基線 30 筆裡有 **29 筆**模型原本就回傳空字串。DB 看到的 19.4 字平均長度，是
`validate_stage1` 在 `taiwan_relationship == "none"` 時補上的
`模型判斷無臺灣關聯，未提供補充說明。`（19 字）。B3 因此無 token 可省。

---

## 6. 這支 branch 最後帶的是什麼

| 項目 | 狀態 |
|---|---|
| B1 decision 六類 rubric + 三個 few-shot | ✅ 保留（實測有效，§3） |
| B2 severity 獨立量表 | ✅ 保留 |
| B4 長度上限（summary 80／reason 50）＋ `length_hints` | ✅ 保留（summary/reason 各縮 3 成） |
| B5 標題不留引號 lead／報社後綴／地名譯臺灣慣用 | ✅ 保留 |
| B3 `taiwan_impact` 留空 | ✅ 保留（無害，但實測 0 效果） |
| **location 段（含 output_contract 的 basis 字串）** | ⚠️ **還原成 v3 逐字原文**，與實測最佳臂 byte-identical |
| A-3 validator basis 放寬 | ✅ 保留（實測 0 效果；只放寬接受條件） |
| `STAGE1_PROMPT_VERSION` v3 → v4 | ✅（cache key 必須改，否則舊快取回放） |
| `STAGE1_RUN_SCHEMA_VERSION` | 不動（observation 結構未變） |
| 未新增任何輸出欄位；required set／enum／range 未動 | ✅ 由 `test_stage1_prompt_v4_adds_no_output_field` 釘住 |

### ⚠️ 尚未實測的組合

本 branch 現在的 prompt =「v4 rubric + v3 location 段」，**這個組合本身沒有跑過**。
已測到的是：rubric 的效果（draft-1）與 location 段改寫的害處（draft-1、final-of-eval）。
**合併前需要再跑一次 `--arms current`（3 次呼叫）確認**：選點率回到 ≥ 21/30、
國家 0 筆標錯、diagnostics 回到 ≤ 1。這正是本 PR 維持 draft 的原因。

---

## 7. 30 筆逐筆對照

`v4-final` 欄是評測期間的第三臂（location 段被改寫最狠的版本），**不是** branch 最後帶的內容。

| # | 原始標題 | v3 decision/sev | v3 國家 | v4-draft1 decision/sev | v4-draft1 國家 | v4-final decision/sev | v4-final 國家 |
|---|---|---|---|---|---|---|---|
| 1 | Pair arrested over home invasion | drop_noise/1 | AS | drop_noise/1 | AS | drop_noise/1 | AS |
| 2 | Australia: First case of H5N1 bird flu reported in Sydney | watch/2 | AS | watch/1 | AS,US | watch/1 | AS,US |
| 3 | Ukraine…Russia Day 1,654…Evening…Peace talks?…Trump money ba | drop_noise/1 | UP | drop_noise/0 | UP | drop_noise/0 | UP |
| 4 | Will We Get Price Relief or a Canadian Trade War? | drop_noise/1 | CA | drop_noise/0 | CA | drop_noise/0 | CA,US |
| 5 | Typhoon Saudel brings torrential rain, flooding to China's c | core/3 | CH | core/3 | CH | core/3 | BX,CH,HK |
| 6 | Lowell continues on westward path as a category 4 hurricane, | watch/2 | — | watch/2 | — | watch/1 | — |
| 7 | Argentine leader threatens to sanction oil firms and reitera | watch/2 | AR | watch/2 | AR,IR,IS,UK | watch/1 | AR,IR,IS,UK |
| 8 | Africa: Zambia General Election Tensions - Council of Anglic | drop_noise/1 | ZA | drop_noise/1 | ZA | drop_noise/0 | BC,SF,ZA |
| 9 | Adieu les baskets, ces ballerines Clarks sont le coup de coe | drop_noise/0 | FR | drop_noise/0 | FR,UK | drop_noise/0 | FR,UK |
| 10 | Typhoon Saudel brings torrential rain, flooding to China's c | core/3 | CH,HK | core/3 | CH | core/3 | BX,CH,HK |
| 11 | Nigeria Debunks Alleged Support for Niger Coup, Reaffirms Co | core/2 | NG,NI | watch/1 | NG,NI | drop_noise/0 | NG,NI |
| 12 | RDOS eases rules for Faulder residents rebuilding after wild | watch/1 | — | drop_noise/1 | — | drop_noise/0 | — |
| 13 | Hurricane Lowell Makes Sharp Turn Towards Hawaii ／ News Radi | core/2 | US | watch/2 | US | watch/2 | US |
| 14 | Blatant ceasefire violation: Fire opened on IDF troops in no | core/2 | GZ,IS | core/2 | GZ,IS | core/2 | IS |
| 15 | Wildfire SNAP benefits extended to Sept. 7 | watch/1 | US | drop_noise/1 | US | drop_noise/0 | US |
| 16 | Man United sanction shock Gabriele Biancheri exit after dead | drop_noise/0 | AU | drop_noise/0 | AU | drop_noise/0 | AU |
| 17 | Hurricane Marie: Baby Born in Ambulance Amid Storm Traffic | watch/1 | MX | drop_noise/0 | MX | drop_noise/0 | MX |
| 18 | Hurricane Lowell Makes Sharp Turn Towards Hawaii ／ Sports Ra | drop_noise/0 | US | watch/2 | US | drop_noise/2 | US |
| 19 | Hurricane Lowell Makes Sharp Turn Towards Hawaii ／ News Radi | drop_noise/0 | US | watch/2 | US | drop_noise/2 | US |
| 20 | Hurricane Lowell Makes Sharp Turn Towards Hawaii ／ KFYR 550  | drop_noise/0 | US | watch/2 | US | drop_noise/2 | US |
| 21 | Public demand surges for advance ballots in Alberta separati | watch/2 | CA | watch/1 | CA | watch/2 | CA |
| 22 | A warning and referendum on the future of America | drop_noise/0 | IS,US | drop_noise/0 | IS,US | drop_noise/0 | IS,US |
| 23 | What to know about Wisconsin's measles outbreak and how to p | core/2 | US | watch/1 | US | drop_noise/1 | US |
| 24 | Stourbridge wildfire appeal receives £500 charity donation | drop_noise/0 | UK | drop_noise/0 | UK | drop_noise/1 | UK |
| 25 | Major Hurricane Lowell forecast to make sharp turn toward Ha | core/3 | US | watch/2 | US | watch/2 | US |
| 26 | Kentucky sees largest measles outbreak in over 35 years | core/2 | US | watch/2 | US | watch/2 | US |
| 27 | Hurricane Lowell To Remain Major Hurricane For Several Days  | core/3 | US | watch/2 | US | watch/2 | US |
| 28 | Hurricane Lowell To Remain Major Hurricane For Several Days  | core/3 | US | watch/2 | US | watch/2 | US |
| 29 | 49 missing, over 28,000 displaced in Manipur conflict: Govt  | core/3 | IN | core/3 | IN | core/3 | IN |
| 30 | Après le coup de feu tiré à Brest, le jeune homme blessé au  | drop_noise/1 | FR | drop_noise/1 | FR | drop_noise/1 | FR |
