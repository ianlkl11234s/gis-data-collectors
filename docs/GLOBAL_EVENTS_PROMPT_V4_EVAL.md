# Stage1 prompt v4 replay 評測

日期：2026-09-05 · 模型：`qwen/qwen3.7-flash`（temperature 0、`reasoning.effort=none`、`max_tokens` 8192，兩臂完全相同）
輸入：S3 `global_events/handoff/batches/` 最近 3 個 batch，各取前 10 筆候選 = **30 個候選 / 3 個 chunk**
腳本：[`scripts/eval_global_event_prompt_ab.py`](../scripts/eval_global_event_prompt_ab.py)
背景與建議編號：[`GLOBAL_EVENTS_GEO_QUALITY.md`](./GLOBAL_EVENTS_GEO_QUALITY.md) §4（B1–B9）、§3 A-3、§6

**呼叫次數：12 次**（v3 基線 3 + v4 draft-1 3 + v4 draft-2 3 + v4 FINAL 3），預算上限 12。
成本合計約 US$0.008。評測不寫回任何 run manifest／handoff／候選觀測／checkpoint。

四臂定義：

| 臂 | prompt |
|---|---|
| **v3 基線** | 現行 production prompt |
| **v4 draft-1** | v4 rubric ＋ 改寫過的 location 段（bullet 化、「可以是英文地名」） |
| **v4 draft-2** | v4 rubric ＋ 更嚴格的 location 段（「原字複製」） |
| **v4 FINAL** | v4 rubric ＋ **location 段與 output_contract basis 字串逐字回到 v3** ← branch 現況 |

---

## 0. 結論（先講）

**§6.3 的硬門檻仍然沒過（6 項中 3 過 3 不過），所以這支 PR 維持 draft。**

1. **v4 的「重要性判準」是明確的勝利，而且三個 v4 臂都穩定重現**——災後行政、人情趣味、
   預報階段全部被正確重新分類，同一則新聞的多家轉載終於判得一致（見 §3）。
2. **⚠️ 原本「location 段改寫是唯一元凶」的診斷是錯的。** 把該段逐字還原成 v3 之後
   （FINAL 臂），有效選點只回到 **18/30**，仍低於基線 21/30，而被丟棄的選點是 **18 筆**
   （基線 1 筆）。location 段的措辭解釋了 4→18 這 14 個點，但剩下的 18→21 這 3 個點
   以及暴增的丟棄數，來自 **prompt 的其他部分**（指令變長、長度上限變緊，模型變得更精簡，
   也就更不願意逐字複製標題片段）。**沒有任何一段 location 文字能把它救回來。**
3. **A-3（validator 放寬）實測效果為零**：四臂裡 `strict == relaxed`，
   模型產出的 basis 只要能過放寬版，就一定也能過 v3 嚴格版。
   → 原提案「51.7% 選點率主因是 basis 綁定太嚴」的假說**被推翻**（詳見 §5）。
4. **B3（`taiwan_impact_zh_tw` 留空）也是零效果**：v3 基線本來就有 29/30 回傳空字串，
   19.4 字的平均長度是 validator 自動補的 `模型判斷無臺灣關聯，未提供補充說明。`（正好 19 字），
   不是模型寫的。**原提案這一條的前提是錯的。**
5. 輸出縮減只有 −2.6%（FINAL），未達 −20% 目標；`summary` −35%／`reason` −40% 確實有效，
   但 `title` 反而 +11%，而 prompt token 增加 12%，兩邊互相抵銷。
6. **user-visible 的地理覆蓋完全沒有退步**：四臂的 `places` 非空都是 28/30——
   #86 的確定性定位層（batch 國家 anchor ＋ headline gazetteer ＋ metadata fallback）
   把選點下降完全吸收掉了。退步的是**出處品質**（3 筆從「模型選定」變成「概略提及」），
   不是「有沒有點」。這是判斷要不要上的關鍵（見 §8）。

---

## 1. §6.3 指標對照（四臂）

| 指標 | 門檻 | v3 基線 | draft-1 | draft-2 | **FINAL** | 判定 |
|---|---|---|---|---|---|---|
| 有有效 `location_evidence_ids` 的候選 | ≥ 21/30 | **21/30** | 16/30 | 4/30 | **18/30** | ❌ |
| ⤙ 同時也過 v3 嚴格 basis 規則 | — | 21 | 16 | 4 | 18 | A-3 效果 = **0** |
| 被丟棄的 location 選點（diagnostics） | ≤ 1 | **1** | 12 | 28 | **18** | ❌ |
| 人工抽 30 筆多出的錯誤國家 | 0 | 0 | 3 | 3 | **1** | ❌（僅 #9 多 `UK`） |
| 最終 `places` 非空 | ≥ 28/30 | 28/30 | 28/30 | 28/30 | **28/30** | ✅ |
| `finish_reason != stop` | 0 | 0 | 0 | 0 | **0** | ✅ |
| validator rejections | 0 | 0 | 0 | 0 | **0** | ✅ |
| decision 維持 rubric 效果（core 不回到 11） | core ≠ 11 | 11 | 4 | 4 | **4** | ✅ |
| 使用者點名同型案例仍 drop | 全部 drop | — | ✅ | ✅ | **✅** | ✅ |
| `content_length` 合計 | −20% | 22,152 | 20,644 | 21,177 | **21,851（−1.4%）** | ❌ |
| `completion_tokens` | −20% | 9,188 | 8,556 | 8,814 | **8,949（−2.6%）** | ❌ |
| `prompt_tokens` | — | 25,343 | 28,496 | 28,631 | **28,373（+12%）** | 新 prompt 較長的代價 |
| 成本 (USD) | — | 0.00195 | 0.00197 | 0.00200 | **0.00201** | 持平 |

**通過 3 項、不通過 3 項。**

**各欄位中文字數合計（30 筆）**

| 欄位 | v3 | draft-1 | draft-2 | **FINAL** |
|---|---|---|---|---|
| `title_zh_tw` | 482 | 546 | 582 | **537（+11%）** |
| `summary_zh_tw` | 1,849 | 1,244 | 1,341 | **1,210（−35%）** |
| `reason_zh_tw` | 1,319 | 892 | 821 | **793（−40%）** |
| `taiwan_impact_zh_tw` | 561 | 555 | 550 | **548** |

`taiwan_impact_zh_tw` 幾乎不動的原因見 §0.4：那 548 字絕大多數是 validator 補的固定句。

## 2. decision 分布

| decision | v3 | draft-1 | draft-2 | **FINAL** |
|---|---|---|---|---|
| keep_core | 11 | 4 | 4 | **4** |
| keep_watch | 7 | 14 | 9 | **14** |
| drop_noise | 12 | 12 | 17 | **12** |

三個 v4 臂的 keep_core 都是 4，**rubric 的效果與 location 段怎麼寫無關、穩定重現**。
逐筆檢查（§3）顯示這個收縮方向是對的：被降級的多是「尚未登陸的風暴預報」與「州級疫情」，
v4 rubric 明文把這兩類放在 keep_watch。

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

draft 臂有 3 筆多出 v3 沒有的國家碼（#2 Sydney 禽流感多 `US`、#7 阿根廷／福克蘭多 `IR,IS`、
#9 Clarks 芭蕾鞋多 `UK`）；**FINAL 臂只剩 #9 一筆**（而且 #10 還少掉一個多餘的 `HK`，是改善）。

但這**不是模型把國家標錯**，而是**選點率下降的連帶結果**：
候選一旦沒有任何模型選點，`candidate_display_records` 的 metadata fallback 就會把該候選
**所有** GKG 地理提及都當概略點輸出（每個點的 `evidence_basis` 都已標明
「來源新聞的地理提及，僅供概略定位；未確認為精確發生地」）。
所以 §1 的「國家正確率」門檻沒過，根因與「選點率」門檻沒過是同一個。

→ 把 location 段還原成 v3（已做）讓這一項從 3 筆降到 1 筆，但沒有回到 0，
因為選點率也沒有完全回到基線（§0.2）。剩下的 #9 是「品牌國籍被當概略提及」，
該點的 `evidence_basis` 已標明「僅供概略定位；未確認為精確發生地」。

---

## 5. 兩個被推翻的假說（本次評測最有價值的產出）

### 5.1 A-3：basis 綁定不是選點率的瓶頸

`GLOBAL_EVENTS_GEO_QUALITY.md` §2 推論「v3 要求 basis 出自 evidence 自己那一篇的標題，
對外文候選近乎不可能通過」，並預估放寬後選點率 51.7% → ≥70%。

實測：四臂的 `selection_strict` 與 `selection_relaxed` **完全相同**（21/21、16/16、4/4、18/18）。
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
| **location 段（含 output_contract 的 basis 字串）** | ⚠️ **還原成 v3 逐字原文**；但實測顯示這樣做只把選點從 4/30 救到 18/30，救不回基線 21/30（§0.2） |
| A-3 validator basis 放寬 | ✅ 保留（實測 0 效果；只放寬接受條件） |
| `STAGE1_PROMPT_VERSION` v3 → v4 | ✅（cache key 必須改，否則舊快取回放） |
| `STAGE1_RUN_SCHEMA_VERSION` | 不動（observation 結構未變） |
| 未新增任何輸出欄位；required set／enum／range 未動 | ✅ 由 `test_stage1_prompt_v4_adds_no_output_field` 釘住 |

### ✅ 已實測（FINAL 臂，2026-09-05 10:55，3 次呼叫）

branch 現況的組合已經跑過，數字見 §1 的 FINAL 欄。結果：**3 過 3 不過**，維持 draft。

---

## 7. 判斷：值不值得上？

**技術上已經沒有可調的東西了。** 三個 v4 臂證明：

- rubric 的好處**穩定重現**（三臂的 keep_core 都是 4，點名同型案例都正確 drop）；
- 選點下降**不是 location 措辭造成的**——那一段逐字回到 v3 之後仍是 18/30；
  它是「prompt 變長 ＋ 長度上限變緊」的副作用，改 location 文字救不回來（§0.2）。

所以這不再是技術取捨，而是**產品取捨**，需要人拍板：

| | v3 | v4 FINAL |
|---|---|---|
| 重要性分類（使用者的原始抱怨） | 災後募款／風暴預報被判 keep_core；同一則新聞 4 家轉載 4 種判斷 | 全部正確；轉載一致 |
| 有幾個候選定得出位置 | 28/30 | **28/30（完全沒退步）** |
| 這些位置的出處品質 | 21 筆模型選定 / 7 筆概略提及 | 18 筆模型選定 / 10 筆概略提及 |
| 多出的錯誤國家 | 0 | 1（#9 Clarks 品牌國籍，且已標「僅供概略定位」） |
| 輸出 token | 9,188 | 8,949 |

**我的建議：值得上，但要由人接受這個交換，並補一項 production 監控。**

理由：使用者的抱怨是「看不到重要的事」，而 v4 正面解決它；代價是 3/30 候選的地點
從「模型選定」降級為「來源概略提及」——而那 3 筆**仍然畫得出來**（#86 的確定性定位層吸收掉了），
只是 popup 上的 `evidence_basis` 從逐字引文變成「僅供概略定位；未確認為精確發生地」。
換句話說，退步的是**出處強度**，不是**可見性**。

若要上，建議一併做（皆不在本 PR）：

1. 把 `location_diagnostics`（被丟棄的選點數）寫進 run manifest 的 observation，
   讓 1 → 18 這種變化在 production 可被觀測，而不是只能靠離線 replay 發現。
2. 若之後仍想把選點率拉回 21+，方向不是再改 location 文字，而是**縮短 prompt**
   （例如把 rubric 的六類壓成一行列舉）或**放寬長度上限**，因為根因是指令量與長度壓力。

**若不接受這個交換**，退路是只保留 rubric 而放棄長度上限（`length_hints` 與
「summary ≤80／reason ≤50」），先驗證選點率是否回到 21——但那需要第 5 次呼叫，
本次評測預算已用盡（12/12），不在本輪範圍。

---

## 8. 30 筆逐筆對照

`draft-2` 欄是 location 段被改寫最狠的版本；**`FINAL` 欄才是 branch 最後帶的內容**。

| # | 原始標題 | v3 | v4 draft-1 | v4 draft-2 | **v4 FINAL（branch 現況）** |
|---|---|---|---|---|---|
| 1 | Pair arrested over home invasion | drop_noise/1 AS | drop_noise/1 AS | drop_noise/1 AS | **drop_noise/1 AS** |
| 2 | Australia: First case of H5N1 bird flu reported in Sydne | watch/2 AS | watch/1 AS,US | watch/1 AS,US | **watch/1 AS** |
| 3 | Ukraine…Russia Day 1,654…Evening…Peace talks?…Trump mone | drop_noise/1 UP | drop_noise/0 UP | drop_noise/0 UP | **drop_noise/0 UP** |
| 4 | Will We Get Price Relief or a Canadian Trade War? | drop_noise/1 CA | drop_noise/0 CA | drop_noise/0 CA,US | **drop_noise/0 CA** |
| 5 | Typhoon Saudel brings torrential rain, flooding to China | core/3 CH | core/3 CH | core/3 BX,CH,HK | **core/3 CH** |
| 6 | Lowell continues on westward path as a category 4 hurric | watch/2 — | watch/2 — | watch/1 — | **watch/2 —** |
| 7 | Argentine leader threatens to sanction oil firms and rei | watch/2 AR | watch/2 AR,IR,IS,UK | watch/1 AR,IR,IS,UK | **watch/1 AR** |
| 8 | Africa: Zambia General Election Tensions - Council of An | drop_noise/1 ZA | drop_noise/1 ZA | drop_noise/0 BC,SF,ZA | **drop_noise/0 ZA** |
| 9 | Adieu les baskets, ces ballerines Clarks sont le coup de | drop_noise/0 FR | drop_noise/0 FR,UK | drop_noise/0 FR,UK | **drop_noise/0 FR,UK** |
| 10 | Typhoon Saudel brings torrential rain, flooding to China | core/3 CH,HK | core/3 CH | core/3 BX,CH,HK | **core/3 CH** |
| 11 | Nigeria Debunks Alleged Support for Niger Coup, Reaffirm | core/2 NG,NI | watch/1 NG,NI | drop_noise/0 NG,NI | **watch/1 NG,NI** |
| 12 | RDOS eases rules for Faulder residents rebuilding after  | watch/1 — | drop_noise/1 — | drop_noise/0 — | **drop_noise/0 —** |
| 13 | Hurricane Lowell Makes Sharp Turn Towards Hawaii ／ News  | core/2 US | watch/2 US | watch/2 US | **watch/2 US** |
| 14 | Blatant ceasefire violation: Fire opened on IDF troops i | core/2 GZ,IS | core/2 GZ,IS | core/2 IS | **core/2 GZ,IS** |
| 15 | Wildfire SNAP benefits extended to Sept. 7 | watch/1 US | drop_noise/1 US | drop_noise/0 US | **drop_noise/0 US** |
| 16 | Man United sanction shock Gabriele Biancheri exit after  | drop_noise/0 AU | drop_noise/0 AU | drop_noise/0 AU | **drop_noise/0 AU** |
| 17 | Hurricane Marie: Baby Born in Ambulance Amid Storm Traff | watch/1 MX | drop_noise/0 MX | drop_noise/0 MX | **drop_noise/0 MX** |
| 18 | Hurricane Lowell Makes Sharp Turn Towards Hawaii ／ Sport | drop_noise/0 US | watch/2 US | drop_noise/2 US | **watch/2 US** |
| 19 | Hurricane Lowell Makes Sharp Turn Towards Hawaii ／ News  | drop_noise/0 US | watch/2 US | drop_noise/2 US | **watch/2 US** |
| 20 | Hurricane Lowell Makes Sharp Turn Towards Hawaii ／ KFYR  | drop_noise/0 US | watch/2 US | drop_noise/2 US | **watch/2 US** |
| 21 | Public demand surges for advance ballots in Alberta sepa | watch/2 CA | watch/1 CA | watch/2 CA | **watch/2 CA** |
| 22 | A warning and referendum on the future of America | drop_noise/0 IS,US | drop_noise/0 IS,US | drop_noise/0 IS,US | **drop_noise/0 IS,US** |
| 23 | What to know about Wisconsin's measles outbreak and how  | core/2 US | watch/1 US | drop_noise/1 US | **watch/1 US** |
| 24 | Stourbridge wildfire appeal receives £500 charity donati | drop_noise/0 UK | drop_noise/0 UK | drop_noise/1 UK | **drop_noise/0 UK** |
| 25 | Major Hurricane Lowell forecast to make sharp turn towar | core/3 US | watch/2 US | watch/2 US | **watch/2 US** |
| 26 | Kentucky sees largest measles outbreak in over 35 years | core/2 US | watch/2 US | watch/2 US | **watch/2 US** |
| 27 | Hurricane Lowell To Remain Major Hurricane For Several D | core/3 US | watch/2 US | watch/2 US | **watch/2 US** |
| 28 | Hurricane Lowell To Remain Major Hurricane For Several D | core/3 US | watch/2 US | watch/2 US | **watch/2 US** |
| 29 | 49 missing, over 28,000 displaced in Manipur conflict: G | core/3 IN | core/3 IN | core/3 IN | **core/3 IN** |
| 30 | Après le coup de feu tiré à Brest, le jeune homme blessé | drop_noise/1 FR | drop_noise/1 FR | drop_noise/1 FR | **drop_noise/1 FR** |
