# Stage1 prompt v4 replay 評測

日期：2026-09-05 · 模型：`qwen/qwen3.7-flash`（temperature 0、`reasoning.effort=none`、`max_tokens` 8192，兩臂完全相同）
輸入：S3 `global_events/handoff/batches/` 最近 3 個 batch，各取前 10 筆候選 = **30 個候選 / 3 個 chunk**
腳本：[`scripts/eval_global_event_prompt_ab.py`](../scripts/eval_global_event_prompt_ab.py)
背景與建議編號：[`GLOBAL_EVENTS_GEO_QUALITY.md`](./GLOBAL_EVENTS_GEO_QUALITY.md) §4（B1–B9）、§3 A-3、§6

**呼叫次數：15 次**（v3 基線 3 ＋ draft-1 3 ＋ draft-2 3 ＋ FINAL 3 ＋ FINAL-2 3），預算上限 15。
成本合計約 US$0.010。評測不寫回任何 run manifest／handoff／候選觀測／checkpoint。

四臂定義：

| 臂 | prompt |
|---|---|
| **v3 基線** | 現行 production prompt |
| **v4 draft-1** | v4 rubric ＋ 改寫過的 location 段（bullet 化、「可以是英文地名」） |
| **v4 draft-2** | v4 rubric ＋ 更嚴格的 location 段（「原字複製」） |
| **v4 FINAL** | v4 rubric ＋ **location 段與 output_contract basis 字串逐字回到 v3** ← branch 現況 |
| **v4 FINAL-2** | FINAL **再拿掉 B4 長度上限**（`length_hints` 移除、長度文字回 v3 的 40/120/80）＋ few-shot 壓成一行（20.3%→14.7% prompt 佔比）|

---

## 0. 結論（先講）

**五臂跑完，硬門檻始終沒過。建議「prompt v4 不上」，把重要性移到 pipeline 之外解決（§7）。**

> **給決策者的一句話**：加 rubric 就會掉 3/30 個模型選點，這件事**改 prompt 救不回來**——
> location 措辭改過（4/30）、還原過（18/30）、長度上限也拿掉過（18/30），三種都試完了。
> 使用者第一優先是地理標記且不接受這個交換，所以結論是 v4 不上。

1. **v4 的「重要性判準」是明確的勝利，而且三個 v4 臂都穩定重現**——災後行政、人情趣味、
   預報階段全部被正確重新分類，同一則新聞的多家轉載終於判得一致（見 §3）。
2. **⚠️ 原本「location 段改寫是唯一元凶」的診斷是錯的。** 把該段逐字還原成 v3 之後
   （FINAL 臂），有效選點只回到 **18/30**，仍低於基線 21/30，被丟棄的選點是 **18 筆**（基線 1 筆）。
3. **⚠️「長度上限變緊」的後續假說也被推翻。** FINAL-2 拿掉 `length_hints`、長度文字回 v3 的
   40/120/80、few-shot 壓成一行（prompt 佔比 20.3%→14.7%）之後，有效選點**一動也沒動：
   仍是 18/30**，而且付出額外代價——多出 4 筆錯誤國家（FINAL 只有 1 筆）、
   出現 1 筆 validator rejection（前四臂都是 0）、輸出 token 反而 +9%、
   而且 4 家轉載的判斷一致性又消失了（#13 keep/1 vs #18-20 drop/1）。
   **FINAL-2 全面差於 FINAL。**
4. **所以根因是 rubric 本身**（決策指令的量），不是 location 措辭、也不是長度壓力。
   在這個樣本上，「加上重要性判準」與「維持 21/30 選點率」無法並存。
5. **A-3（validator 放寬）實測效果為零**：四臂裡 `strict == relaxed`，
   模型產出的 basis 只要能過放寬版，就一定也能過 v3 嚴格版。
   → 原提案「51.7% 選點率主因是 basis 綁定太嚴」的假說**被推翻**（詳見 §5）。
6. **B3（`taiwan_impact_zh_tw` 留空）也是零效果**：v3 基線本來就有 29/30 回傳空字串，
   19.4 字的平均長度是 validator 自動補的 `模型判斷無臺灣關聯，未提供補充說明。`（正好 19 字），
   不是模型寫的。**原提案這一條的前提是錯的。**
7. 輸出縮減只有 −2.6%（FINAL），未達 −20% 目標；`summary` −35%／`reason` −40% 確實有效，
   但 `title` 反而 +11%，而 prompt token 增加 12%，兩邊互相抵銷。
8. **user-visible 的地理覆蓋完全沒有退步**：五臂的 `places` 非空都是 28/30——
   #86 的確定性定位層（batch 國家 anchor ＋ headline gazetteer ＋ metadata fallback）
   把選點下降完全吸收掉了。退步的是**出處品質**（3 筆從「模型選定」變成「概略提及」），
   不是「有沒有點」。這是判斷要不要上的關鍵（見 §8）。

---

## 1. §6.3 指標對照（五臂）

| 指標 | 門檻 | v3 基線 | draft-1 | draft-2 | **FINAL** | **FINAL-2** |
|---|---|---|---|---|---|---|
| 有有效 `location_evidence_ids` 的候選 | ≥ 21/30 | **21/30** | 16/30 | 4/30 | **18/30** ❌ | **18/30** ❌ |
| ⤙ 同時也過 v3 嚴格 basis 規則 | — | 21 | 16 | 4 | 18 | 18 → A-3 效果 = **0** |
| 被丟棄的 location 選點（diagnostics） | ≤ 1 | **1** | 12 | 28 | **18** ❌ | **16** ❌ |
| 人工抽 30 筆多出的錯誤國家 | 0 | 0 | 3 | 3 | **1** ❌ | **4** ❌ |
| 回傳的 assessment 數 | 30 | 30 | 30 | 30 | **30** ✅ | **29** ❌ |
| validator rejections | 0 | 0 | 0 | 0 | **0** ✅ | **1** ❌ |
| 最終 `places` 非空 | ≥ 28/30 | 28/30 | 28/30 | 28/30 | **28/30** ✅ | **28/30** ✅ |
| `finish_reason != stop` | 0 | 0 | 0 | 0 | **0** ✅ | **0** ✅ |
| decision core（不回到 11） | ≠ 11 | 11 | 4 | 4 | **4** ✅ | **4** ✅ |
| 4 家轉載判斷一致 | 一致 | ❌ 4 種 | ✅ | — | **✅** | **❌ 又不一致** |
| `content_length` 合計 | −20% | 22,152 | 20,644 | 21,177 | **21,851（−1.4%）** ❌ | **23,597（+6.5%）** ❌ |
| `completion_tokens` | −20% | 9,188 | 8,556 | 8,814 | **8,949（−2.6%）** ❌ | **9,744（+6.1%）** ❌ |
| `prompt_tokens` | — | 25,343 | 28,496 | 28,631 | 28,373（+12%） | 28,112（+11%） |
| 成本 (USD) | — | 0.00195 | 0.00197 | 0.00200 | 0.00201 | 0.00211 |

**FINAL：6 項門檻 3 過 3 不過。FINAL-2：3 過 5 不過，且每一項都不優於 FINAL。**

**各欄位中文字數合計（30 筆）**

| 欄位 | v3 | draft-1 | draft-2 | **FINAL** | **FINAL-2** |
|---|---|---|---|---|---|
| `title_zh_tw` | 482 | 546 | 582 | **537（+11%）** | 541 |
| `summary_zh_tw` | 1,849 | 1,244 | 1,341 | **1,210（−35%）** | 1,501（−19%） |
| `reason_zh_tw` | 1,319 | 892 | 821 | **793（−40%）** | 1,568（**+19%**） |
| `taiwan_impact_zh_tw` | 561 | 555 | 550 | **548** | — |

`taiwan_impact_zh_tw` 幾乎不動的原因見 §0.6：那 548 字絕大多數是 validator 補的固定句。
FINAL-2 拿掉長度上限後 `reason_zh_tw` 反彈到比 v3 還長，這是它 token 變多的來源。

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

## 5. 三個被推翻的假說（本次評測最有價值的產出）

### 5.1 A-3：basis 綁定不是選點率的瓶頸

`GLOBAL_EVENTS_GEO_QUALITY.md` §2 推論「v3 要求 basis 出自 evidence 自己那一篇的標題，
對外文候選近乎不可能通過」，並預估放寬後選點率 51.7% → ≥70%。

實測：五臂的 `selection_strict` 與 `selection_relaxed` **完全相同**（21/21、16/16、4/4、18/18、18/18）。
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

## 7. 結論與建議：prompt v4 不上

### 7.1 為什麼停在這裡

使用者已明確表態：**第一優先是地理標記，不接受用它換重要性判準**。而五臂實測顯示，
「加上重要性 rubric」與「維持 21/30 模型選點率」在這個 pipeline 上**無法並存**，
而且三種修法都試過了：

| 嘗試 | 有效選點 | 結果 |
|---|---|---|
| v3 基線（無 rubric） | **21/30** | — |
| 改寫 location 段（bullet 化、「可以是英文地名」） | 16/30 | 更差 |
| 再收緊 location 段（「原字複製」） | 4/30 | 最差 |
| location 段逐字還原 v3（FINAL） | 18/30 | 仍差 3 個點 |
| 再拿掉長度上限、few-shot 壓成一行（FINAL-2） | **18/30** | **完全沒動**，其餘指標更差 |

FINAL-2 是最後一發子彈，而它證明長度壓力**不是**原因——拿掉之後選點一動也沒動，
只是讓 `reason_zh_tw` 反彈到比 v3 還長、錯誤國家從 1 筆變 4 筆、多出 1 筆 validator rejection、
轉載一致性又消失。**根因是 rubric 本身的指令量**，這在 prompt 層面無解。

### 7.2 建議：把「重要性」移到 pipeline 之外

rubric 的價值是真的（三個 v4 臂穩定重現：`keep_core` 11→4、災後行政／人情趣味／純聲明／
未登陸預報全部正確降級）。要拿到這個價值又不動 stage1 的地理行為，有兩條路，
**都不需要改 stage1 prompt**：

1. **前端顯示層（最便宜，建議先做）**——即 [GEO_QUALITY §5 C-4](./GLOBAL_EVENTS_GEO_QUALITY.md)。
   `GlobalEventsList.tsx` 目前對 decision 零過濾。把 drop_noise 收進「顯示全部」開關之外，
   再加一組**降權**（不是刪除）規則處理 rubric 想抓的類別（災後募款／理賠／志工表揚、
   preparedness 解說、體育娛樂）。這些規則在 GEO_QUALITY §5 C-2 量過：
   當**路由過濾器**用精確度只有 39~54%（會誤殺重大事件），但當**排序降權**用是安全的——
   資料照存、只改預設視圖，符合 handoff「重要性是顯示篩選，而非資料門檻」。
2. **獨立的第二次 LLM 判斷**——對已完成 stage1 的候選只重評重要性，
   完全不碰 stage1 的 prompt 與地理選點。代價是每 chunk 多一次呼叫。

### 7.3 這支 branch 怎麼處理

**維持 draft，等使用者拍板。** branch 已回到 **FINAL** 變體（FINAL-2 已還原，因為它全面更差）。
三種可能的處置：

| 選項 | 內容 |
|---|---|
| **A（建議）** | 關掉此 PR，只把 §7.2 第 1 條開成 mini-taiwan-pulse 的前端 PR；stage1 prompt 保持 v3 |
| B | 只 merge A-3（validator basis 放寬）＋ 本評測文件，prompt 與版本號都回 v3。A-3 實測 0 效果但無害 |
| C | 接受 21→18 的交換照現況 merge（使用者已表態不接受，僅列出保持完整） |

無論選哪一個，建議一併補一項觀測（本 PR 未做，因為門檻未過）：
把 `location_diagnostics`（被丟棄的選點數）寫進 run receipt 的 jsonb（非契約欄位），
讓 1 → 18 這種變化在 production 就能看到，而不是只能靠離線 replay 發現。

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
