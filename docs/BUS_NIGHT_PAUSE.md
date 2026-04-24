# Bus Collector 夜間暫停策略

> 本文件記錄公車 collector 夜間暫停（00:00-04:59 暫停 17 縣市）的分析依據、實作位置、預期成效。

最後更新：2026-04-24

---

## 一、背景：TDX API 呼叫量優化

### 原始配置成本

啟用中的 TDX collector（共 6 個）每日合計呼叫量約 **17,150 calls**，其中 **bus collector 佔 92%（15,840 calls）**：

| Collector | 頻率 | 每次 call 數 | 每日 call |
|-----------|------|------:|------:|
| **bus** | 2 分鐘 | 22 城 × 1 | **15,840** |
| tra_train | 2 分鐘 | 1 | 720 |
| freeway_vd | 10 分鐘 | 2 | 288 |
| youbike | 15 分鐘 | 3 | 288 |
| tra_static | 1440 分鐘 | 5 | 5 |
| rail_timetable | 1440 分鐘 | 2 | 2 |

> 以 TDX 免費方案 10,000 call/day 配額計算，bus 一人就吃光配額並超出。

**優化目標**：不動頻率，找出「call 出去但資料沒意義」的時段暫停。

---

## 二、分析方法：用 GPS 真實位移判定公車狀態

### 錯誤信號：TDX 會回傳假的 `speed`

最初想用 `speed > 0` 判定「移動中公車」，但 SQL 驗證發現：
```
Keelung 基隆 01-04 點：
  車牌 267-U6 共 349 筆記錄（3 天），位置範圍 118m，平均 speed 31 km/h
  車牌 FAC-160 共 349 筆記錄，位置範圍 108m，平均 speed 28 km/h
```
**車子 4 小時內位置只飄 100 公尺卻回報時速 30 km/h** — 車機未關機、TDX 傳回上次的 speed 記憶值。

### 正確信號：位置真實位移

改用 `4 小時內 bus_lat / bus_lng 範圍換算公尺距離`：

| 分類 | 位移距離 | 意義 |
|------|---:|------|
| `PARKED` | < 300m | 停在原地（GPS 漂移） |
| `SHORT` | 300m - 2km | 可能是短程或收班前 |
| `RUNNING` | > 2km | 真正在運行 |

SQL 位於 commit `938c55f3` 的分析腳本中，樣本：過去 14 天 01:00-04:59 資料。

---

## 三、22 縣市凌晨 01-04 停駛比例排行

樣本：過去 14 天，凌晨 01-04 台北時間有出現的公車。

| 排名 | 縣市 | 停駛比例 | 真跑台數 | 樣本 |
|---:|------|---:|---:|---:|
| 1-7 | MiaoliCounty, ChanghuaCounty, NantouCounty, YunlinCounty, PingtungCounty, HualienCounty, TaitungCounty | **100%** | 0 | 3-8 |
| 8 | Hsinchu | 95% | 1 | 20 |
| 9 | HsinchuCounty | 91% | 0 | 22 |
| 10 | Keelung | 89% | 1 | **79** |
| 11 | ChiayiCounty | 88% | 1 | 26 |
| 12 | PenghuCounty | 84% | 2 | 19 |
| 13 | **Taichung** | 82% | 28 | **188** |
| 14 | KinmenCounty | 82% | 4 | 65 |
| 15 | YilanCounty | 81% | 4 | 21 |
| 16 | Chiayi | 79% | 1 | 19 |
| 17 | Kaohsiung | 76% | 7 | 62 |
| 18 | Tainan | 72% | 8 | 32 |
| 19 | LienchiangCounty | 71% | 0 | 14 |
| 20 | Taoyuan | 68% | 5 | 38 |
| 21 | NewTaipei | 51% | **26** | 68 |
| 22 | Taipei | 35% | **36** | 85 |

---

## 四、分組決策

以「真跑台數」為主要門檻（容忍度：一兩台 GPS 忘關可接受）：

### 保留組（5 都，24hr 抓取）

| 縣市 | 理由 |
|------|------|
| **Taipei** | 凌晨真跑 36 台（最多） |
| **NewTaipei** | 凌晨真跑 26 台 |
| **Tainan** | 凌晨真跑 8 台 |
| **Kaohsiung** | 凌晨真跑 7 台 |
| **Taichung** | 樣本 188 台中真跑 28 台，且有深夜公車路線 |

> 注意：**Taichung 雖然停駛比例 82% 看似偏高，但絕對數量（28 台）多於 Tainan/Kaohsiung**，加上台中市府明確營運深夜公車，故保留。

### 暫停組（17 縣市，00:00-04:59 暫停）

桃園 + 16 個省轄市/縣/離島。凌晨真跑台數皆 ≤ 5 台，多數為 GPS 漂移/停車場車輛。

---

## 五、實作

**檔案**：`collectors/bus.py`

### 模組層常數

```python
TAIPEI_TZ = ZoneInfo('Asia/Taipei')
NIGHT_PAUSE_HOURS = frozenset(range(0, 5))   # 00:00-04:59
NIGHT_KEEP_CITIES = frozenset({
    'Taipei', 'NewTaipei', 'Taichung', 'Tainan', 'Kaohsiung'
})
```

### `collect()` 過濾邏輯

在平行抓取前加判斷：
```python
hour = datetime.now(TAIPEI_TZ).hour
if hour in NIGHT_PAUSE_HOURS:
    paused = [c for c in cities if c not in NIGHT_KEEP_CITIES]
    cities = [c for c in cities if c in NIGHT_KEEP_CITIES]
    for c in paused:
        city_stats[c] = {'skipped': 'night_pause'}
```

被暫停的城市會標記在 `city_stats[city] = {'skipped': 'night_pause'}`，方便未來查詢「哪些城市在哪些時段被暫停」。

### 日誌輸出

```
夜間暫停（02時）：跳過 17 縣市，只抓 5 個保留縣市
平行抓取 5 城市 (workers=5)
  Taipei: 120/120 台有 GPS
  ...
  ✓ 合計: 285 台公車（5/5 城市成功，17 夜間暫停）
```

---

## 六、驗證結果

部署驗證（2026-04-24 19:27 CST，日間）：

```
19:25:57 - 19:26:04  bus 平行抓 22 城
✓ 合計: 5695 台公車（22/22 城市成功）
[bus] ✓ DB 寫入 5695 筆
```

✅ 日間邏輯正確：19 點不在暫停區 → 22 城全抓。

Mock 測試覆蓋：
- 02:30 → 只抓 5 都、暫停 17 城 ✅
- 04:30（邊界）→ 只抓 5 都 ✅
- 05:00 → 恢復 22 城 ✅
- 10:30 / 22:30 → 22 城全抓 ✅

---

## 七、預期成效

| 時段 | 抓取城市 | 每日呼叫 |
|------|---:|---:|
| 05:00-23:59 (19 hr) | 22 城 × 30/hr | 12,540 |
| 00:00-04:59 (5 hr) | 5 城 × 30/hr | 750 |
| **優化後合計** | | **13,290** |
| 原始合計 | | 15,840 |
| **節省** | | **2,550 (16.1%)** |

每月節省約 **76,500 calls**，一年約 **93 萬 calls**。

---

## 八、未來擴展方向（未實作）

如果未來需要進一步省 call，可考慮：

1. **偏鄉縣市日間降頻** — 9 個「C 型偏鄉縣市」（MiaoliCounty, NantouCounty, TaitungCounty 等）尖峰也只有 5-17 台活躍，改 10 分鐘間隔可再省 ~4,600 call/day
2. **GPS 漂移型縣市全日降頻** — PenghuCounty / KinmenCounty 全日位置幾乎不動，可降到 10 分鐘
3. **保留組內部細分** — Tainan 01-03 點真跑只 8 台，若容忍度再放寬可列入暫停組 01-03

本次未實作這些，避免一次改動過多、保持策略可回退。

---

## 相關 commit

- `938c55f3` perf(bus): 夜間 00-05 暫停 17 縣市，預計省 ~2,550 call/day
