# 2026-07-23：Disk IO 死亡螺旋——refresh cron 自我重疊 + health_snapshot 全掃

## 症狀

- Supabase 專案橫幅「exhausting multiple resources」：CPU 99%、Disk IO burst 預算 7/16 起逐日爬升（15%→33%→62%→83%→100%→>100%），7/22 起每天燒光後被鎖回 baseline 174 Mbps
- 7/23 07:21 DB stats 歸零（疑資源耗盡觸發平台重啟）→ 08:17 起主站 collector 全面 `canceling statement due to statement timeout`，buffer 積壓近 2 小時（iot_wra / groundwater_level / bus / freeway_vd）
- Hermes 巡檢誤報 `supabase_unavailable`（QueryCanceled）——**其實 DB 活著，只是壅塞到健康檢查自己被砍**

## 根因（三層疊加）

1. **高頻 pg_cron 聚合自我重疊（主因）**：`refresh_*_daily` 系列（bus/ship/flight/road_congestion/parking/youbike_h3…）每 15-20 分全量重算**含昨日不變資料**。來源分區表逐日長大 → 單次執行超過排程間隔 → 上一輪沒跑完下一輪疊上來 → 幾何級數惡化。鐵證：`refresh_bus_trails_daily` 單條卡 2h15m、6+ job 同時 running、pg_cron `job startup timeout` 連發。
2. **health_snapshot 每呼叫全掃 60+ 表（29 分鐘/讀 12GB）**：`COUNT(*) FILTER (WHERE time>…)` 是聚合後過濾、不做 partition pruning，每張月級分區表掃整段歷史。被巡檢每 30 分點燃一次。
3. **前期貢獻者**：`get_waste_schedule_day` 舊版 48 秒 × 每個 pulse 訪客觸發（7/22 mig 301 已修）；autovacuum 被 IO 飢餓拖垮 → `*_current` 表 92-97% dead tuple。

## 鑑識方法（可複用）

```sql
-- 誰在燒磁碟：blocks × 8KB = 讀量
select left(query,120), calls, round(total_exec_time/1000) s, shared_blks_read,
       temp_blks_read+temp_blks_written temp
from pg_stat_statements order by shared_blks_read desc limit 15;
-- 螺旋現場：卡住的 refresh 與等待事件
select pid, now()-query_start dur, wait_event, left(query,80)
from pg_stat_activity where state<>'idle' order by dur desc;
-- cron 重疊證據：單次時長 vs 間隔、startup timeout
select jobid, status, start_time, end_time-start_time dur, left(return_message,60)
from cron.job_run_details where start_time > now()-interval '2 days'
order by start_time desc limit 40;
```

注意：`pg_stat_statements` 高負載下 ORDER BY shared_blks_read 本身會超時 → 改從 CPU 排行附帶欄位讀；表大小改查 `pg_class.relpages*8192`（免碰檔案系統）。

## 修復（gis-platform PR #34 / data-collectors PR #36，全部已 apply）

| Migration | 內容 |
|---|---|
| 302 | cron detangle：date-1 refresh 拆成每日一次（17:xx UTC 錯開）、today 30 分錯開、pressure-index */5→*/15、temperature-dates 每時（prod 直接執行，檔案為紀錄） |
| 303 | health_snapshot 快版設計稿 ⚠️ 未套用（realtime schema 收權 42501）→ 306 取代 |
| 304 | get_waste_stops：`p IS NULL OR city=p` catch-all 拆 IF/ELSE，命中既有 city 索引（193k 全掃→135ms） |
| 305 | 9 張高 churn `*_current` 表 per-table autovacuum（scale 0.02；極小表 scale 0.0 + threshold 500） |
| 306 | `public.health_snapshot`：兩段式探針（2 天窗 pruning + tier-2 fallback）+ 分區鍵感知，71 表 29 分→~8 秒；EXECUTE 授 service_role/gis_monitor_hermes/cloud_agent_ro |

止血同步做了：砍殭屍 backend 兩批（cancel 不動就 terminate）、VACUUM FULL 三張 bloat 表。呼叫端 `tasks/monitoring.py` 改打 public 版（Zeabur push 自動部署生效）。

## 預防守則

1. **date-1 資料不會再變——一天重算一次**。任何 refresh cron 單次執行時間 > 間隔 1/3 就降頻或增量化。
2. **分區表的時間條件寫在 WHERE、落在分區鍵**；`COUNT(*) FILTER` 不 pruning。
3. **監控 RPC 自己不能成為負載**：目標 O(index probe)，巡檢頻率 × 單次成本要算過。
4. **QueryCanceled ≠ 斷線**：巡檢告警先查 `pg_stat_activity` 區分壅塞 vs 斷線，別急著 restart。
5. **realtime schema 可用不可建**（2026-07-23 平台收權）：新物件一律 `public`，見 ADR-0009。

## 誤判紀錄（誠實留檔）

- 鑑識初報 `road_events_current`「264 活列佔 409MB」——實為 21.6 萬活列（pg_stat 過時估計），bloat 僅 ~20%。教訓：`n_live_tup` 是估計值，下結論前用 `count(*)` 或 VACUUM VERBOSE 驗證。
- `youbike_current` 修復後 `n_dead_tup=18394` 同樣是過時統計（VACUUM VERBOSE 實測 0 dead）。
