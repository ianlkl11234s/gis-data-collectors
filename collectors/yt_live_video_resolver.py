"""
YouTube Live Video Resolver — Monitor Mode LiveWall 用（YouTube Data API v3）

2026-07-26 改寫：HTML 爬取 → 官方 Data API v3 + sticky 寫入
  舊版每 5 分鐘抓 `youtube.com/@handle/live` 頁面解 ytInitialPlayerResponse，
  被 YouTube 反爬蟲擋到成功率 ~0.2%（實測 live.yt_live_history：TVBS 11,103 輪
  只有 19 輪拿到 video_id），且失敗時把 video_id **清空覆寫** →
  連 3 年沒換過的 24h 直播（TVBS m_dhMSvUCIc、東森 V1p33hqPrUk）都只在
  每天極短的窗口有值。改用官方 API + 「拿不到就不覆寫」的 sticky 規則。

每輪（5 分鐘）流程
  1. 驗證（便宜）：把所有「已知 video_id」併成一批 videos.list
     (part=snippet,liveStreamingDetails,statistics,status，最多 50 id) = 1 unit
     → live（有 actualStartTime 無 actualEndTime）／ended（下播）／missing（刪除或轉私人）
  2. 發現（貴）：只有「沒有有效 live ID」的頻道才跑 search.list
     (channelId + eventType=live + type=video)，候選集中用 1 次 videos.list 驗證，
     取「可嵌入 → concurrentViewers → viewCount」排序最高的一支
  3. Sticky：API 失敗（網路 / 5xx / quota）一律**保留上次值**只寫 last_error；
     只有「確定下播且搜過沒替代」或「連續 48h 無法驗證」才清空 video_id
     （last_error 前端只在 video_id 為空時才顯示，故 sticky 期間附註不影響畫面）

Quota（2026-07 官方現行制 developers.google.com/youtube/v3/determine_quota_cost）
  - search.list：**獨立配額桶，每專案每天 100 次呼叫**（不是 units），超過硬擋 quotaExceeded
  - videos.list / channels.list：1 unit，扣共用的 10,000 units/day 池
  - 兩者每日重置時間 = 太平洋時間（PT）午夜
  正常日：13 次 search（開機各 1 次；24h 台之後幾乎不用再搜）
          + 288 次 videos.list ≈ 288 units（288 ticks × 1 call）
  最壞日：13 台全都沒直播、指數退避到 6h → 13 × 4 ≈ 52 次 search
          （本 collector 自設 80/100 上限，留 20 buffer 給手動查詢）
          + ~300 units（<< 10,000）
  防護：每頻道 search 冷卻 60 min（連續槓龜指數退避到 6h 上限）
       + 程序內每日 search 計數器達 80 就停到次日（PT 午夜重置）
       + 每輪 log 一行 quota 消耗摘要

寫入（shape 與舊版完全相同，supabase_writer._transform_yt_live_video_resolver 不用改）
  - live.yt_live_history（UNIQUE(handle,video_id,observed_at)）DO NOTHING
  - live.yt_live_current（PK=handle）UPSERT
對應 migration：gis-platform/migrations/209_realtime_yt_live_videos.sql（312 搬 live.*）

環境變數：YOUTUBE_API_KEY（GCP 專案需啟用 YouTube Data API v3）
  缺 key → registry required_env 會直接跳過本 collector；若仍被呼叫則整輪跳過不寫入

Standalone dry-run（不寫 DB）：
  cd data-collectors && python3 -m collectors.yt_live_video_resolver
頻率：5 分鐘（沿用；驗證只花 1 unit，可即時反映換 ID）
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

import requests

import config
from collectors.base import BaseCollector, TAIPEI_TZ

API_BASE = "https://www.googleapis.com/youtube/v3"
PT_TZ = ZoneInfo("America/Los_Angeles")  # YouTube quota 每日重置時區

# (handle, channel_id, 台名)
# handle 同時是 live.yt_live_current 的 PK，前端 LiveWall.tsx LIVE_CHANNELS 靠它對照
#   → 不可隨意改字串（要改須與 mini-taiwan-pulse 同步）。
# channel_id 由 channels.list?forHandle 一次性解出後寫死（省掉每輪 13 units）。
CHANNELS: tuple[tuple[str, str, str], ...] = (
    ("@ptslivestream", "UCXgIO9jJVsX5_2ideiSkfvA", "公視 網路直播頻道"),
    ("@CtsTw",         "UCDCJyLpbfgeVE9iZiEam-Kg", "華視新聞 CH52"),
    ("@TVBSNEWS01",    "UC5nwNW4KdC0SzrhF9BXEYOQ", "TVBS NEWS"),
    ("@SETN",          "UCIU8ha-NHmLjtUwU7dFiXUA", "三立新聞網 SETN"),
    ("@newsebc",       "UCR3asjvr_WAaxwJYEDV_Bfw", "東森新聞 CH51"),
    ("@FTV_News",      "UC2VmWn8dAqkzlQqvy02E1PA", "民視新聞網"),
    ("@era_news",      "UCcjjdWilgUC6xCaCWRL30wQ", "年代新聞 Era News"),
    # ⚠ 真 handle 是 @mnews-tw（舊 @MnewsTw 404）。PK 沿用舊字串以免前端對不上，
    #   channel_id 已改成正確的鏡新聞頻道 → 這台從此可解出直播。
    ("@MnewsTw",       "UC4LjkybVKXCDlneVXlKAbmw", "鏡新聞"),
    ("@TTV_NEWS",      "UC8ROUUjHzEQm-ndb69CX8Ww", "台視新聞 TTV NEWS"),
    ("@twctvnews",     "UCmH4q-YjeazayYCVHHkGAMA", "中視新聞 HD 直播"),
    ("@globalnewstw",  "UCp2f7tGJGN6R9Muxipem8Nw", "寰宇新聞"),
    # ⚠ 真 handle 是 @ustv（舊 @ustvnews 404）。同上，PK 不動只換 channel_id。
    ("@ustvnews",      "UCYIVkruUoN04UjV9pkBTswg", "USTV 非凡電視"),
    ("@CNAvideo",      "UCVNbTzo72WMFZUN9_sdBpBA", "中央社攝影看世界"),
)

SEARCH_DAILY_CAP = 80        # 官方硬上限 100 calls/day/project，留 20 buffer
SEARCH_COOLDOWN_MIN = 60     # 每頻道 search 基礎冷卻
SEARCH_BACKOFF_MAX_MIN = 360 # 連續槓龜指數退避上限（6h）
SEARCH_MAX_RESULTS = 5       # 一次搜幾支候選（再用 videos.list 挑）
VIDEOS_BATCH = 50            # videos.list id 上限
STALE_CLEAR_HOURS = 48       # 連續無法驗證多久才清空 video_id
UNITS_DAILY_POOL = 10000     # 共用 units 池（僅供 log 對照）

# 403 代表配額問題的 reason（碰到就停到次日，不重試）
QUOTA_REASONS = {"quotaExceeded", "dailyLimitExceeded", "rateLimitExceeded", "userRateLimitExceeded"}

# search 失敗（不是「查完發現沒直播」）→ 沿用舊值 sticky，不清空
STICKY_NOTE_PREFIXES = (
    "search_budget", "search_cooldown", "search_failed",
    "search_quota", "candidate_verify",
)


class YtApiError(Exception):
    """YouTube API 非 200 回應"""


class YtQuotaExceeded(YtApiError):
    """配額用盡（search 獨立桶 100/day 或共用 units 池）"""


def _int(v) -> Optional[int]:
    try:
        return int(str(v).strip())
    except (TypeError, ValueError):
        return None


def _error_reason(resp: requests.Response) -> str:
    """從 Google API error body 撈 errors[0].reason"""
    try:
        errs = (resp.json().get("error") or {}).get("errors") or []
        return errs[0].get("reason", "") if errs else ""
    except Exception:
        return ""


def _video_state(item: dict) -> dict:
    """videos.list 單筆 → 直播狀態判定

    live: 有 actualStartTime、無 actualEndTime、liveBroadcastContent=='live'
    ended: 已有 actualEndTime 或已變成一般影片（'none'）
    upcoming: 排程中還沒開播（對 LiveWall 而言等同不能播）
    """
    snip = item.get("snippet") or {}
    live = item.get("liveStreamingDetails") or {}
    stats = item.get("statistics") or {}
    status = item.get("status") or {}
    started, ended = live.get("actualStartTime"), live.get("actualEndTime")
    broadcast = snip.get("liveBroadcastContent")

    if started and not ended and broadcast == "live":
        state = "live"
    elif ended or broadcast == "none":
        state = "ended"
    else:
        state = "upcoming"

    return {
        "state":      state,
        "title":      snip.get("title"),
        "view_count": _int(stats.get("viewCount")),
        "concurrent": _int(live.get("concurrentViewers")),
        "embeddable": bool(status.get("embeddable", True)),
    }


class YtLiveVideoResolverCollector(BaseCollector):
    """每 5 分鐘把 13 家新聞台的當前直播 videoId 解析到 Supabase（sticky 寫入）"""

    name = "yt_live_video_resolver"
    interval_minutes = config.YT_LIVE_VIDEO_RESOLVER_INTERVAL

    def __init__(self):
        super().__init__()
        self._init_state()

    def _init_state(self) -> None:
        """程序內狀態（scheduler 持有同一個 collector 實例 → 跨輪累積）"""
        self._session = requests.Session()
        # handle → {video_id, title, view_count, is_live}：最後一次「確認過」的值
        self._known: dict[str, dict] = {}
        self._verified_at: dict[str, datetime] = {}      # handle → 最後成功驗證時間
        self._cooldown_until: dict[str, datetime] = {}   # handle → 下次可 search 時間
        self._search_streak: dict[str, int] = {}         # handle → 連續槓龜次數（指數退避）
        self._quota_day: Optional[date] = None           # PT 日期（換日重置計數）
        self._search_today = 0                           # 今日 search.list 呼叫次數
        self._units_today = 0                            # 今日 videos/channels units（估）
        self._seeded = False

    # ------------------------------------------------------------
    # API 呼叫
    # ------------------------------------------------------------

    def _api(self, endpoint: str, params: dict, cost_units: int = 1) -> dict:
        """呼叫 API；search 走獨立配額桶故 cost_units=0（不計 units）"""
        resp = self._session.get(
            f"{API_BASE}/{endpoint}",
            params={**params, "key": config.YOUTUBE_API_KEY},
            timeout=config.REQUEST_TIMEOUT,
        )
        if resp.status_code == 200:
            self._units_today += cost_units
            return resp.json()

        reason = _error_reason(resp)
        if resp.status_code == 403 and reason in QUOTA_REASONS:
            raise YtQuotaExceeded(f"{endpoint}:{reason}")
        raise YtApiError(f"{endpoint} HTTP {resp.status_code} {reason or resp.text[:80]}")

    def _verify(self, video_ids: list[str]) -> dict[str, dict]:
        """批次驗證 video_id 現況（每 50 個 1 call / 1 unit）"""
        out: dict[str, dict] = {}
        for i in range(0, len(video_ids), VIDEOS_BATCH):
            chunk = video_ids[i:i + VIDEOS_BATCH]
            data = self._api("videos", {
                "part": "snippet,liveStreamingDetails,statistics,status",
                "id": ",".join(chunk),
                "maxResults": len(chunk),
            })
            self._videos_calls_tick += 1
            for item in data.get("items", []):
                if item.get("id"):
                    out[item["id"]] = _video_state(item)
            # 回應裡沒出現的 = 已刪除 / 轉私人
            for vid in chunk:
                out.setdefault(vid, {
                    "state": "missing", "title": None, "view_count": None,
                    "concurrent": None, "embeddable": False,
                })
        return out

    def _search_live(self, channel_id: str) -> list[str]:
        """搜某頻道當前直播（1 call，扣 search 獨立桶 100/day）"""
        data = self._api("search", {
            "part": "id",
            "channelId": channel_id,
            "eventType": "live",
            "type": "video",
            "maxResults": SEARCH_MAX_RESULTS,
        }, cost_units=0)
        self._search_today += 1
        self._search_calls_tick += 1
        return [
            it["id"]["videoId"]
            for it in data.get("items", [])
            if (it.get("id") or {}).get("videoId")
        ]

    # ------------------------------------------------------------
    # 節流 / 狀態
    # ------------------------------------------------------------

    def _roll_quota_day(self, now: datetime) -> None:
        """PT 午夜換日 → 重置 search 計數與 units 估計"""
        pt_day = now.astimezone(PT_TZ).date()
        if self._quota_day != pt_day:
            if self._quota_day is not None:
                print(f"[{self.name}] PT 換日 {self._quota_day} → {pt_day}，"
                      f"重置 quota 計數（前日 search={self._search_today}, units≈{self._units_today}）")
            self._quota_day = pt_day
            self._search_today = 0
            self._units_today = 0

    def _bump_cooldown(self, handle: str, now: datetime) -> None:
        """搜過但沒直播 → 指數退避（60 → 120 → 240 → 360 分鐘上限）"""
        streak = self._search_streak.get(handle, 0)
        minutes = min(SEARCH_COOLDOWN_MIN * (2 ** streak), SEARCH_BACKOFF_MAX_MIN)
        self._search_streak[handle] = streak + 1
        self._cooldown_until[handle] = now + timedelta(minutes=minutes)

    def _seed_from_db(self) -> None:
        """開機第一輪：從 live.yt_live_current 撿回已知 video_id（省掉 13 次 search）"""
        if self._seeded:
            return
        self._seeded = True  # 只試一次，失敗就靠 search 重建
        if not self.supabase_writer:
            return
        try:
            with self.supabase_writer.with_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT handle, video_id, title, is_live, view_count, observed_at "
                        "FROM live.yt_live_current WHERE video_id IS NOT NULL"
                    )
                    rows = cur.fetchall()
            valid = {h for h, _, _ in CHANNELS}
            for handle, vid, title, is_live, view_count, observed_at in rows:
                if handle not in valid:
                    continue
                self._known[handle] = {
                    "video_id": vid, "title": title,
                    "is_live": bool(is_live), "view_count": view_count,
                }
                self._verified_at[handle] = observed_at or datetime.now(tz=TAIPEI_TZ)
            print(f"[{self.name}] seed: 從 live.yt_live_current 撿回 {len(self._known)} 個已知 video_id")
        except Exception as e:
            print(f"[{self.name}] ⚠ seed 讀 live.yt_live_current 失敗（改由 search 重建）: {e}")

    # ------------------------------------------------------------
    # 發現（search）
    # ------------------------------------------------------------

    def _discover(
        self,
        targets: list[tuple[str, str]],
        forced: set[str],
        now: datetime,
    ) -> tuple[dict[str, dict], dict[str, str]]:
        """對沒有有效 live ID 的頻道跑 search + 候選驗證

        Args:
            targets: [(handle, channel_id)] 待搜清單
            forced:  已確認下播/被刪的 handle（忽略冷卻，但仍受每日上限）
        Returns:
            (found, notes) — found: handle → 新的 {video_id,title,view_count,is_live}
                             notes: handle → 沒找到的原因（寫進 last_error）
        """
        found: dict[str, dict] = {}
        notes: dict[str, str] = {}
        candidates: dict[str, list[str]] = {}

        for handle, channel_id in targets:
            if self._search_today >= SEARCH_DAILY_CAP:
                notes[handle] = f"search_budget_exhausted({self._search_today}/{SEARCH_DAILY_CAP}_calls_today)"
                continue
            cd = self._cooldown_until.get(handle)
            if cd and now < cd and handle not in forced:
                notes[handle] = f"search_cooldown_until_{cd.astimezone(TAIPEI_TZ).strftime('%H:%M')}"
                continue
            try:
                ids = self._search_live(channel_id)
            except YtQuotaExceeded as e:
                # 獨立桶爆了 → 本日停搜（不重試），其餘 target 走上面的 budget 分支
                self._search_today = SEARCH_DAILY_CAP
                notes[handle] = f"search_quota_exceeded: {e}"
                print(f"[{self.name}] ⚠ search 配額用盡（{e}）→ 停搜至 PT 次日，既有 video_id 一律保留")
                continue
            except Exception as e:
                notes[handle] = f"search_failed: {type(e).__name__}: {e}"
                continue

            if ids:
                candidates[handle] = ids
            else:
                notes[handle] = "channel_not_live"
                self._bump_cooldown(handle, now)

        if not candidates:
            return found, notes

        # 候選集中驗證：不管幾個頻道都只花 1 call（≤50 支）
        all_ids = sorted({v for ids in candidates.values() for v in ids})
        try:
            info = self._verify(all_ids)
        except Exception as e:
            for handle in candidates:
                notes[handle] = f"candidate_verify_failed: {type(e).__name__}: {e}"
            return found, notes

        for handle, ids in candidates.items():
            best: Optional[tuple[tuple, str, dict]] = None
            for vid in ids:
                st = info.get(vid) or {}
                if st.get("state") != "live":
                    continue
                rank = (
                    1 if st.get("embeddable") else 0,   # 不能嵌入的排後面
                    st.get("concurrent") or 0,          # 同時觀看數
                    st.get("view_count") or 0,
                )
                if best is None or rank > best[0]:
                    best = (rank, vid, st)
            if best is None:
                notes[handle] = "search_hit_but_not_live"
                self._bump_cooldown(handle, now)
                continue
            _, vid, st = best
            found[handle] = {
                "video_id": vid, "title": st["title"],
                "view_count": st["view_count"], "is_live": True,
            }
            self._cooldown_until.pop(handle, None)
            self._search_streak.pop(handle, None)

        return found, notes

    # ------------------------------------------------------------
    # 主流程
    # ------------------------------------------------------------

    def collect(self) -> dict:
        now = datetime.now(tz=TAIPEI_TZ)
        collected_iso = now.isoformat()
        self._videos_calls_tick = 0
        self._search_calls_tick = 0

        if not config.YOUTUBE_API_KEY:
            # 沒有 'data' key → base.py 不寫入任何東西（既有 video_id 完整保留）
            print(f"[{self.name}] ✗ YOUTUBE_API_KEY 未設定 → 整輪跳過，不覆寫既有資料")
            return {
                "total": 0, "live_count": 0, "failed_count": 0,
                "skipped": "missing_api_key", "collected_at": collected_iso,
            }

        self._roll_quota_day(now)
        self._seed_from_db()

        # ---- 1) 驗證已知 ID（全部併成 1 call / 1 unit）----
        known_ids = {h: st["video_id"] for h, st in self._known.items() if st.get("video_id")}
        verify: dict[str, dict] = {}
        verify_err: Optional[str] = None
        if known_ids:
            try:
                verify = self._verify(sorted(set(known_ids.values())))
            except Exception as e:
                verify_err = f"{type(e).__name__}: {e}"
                print(f"[{self.name}] ⚠ videos.list 驗證失敗 → 全部 sticky 保留: {verify_err}")

        # ---- 2) 分類：還活著 / 已下播（要補搜）----
        gone: dict[str, str] = {}     # handle → 已確認的非 live 狀態
        verified_live: set[str] = set()
        for handle, _channel_id, _label in CHANNELS:
            vid = known_ids.get(handle)
            if not vid or verify_err:
                continue  # 沒有已知 ID → 走發現；驗不到 → sticky（verified_at 不動）
            st = verify.get(vid) or {"state": "missing"}
            if st["state"] == "live":
                self._known[handle].update({
                    "title": st["title"], "view_count": st["view_count"], "is_live": True,
                })
                self._verified_at[handle] = now
                self._search_streak.pop(handle, None)
                verified_live.add(handle)
            else:
                gone[handle] = st["state"]

        # ---- 3) 發現：沒有有效 live ID 的頻道才 search ----
        # verify 整批失敗時不搜（同一條網路/配額問題，省 search 桶），全 sticky
        targets = (
            []
            if verify_err
            else [(h, c) for h, c, _ in CHANNELS if h not in known_ids or h in gone]
        )
        discovered, notes = self._discover(targets, forced=set(gone), now=now)
        verified_live |= set(discovered)

        # ---- 4) 組 row（shape 與舊版相同）----
        rows: list[dict] = []
        sticky_count = cleared_count = 0
        for handle, channel_id, _label in CHANNELS:
            st = dict(self._known.get(handle) or {})
            err: Optional[str] = None

            if handle in discovered:
                st.update(discovered[handle])
                self._known[handle] = st
                self._verified_at[handle] = now
            elif handle in verified_live:
                pass  # 驗證過還在播，值已更新
            elif verify_err and st.get("video_id"):
                err = f"verify_unavailable: {verify_err}"
                sticky_count += 1
            elif handle in gone:
                note = notes.get(handle, "no_search_attempted")
                if note.startswith(STICKY_NOTE_PREFIXES):
                    # 確定下播但「還沒能搜」→ 先留舊 ID（下輪 forced 再搜），不清空
                    err = f"live_{gone[handle]}_but_{note}"
                    sticky_count += 1
                else:
                    st = {"video_id": None, "title": None, "view_count": None, "is_live": False}
                    self._known[handle] = st
                    self._verified_at.pop(handle, None)
                    err = f"live_{gone[handle]}_no_replacement"
                    cleared_count += 1
            elif not st.get("video_id"):
                err = notes.get(handle) or (
                    f"discovery_skipped: {verify_err}" if verify_err else "no_live_found"
                )
            else:
                # 有舊值、這輪沒驗到也沒搜到（例：verify 成功但該 handle 不在 targets）
                err = notes.get(handle) or "kept_previous"
                sticky_count += 1

            # 連續 48h 無法驗證 → 才真的清空（避免殭屍 ID 長期掛著）
            if st.get("video_id") and handle not in verified_live:
                last_ok = self._verified_at.get(handle)
                if last_ok and (now - last_ok) > timedelta(hours=STALE_CLEAR_HOURS):
                    st = {"video_id": None, "title": None, "view_count": None, "is_live": False}
                    self._known[handle] = st
                    self._verified_at.pop(handle, None)
                    err = f"unverified_over_{STALE_CLEAR_HOURS}h_cleared"
                    sticky_count = max(0, sticky_count - 1)
                    cleared_count += 1

            rows.append({
                "handle":       handle,
                "channel_id":   channel_id,
                "video_id":     st.get("video_id"),
                "title":        st.get("title"),
                "is_live":      bool(st.get("video_id")) and bool(st.get("is_live")),
                "view_count":   st.get("view_count"),
                "last_error":   err[:200] if err else None,
                "observed_at":  collected_iso,
                "collected_at": collected_iso,
            })

        live_count = sum(1 for r in rows if r["is_live"])
        print(f"[{self.name}] quota: videos.list={self._videos_calls_tick} call "
              f"· search={self._search_calls_tick} call"
              f" | 今日 search {self._search_today}/{SEARCH_DAILY_CAP}（官方硬上限 100 calls/day）"
              f"，units≈{self._units_today}/{UNITS_DAILY_POOL}，PT 日 {self._quota_day}")
        print(f"[{self.name}] live={live_count}/{len(rows)} sticky={sticky_count} cleared={cleared_count}")

        return {
            # base.py 需 'data' 才寫入 → 同時當 history；其餘 key 會進 stats/webhook，
            # 所以不放 rows 別名（舊版的 'videos' 會讓每輪 webhook 夾 13 筆全文）
            "data":               rows,
            "total":              len(rows),
            "live_count":         live_count,
            "sticky_count":       sticky_count,
            "cleared_count":      cleared_count,
            "failed_count":       sum(1 for r in rows if r["last_error"]),
            "search_calls_today": self._search_today,
            "units_today":        self._units_today,
            "collected_at":       collected_iso,
        }


if __name__ == "__main__":
    # 離線試跑（不寫 DB）：python3 -m collectors.yt_live_video_resolver
    c = YtLiveVideoResolverCollector.__new__(YtLiveVideoResolverCollector)
    c._init_state()
    c.supabase_writer = None
    out = c.collect()
    print(f"\ntotal: {out['total']}  live: {out.get('live_count')}  "
          f"failed: {out.get('failed_count')}  skipped: {out.get('skipped', '-')}")
    for r in out.get("data", []):
        flag = "●" if r["is_live"] else "○"
        print(f"  {flag} {r['handle']:<15} vid={r['video_id'] or '—':<13} "
              f"vc={r['view_count'] or '—':<12} {r['last_error'] or r['title'] or ''}"[:150])
