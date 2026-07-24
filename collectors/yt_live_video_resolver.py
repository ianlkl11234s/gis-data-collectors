"""
YouTube Live Video Resolver — Monitor Mode LiveWall 用

抓 14 家新聞台的 `youtube.com/@<handle>/live` 頁面，解析出當前直播 videoId 和 UC channel_id，
寫進 `live.yt_live_current`，前端用 `embed/<videoId>` 才能可靠播放。

為何不用 `embed/live_stream?channel=UCxxx`？
  - YouTube 此 URL 要查頻道的「primary live event」，但很多新聞台沒設這個欄位
    （即使有直播也找不到），會跳「無法播放這部影片」。
  - 改用 videoId 直接 embed 100% 可靠，缺點是 video_id 每 1-7 天會換（直播重啟）→ 所以 cron。

寫入（schema=realtime）：
  - live.yt_live_current（current snapshot, PK=handle）UPSERT
  - live.yt_live_history（變更歷史, UNIQUE(handle,video_id,observed_at)）DO NOTHING

對應 migration：gis-platform/migrations/209_realtime_yt_live_videos.sql
頻率：5 分鐘 cron（直播 ID 約 1-7 天一換，5 分鐘恰好）
"""

from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Optional

import requests

import config
from collectors.base import BaseCollector, TAIPEI_TZ

# 13 家 24h 新聞直播 handle —— 與前端 LiveWall.tsx LIVE_CHANNELS 對齊
# ⚠ TODO 待補正確 handle：@MnewsTw（鏡新聞）/ @ustvnews（非凡）
#   找的時候可從 youtube.com/@xxx/live 回 200 判斷
HANDLES: tuple[str, ...] = (
    "@ptslivestream",   # 公視 PTS
    "@CtsTw",           # 華視 CTS
    "@TVBSNEWS01",      # TVBS NEWS
    "@SETN",            # 三立新聞網（原 @setnews 404）
    "@newsebc",         # 東森
    "@FTV_News",        # 民視
    "@era_news",        # 年代（原 @eranews 404）
    "@MnewsTw",         # 鏡新聞（待補正確 handle，目前 404）
    "@TTV_NEWS",        # 台視
    "@twctvnews",       # 中視
    "@globalnewstw",    # 寰宇
    "@ustvnews",        # 非凡（待補正確 handle，目前 404）
    "@CNAvideo",        # 中央社攝影看世界（原 @cnavideonews 404）
)

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# 從 YouTube /live page 取 ytInitialPlayerResponse JSON —— 對應「目前 player 載入的影片」
# 比起獨立 regex 抓「第一個 videoId / isLiveContent」可靠太多
# （/live page 即使沒直播仍會 redirect 載入頻道頭推薦影片，那些影片的 meta 也會出現在頁面 HTML）
RE_PLAYER_RESPONSE = re.compile(
    r'(?:var\s+)?ytInitialPlayerResponse\s*=\s*(\{.+?\})\s*;\s*(?:var|</script>)',
    re.DOTALL,
)
RE_CHANNEL_ID = re.compile(r'"(?:externalChannelId|channelId)":"(UC[A-Za-z0-9_-]{22})"')


def _extract_player_response(html: str) -> Optional[dict]:
    """從 page HTML 撈 ytInitialPlayerResponse JSON 物件"""
    m = RE_PLAYER_RESPONSE.search(html)
    if not m:
        return None
    try:
        return json.loads(m.group(1))
    except (json.JSONDecodeError, ValueError):
        return None


class YtLiveVideoResolverCollector(BaseCollector):
    """每 5 分鐘把 14 家新聞台的當前直播 videoId 解析到 Supabase"""

    name = "yt_live_video_resolver"
    interval_minutes = config.YT_LIVE_VIDEO_RESOLVER_INTERVAL

    def __init__(self):
        super().__init__()
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": UA,
            "Accept-Language": "en-US,en;q=0.9",
        })

    def _resolve(self, handle: str, collected_iso: str) -> dict:
        """抓單一頻道 /live page → 解析欄位"""
        url = f"https://www.youtube.com/{handle}/live"
        row: dict = {
            "handle":       handle,
            "channel_id":   None,
            "video_id":     None,
            "title":        None,
            "is_live":      False,
            "view_count":   None,
            "last_error":   None,
            "observed_at":  collected_iso,
            "collected_at": collected_iso,
        }
        try:
            resp = self._session.get(url, timeout=config.REQUEST_TIMEOUT, allow_redirects=True)
            resp.raise_for_status()
            html = resp.text
        except Exception as e:
            row["last_error"] = f"fetch_failed: {type(e).__name__}: {e}"[:200]
            return row

        # 從 ytInitialPlayerResponse 取「目前 player 載入的這支影片」的 metadata
        player = _extract_player_response(html)
        # channel_id 可以從整頁找（fallback），player 內也有 videoDetails.channelId
        m_ch = RE_CHANNEL_ID.search(html)
        row["channel_id"] = m_ch.group(1) if m_ch else None

        if not player:
            row["last_error"] = "no_player_response"
            return row

        details = player.get("videoDetails") or {}
        live_details = player.get("microformat", {}).get("playerMicroformatRenderer", {}).get("liveBroadcastDetails") or {}
        playability = player.get("playabilityStatus") or {}

        video_id   = details.get("videoId")
        is_live_content = bool(details.get("isLiveContent"))
        is_live_now_meta = live_details.get("isLiveNow")
        # videoDetails 沒有直接 isLiveNow，但有 isLive（罕見）— 兩個都看
        is_live_now_top = details.get("isLive")
        # 嚴格判：必須是直播類型 (isLiveContent) AND 現在正在播 (isLiveNow not false)
        is_actually_live = (
            is_live_content
            and is_live_now_meta is not False
            and is_live_now_top is not False
            and playability.get("status") == "OK"
        )

        # channel_id from player 比 regex 更準
        if details.get("channelId"):
            row["channel_id"] = details["channelId"]
        row["is_live"]    = is_actually_live
        view_s = details.get("viewCount")
        row["view_count"] = int(view_s) if view_s and str(view_s).isdigit() else None

        # 只在真正在播時寫 video_id / title
        if is_actually_live:
            row["video_id"] = video_id
            row["title"]    = details.get("title")
        else:
            row["video_id"] = None
            row["title"]    = None
            if not video_id:
                row["last_error"] = "no_video_id_in_player"
            elif not is_live_content:
                row["last_error"] = "channel_has_no_active_live"
            elif is_live_now_meta is False or is_live_now_top is False:
                row["last_error"] = "live_ended"
            elif playability.get("status") != "OK":
                row["last_error"] = f"playability:{playability.get('status')}"

        return row

    def collect(self) -> dict:
        now = datetime.now(tz=TAIPEI_TZ)
        collected_iso = now.isoformat()

        rows: list[dict] = []
        for h in HANDLES:
            try:
                rows.append(self._resolve(h, collected_iso))
            except Exception as e:
                # 單一 handle 死掉不應該打斷其他人
                rows.append({
                    "handle":       h,
                    "channel_id":   None,
                    "video_id":     None,
                    "title":        None,
                    "is_live":      False,
                    "view_count":   None,
                    "last_error":   f"resolver_crash: {type(e).__name__}: {e}"[:200],
                    "observed_at":  collected_iso,
                    "collected_at": collected_iso,
                })

        live_count = sum(1 for r in rows if r["is_live"])
        return {
            "data":         rows,           # base.py 需 'data' 才寫入 → 同時當 history
            "videos":       rows,           # 顯式別名
            "total":        len(rows),
            "live_count":   live_count,
            "failed_count": sum(1 for r in rows if r["last_error"]),
            "collected_at": collected_iso,
        }


if __name__ == "__main__":
    # 離線試跑：python3 -m collectors.yt_live_video_resolver
    c = YtLiveVideoResolverCollector.__new__(YtLiveVideoResolverCollector)
    c._session = requests.Session()
    c._session.headers.update({"User-Agent": UA, "Accept-Language": "en-US,en;q=0.9"})
    out = c.collect()
    print(f"total: {out['total']}  live: {out['live_count']}  failed: {out['failed_count']}")
    for r in out["data"]:
        flag = "●" if r["is_live"] else "○"
        print(f"  {flag} {r['handle']:<18} ch={r['channel_id'] or '—':<26} "
              f"vid={r['video_id'] or '—':<13} {r['last_error'] or ''}")
