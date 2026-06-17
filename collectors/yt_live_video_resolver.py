"""
YouTube Live Video Resolver — Monitor Mode LiveWall 用

抓 14 家新聞台的 `youtube.com/@<handle>/live` 頁面，解析出當前直播 videoId 和 UC channel_id，
寫進 `realtime.yt_live_current`，前端用 `embed/<videoId>` 才能可靠播放。

為何不用 `embed/live_stream?channel=UCxxx`？
  - YouTube 此 URL 要查頻道的「primary live event」，但很多新聞台沒設這個欄位
    （即使有直播也找不到），會跳「無法播放這部影片」。
  - 改用 videoId 直接 embed 100% 可靠，缺點是 video_id 每 1-7 天會換（直播重啟）→ 所以 cron。

寫入（schema=realtime）：
  - realtime.yt_live_current（current snapshot, PK=handle）UPSERT
  - realtime.yt_live_history（變更歷史, UNIQUE(handle,video_id,observed_at)）DO NOTHING

對應 migration：gis-platform/migrations/209_realtime_yt_live_videos.sql
頻率：5 分鐘 cron（直播 ID 約 1-7 天一換，5 分鐘恰好）
"""

from __future__ import annotations

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

# regex 從 YouTube 頁面 HTML 撈關鍵欄位
RE_VIDEO_ID    = re.compile(r'"videoId":"([A-Za-z0-9_-]{11})"')
RE_CHANNEL_ID  = re.compile(r'"(?:channelId|browseId)":"(UC[A-Za-z0-9_-]{22})"')
RE_IS_LIVE     = re.compile(r'"isLive":(true|false)')
RE_TITLE       = re.compile(r'"title":"([^"]+)","lengthSeconds"')  # 影片 title（直播也吃）
RE_VIEW_COUNT  = re.compile(r'"viewCount":"(\d+)"')


def _first(regex: re.Pattern, text: str) -> Optional[str]:
    m = regex.search(text)
    return m.group(1) if m else None


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

        # 頻道頁如果當下沒直播，會 redirect 回頻道首頁 → videoId 抓不到或是頻道頭照
        video_id   = _first(RE_VIDEO_ID, html)
        channel_id = _first(RE_CHANNEL_ID, html)
        is_live_s  = _first(RE_IS_LIVE, html)
        title      = _first(RE_TITLE, html)
        view_s     = _first(RE_VIEW_COUNT, html)

        row["channel_id"] = channel_id
        row["video_id"]   = video_id
        row["title"]      = title
        row["is_live"]    = is_live_s == "true"
        row["view_count"] = int(view_s) if view_s else None

        if not video_id:
            row["last_error"] = "no_video_id_in_html"
        elif not is_live_s:
            row["last_error"] = "no_isLive_flag"
        elif not row["is_live"]:
            row["last_error"] = "not_currently_live"

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
