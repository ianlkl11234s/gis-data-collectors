"""
通知模組（只走 Telegram）

送不送、送幾則由 utils/alerting.py 的分級狀態機決定；這裡只放送出函式與
collector／排程任務呼叫的薄介面。WEBHOOK_URL／LINE Notify 路徑已移除（2026-11）。
"""

import requests
import config


def _instance_tag() -> str:
    """取得實例標識（用於多實例部署辨識來源）"""
    if config.INSTANCE_NAME:
        return f" [{config.INSTANCE_NAME}]"
    return ""


def send_telegram(message: str, parse_mode: str = 'Markdown') -> bool:
    """發送 Telegram 訊息

    Args:
        message: 訊息內容（支援 Markdown）
        parse_mode: 解析模式（Markdown 或 HTML）

    Returns:
        True 代表發送成功；False 代表未設定或失敗（會 print 明顯錯誤）
    """
    if not config.TELEGRAM_BOT_TOKEN or not config.TELEGRAM_CHAT_ID:
        print("❌ Telegram 未設定：TELEGRAM_BOT_TOKEN 或 TELEGRAM_CHAT_ID 為空（env 沒掛好？）")
        return False

    url = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage"

    # 組裝 payload — parse_mode 為 None 時不送此欄位，避免 null 被 API 拒絕
    payload = {
        'chat_id': config.TELEGRAM_CHAT_ID,
        'text': message,
    }
    if parse_mode:
        payload['parse_mode'] = parse_mode

    try:
        resp = requests.post(url, json=payload, timeout=10)
    except Exception as e:
        print(f"❌ Telegram 網路/連線失敗: {type(e).__name__}: {e}")
        return False

    if resp.status_code != 200:
        # 常見：Markdown parse error 會 400。降級成純文字重試一次。
        print(f"❌ Telegram API {resp.status_code}: {resp.text[:300]}")
        if resp.status_code == 400 and parse_mode:
            try:
                resp2 = requests.post(
                    url,
                    json={'chat_id': config.TELEGRAM_CHAT_ID, 'text': message},
                    timeout=10,
                )
                if resp2.status_code == 200:
                    print("✓ Telegram 以純文字模式重送成功")
                    return True
                print(f"❌ 純文字重送仍失敗 {resp2.status_code}: {resp2.text[:300]}")
            except Exception as e:
                print(f"❌ 純文字重送例外: {e}")
        return False

    return True


# Telegram 單則訊息官方上限 4096，留一點 buffer 給分頁 header
_TG_CHUNK_LIMIT = 3800


def _split_for_telegram(message: str, limit: int = _TG_CHUNK_LIMIT) -> list:
    """把長訊息切成 ≤ limit 的多段。優先在 \n\n 邊界切，其次 \n，再次強切。"""
    if len(message) <= limit:
        return [message]

    chunks = []
    remaining = message
    while len(remaining) > limit:
        cut = remaining.rfind("\n\n", 0, limit)
        if cut < limit // 2:
            cut = remaining.rfind("\n", 0, limit)
        if cut < limit // 2:
            cut = limit
        chunks.append(remaining[:cut].rstrip())
        remaining = remaining[cut:].lstrip()
    if remaining:
        chunks.append(remaining)
    return chunks


def send_telegram_long(message: str, parse_mode: str = 'Markdown') -> bool:
    """發送可能超過 4096 字元上限的長訊息（自動分段）

    Returns:
        全部分段都成功才回 True；任何一段失敗回 False。
    """
    chunks = _split_for_telegram(message)
    if len(chunks) == 1:
        return send_telegram(message, parse_mode=parse_mode)

    total = len(chunks)
    all_ok = True
    for i, chunk in enumerate(chunks, 1):
        header = f"（{i}/{total}）\n"
        ok = send_telegram(header + chunk, parse_mode=parse_mode)
        if not ok:
            all_ok = False
    return all_ok


def notify_error(collector_name: str, error: str, consecutive_errors: int = 0,
                 interval_minutes: int | None = None):
    """collector 一次失敗：交給告警狀態機（不發即時訊息；連續失敗達門檻只進日報 🟡）。

    consecutive_errors 保留給舊呼叫端相容；連續次數改由 alert_state.json 持久化計算
    （watchdog 重啟後不會歸零）。
    """
    from utils import alerting
    try:
        alerting.collector_failed(collector_name, error, interval_minutes=interval_minutes)
    except Exception as e:  # 告警失敗不能讓收集掛掉
        print(f"⚠️  alerting 記錄失敗 [{collector_name}]: {e}")


def notify_success(collector_name: str, stats: dict | None = None):
    """collector 一次成功：清掉收集失敗（不送訊息；即時恢復通知由 tasks/staleness_watch 判斷）。"""
    from utils import alerting
    try:
        alerting.collector_succeeded(collector_name)
    except Exception as e:
        print(f"⚠️  alerting 記錄失敗 [{collector_name}]: {e}")


def notify_archive_complete(stats: dict):
    """03:00 歸檔結果：成功不送（列在日報「例行」行），有失敗才 🔴。"""
    from utils import alerting
    archive = stats.get('archive', {}) if isinstance(stats, dict) else {}
    failed = archive.get('failed', 0)
    alerting.record_routine('archive', not failed,
                            f"{archive.get('uploaded', 0)} 檔" + (f"，失敗 {failed}" if failed else ""))
    if failed:
        alerting.raise_alert(
            'archive', alerting.RED,
            f"S3 歸檔有 {failed} 個失敗（上傳 {archive.get('uploaded', 0)}、跳過 {archive.get('skipped', 0)}）；"
            "本機原始檔不會被清，請查 S3 權限或網路",
        )
    else:
        alerting.clear_alert('archive', "S3 歸檔")


def notify_trails_export(stats: dict):
    """02:00 軌跡凍結匯出結果：成功不送；失敗 🔴（bus 只留 3 天，漏一晚就永久遺失）。"""
    from utils import alerting
    failed = stats.get('failed', 0)
    alerting.record_routine('trails_export', not failed,
                            f"{stats.get('ok', 0)} 個" + (f"，失敗 {failed}" if failed else ""))
    if failed:
        failures = "；".join(str(f)[:120] for f in (stats.get('failures') or [])[:5])
        alerting.raise_alert(
            'trails_export', alerting.RED,
            f"軌跡凍結匯出失敗 {failed} 個 dataset-day（bus 只留 3 天，要在期限內補跑）：{failures}",
        )
    else:
        alerting.clear_alert('trails_export', "軌跡凍結匯出")


def _escape_md(text: str) -> str:
    """簡易 Markdown 特殊字元跳脫"""
    for char in ('_', '*', '`', '['):
        text = text.replace(char, f'\\{char}')
    return text
