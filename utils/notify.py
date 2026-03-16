"""
通知模組

支援 Webhook、LINE Notify 和 Telegram Bot。
"""

import requests
import config


def _instance_tag() -> str:
    """取得實例標識（用於多實例部署辨識來源）"""
    if config.INSTANCE_NAME:
        return f" [{config.INSTANCE_NAME}]"
    return ""


def send_webhook(event: str, data: dict = None):
    """發送 Webhook 通知"""
    if not config.WEBHOOK_URL:
        return

    try:
        payload = {'event': event}
        if data:
            payload.update(data)

        requests.post(
            config.WEBHOOK_URL,
            json=payload,
            timeout=10
        )
    except Exception as e:
        print(f"⚠️  Webhook 發送失敗: {e}")


def send_line_notify(message: str):
    """發送 LINE Notify 通知"""
    if not config.LINE_TOKEN:
        return

    try:
        requests.post(
            'https://notify-api.line.me/api/notify',
            headers={'Authorization': f'Bearer {config.LINE_TOKEN}'},
            data={'message': message},
            timeout=10
        )
    except Exception as e:
        print(f"⚠️  LINE Notify 發送失敗: {e}")


def send_telegram(message: str, parse_mode: str = 'Markdown'):
    """發送 Telegram 訊息

    Args:
        message: 訊息內容（支援 Markdown）
        parse_mode: 解析模式（Markdown 或 HTML）
    """
    if not config.TELEGRAM_BOT_TOKEN or not config.TELEGRAM_CHAT_ID:
        return

    try:
        url = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage"
        requests.post(
            url,
            json={
                'chat_id': config.TELEGRAM_CHAT_ID,
                'text': message,
                'parse_mode': parse_mode,
            },
            timeout=10
        )
    except Exception as e:
        print(f"⚠️  Telegram 發送失敗: {e}")


def notify_error(collector_name: str, error: str, consecutive_errors: int = 0):
    """通知錯誤

    Args:
        collector_name: 收集器名稱
        error: 錯誤訊息
        consecutive_errors: 連續錯誤次數
    """
    tag = _instance_tag()
    message = f"❌{tag} [{collector_name}] 錯誤: {error}"
    send_webhook('error', {'collector': collector_name, 'error': error})
    send_line_notify(message)

    # Telegram 即時錯誤通知
    if consecutive_errors >= config.CONSECUTIVE_ERROR_THRESHOLD:
        tg_msg = (
            f"🚨 *收集器連續錯誤告警*{tag}\n\n"
            f"收集器: `{collector_name}`\n"
            f"連續錯誤: *{consecutive_errors} 次*\n"
            f"錯誤: {_escape_md(error)}"
        )
        send_telegram(tg_msg)
    else:
        tg_msg = (
            f"❌ *收集器錯誤*{tag}\n\n"
            f"收集器: `{collector_name}`\n"
            f"錯誤: {_escape_md(error)}"
        )
        send_telegram(tg_msg)


def notify_success(collector_name: str, stats: dict):
    """通知成功"""
    send_webhook('success', {'collector': collector_name, **stats})


def notify_archive_complete(stats: dict):
    """通知歸檔完成"""
    tag = _instance_tag()
    archive = stats.get('archive', {})
    cleanup = stats.get('cleanup', {})

    uploaded = archive.get('uploaded', 0)
    skipped = archive.get('skipped', 0)
    failed = archive.get('failed', 0)
    deleted = cleanup.get('deleted', 0)

    icon = "⚠️" if failed > 0 else "📦"

    tg_msg = (
        f"{icon} *歸檔任務完成*{tag}\n\n"
        f"上傳: {uploaded} 個 tar.gz\n"
        f"跳過: {skipped} 個（已存在）\n"
        f"失敗: {failed} 個\n"
        f"清理: 刪除 {deleted} 個本地目錄"
    )
    send_telegram(tg_msg)


def notify_disk_alert(used_mb: float, threshold_mb: int):
    """磁碟空間告警"""
    tag = _instance_tag()
    tg_msg = (
        f"💾 *磁碟空間警告*{tag}\n\n"
        f"本地資料已使用: *{used_mb:.0f} MB*\n"
        f"警告門檻: {threshold_mb} MB\n\n"
        f"請確認歸檔是否正常運作"
    )
    send_telegram(tg_msg)


def notify_silence_alert(collector_name: str, last_run_str: str, expected_minutes: int):
    """收集器靜默告警（超過預期間隔仍未執行）"""
    tag = _instance_tag()
    tg_msg = (
        f"🔇 *收集器靜默告警*{tag}\n\n"
        f"收集器: `{collector_name}`\n"
        f"最後執行: {last_run_str}\n"
        f"預期間隔: {expected_minutes} 分鐘\n"
        f"已超過預期間隔 2 倍未更新"
    )
    send_telegram(tg_msg)


def _escape_md(text: str) -> str:
    """簡易 Markdown 特殊字元跳脫"""
    for char in ('_', '*', '`', '['):
        text = text.replace(char, f'\\{char}')
    return text
