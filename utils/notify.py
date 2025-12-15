"""
通知模組

支援 Webhook 和 LINE Notify。
"""

import requests
import config


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


def notify_error(collector_name: str, error: str):
    """通知錯誤"""
    message = f"❌ [{collector_name}] 錯誤: {error}"
    send_webhook('error', {'collector': collector_name, 'error': error})
    send_line_notify(message)


def notify_success(collector_name: str, stats: dict):
    """通知成功"""
    send_webhook('success', {'collector': collector_name, **stats})
