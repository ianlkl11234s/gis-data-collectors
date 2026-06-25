"""Docker HEALTHCHECK 探針

/health 非 200（主迴圈卡死回 503）或 API 無回應 → exit 1，讓平台 liveness 重啟進程。
用 stdlib urllib（python:3.11-slim 無 curl），且**不做** config fallback
（舊 Dockerfile 的 fallback 會在進程卡死時仍判 healthy，等於關掉 watchdog）。
"""
import os
import sys
import urllib.request

PORT = os.getenv("API_PORT", "8080")

try:
    resp = urllib.request.urlopen(f"http://localhost:{PORT}/health", timeout=5)
    sys.exit(0 if resp.status == 200 else 1)
except Exception:
    # HTTPError(503) / 連線失敗 / timeout 一律視為不健康
    sys.exit(1)
