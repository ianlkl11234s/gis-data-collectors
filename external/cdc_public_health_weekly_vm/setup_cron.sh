#!/usr/bin/env bash
# 安裝 weekly cron（每週四 11:00 抓 — CDC 約 10:00 發布上週資料，補 1 小時 buffer）
set -euo pipefail

APP_DIR="/opt/cdc-public-health"
LOG_DIR="/var/log/cdc-public-health"
CRON_USER="${SUDO_USER:-$USER}"

CRON_LINE="0 11 * * 4 $APP_DIR/venv/bin/python $APP_DIR/cdc_public_health_weekly_collect.py >> $LOG_DIR/collect.log 2>&1"
MARKER_BEGIN="# ─── cdc_public_health_weekly cron BEGIN ───"
MARKER_END="# ─── cdc_public_health_weekly cron END ───"

TMP=$(mktemp)
(crontab -u "$CRON_USER" -l 2>/dev/null || true) | grep -v "cdc_public_health_weekly" | grep -v "$MARKER_BEGIN" | grep -v "$MARKER_END" > "$TMP" || true
cat >> "$TMP" <<EOF
$MARKER_BEGIN
$CRON_LINE
$MARKER_END
EOF
sudo crontab -u "$CRON_USER" "$TMP"
rm -f "$TMP"

echo ">>> cron 已安裝（每週四 11:00 跑）："
sudo crontab -u "$CRON_USER" -l | grep -A1 "cdc_public_health_weekly"
