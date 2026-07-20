#!/usr/bin/env bash
# 一鍵裝環境（HiCloud VM, Ubuntu 22.04+）
set -euo pipefail

APP_DIR="/opt/cdc-public-health"
DATA_DIR="/var/lib/cdc-public-health/data"
LOG_DIR="/var/log/cdc-public-health"

echo ">>> 安裝系統套件"
# Ubuntu 22.04+ 的 needrestart 會跳互動框問要不要重啟 daemon → 用 env 自動 "all"
export NEEDRESTART_MODE=a DEBIAN_FRONTEND=noninteractive
sudo -E apt-get update
sudo -E apt-get install -y python3 python3-pip python3-venv ca-certificates

echo ">>> 建立目錄"
sudo mkdir -p "$APP_DIR" "$DATA_DIR" "$LOG_DIR"
sudo chown -R "$USER":"$USER" "$APP_DIR" "$DATA_DIR" "$LOG_DIR"

echo ">>> 拷貝檔案到 $APP_DIR"
cp "$(dirname "$0")"/cdc_public_health_weekly_collect.py "$APP_DIR/"
[ -f "$(dirname "$0")"/.env ] && cp "$(dirname "$0")"/.env "$APP_DIR/" || \
  { cp "$(dirname "$0")"/.env.example "$APP_DIR/.env"; echo "!! 請編輯 $APP_DIR/.env 填入 SUPABASE_DB_URL"; }

echo ">>> 建立 venv + 安裝 Python 套件"
python3 -m venv "$APP_DIR/venv"
"$APP_DIR/venv/bin/pip" install --quiet --upgrade pip
"$APP_DIR/venv/bin/pip" install --quiet psycopg2-binary requests urllib3 python-dotenv

echo ">>> 完成。執行測試："
echo "    $APP_DIR/venv/bin/python $APP_DIR/cdc_public_health_weekly_collect.py"
echo
echo ">>> 安裝 cron："
echo "    bash $(dirname "$0")/setup_cron.sh"
