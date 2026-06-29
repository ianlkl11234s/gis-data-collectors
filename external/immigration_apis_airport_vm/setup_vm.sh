#!/usr/bin/env bash
# HiCloud VM 一鍵環境設置（immigration_apis_airport collector + S3 daily archive）
set -euo pipefail

APP_DIR=/opt/immigration-apis-airport
DATA_DIR=/var/lib/immigration-apis-airport/data
LOG_DIR=/var/log/immigration-apis-airport

echo "[1/5] apt update & 安裝系統套件"
apt-get update -qq
DEBIAN_FRONTEND=noninteractive apt-get install -y -qq \
  python3 python3-pip \
  libpq5 \
  tzdata cron

echo "[2/5] 設定時區為 Asia/Taipei"
timedatectl set-timezone Asia/Taipei || true

echo "[3/5] 建立目錄 & 安裝 Python 套件"
mkdir -p "$APP_DIR" "$DATA_DIR" "$LOG_DIR"
PIP_FLAGS=""
if pip3 install --help 2>/dev/null | grep -q break-system-packages; then
  PIP_FLAGS="--break-system-packages"
fi
pip3 install $PIP_FLAGS --quiet \
  'requests>=2.28' \
  'psycopg2-binary>=2.9' \
  'python-dotenv>=1.0' \
  'urllib3>=1.26' \
  'boto3>=1.28'

echo "[4/5] 部署程式"
install -m 755 /tmp/immigration_apis_airport_collect.py  "$APP_DIR/immigration_apis_airport_collect.py"
install -m 755 /tmp/archive_immigration_apis_airport.py  "$APP_DIR/archive_immigration_apis_airport.py"

if [ ! -f "$APP_DIR/.env" ]; then
  cat >"$APP_DIR/.env" <<'ENVEOF'
# ─── Supabase（必填）───
SUPABASE_DB_URL=

# ─── S3（archive 用，可選）───
S3_BUCKET=migu-gis-data-collector
S3_REGION=ap-southeast-2
S3_ACCESS_KEY=
S3_SECRET_KEY=

# ─── 本地設定 ───
DATA_DIR=/var/lib/immigration-apis-airport/data
REQUEST_TIMEOUT=30
ARCHIVE_RETENTION_DAYS=7
ENVEOF
  chmod 600 "$APP_DIR/.env"
  echo "  ✓ .env 範本已建立 → $APP_DIR/.env (請編輯填入 SUPABASE_DB_URL + S3 key)"
fi

echo "[5/5] 完成。下一步："
echo "  1. 編輯 $APP_DIR/.env 填入 SUPABASE_DB_URL（+ 可選 S3 key）"
echo "  2. 跑連線測試：python3 $APP_DIR/immigration_apis_airport_collect.py"
echo "  3. 安裝 cron：bash /tmp/setup_cron.sh"
