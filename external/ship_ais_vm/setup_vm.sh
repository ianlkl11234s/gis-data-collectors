#!/usr/bin/env bash
# HiCloud VM 一鍵環境設置（單檔 ship_ais collector + S3 daily archive）
set -euo pipefail

APP_DIR=/opt/ship-ais
DATA_DIR=/var/lib/ship-ais/data
LOG_DIR=/var/log/ship-ais

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
# Ubuntu 22.04 沒有 PEP 668，不需要 --break-system-packages
PIP_FLAGS=""
if pip3 install --help 2>/dev/null | grep -q break-system-packages; then
  PIP_FLAGS="--break-system-packages"
fi
pip3 install $PIP_FLAGS --quiet \
  'requests>=2.28' \
  'psycopg2-binary>=2.9' \
  'python-dotenv>=1.0' \
  'boto3>=1.28'

echo "[4/5] 部署程式"
install -m 755 /tmp/ship_ais_collect.py  "$APP_DIR/ship_ais_collect.py"
install -m 755 /tmp/archive_ship_ais.py  "$APP_DIR/archive_ship_ais.py"

if [ ! -f "$APP_DIR/.env" ]; then
  cat >"$APP_DIR/.env" <<'ENVEOF'
# ─── Supabase（必填）───
SUPABASE_DB_URL=

# ─── S3 歸檔（必填，跟 Zeabur 同一組）───
S3_BUCKET=migu-gis-data-collector
S3_REGION=ap-southeast-2
S3_ACCESS_KEY=
S3_SECRET_KEY=

# ─── 進階（可不動）───
DATA_DIR=/var/lib/ship-ais/data
ARCHIVE_RETENTION_DAYS=7
REQUEST_TIMEOUT=30
ENVEOF
  chmod 600 "$APP_DIR/.env"
  echo ""
  echo "⚠️  已建立 $APP_DIR/.env 範本，請填入 SUPABASE_DB_URL / S3_ACCESS_KEY / S3_SECRET_KEY"
fi

echo "[5/5] 完成"
echo ""
echo "✓ 環境設置完成"
echo "  - app dir : $APP_DIR"
echo "  - collector : $APP_DIR/ship_ais_collect.py"
echo "  - archiver  : $APP_DIR/archive_ship_ais.py"
echo "  - data      : $DATA_DIR"
echo "  - logs      : $LOG_DIR"
echo ""
echo "下一步："
echo "  1. nano $APP_DIR/.env                              # 填密碼"
echo "  2. python3 $APP_DIR/ship_ais_collect.py            # 手動測試 collector"
echo "  3. python3 $APP_DIR/archive_ship_ais.py            # 手動測試 archiver (無歷史檔就只會 noop)"
echo "  4. cron 設定等驗證後再做"
