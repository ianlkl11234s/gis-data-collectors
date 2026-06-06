#!/usr/bin/env bash
# waste_positions: 在已有 ship_ais 的 VM 上加裝（共用 Python 套件）
set -euo pipefail

APP_DIR=/opt/waste-positions
DATA_DIR=/var/lib/waste-positions/data
LOG_DIR=/var/log/waste-positions

echo "[1/3] 建立目錄"
mkdir -p "$APP_DIR" "$DATA_DIR" "$LOG_DIR"

echo "[2/3] 部署程式"
install -m 755 /tmp/waste_positions_collect.py  "$APP_DIR/waste_positions_collect.py"
install -m 755 /tmp/archive_waste_positions.py  "$APP_DIR/archive_waste_positions.py"

if [ ! -f "$APP_DIR/.env" ]; then
  cat >"$APP_DIR/.env" <<'ENVEOF'
# ─── Supabase（必填，與 ship_ais 同一條）───
SUPABASE_DB_URL=

# ─── S3 歸檔（必填，與 ship_ais 同一組）───
S3_BUCKET=migu-gis-data-collector
S3_REGION=ap-southeast-2
S3_ACCESS_KEY=
S3_SECRET_KEY=

# ─── 進階（可不動）───
DATA_DIR=/var/lib/waste-positions/data
CITIES=Kaohsiung,NewTaipei,Tainan
QUIET_HOURS=01-06
ARCHIVE_RETENTION_DAYS=7
ENVEOF
  chmod 600 "$APP_DIR/.env"
  echo "⚠️  已建立 $APP_DIR/.env 範本"
fi

echo "[3/3] 完成"
echo ""
echo "✓ 環境設置完成"
echo "  - app dir : $APP_DIR"
echo "  - collector : $APP_DIR/waste_positions_collect.py"
echo "  - archiver  : $APP_DIR/archive_waste_positions.py"
echo "  - data      : $DATA_DIR"
echo "  - logs      : $LOG_DIR"
echo ""
echo "下一步："
echo "  1. cp /opt/ship-ais/.env $APP_DIR/.env  # 直接複用（key 一樣，僅 DATA_DIR/QUIET_HOURS 不同）"
echo "     或 nano $APP_DIR/.env"
echo "  2. python3 $APP_DIR/waste_positions_collect.py   # 手動測試"
echo "  3. bash /tmp/setup_waste_cron.sh                 # 加 cron"
