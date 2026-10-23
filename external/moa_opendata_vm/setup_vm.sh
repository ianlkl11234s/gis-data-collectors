#!/usr/bin/env bash
# 在 VM 上安裝／更新 moa_opendata runner（從 deploy.sh 的 tarball 解出的 /tmp/moa-opendata 執行）。
# 只裝檔案與依賴；不裝 cron（驗證後另跑 setup_cron.sh）。絕不覆蓋既有 .env。
set -euo pipefail

SRC_DIR=$(cd "$(dirname "$0")" && pwd)
APP_DIR=/opt/moa-opendata
DATA_DIR=/var/lib/moa-opendata/data
STATE_DIR=/var/lib/moa-opendata/state
LOG_DIR=/var/log/moa-opendata

if [ "$SRC_DIR" = "$APP_DIR" ]; then
  echo "請從解壓後的 /tmp/moa-opendata 執行，不要在 $APP_DIR 內執行" >&2
  exit 2
fi

timedatectl set-timezone Asia/Taipei || true
mkdir -p "$APP_DIR" "$DATA_DIR" "$STATE_DIR" "$LOG_DIR"

# 依賴：VM 已有系統版 requests／psycopg2／boto3；缺才補裝（不需要其他套件）。
if ! python3 -c 'import requests, psycopg2, boto3' 2>/dev/null; then
  PIP_FLAGS=""
  if pip3 install --help 2>/dev/null | grep -q break-system-packages; then
    PIP_FLAGS="--break-system-packages"
  fi
  pip3 install $PIP_FLAGS --quiet 'requests>=2.28' 'psycopg2-binary>=2.9' 'boto3>=1.28'
fi

# 程式碼整包替換（避免舊版殘檔被 import）；.env 與資料目錄不動。
for d in collectors storage utils tasks; do
  rm -rf "${APP_DIR:?}/$d"
  cp -R "$SRC_DIR/$d" "$APP_DIR/$d"
done
install -m 644 "$SRC_DIR/config.py" "$APP_DIR/config.py"
install -m 755 "$SRC_DIR/moa_opendata_run.py" "$APP_DIR/moa_opendata_run.py"
install -m 755 "$SRC_DIR/setup_cron.sh" "$APP_DIR/setup_cron.sh"
install -m 644 "$SRC_DIR/README.md" "$SRC_DIR/REVISION" "$SRC_DIR/.env.example" "$APP_DIR/"
find "$APP_DIR" -name '__pycache__' -type d -prune -exec rm -rf {} +

if [ ! -f "$APP_DIR/.env" ]; then
  install -m 600 "$SRC_DIR/.env.example" "$APP_DIR/.env"
  echo "⚠ 已建立 $APP_DIR/.env 範本，請填入值（見 README）"
else
  echo "✓ 保留既有 $APP_DIR/.env"
fi

echo "✓ 已安裝 $(cat "$APP_DIR/REVISION") 到 $APP_DIR"
echo "  下一步：cd $APP_DIR && set -a && . ./.env && set +a && python3 moa_opendata_run.py --dry-check"
