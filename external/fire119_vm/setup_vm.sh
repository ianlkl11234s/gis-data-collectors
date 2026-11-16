#!/usr/bin/env bash
# 安裝 fire119 runner（只裝檔案與套件，不排程）。檔案先 scp 到 /tmp/fire119/（見 README 部署步驟）。
set -euo pipefail

APP_DIR=/opt/fire119
SRC=${1:-/tmp/fire119}
mkdir -p "$APP_DIR" /var/lib/fire119/data /var/lib/fire119/state /var/lib/fire119/raw /var/log/fire119

PIP_FLAGS=""
if pip3 install --help 2>/dev/null | grep -q break-system-packages; then
  PIP_FLAGS="--break-system-packages"
fi
# 其他 VM 收集器已裝 requests／psycopg2／dotenv／boto3；本支另需 beautifulsoup4（HTML 來源）
pip3 install $PIP_FLAGS --quiet 'requests>=2.28' 'psycopg2-binary>=2.9' 'python-dotenv>=1.0' 'boto3>=1.28' \
  'beautifulsoup4>=4.12'

for f in fire119_collect.py archive_fire119.py run_fire119.sh vmstat_monitor.sh setup_cron.sh; do
  install -m 755 "$SRC/$f" "$APP_DIR/$f"
done
for f in fire119_parse.py fire119_store.py match.py vm_buffer.py bulk.py; do
  install -m 644 "$SRC/$f" "$APP_DIR/$f"
done
if [ -f "$SRC/road_gazetteer.sqlite" ]; then
  install -m 644 "$SRC/road_gazetteer.sqlite" "$APP_DIR/road_gazetteer.sqlite"
fi
if [ ! -f "$APP_DIR/.env" ]; then
  install -m 600 "$SRC/.env.example" "$APP_DIR/.env"
fi
echo "fire119 已安裝到 $APP_DIR。填 .env → 手動 dry-run 一輪 → 手動正式一輪 → 再跑 setup_cron.sh。"
