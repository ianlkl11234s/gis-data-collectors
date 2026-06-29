#!/usr/bin/env bash
# 安裝 immigration_apis_airport 兩條 cron：每 60 分鐘抓資料 + 每日 03:15 歸檔
set -euo pipefail

mkdir -p /var/log/immigration-apis-airport

TMP=$(mktemp)
crontab -l 2>/dev/null > "$TMP" || true

# 移除既有規則（用標記做精準替換）
sed -i '/# ─── immigration_apis_airport cron BEGIN ───/,/# ─── immigration_apis_airport cron END ───/d' "$TMP"

cat >>"$TMP" <<'EOF'
# ─── immigration_apis_airport cron BEGIN ───
SHELL=/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
# 每小時 7 分抓一次（避開 00 分共用尖峰）
7 * * * * /usr/bin/python3 /opt/immigration-apis-airport/immigration_apis_airport_collect.py >> /var/log/immigration-apis-airport/collect.log 2>&1
# 每天 03:15 把 8 天前的 JSON 打包上 S3
15 3 * * * /usr/bin/python3 /opt/immigration-apis-airport/archive_immigration_apis_airport.py >> /var/log/immigration-apis-airport/archive.log 2>&1
# ─── immigration_apis_airport cron END ───
EOF

crontab "$TMP"
rm -f "$TMP"

echo "✓ cron 已安裝。目前 crontab："
echo "──────────────────────────"
crontab -l
echo "──────────────────────────"
