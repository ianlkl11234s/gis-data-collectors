#!/usr/bin/env bash
# 安裝 ship_ais 的兩條 cron：每 10 分鐘抓資料 + 每日 03:00 歸檔
set -euo pipefail

mkdir -p /var/log/ship-ais

# 留住既有 crontab，附加新規則（避免複寫）
TMP=$(mktemp)
crontab -l 2>/dev/null > "$TMP" || true

# 移除之前可能殘留的 ship-ais 規則（用標記做精準替換）
sed -i '/# ─── ship_ais cron BEGIN ───/,/# ─── ship_ais cron END ───/d' "$TMP"

cat >>"$TMP" <<'EOF'
# ─── ship_ais cron BEGIN ───
SHELL=/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
# 每 10 分鐘抓一次（XX:00, XX:10, ... XX:50）
*/10 * * * * /usr/bin/python3 /opt/ship-ais/ship_ais_collect.py >> /var/log/ship-ais/collect.log 2>&1
# 每天 03:00 把 8 天前的 JSON 打包上 S3
0 3 * * * /usr/bin/python3 /opt/ship-ais/archive_ship_ais.py >> /var/log/ship-ais/archive.log 2>&1
# ─── ship_ais cron END ───
EOF

crontab "$TMP"
rm -f "$TMP"

echo "✓ cron 已安裝。目前 crontab："
echo "──────────────────────────"
crontab -l
echo "──────────────────────────"
echo ""
echo "預期下一次 collector 跑的時間："
date -d "$(date +'%Y-%m-%d %H:%M:00') $(( 10 - $(date +%M) % 10 )) minutes" '+%Y-%m-%d %H:%M:%S'
