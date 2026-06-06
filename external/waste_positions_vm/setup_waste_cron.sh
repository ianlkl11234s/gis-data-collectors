#!/usr/bin/env bash
# 安裝 waste_positions 的 cron：每 2 分鐘抓 + 每日 03:05 歸檔
set -euo pipefail

mkdir -p /var/log/waste-positions

TMP=$(mktemp)
crontab -l 2>/dev/null > "$TMP" || true
sed -i '/# ─── waste_positions cron BEGIN ───/,/# ─── waste_positions cron END ───/d' "$TMP"

cat >>"$TMP" <<'EOF'
# ─── waste_positions cron BEGIN ───
# 每 2 分鐘抓垃圾車 GPS（quiet hours 01-06 內 collector 自己會 skip）
*/2 * * * * /usr/bin/python3 /opt/waste-positions/waste_positions_collect.py >> /var/log/waste-positions/collect.log 2>&1
# 每天 03:05 歸檔（與 ship_ais 03:00 錯開避免同時跑）
5 3 * * * /usr/bin/python3 /opt/waste-positions/archive_waste_positions.py >> /var/log/waste-positions/archive.log 2>&1
# ─── waste_positions cron END ───
EOF

crontab "$TMP"
rm -f "$TMP"

echo "✓ cron 已安裝。目前 crontab："
echo "──────────────────────────"
crontab -l
echo "──────────────────────────"
