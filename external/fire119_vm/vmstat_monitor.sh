#!/bin/bash
# 部署後第一天監測：每分鐘記一行 free -m＋load＋上一輪 runner 摘要（耗時、峰值 RSS）。
# 一天後關閉：bash /opt/fire119/setup_cron.sh --no-monitor（見 README「關閉第一天監測」）。
LOG=${FIRE119_VMSTAT_LOG:-/var/log/fire119/vmstat.log}
STATE=${FIRE119_STATE_DIR:-/var/lib/fire119/state}/last_round.txt
mem=$(free -m | awk '/^Mem:/{printf "used=%s free=%s avail=%s", $3, $4, $7} /^Swap:/{printf " swap_used=%s", $3}')
last=$(cat "$STATE" 2>/dev/null || echo "-")
echo "$(date '+%Y-%m-%d %H:%M:%S') ${mem} load=$(cut -d' ' -f1 /proc/loadavg) | last_round: ${last}" >> "$LOG"
