#!/usr/bin/env bash
# 安裝／更新 fire119 的 root crontab 區塊（每次都先備份現有 crontab）。
#   bash setup_cron.sh               收集＋歸檔＋第一天監測
#   bash setup_cron.sh --no-monitor  收集＋歸檔（關閉第一天監測）
#   bash setup_cron.sh --remove      整個區塊移除（回滾）
set -euo pipefail
MODE=${1:-}
tmp_file=$(mktemp)
trap 'rm -f "$tmp_file"' EXIT
crontab -l 2>/dev/null >"$tmp_file" || true
cp "$tmp_file" "/root/crontab.bak.$(date +%Y%m%d%H%M%S)"
sed -i '/# ─── fire119 cron BEGIN ───/,/# ─── fire119 cron END ───/d' "$tmp_file"
if [ "$MODE" != "--remove" ]; then
  {
    echo '# ─── fire119 cron BEGIN ───'
    echo 'SHELL=/bin/bash'
    echo 'PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin'
    echo '# 每分鐘觸發；run_fire119.sh 先 sleep 25 秒（避開 :00 秒起跑的垃圾車 */2、ship */10、isohe :x8），'
    echo '# 再 flock -n、systemd-run MemoryMax=200M、nice 10、ionice idle、timeout -k 10 90。'
    echo '* * * * * /opt/fire119/run_fire119.sh'
    echo '# 歸檔 03:33（避開 ship 03:00、waste 03:05、isohe 03:10、immigration 03:15、cdc 03:20、moa 03:21）'
    echo '33 3 * * * cd /opt/fire119 && /usr/bin/nice -n 10 /usr/bin/python3 ./archive_fire119.py >> /var/log/fire119/archive.log 2>&1'
    if [ "$MODE" != "--no-monitor" ]; then
      echo '# 第一天監測（一天後用 setup_cron.sh --no-monitor 關閉）'
      echo '* * * * * sleep 50; /opt/fire119/vmstat_monitor.sh'
    fi
    echo '# ─── fire119 cron END ───'
  } >>"$tmp_file"
fi
crontab "$tmp_file"
echo "fire119 cron 已更新（模式：${MODE:-預設}）；舊 crontab 備份在 /root/crontab.bak.*"
