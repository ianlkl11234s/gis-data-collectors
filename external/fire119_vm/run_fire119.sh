#!/bin/bash
# fire119 runner 的 cron 包裝（每分鐘一次）：秒偏移 → flock -n → 記憶體上限 → nice／ionice → timeout → python。
# 設定都可用環境變數覆寫（寫在 crontab 該行前面），預設值見下方。
set -u
APP_DIR=${FIRE119_APP_DIR:-/opt/fire119}
LOG=${FIRE119_LOG:-/var/log/fire119/collect.log}
LOCK=${FIRE119_LOCK:-/run/lock/fire119.lock}
OFFSET=${FIRE119_SECOND_OFFSET:-25}            # 秒偏移：避開 :00 秒起跑的垃圾車／ship／isohe
MEM_MODE=${FIRE119_MEM_MODE:-systemd}          # systemd（cgroup MemoryMax，預設）| ulimit（位址空間上限）| off
MEM_MAX_MB=${FIRE119_MEM_MAX_MB:-200}          # systemd 模式：實際記憶體上限，並禁用 swap
ULIMIT_VMEM_MB=${FIRE119_ULIMIT_VMEM_MB:-400}  # ulimit 模式：虛擬位址空間上限（比 RSS 大很多，見 README）

ts() { date '+%Y-%m-%d %H:%M:%S'; }
sleep "$OFFSET"
cd "$APP_DIR" || exit 1

cmd=(/usr/bin/nice -n 10 /usr/bin/ionice -c3 /usr/bin/timeout -k 10 90 /usr/bin/python3 "$APP_DIR/fire119_collect.py")
case "$MEM_MODE" in
  systemd)
    cmd=(/usr/bin/systemd-run --scope --quiet --collect -p "MemoryMax=${MEM_MAX_MB}M" -p MemorySwapMax=0 "${cmd[@]}") ;;
  ulimit)
    ulimit -v $((ULIMIT_VMEM_MB * 1024)) ;;
  off) ;;
  *) echo "$(ts) [ERROR] FIRE119_MEM_MODE=$MEM_MODE 不認得" >> "$LOG"; exit 2 ;;
esac

/usr/bin/flock -n -E 75 "$LOCK" "${cmd[@]}" >> "$LOG" 2>&1
rc=$?
case $rc in
  75) echo "$(ts) [WARNING] 上一輪還在跑（flock 被占用），跳過本輪" >> "$LOG" ;;
  124|137) echo "$(ts) [WARNING] 本輪超過 90 秒被 timeout 終止（rc=$rc），下一輪會計一次逾時" >> "$LOG" ;;
esac
exit $rc
