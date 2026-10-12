#!/usr/bin/env bash
set -euo pipefail

APP_DIR=/opt/isohe-port-marine
LOG_DIR=/var/log/isohe-port-marine
mkdir -p "$LOG_DIR"

tmp_file=$(mktemp)
trap 'rm -f "$tmp_file"' EXIT
crontab -l 2>/dev/null >"$tmp_file" || true
sed -i '/# ─── isohe_port_marine cron BEGIN ───/,/# ─── isohe_port_marine cron END ───/d' "$tmp_file"
cat >>"$tmp_file" <<'CRON'
# ─── isohe_port_marine cron BEGIN ───
SHELL=/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
# :08/:18/…/:58 avoids DB cron peaks (:14/:15 trails/temperature, :40 marine hourly) and CWA :07/:22/:37/:52.
# The runner self-terminates after ISOHE_RUN_DEADLINE_SEC (480s) and refuses to stack runs; timeout is a belt.
8-59/10 * * * * cd /opt/isohe-port-marine && set -a && . ./.env && set +a && /usr/bin/timeout -k 30 540 /usr/bin/python3 ./isohe_port_marine_collect.py >>/var/log/isohe-port-marine/collect.log 2>&1
10 3 * * * cd /opt/isohe-port-marine && set -a && . ./.env && set +a && /usr/bin/python3 ./archive_isohe_port_marine.py >>/var/log/isohe-port-marine/archive.log 2>&1
# ─── isohe_port_marine cron END ───
CRON
crontab "$tmp_file"
echo "ISOHE collect/archive cron installed"
