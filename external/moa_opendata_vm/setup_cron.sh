#!/usr/bin/env bash
# 安裝 moa_opendata cron（idempotent：只替換自己的 BEGIN/END 區塊）。
set -euo pipefail

mkdir -p /var/log/moa-opendata
tmp_file=$(mktemp)
trap 'rm -f "$tmp_file"' EXIT
crontab -l 2>/dev/null >"$tmp_file" || true
sed -i '/# ─── moa_opendata cron BEGIN ───/,/# ─── moa_opendata cron END ───/d' "$tmp_file"
cat >>"$tmp_file" <<'CRON'
# ─── moa_opendata cron BEGIN ───
SHELL=/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
# 每日 00:53 一次 --all-due：依序跑到期的 collector（每日 2 個；月／半年／年的依本地 state 判斷）。
# :53 與 :21 是奇數且非 x8 分，避開 waste */2、ship */10、isohe :08/:18/…、immigration :07、03:00–03:15 歸檔。
53 0 * * * cd /opt/moa-opendata && set -a && . ./.env && set +a && /usr/bin/timeout -k 60 3300 /usr/bin/python3 ./moa_opendata_run.py --all-due >>/var/log/moa-opendata/collect.log 2>&1
21 3 * * * cd /opt/moa-opendata && set -a && . ./.env && set +a && /usr/bin/timeout -k 60 1500 /usr/bin/python3 ./moa_opendata_run.py --archive >>/var/log/moa-opendata/archive.log 2>&1
# ─── moa_opendata cron END ───
CRON
crontab "$tmp_file"
echo "✓ moa_opendata cron 已安裝"
crontab -l | sed -n '/moa_opendata cron BEGIN/,/moa_opendata cron END/p'
