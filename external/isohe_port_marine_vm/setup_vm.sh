#!/usr/bin/env bash
set -euo pipefail

APP_DIR=/opt/isohe-port-marine
DATA_DIR=/var/lib/isohe-port-marine/data
LOG_DIR=/var/log/isohe-port-marine

apt-get update -qq
DEBIAN_FRONTEND=noninteractive apt-get install -y -qq python3 python3-pip libpq5 tzdata cron
timedatectl set-timezone Asia/Taipei || true
mkdir -p "$APP_DIR" "$DATA_DIR" "$LOG_DIR"
PIP_FLAGS=""
if pip3 install --help 2>/dev/null | grep -q break-system-packages; then
  PIP_FLAGS="--break-system-packages"
fi
pip3 install $PIP_FLAGS --quiet 'requests>=2.28' 'psycopg2-binary>=2.9' 'python-dotenv>=1.0' 'boto3>=1.28'

install -m 755 /tmp/isohe_port_marine_collect.py "$APP_DIR/isohe_port_marine_collect.py"
install -m 644 /tmp/marine_observation.py "$APP_DIR/marine_observation.py"
install -m 644 /tmp/vm_buffer.py "$APP_DIR/vm_buffer.py"
install -m 755 /tmp/archive_isohe_port_marine.py "$APP_DIR/archive_isohe_port_marine.py"
install -m 755 /tmp/test_egress.py "$APP_DIR/test_egress.py"

if [ ! -f "$APP_DIR/.env" ]; then
  install -m 600 /tmp/.env.example "$APP_DIR/.env"
fi

echo "Installed ISOHE runner + vm_buffer in $APP_DIR. Fill .env, run test_egress.py, then one manual collect/archive smoke before installing cron."
