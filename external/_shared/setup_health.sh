#!/usr/bin/env bash
# VM 上一鍵安裝 health_report.py（部署到 /opt/external-health/）+ cron
set -euo pipefail

APP_DIR=/opt/external-health
LOG_DIR=/var/log/external-health

echo "[1/3] 建目錄"
mkdir -p "$APP_DIR" "$LOG_DIR"

echo "[2/3] 安裝程式 + 依賴（boto3 / python-dotenv / PyYAML — 已隨 ship_ais 部署）"
install -m 755 /tmp/health_report.py "$APP_DIR/health_report.py"
# 補裝 PyYAML（健康監測選用）
pip3 install --quiet PyYAML>=6.0 2>/dev/null || true

echo "[3/3] 安裝 cron（每天 07:00 推 health snapshot 到 S3）"
TMP=$(mktemp)
crontab -l 2>/dev/null > "$TMP" || true
sed -i '/# ─── external-health cron BEGIN ───/,/# ─── external-health cron END ───/d' "$TMP"
cat >>"$TMP" <<'EOF'
# ─── external-health cron BEGIN ───
# 每天 07:00 推 VM health snapshot 到 S3 給 Zeabur daily_report 撈
0 7 * * * /usr/bin/python3 /opt/external-health/health_report.py >> /var/log/external-health/health.log 2>&1
# ─── external-health cron END ───
EOF
crontab "$TMP"
rm -f "$TMP"

echo ""
echo "✓ external-health 安裝完成"
echo "  - app dir : $APP_DIR"
echo "  - runner  : $APP_DIR/health_report.py"
echo "  - log     : $LOG_DIR/health.log"
echo ""
echo "目前 crontab："
crontab -l
echo ""
echo "下一步："
echo "  - 手動跑一次驗證：python3 $APP_DIR/health_report.py"
echo "  - 隔天去 S3 看：aws s3 ls s3://migu-gis-data-collector/_external_vm_health/"
