#!/usr/bin/env bash
# 在操作者電腦（repo 根目錄）打包 moa_opendata VM 部署檔；只印 scp/ssh 指令，不執行任何遠端動作。
#
#   bash external/moa_opendata_vm/deploy.sh [輸出目錄，預設 /tmp]
#
# 打包的是「工作樹」的檔案（不是 git HEAD），請在乾淨、已合併的 checkout 執行。
set -euo pipefail

REPO_ROOT=$(git rev-parse --show-toplevel)
OUT_DIR=${1:-/tmp}
HERE="$REPO_ROOT/external/moa_opendata_vm"

# 主 repo 程式：只帶這 7 個 collector 的 import closure。
# collectors/__init__.py、tasks/__init__.py 刻意不帶（重量級 registry；runner 另以空殼 package 載入）。
REPO_FILES=(
  config.py
  collectors/base.py
  collectors/food_prices.py
  collectors/animal_adoption.py
  collectors/animal_shelter_outcomes.py
  collectors/animal_welfare_points.py
  storage/__init__.py
  storage/local.py
  storage/s3.py
  storage/db.py
  storage/supabase_writer.py
  storage/supabase_tables.py
  storage/marine_write.py
  utils/__init__.py
  utils/notify.py
  utils/auth.py
  utils/tdx_session.py
  utils/rate_limiter.py
  tasks/archive.py
  tasks/mini_taipei_publish.py
)
VM_FILES=(moa_opendata_run.py setup_vm.sh setup_cron.sh .env.example README.md)

STAGE=$(mktemp -d)
trap 'rm -rf "$STAGE"' EXIT
APP="$STAGE/moa-opendata"
mkdir -p "$APP"
for f in "${REPO_FILES[@]}"; do
  mkdir -p "$APP/$(dirname "$f")"
  cp "$REPO_ROOT/$f" "$APP/$f"
done
for f in "${VM_FILES[@]}"; do
  cp "$HERE/$f" "$APP/$f"
done
REV=$(git -C "$REPO_ROOT" rev-parse --short HEAD)
DIRTY=$(git -C "$REPO_ROOT" status --porcelain -- "${REPO_FILES[@]}" external/moa_opendata_vm | wc -l | tr -d ' ')
echo "$REV dirty_files=$DIRTY built=$(date -u +%FT%TZ)" > "$APP/REVISION"

TARBALL="$OUT_DIR/moa-opendata-$REV.tar.gz"
tar -czf "$TARBALL" -C "$STAGE" moa-opendata
echo "✓ 打包完成：${TARBALL} ($(du -h "${TARBALL}" | cut -f1), git ${REV}, 未提交改動 ${DIRTY} 檔)"
cat <<MSG

下一步（手動執行；<帳號>@<VM_HOST> 見私人筆記）：
  scp "$TARBALL" <帳號>@<VM_HOST>:/tmp/moa-opendata.tar.gz
  ssh <帳號>@<VM_HOST>
  sudo tar -xzf /tmp/moa-opendata.tar.gz -C /tmp && sudo bash /tmp/moa-opendata/setup_vm.sh
  # 首次：sudo -e /opt/moa-opendata/.env（見 README「.env」）→ README「驗證」段的 sudo dry-check／手動跑 → 才裝 cron
  sudo bash /opt/moa-opendata/setup_cron.sh
MSG
