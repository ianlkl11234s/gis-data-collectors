#!/usr/bin/env bash
# Deliver only meaningful GIS collector incident transitions. Empty stdout is silent in no-agent cron mode.
set -euo pipefail

snapshot="$(/opt/data/scripts/gis_collectors_monitor_snapshot.sh)"
export GIS_MONITOR_SNAPSHOT="$snapshot"

uv run python - <<'PY'
import json
import os

payload = json.loads(os.environ["GIS_MONITOR_SNAPSHOT"])
events = payload.get("channel_events", [])
if not events:
    raise SystemExit(0)

labels = {"new": "新事件", "worsened": "惡化", "recovered": "恢復"}
icons = {"critical": "🔴", "watch": "🟡", "ok": "🟢"}
lines = []
for event in events:
    candidate = event.get("candidate", {})
    level = event.get("level", "watch")
    subject = candidate.get("target") or candidate.get("collector") or candidate.get("host") or candidate.get("kind", "監控")
    lines.append(f"{icons.get(level, '🟡')} {labels.get(event.get('event'), '狀態變化')}｜{subject}")
    if event.get("event") != "recovered":
        lines.append("請查看下一輪完整巡檢；本階段不會自動 restart、redeploy 或修改資料。")
print("\n".join(lines))
PY
