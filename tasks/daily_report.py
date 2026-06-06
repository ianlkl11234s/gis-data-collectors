"""
每日報告任務

每天早上發送 Telegram 訊息，彙整昨日資料收集狀態、檔案統計、歸檔結果與系統資訊。
同時執行靜默檢測與磁碟空間檢查。
"""

from datetime import datetime, timedelta, timezone
from pathlib import Path

import config
from utils.notify import (
    send_telegram,
    notify_disk_alert,
    notify_silence_alert,
)

# 與 collectors/base.py 保持一致的 Taipei 時區
# 避免和 tz-aware 的 last_run 比較時爆出 offset-naive/aware 錯誤
TAIPEI_TZ = timezone(timedelta(hours=8))


class DailyReportTask:
    """每日報告任務"""

    def __init__(self, collectors: list, archive_task=None):
        """
        Args:
            collectors: 所有啟用的收集器實例
            archive_task: 歸檔任務實例（可選）
        """
        self.collectors = collectors
        self.archive_task = archive_task
        self.last_archive_result = None  # 由外部設定最近一次歸檔結果
        self._start_time = datetime.now()

    def run(self):
        """產生並發送每日報告"""
        print(f"\n{'=' * 60}")
        print(f"📊 產生每日報告 ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
        print(f"   collectors: {len(self.collectors)} 個")
        print(f"   archive_task: {'有' if self.archive_task else '無'}")
        print(f"   last_archive_result: {'有' if self.last_archive_result else '無'}")
        print(f"{'=' * 60}")

        # 組報告 — 每個區塊都有獨立防護，不應該整個掛掉
        try:
            report = self._build_report()
        except Exception as e:
            import traceback
            tb = traceback.format_exc()
            print(f"❌ _build_report 失敗: {e}\n{tb}")
            # 用純文字回報失敗，不帶 parse_mode 避免 Markdown 再出錯
            send_telegram(
                f"🚨 每日報告產生失敗\n\n{type(e).__name__}: {e}\n\n{tb[-500:]}",
                parse_mode=None,
            )
            report = None

        if report:
            print(f"   報告長度: {len(report)} 字元")
            ok = send_telegram(report)
            if ok:
                print("✓ 每日報告已發送到 Telegram")
            else:
                print("❌ Telegram Markdown 發送失敗，嘗試純文字...")
                # Markdown 失敗時，主動用純文字再試一次
                ok2 = send_telegram(report, parse_mode=None)
                if ok2:
                    print("✓ 每日報告已以純文字發送")
                else:
                    print("❌ 每日報告純文字發送也失敗")

        # 健康檢查不能被日報失敗擋住
        try:
            self._check_silence()
        except Exception as e:
            print(f"⚠️ _check_silence 失敗: {e}")
        try:
            self._check_disk_usage()
        except Exception as e:
            print(f"⚠️ _check_disk_usage 失敗: {e}")

    def _build_report(self) -> str:
        """組裝報告訊息（每個區塊獨立 try/except，避免一個掛全掛）"""
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        tag = f" [{config.INSTANCE_NAME}]" if config.INSTANCE_NAME else ""
        lines = [f"📊 *資料收集日報{tag} — {yesterday}*\n"]

        sections = [
            ("收集狀態", self._section_collector_status),
            ("Supabase realtime 寫入", self._section_supabase_realtime),
            ("S3 archives 心跳", self._section_s3_archives),
            ("跨層一致性", self._section_cross_layer),
            ("HiCloud VM 健康", self._section_external_vm_health),
            ("異常 7 天趨勢", self._section_anomaly_trend),
            ("檔案統計", self._section_file_stats),
            ("S3 統計", self._section_s3_stats),
        ]

        # 歸檔結果（有設定才加）
        if self.archive_task:
            sections.append(("歸檔結果", self._section_archive))

        sections.append(("系統資訊", self._section_system_info))

        for name, fn in sections:
            try:
                lines.append(fn())
            except Exception as e:
                print(f"⚠️ 日報區塊 [{name}] 失敗: {e}")
                lines.append(f"\n⚠️ *{name}*\n  產生失敗: {e}")

        return '\n'.join(lines)

    def _section_collector_status(self) -> str:
        """收集器狀態區塊"""
        normal = []
        has_errors = []
        silent = []

        # 使用 tz-aware now，因 collector.last_run 是 tz-aware（Asia/Taipei）
        now = datetime.now(TAIPEI_TZ)

        for c in self.collectors:
            status = c.get_status()
            name = status['name']
            run_count = status['run_count']
            error_count = status['error_count']
            last_run = status['last_run']

            # 檢查是否靜默（超過預期間隔 2 倍）
            is_silent = False
            if last_run:
                last_dt = datetime.fromisoformat(last_run)
                silence_threshold = timedelta(minutes=c.interval_minutes * 2)
                if now - last_dt > silence_threshold:
                    is_silent = True

            if is_silent:
                last_str = datetime.fromisoformat(last_run).strftime('%m-%d %H:%M')
                silent.append(f"  `{name}`: 最後執行 {last_str}")
            elif error_count > 0:
                rate = (error_count / run_count * 100) if run_count > 0 else 0
                has_errors.append(f"  `{name}`: {run_count}次, {error_count}次錯誤 ({rate:.1f}%)")
            else:
                normal.append(f"`{name}` {run_count}次")

        total = len(self.collectors)
        parts = []

        if normal:
            parts.append(f"✅ *正常運作* ({len(normal)}/{total})")
            # 正常的用一行顯示，節省空間
            parts.append(f"  {' | '.join(normal)}")

        if has_errors:
            parts.append(f"\n⚠️ *有錯誤* ({len(has_errors)}/{total})")
            parts.extend(has_errors)

        if silent:
            parts.append(f"\n❌ *疑似停止* ({len(silent)}/{total})")
            parts.extend(silent)

        return '\n'.join(parts)

    def _section_supabase_realtime(self) -> str:
        """Supabase realtime 表寫入新鮮度（一次 RPC 撈全表）"""
        from tasks import monitoring

        tables = monitoring.load_realtime_tables()
        if not tables:
            return "\n📦 *Supabase realtime*\n  config/realtime_tables.yaml 為空"

        results = monitoring.query_realtime_health(tables)
        if not results:
            return "\n📦 *Supabase realtime*\n  RPC 撈不到資料（檢查 migration 149 是否套用）"

        # join 回 expected_interval
        meta = {(t["schema"], t["table"]): t for t in tables}
        now = datetime.now(TAIPEI_TZ)
        buckets = {"DEAD": [], "STALE": [], "NEVER": [], "OK_CRITICAL": [], "ERR": []}
        ok_count = 0
        for r in results:
            t = meta.get((r["schema"], r["table"]), {})
            interval = t.get("expected_interval_min", 60)
            critical = t.get("critical", False)
            if r.get("error"):
                buckets["ERR"].append((r, t))
                continue
            status, age_min = monitoring.classify_freshness(r["max_time"], interval, now)
            if status == "OK":
                ok_count += 1
                if critical:
                    buckets["OK_CRITICAL"].append((r, t, age_min))
            elif status == "NEVER":
                buckets["NEVER"].append((r, t))
            else:
                buckets[status].append((r, t, age_min))

        parts = [f"\n📦 *Supabase realtime* ({len(results)} 表 / {ok_count} OK)"]

        def _fmt(r, t, age_min=None):
            tag = "⚠️" if t.get("critical") else ""
            label = f"`{r['schema']}.{r['table']}`{tag}"
            if age_min is None:
                return label
            return f"{label} 落後 {age_min} 分"

        if buckets["DEAD"]:
            parts.append("  🔴 DEAD（>12x interval）")
            for r, t, age in buckets["DEAD"][:8]:
                parts.append(f"    {_fmt(r, t, age)}")
        if buckets["STALE"]:
            parts.append("  🟡 STALE（>3x interval）")
            for r, t, age in buckets["STALE"][:8]:
                parts.append(f"    {_fmt(r, t, age)}")
        if buckets["NEVER"]:
            parts.append("  ⚪ NEVER（無資料）")
            for r, t in buckets["NEVER"][:8]:
                parts.append(f"    {_fmt(r, t)}")
        if buckets["ERR"]:
            parts.append("  ❌ ERR")
            for r, t in buckets["ERR"][:5]:
                parts.append(f"    {_fmt(r, t)}: {r['error'][:60]}")

        if not any(buckets[k] for k in ("DEAD", "STALE", "NEVER", "ERR")):
            parts.append("  ✅ 全部 OK")
        return "\n".join(parts)

    def _section_s3_archives(self) -> str:
        """每 collector S3 archives 心跳 — 解 flight_fr24 silent fail 32 天那種"""
        from tasks import monitoring

        cmap = monitoring.load_cross_layer_map()
        if not cmap:
            return "\n📦 *S3 archives*\n  cross_layer_map.yaml 為空"

        latest_dates = monitoring.list_archive_dates_per_collector()
        today = datetime.now(TAIPEI_TZ).strftime("%Y-%m-%d")
        yesterday = (datetime.now(TAIPEI_TZ) - timedelta(days=1)).strftime("%Y-%m-%d")

        stale = []   # 應有但落後
        ok = []
        missing = [] # 應有但完全沒有
        ignored = 0  # disabled / expected_daily=false

        for name, cfg in cmap.items():
            if not cfg.get("enabled"):
                ignored += 1
                continue
            for s3p in cfg.get("s3_prefixes", []):
                if not s3p.get("expected_daily"):
                    ignored += 1
                    continue
                last = latest_dates.get(name)
                if last is None:
                    missing.append((name, cfg))
                elif last < yesterday:
                    days_behind = (datetime.strptime(today, "%Y-%m-%d") - datetime.strptime(last, "%Y-%m-%d")).days
                    stale.append((name, cfg, last, days_behind))
                else:
                    ok.append(name)
                break  # 一個 collector 只看第一個 expected_daily prefix

        parts = [f"\n☁️ *S3 archives* ({len(ok)} OK / {len(stale) + len(missing)} 異常)"]
        if missing:
            parts.append("  ⚪ 從未歸檔")
            for name, cfg in missing[:8]:
                tag = " ⚠️" if cfg.get("critical") else ""
                parts.append(f"    `{name}`{tag}")
        if stale:
            parts.append("  🔴 落後")
            for name, cfg, last, days in sorted(stale, key=lambda x: -x[3])[:8]:
                tag = " ⚠️" if cfg.get("critical") else ""
                parts.append(f"    `{name}`{tag} 最新 {last}（落後 {days} 天）")
        if not stale and not missing:
            parts.append("  ✅ 全部 enabled+expected_daily 的 collector 都有昨日歸檔")
        return "\n".join(parts)

    def _section_cross_layer(self) -> str:
        """跨層一致性：collector heartbeat × Supabase × S3 三向交叉。

        對每個 enabled collector，三層都要動才算 OK：
          [A] collector last_run / last_success（in-memory，有則用）
          [B] Supabase 對應的 history table 有 24h 寫入
          [C] S3 archives 有昨天的 tar.gz（expected_daily=true 才檢查）
        """
        from tasks import monitoring

        cmap = monitoring.load_cross_layer_map()
        if not cmap:
            return "\n🔁 *跨層一致性*\n  cross_layer_map.yaml 為空"

        # 預先撈 SB / S3 兩層全量
        rt_tables = monitoring.load_realtime_tables()
        rt_results = monitoring.query_realtime_health(rt_tables) if rt_tables else []
        rt_24h_count = {(r["schema"], r["table"]): r.get("count_24h", 0) for r in rt_results}
        s3_dates = monitoring.list_archive_dates_per_collector()
        yesterday = (datetime.now(TAIPEI_TZ) - timedelta(days=1)).strftime("%Y-%m-%d")

        # 收集器 in-memory 狀態（last_success_at）
        collector_state: dict[str, datetime | None] = {}
        for c in self.collectors:
            collector_state[c.name] = getattr(c, "last_success_at", None)

        misaligned = []  # [(name, [A?, B?, C?, reason]), ...]
        ok_count = 0
        for name, cfg in cmap.items():
            if not cfg.get("enabled"):
                continue
            # [A] collector heartbeat — 寬鬆判斷（in-memory 失憶就略過 A，不算問題）
            a_ok = True
            if name in collector_state:
                last = collector_state[name]
                if last is not None:
                    age_min = (datetime.now(TAIPEI_TZ) - last).total_seconds() / 60
                    a_ok = age_min < cfg["expected_interval_min"] * 3
            # [B] SB 寫入
            sb_tables = cfg.get("supabase_tables") or []
            if sb_tables:
                b_ok = any(rt_24h_count.get(tuple(t.split(".", 1)), 0) > 0 for t in sb_tables)
            else:
                b_ok = True  # 沒設 SB 表就略過
            # [C] S3 archive 昨天
            has_daily_prefix = any(p.get("expected_daily") for p in cfg.get("s3_prefixes", []))
            if has_daily_prefix:
                latest = s3_dates.get(name)
                c_ok = latest is not None and latest >= yesterday
            else:
                c_ok = True
            if a_ok and b_ok and c_ok:
                ok_count += 1
            else:
                misaligned.append((name, cfg, a_ok, b_ok, c_ok))

        parts = [f"\n🔁 *跨層一致性* ({ok_count} OK / {len(misaligned)} 斷層)"]
        if not misaligned:
            parts.append("  ✅ 所有 enabled collector 三層都對齊")
            return "\n".join(parts)

        for name, cfg, a_ok, b_ok, c_ok in misaligned[:8]:
            tag = " ⚠️" if cfg.get("critical") else ""
            flags = "".join(["A" if a_ok else "a", "B" if b_ok else "b", "C" if c_ok else "c"])
            parts.append(f"  🔴 `{name}`{tag} [{flags}] {self._cross_layer_diagnosis(a_ok, b_ok, c_ok)}")
        return "\n".join(parts)

    @staticmethod
    def _cross_layer_diagnosis(a: bool, b: bool, c: bool) -> str:
        """用文字描述 (A,B,C) 斷在哪。大寫=OK 小寫=異常。"""
        if not a and not b and not c:
            return "三層全斷（collector 沒跑 → SB / S3 全停）"
        if not a and not b:
            return "collector + SB 雙斷（極可能完全沒跑）"
        if not c and a and b:
            return "SB / collector 動但 S3 archive 沒上（archive task silent fail）"
        if not b and a and c:
            return "S3 動但 SB 停寫（transform/connection 異常）"
        if not a and b:
            return "SB 有寫但 collector 失憶（可能剛重啟）"
        return "至少一層 missing"

    def _section_external_vm_health(self) -> str:
        """從 s3://.../_external_vm_health/<host>/YYYY-MM-DD.json 撈所有 VM 心跳。

        無 snapshot = 該 VM 失聯（snapshot age > 26h）。
        每 host 列：uptime / load / disk / 各 collector 24h 成功率 / outbound 健檢。
        """
        from tasks import monitoring

        snapshots = monitoring.list_vm_health_snapshots(max_age_hours=26)
        if not snapshots:
            return "\n🖥️ *HiCloud VM 健康*\n  尚未收到任何 VM snapshot（external/_shared/ 未部署？）"

        parts = ["\n🖥️ *HiCloud VM 健康*"]
        for entry in snapshots:
            host = entry["host"]
            if entry["is_lost"] or entry["snapshot"] is None:
                parts.append(f"  🔴 `{host}` 失聯（snapshot age {entry['age_hours']:.0f}h）")
                continue
            snap = entry["snapshot"]
            sys_ = snap.get("system", {})
            disk = sys_.get("disk_used_pct")
            load = sys_.get("load_avg_1m")
            up = sys_.get("uptime_days")
            egress = snap.get("egress_ip", "?")
            parts.append(
                f"  🟢 `{host}` ({egress}) up {up}d / load {load} / disk {disk}%"
            )

            for cname, c in (snap.get("collectors") or {}).items():
                if "error" in c:
                    parts.append(f"    ⚠️ `{cname}` log 讀取失敗: {c['error']}")
                    continue
                runs = c.get("runs_24h", 0)
                ok = c.get("success_24h", 0)
                last = c.get("last_success_at") or "?"
                last_short = last.split("T")[1][:5] if "T" in last else last
                last_count = c.get("last_count")
                snap_mb = c.get("snapshot_dir_mb")
                cnt_str = f" {last_count}" if last_count else ""
                rate = "✓" if runs and ok / runs >= 0.95 else "⚠️"
                parts.append(
                    f"    {rate} `{cname}` {ok}/{runs}  最後 {last_short}{cnt_str}  snapshot {snap_mb}MB"
                )

            ob = snap.get("outbound_health") or {}
            bad = [k for k, v in ob.items() if v.get("http_status") is None and v.get("tcp_ms") is None]
            if bad:
                parts.append(f"    🔴 outbound 異常: {', '.join(bad)}")
            elif ob:
                parts.append(f"    ✓ outbound {len(ob)}/{len(ob)} OK")
        return "\n".join(parts)

    def _section_anomaly_trend(self) -> str:
        """異常 rolling 7 天趨勢 + D1/D3/D7 去重提醒。

        本輪偵測到的異常：
          - SB DEAD/STALE/NEVER 的 table
          - S3 expected_daily 缺檔的 collector
          - 跨層斷層的 collector
        以 anomaly id 標記，存 data/anomaly_state.json，分新發生/持續/已修復。
        """
        from tasks import monitoring

        # 偵測本輪所有異常（與上面 sections 邏輯一致，重跑一次）
        current: set[str] = set()

        # SB
        rt_tables = monitoring.load_realtime_tables()
        rt_meta = {(t["schema"], t["table"]): t for t in rt_tables}
        for r in monitoring.query_realtime_health(rt_tables) if rt_tables else []:
            if r.get("error"):
                current.add(f"sb:{r['schema']}.{r['table']}:err")
                continue
            t = rt_meta.get((r["schema"], r["table"]), {})
            status, _ = monitoring.classify_freshness(r["max_time"], t.get("expected_interval_min", 60))
            if status in ("DEAD", "STALE", "NEVER"):
                current.add(f"sb:{r['schema']}.{r['table']}:{status.lower()}")

        # S3
        cmap = monitoring.load_cross_layer_map()
        s3_dates = monitoring.list_archive_dates_per_collector()
        yesterday = (datetime.now(TAIPEI_TZ) - timedelta(days=1)).strftime("%Y-%m-%d")
        for name, cfg in cmap.items():
            if not cfg.get("enabled"):
                continue
            for s3p in cfg.get("s3_prefixes", []):
                if not s3p.get("expected_daily"):
                    continue
                last = s3_dates.get(name)
                if last is None:
                    current.add(f"s3:{name}:missing")
                elif last < yesterday:
                    current.add(f"s3:{name}:stale")
                break

        # update state（檔案讀寫失敗時 silently 空 dict）
        new_ones, persistent, resolved = monitoring.update_anomaly_state(current)
        state = monitoring.load_anomaly_state()

        parts = [f"\n📈 *異常 7 天趨勢* (新 {len(new_ones)} / 持續 {len(persistent)} / 已修復 {len(resolved)})"]
        if new_ones:
            parts.append("  🆕 新發生")
            for aid in sorted(new_ones)[:8]:
                parts.append(f"    `{aid}`")
        if resolved:
            parts.append("  ✅ 已修復")
            for aid in sorted(resolved)[:6]:
                parts.append(f"    `{aid}`")
        # 持續中：依 D1/D3/D7 規則篩選提報
        notify_persistent = [aid for aid in persistent if monitoring.should_notify_persistent(aid, state)]
        if notify_persistent:
            parts.append("  ⏳ 持續中（D1/D3/D7 提醒）")
            for aid in sorted(notify_persistent)[:6]:
                entry = state.get(aid, {})
                first = entry.get("first_seen", "?")
                parts.append(f"    `{aid}` 自 {first}")
        elif persistent:
            silent_count = len(persistent) - len(notify_persistent)
            parts.append(f"  ⏳ 另有 {silent_count} 個持續異常（非 D1/D3/D7 靜音）")
        if not (new_ones or resolved or notify_persistent or persistent):
            parts.append("  ✅ 無異常")
        return "\n".join(parts)

    def _section_file_stats(self) -> str:
        """本地檔案統計區塊"""
        data_dir = config.LOCAL_DATA_DIR
        if not data_dir.exists():
            return "\n📁 *本地檔案*\n  資料目錄不存在"

        total_files = 0
        total_size = 0
        today_files = 0
        collector_stats = []
        today_str = datetime.now().strftime('%Y/%m/%d')

        for collector_dir in sorted(data_dir.iterdir()):
            if not collector_dir.is_dir():
                continue

            # 計算所有 JSON（排除 latest.json）
            files = [f for f in collector_dir.glob('**/*.json') if f.name != 'latest.json']
            size = sum(f.stat().st_size for f in files)
            count = len(files)

            # 計算今日檔案
            today_dir = collector_dir / today_str
            today_count = len(list(today_dir.glob('*.json'))) if today_dir.exists() else 0

            total_files += count
            total_size += size
            today_files += today_count

            if count > 0:
                collector_stats.append(f"`{collector_dir.name}` {count}")

        size_mb = total_size / (1024 * 1024)

        parts = [
            f"\n📁 *本地檔案*",
            f"  總計: *{total_files}* 個 ({size_mb:.1f} MB)",
            f"  今日新增: *{today_files}* 個",
        ]
        if collector_stats:
            parts.append(f"  {' | '.join(collector_stats)}")

        return '\n'.join(parts)

    def _section_s3_stats(self) -> str:
        """S3 儲存統計區塊"""
        if not config.S3_BUCKET:
            return "\n☁️ *S3 儲存*\n  未設定"

        try:
            from storage.s3 import S3Storage
            s3 = S3Storage()
            stats = s3.get_bucket_stats()
        except Exception as e:
            return f"\n☁️ *S3 儲存*\n  查詢失敗: {e}"

        total_objects = stats['total_objects']
        total_gb = stats['total_size_bytes'] / (1024 ** 3)

        # 費用估算：若有 by_storage_class 就分層估，否則 fallback 成 Standard 單一價
        price_table = getattr(config, 'S3_PRICE_BY_STORAGE_CLASS', None) or {}
        by_sc = stats.get('by_storage_class') or {}
        if by_sc and price_table:
            estimated_cost = sum(
                (info['size_bytes'] / (1024 ** 3))
                * price_table.get(sc, config.S3_PRICE_PER_GB)
                for sc, info in by_sc.items()
            )
        else:
            estimated_cost = total_gb * config.S3_PRICE_PER_GB

        parts = [
            f"\n☁️ *S3 儲存* ({config.S3_BUCKET})",
            f"  總計: *{total_objects}* 個物件 ({total_gb:.2f} GB)",
            f"  估算月費: *${estimated_cost:.2f}* USD（含 Lifecycle 分層折扣）",
        ]

        # 若有 lifecycle 分層，顯示各 class 的佔比
        if by_sc and len(by_sc) > 1:
            sc_items = sorted(by_sc.items(), key=lambda x: x[1]['size_bytes'], reverse=True)
            tiers = []
            for sc, info in sc_items:
                sc_gb = info['size_bytes'] / (1024 ** 3)
                if sc_gb < 0.01:
                    continue
                tiers.append(f"{sc} {sc_gb:.2f}GB")
            if tiers:
                parts.append(f"  分層: {' | '.join(tiers)}")

        # 按收集器顯示（只顯示前幾大的）
        by_collector = stats['by_collector']
        if by_collector:
            sorted_collectors = sorted(
                by_collector.items(),
                key=lambda x: x[1]['size_bytes'],
                reverse=True
            )
            top_items = []
            for name, info in sorted_collectors[:5]:
                size_mb = info['size_bytes'] / (1024 ** 2)
                top_items.append(f"`{name}` {size_mb:.0f}MB")
            parts.append(f"  {' | '.join(top_items)}")

        return '\n'.join(parts)

    def _section_archive(self) -> str:
        """歸檔結果區塊"""
        parts = [f"\n📦 *歸檔結果* ({config.ARCHIVE_TIME})"]

        if self.last_archive_result:
            archive = self.last_archive_result.get('archive', {})
            cleanup = self.last_archive_result.get('cleanup', {})
            uploaded = archive.get('uploaded', 0)
            skipped = archive.get('skipped', 0)
            failed = archive.get('failed', 0)
            deleted = cleanup.get('deleted', 0)

            parts.append(f"  上傳: {uploaded} 個 | 跳過: {skipped} 個 | 失敗: {failed} 個")
            parts.append(f"  清理: 刪除 {deleted} 個本地目錄")
        else:
            parts.append(f"  昨日無歸檔記錄")

        return '\n'.join(parts)

    def _section_system_info(self) -> str:
        """系統資訊區塊"""
        uptime = datetime.now() - self._start_time
        days = uptime.days
        hours = uptime.seconds // 3600

        # 磁碟使用
        data_dir = config.LOCAL_DATA_DIR
        used_mb = 0
        if data_dir.exists():
            used_mb = sum(
                f.stat().st_size for f in data_dir.glob('**/*') if f.is_file()
            ) / (1024 * 1024)

        parts = [
            f"\n⚙️ *系統資訊*",
            f"  運行時間: {days}天{hours}小時",
            f"  本地磁碟: {used_mb:.0f} MB",
        ]

        return '\n'.join(parts)

    def _check_silence(self):
        """檢查收集器是否靜默（即時告警）"""
        # tz-aware now，對齊 collector.last_run
        now = datetime.now(TAIPEI_TZ)

        for c in self.collectors:
            status = c.get_status()
            last_run = status['last_run']

            if not last_run:
                continue

            last_dt = datetime.fromisoformat(last_run)
            silence_threshold = timedelta(minutes=c.interval_minutes * 2)

            if now - last_dt > silence_threshold:
                last_str = last_dt.strftime('%m-%d %H:%M')
                notify_silence_alert(c.name, last_str, c.interval_minutes)

    def _check_disk_usage(self):
        """檢查磁碟使用量"""
        data_dir = config.LOCAL_DATA_DIR
        if not data_dir.exists():
            return

        used_bytes = sum(f.stat().st_size for f in data_dir.glob('**/*') if f.is_file())
        used_mb = used_bytes / (1024 * 1024)

        if used_mb > config.DISK_ALERT_THRESHOLD_MB:
            notify_disk_alert(used_mb, config.DISK_ALERT_THRESHOLD_MB)
