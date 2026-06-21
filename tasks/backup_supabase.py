"""
Supabase → S3 備份任務（三機器人架構）
All snapshot windows operate in Asia/Taipei timezone.

Robot A — run_static_snapshot:
    逐表計算指紋（row_count + max_updated_at），比對 metadata.backup_state，
    有變動才重新 COPY → gzip → S3 GLACIER_IR（overwrite-in-place）。

Robot B — run_realtime_snapshot:
    昨日即時表資料 dump；跳過 archive.py 已覆蓋的表。

Robot C — run_reconcile:
    每日盤點：比對 information_schema、backup_state、S3 物件，
    輸出 missing / orphan / new_table 告警，批次寫入 backup_audit_log。

使用方式：
    python3 -m tasks.backup_supabase
    DRY_RUN=true python3 -m tasks.backup_supabase
"""

from __future__ import annotations

import gzip
import io
import json
import os
import time
from datetime import date, datetime, time as dtime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import psycopg2
import psycopg2.extras
import yaml

import config

# ────────────────────────────────────────────────────────────────
# 常數
# ────────────────────────────────────────────────────────────────

TAIPEI_TZ = ZoneInfo("Asia/Taipei")

# S3 key 前綴
_S3_STATIC_PREFIX  = "supabase-snapshots/static"
_S3_REALTIME_PREFIX = "supabase-snapshots/realtime"

# 時間欄位候選（依優先順序嘗試）
_TIME_COL_CANDIDATES = ("observed_at", "created_at", "snapshot_at", "event_time", "collected_at")

# 系統 schema（information_schema 查詢時排除）
_SYSTEM_SCHEMAS = {"information_schema", "pg_catalog", "pg_toast", "pg_temp"}


# ────────────────────────────────────────────────────────────────
# 內部工具函式
# ────────────────────────────────────────────────────────────────

def _now_taipei() -> datetime:
    return datetime.now(TAIPEI_TZ)


def _human_size(n_bytes: int) -> str:
    """回傳人類可讀的 bytes 大小（如 '1.23 MB'）"""
    for unit in ("B", "KB", "MB", "GB"):
        if n_bytes < 1024:
            return f"{n_bytes:.1f} {unit}"
        n_bytes /= 1024
    return f"{n_bytes:.1f} TB"



# ────────────────────────────────────────────────────────────────
# BackupSupabaseTask
# ────────────────────────────────────────────────────────────────

class BackupSupabaseTask:
    """Supabase → S3 備份主控器（三機器人）"""

    def __init__(self):
        self.s3 = None
        self.db_conn = None
        self.manifest: dict = {}
        self.audit_buf: list[dict] = []  # 批次 INSERT 暫存區

        # 從 config 讀取備份相關設定（DRY_RUN 亦相容舊的 os.getenv("DRY_RUN")）
        self.dry_run: bool = (
            config.BACKUP_DRY_RUN
            or os.getenv("DRY_RUN", "false").lower() in ("true", "1", "yes")
        )
        self.static_storage_class: str = config.BACKUP_STATIC_STORAGE_CLASS
        self.realtime_storage_class: str = config.BACKUP_REALTIME_STORAGE_CLASS
        self.stmt_timeout_ms: int = config.BACKUP_STATEMENT_TIMEOUT_MS
        self.manifest_path = config.BACKUP_MANIFEST_PATH

        timeout_s = self.stmt_timeout_ms // 1000
        print(
            f"BACKUP cfg: dry_run={self.dry_run} "
            f"static={self.static_storage_class} "
            f"realtime={self.realtime_storage_class} "
            f"timeout={timeout_s}s"
        )

        self._init_s3()
        self._init_db()
        self._load_manifest()

    # ──────────────────────────────────────────────
    # 初始化
    # ──────────────────────────────────────────────

    def _init_s3(self) -> None:
        """初始化 S3Storage（upload_snapshot 由 storage/s3.py 提供）。"""
        if not config.S3_BUCKET:
            print("⚠️  S3_BUCKET 未設定，備份功能無法啟動")
            return
        try:
            from storage.s3 import S3Storage
            self.s3 = S3Storage()
            print(f"✓ S3 連接完成: {config.S3_BUCKET}")
        except Exception as exc:
            print(f"✗ S3Storage 初始化失敗: {exc}")
            self.s3 = None

    def _init_db(self) -> None:
        """建立 psycopg2 連線（autocommit，適合 DDL-free COPY 查詢）。"""
        db_url = config.SUPABASE_DB_URL
        if not db_url:
            print("⚠️  SUPABASE_DB_URL 未設定，DB 操作將跳過")
            return
        try:
            self.db_conn = psycopg2.connect(db_url, connect_timeout=30)
            self.db_conn.autocommit = True
            print(f"✓ Supabase DB 連接完成")
        except Exception as exc:
            print(f"✗ DB 連接失敗: {exc}")
            self.db_conn = None

    def _load_manifest(self) -> None:
        """載入 backup_manifest.yaml，解析所需欄位。"""
        path = self.manifest_path
        if not Path(path).exists():
            print(f"⚠️  manifest 不存在: {path}")
            self.manifest = {}
        else:
            with open(path, encoding="utf-8") as f:
                self.manifest = yaml.safe_load(f) or {}
        classification = self.manifest.get("classification", {})
        self._static_schemas  = set(classification.get("static_schemas", []))
        self._realtime_schemas = set(classification.get("realtime_schemas", []))
        self._exclude_schemas  = set(classification.get("exclude_schemas", [])) | _SYSTEM_SCHEMAS
        self._exclude_tables   = set(self.manifest.get("exclude", []))
        self._archive_covered  = set(self.manifest.get("archive_py_covered", []))
        self._overrides        = self.manifest.get("overrides", {})
        print(f"✓ manifest 載入: static={len(self._static_schemas)} schemas, "
              f"realtime={len(self._realtime_schemas)} schemas, "
              f"exclude={len(self._exclude_tables)} tables, "
              f"archive_covered={len(self._archive_covered)} tables")

    # ──────────────────────────────────────────────
    # DB helpers
    # ──────────────────────────────────────────────

    def _cursor(self):
        """回傳新 cursor，並設定 statement_timeout。"""
        if not self.db_conn:
            raise RuntimeError("DB 未連線")
        cur = self.db_conn.cursor()
        cur.execute(f"SET statement_timeout = {self.stmt_timeout_ms}")
        return cur

    def _list_schema_tables(self, schemas: set[str]) -> list[tuple[str, str]]:
        """列出指定 schema 的所有 user 表（**排除 partition 子表**）。

        改用 pg_class 而非 information_schema，因為後者無法區分 parent 與
        partition child（如 realtime.bus_positions 是 parent，bus_positions_20260619
        是 child）。Robot B 只應 dump parent，partition pruning 會自動處理該日 child。

        Returns:
            list of (schema_name, table_name)
        """
        if not self.db_conn or not schemas:
            return []
        schema_list = list(schemas)
        with self._cursor() as cur:
            cur.execute(
                """
                SELECT n.nspname, c.relname
                FROM   pg_class c
                JOIN   pg_namespace n ON c.relnamespace = n.oid
                WHERE  n.nspname = ANY(%s)
                  AND  c.relkind IN ('r', 'p')      -- regular + partitioned parent
                  AND  NOT c.relispartition         -- exclude partition children
                ORDER BY n.nspname, c.relname
                """,
                (schema_list,),
            )
            return cur.fetchall()

    def _column_exists(self, schema: str, table: str, column: str) -> bool:
        """確認某欄位是否存在於 information_schema。"""
        with self._cursor() as cur:
            cur.execute(
                """
                SELECT 1 FROM information_schema.columns
                WHERE  table_schema = %s
                  AND  table_name   = %s
                  AND  column_name  = %s
                LIMIT 1
                """,
                (schema, table, column),
            )
            return cur.fetchone() is not None

    def _find_time_column(self, schema: str, table: str) -> str | None:
        """依 _TIME_COL_CANDIDATES 順序找第一個存在的時間欄位。"""
        for col in _TIME_COL_CANDIDATES:
            if self._column_exists(schema, table, col):
                return col
        return None

    # ──────────────────────────────────────────────
    # Audit log helpers
    # ──────────────────────────────────────────────

    def _audit(
        self,
        run_kind: str,
        code: str,
        severity: str = "ok",
        schema_name: str | None = None,
        table_name: str | None = None,
        message: str | None = None,
        details: dict | None = None,
    ) -> None:
        """附加一筆 audit 記錄到 self.audit_buf（待批次寫入）。"""
        self.audit_buf.append({
            "run_at":      _now_taipei().isoformat(),
            "run_kind":    run_kind,
            "schema_name": schema_name,
            "table_name":  table_name,
            "severity":    severity,
            "code":        code,
            "message":     message,
            "details":     json.dumps(details, default=str) if details else None,
        })

    def _flush_audit(self) -> int:
        """批次 INSERT self.audit_buf 到 metadata.backup_audit_log，並清空暫存區。

        Returns:
            int: 實際寫入筆數
        """
        if not self.audit_buf or not self.db_conn:
            count = len(self.audit_buf)
            self.audit_buf.clear()
            return count

        rows = self.audit_buf[:]
        self.audit_buf.clear()

        if self.dry_run:
            print(f"   [DRY_RUN] 跳過 flush audit ({len(rows)} 筆)")
            return len(rows)

        try:
            with self.db_conn.cursor() as cur:
                psycopg2.extras.execute_batch(
                    cur,
                    """
                    INSERT INTO metadata.backup_audit_log
                        (run_at, run_kind, schema_name, table_name, severity, code, message, details)
                    VALUES
                        (%(run_at)s, %(run_kind)s, %(schema_name)s, %(table_name)s,
                         %(severity)s, %(code)s, %(message)s, %(details)s::jsonb)
                    """,
                    rows,
                    page_size=200,
                )
        except Exception as exc:
            print(f"   ⚠️  audit flush 失敗: {exc}")
            return 0

        return len(rows)

    # ──────────────────────────────────────────────
    # ROBOT A — Static Snapshot
    # ──────────────────────────────────────────────

    def run_static_snapshot(self) -> dict:
        """Robot A：逐張靜態表計算指紋，有變動才 dump 到 S3 GLACIER_IR。

        Returns:
            dict: {'uploaded': int, 'skipped': int, 'failed': int}
        """
        stats = {"uploaded": 0, "skipped": 0, "failed": 0}

        if not self.db_conn:
            print("⚠️  [Robot A] DB 未連線，跳過")
            return stats
        if not self.s3:
            print("⚠️  [Robot A] S3 未設定，跳過")
            return stats

        print("\n📸 [Robot A] Static Snapshot 開始")

        tables = self._list_schema_tables(self._static_schemas)
        print(f"   發現 {len(tables)} 張靜態表")

        for schema, table in tables:
            qualified = f"{schema}.{table}"

            # 排除清單
            if qualified in self._exclude_tables:
                continue

            t0 = time.monotonic()
            try:
                uploaded = self._process_static_table(schema, table)
                elapsed = time.monotonic() - t0
                if uploaded:
                    stats["uploaded"] += 1
                    print(f"   ✓ {qualified} [{elapsed:.1f}s]")
                else:
                    stats["skipped"] += 1
            except Exception as exc:
                stats["failed"] += 1
                elapsed = time.monotonic() - t0
                print(f"   ✗ {qualified} [{elapsed:.1f}s]: {exc}")
                self._audit(
                    run_kind="static_snapshot",
                    schema_name=schema,
                    table_name=table,
                    severity="warn",
                    code="upload_failed",
                    message=str(exc),
                )

        flushed = self._flush_audit()
        print(f"   audit flush: {flushed} 筆")
        return stats

    def _compute_fingerprint(self, schema: str, table: str) -> str:
        """計算單張靜態表的指紋字串。

        策略由 manifest overrides 決定：
        - row_count_only（如 foursquare_poi）：只算 count(*)
        - 預設（row_count_plus_max_updated_at）：count(*) + max(updated_at or created_at)
        """
        qualified = f"{schema}.{table}"
        override = self._overrides.get(qualified, {})
        strategy = override.get(
            "fingerprint_strategy",
            self.manifest.get("defaults", {}).get("static", {}).get(
                "fingerprint_strategy", "row_count_plus_max_updated_at"
            ),
        )

        with self._cursor() as cur:
            cur.execute(f'SELECT count(*) FROM "{schema}"."{table}"')
            row_count = cur.fetchone()[0]

        if strategy == "row_count_only":
            return f"rc:{row_count}"

        # row_count_plus_max_updated_at：先試 updated_at，再試 created_at
        max_ts = None
        for col in ("updated_at", "created_at"):
            if self._column_exists(schema, table, col):
                with self._cursor() as cur:
                    cur.execute(f'SELECT max("{col}") FROM "{schema}"."{table}"')
                    max_ts = cur.fetchone()[0]
                break

        return f"rc:{row_count}|mut:{max_ts}"

    def _get_backup_state(self, schema: str, table: str) -> dict | None:
        """讀 metadata.backup_state 的現有記錄（可能為 None）。"""
        with self._cursor() as cur:
            cur.execute(
                """
                SELECT last_fingerprint, last_s3_key, last_size_bytes
                FROM   metadata.backup_state
                WHERE  schema_name = %s AND table_name = %s
                """,
                (schema, table),
            )
            row = cur.fetchone()
        if row is None:
            return None
        return {"last_fingerprint": row[0], "last_s3_key": row[1], "last_size_bytes": row[2]}

    def _upsert_backup_state(
        self,
        schema: str,
        table: str,
        fingerprint: str,
        s3_key: str,
        size_bytes: int,
        storage_class: str,
    ) -> None:
        """UPSERT metadata.backup_state（autocommit 模式，無需額外 commit）。"""
        if self.dry_run:
            return
        with self.db_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO metadata.backup_state
                    (schema_name, table_name, last_backup_at, last_fingerprint,
                     last_s3_key, last_size_bytes, last_storage_class, backup_count)
                VALUES (%s, %s, now(), %s, %s, %s, %s, 1)
                ON CONFLICT (schema_name, table_name) DO UPDATE SET
                    last_backup_at    = EXCLUDED.last_backup_at,
                    last_fingerprint  = EXCLUDED.last_fingerprint,
                    last_s3_key       = EXCLUDED.last_s3_key,
                    last_size_bytes   = EXCLUDED.last_size_bytes,
                    last_storage_class = EXCLUDED.last_storage_class,
                    backup_count      = metadata.backup_state.backup_count + 1
                """,
                (schema, table, fingerprint, s3_key, size_bytes, storage_class),
            )

    def _process_static_table(self, schema: str, table: str) -> bool:
        """處理單張靜態表：計算指紋 → 比對 → 必要時 dump。

        Returns:
            bool: True 表示實際上傳，False 表示跳過（未變更）
        """
        qualified = f"{schema}.{table}"

        # 1. 計算指紋
        fingerprint = self._compute_fingerprint(schema, table)

        # 2. 讀舊指紋
        state = self._get_backup_state(schema, table)
        if state and state["last_fingerprint"] == fingerprint:
            self._audit(
                run_kind="static_snapshot",
                schema_name=schema,
                table_name=table,
                severity="ok",
                code="skipped_unchanged",
                message=f"fingerprint 未變: {fingerprint}",
            )
            return False  # 未變更，跳過

        # 3. 需要重新 dump
        s3_key = f"{_S3_STATIC_PREFIX}/{schema}.{table}.csv.gz"

        # 取得 per-table storage class（manifest override 優先，再 fallback 到 config）
        override = self._overrides.get(qualified, {})
        storage_class = override.get(
            "storage_class",
            self.manifest.get("defaults", {}).get("static", {}).get(
                "storage_class", self.static_storage_class
            ),
        )

        if self.dry_run:
            print(f"   [DRY_RUN] 跳過 COPY + upload: {qualified} → {s3_key}")
            self._audit(
                run_kind="static_snapshot",
                schema_name=schema,
                table_name=table,
                severity="ok",
                code="snapshot_uploaded",
                message=f"[DRY_RUN] fingerprint={fingerprint}",
            )
            return True

        # 4. COPY → gzip → bytes
        gz_bytes = self._copy_table_to_gzip(schema, table)

        # 5. 上傳 S3
        ok = self.s3.upload_snapshot(s3_key, gz_bytes, storage_class=storage_class)
        if not ok:
            raise RuntimeError(f"S3 upload_snapshot 回傳 False")

        size_bytes = len(gz_bytes)

        # 6. UPSERT backup_state
        self._upsert_backup_state(schema, table, fingerprint, s3_key, size_bytes, storage_class)

        # 7. Audit log
        self._audit(
            run_kind="static_snapshot",
            schema_name=schema,
            table_name=table,
            severity="ok",
            code="snapshot_uploaded",
            message=f"{_human_size(size_bytes)} → s3:{s3_key}",
            details={
                "fingerprint": fingerprint,
                "size_bytes": size_bytes,
                "storage_class": storage_class,
                "s3_key": s3_key,
            },
        )
        return True

    def _copy_table_to_gzip(self, schema: str, table: str) -> bytes:
        """用 psycopg2 copy_expert 將整張表 COPY 成 gzip CSV bytes。

        Raises:
            Exception: COPY 或壓縮失敗時往上拋出
        """
        raw_buf = io.BytesIO()
        sql = f'COPY (SELECT * FROM "{schema}"."{table}") TO STDOUT WITH CSV HEADER'
        with self._cursor() as cur:
            cur.copy_expert(sql, raw_buf)
        raw_buf.seek(0)

        gz_buf = io.BytesIO()
        with gzip.GzipFile(fileobj=gz_buf, mode="wb") as gz:
            gz.write(raw_buf.read())
        return gz_buf.getvalue()

    # ──────────────────────────────────────────────
    # ROBOT B — Realtime Daily Snapshot
    # ──────────────────────────────────────────────

    def run_realtime_snapshot(self, target_date: date | None = None) -> dict:
        """Robot B：昨日即時表 partition dump 到 S3。

        Args:
            target_date: 要備份的日期（預設昨天）

        Returns:
            dict: {'uploaded': int, 'skipped': int, 'failed': int, 'empty': int}
        """
        stats = {"uploaded": 0, "skipped": 0, "failed": 0, "empty": 0}

        if not self.db_conn:
            print("⚠️  [Robot B] DB 未連線，跳過")
            return stats
        if not self.s3:
            print("⚠️  [Robot B] S3 未設定，跳過")
            return stats

        if target_date is None:
            target_date = (datetime.now(TAIPEI_TZ) - timedelta(days=1)).date()

        print(f"\n🕐 [Robot B] Realtime Snapshot 開始 (target_date={target_date})")

        tables = self._list_schema_tables(self._realtime_schemas)
        print(f"   發現 {len(tables)} 張 realtime 表")

        for schema, table in tables:
            qualified = f"{schema}.{table}"

            # 排除清單
            if qualified in self._exclude_tables:
                stats["skipped"] += 1
                continue

            # 跳過 archive.py 已覆蓋的表
            if qualified in self._archive_covered:
                stats["skipped"] += 1
                continue

            t0 = time.monotonic()
            try:
                result = self._process_realtime_table(schema, table, target_date)
                elapsed = time.monotonic() - t0
                if result == "uploaded":
                    stats["uploaded"] += 1
                    print(f"   ✓ {qualified} [{elapsed:.1f}s]")
                elif result == "empty":
                    stats["empty"] += 1
                else:
                    stats["skipped"] += 1
            except Exception as exc:
                stats["failed"] += 1
                elapsed = time.monotonic() - t0
                print(f"   ✗ {qualified} [{elapsed:.1f}s]: {exc}")
                self._audit(
                    run_kind="realtime_snapshot",
                    schema_name=schema,
                    table_name=table,
                    severity="warn",
                    code="upload_failed",
                    message=str(exc),
                    details={"target_date": str(target_date)},
                )

        flushed = self._flush_audit()
        print(f"   audit flush: {flushed} 筆")
        return stats

    def _process_realtime_table(self, schema: str, table: str, target_date: date) -> str:
        """處理單張即時表的昨日 partition dump。

        Returns:
            'uploaded' | 'empty' | 'skipped'
        """
        qualified = f"{schema}.{table}"
        date_str = target_date.strftime("%Y-%m-%d")

        # 1. 找時間欄位
        time_col = self._find_time_column(schema, table)
        if time_col is None:
            self._audit(
                run_kind="realtime_snapshot",
                schema_name=schema,
                table_name=table,
                severity="warn",
                code="skipped_no_time_column",
                message="找不到時間欄位，跳過（請在 manifest 排除或補充時間欄位）",
            )
            return "skipped"

        # 2. 計算昨日時間範圍（Asia/Taipei 時區）
        day_start = datetime.combine(target_date, dtime.min, tzinfo=TAIPEI_TZ)
        day_end = day_start + timedelta(days=1)

        # 3. 先確認是否有資料（避免上傳空檔案）
        with self._cursor() as cur:
            cur.execute(
                f'SELECT count(*) FROM "{schema}"."{table}" '
                f'WHERE "{time_col}" >= %s AND "{time_col}" < %s',
                (day_start, day_end),
            )
            row_count = cur.fetchone()[0]

        if row_count == 0:
            self._audit(
                run_kind="realtime_snapshot",
                schema_name=schema,
                table_name=table,
                severity="info",
                code="skipped_empty_day",
                message=f"昨日 ({date_str}) 無資料，跳過上傳",
                details={"target_date": date_str, "time_col": time_col},
            )
            return "empty"

        # 4. COPY 昨日資料
        s3_key = f"{_S3_REALTIME_PREFIX}/{date_str}/{schema}.{table}.csv.gz"

        # per-table storage class override（manifest override 優先，再 fallback 到 config）
        override = self._overrides.get(qualified, {})
        storage_class = override.get(
            "storage_class",
            self.manifest.get("defaults", {}).get("realtime", {}).get(
                "storage_class", self.realtime_storage_class
            ),
        )

        if self.dry_run:
            print(f"   [DRY_RUN] 跳過 COPY + upload: {qualified} ({row_count} rows) → {s3_key}")
            self._audit(
                run_kind="realtime_snapshot",
                schema_name=schema,
                table_name=table,
                severity="ok",
                code="snapshot_uploaded",
                message=f"[DRY_RUN] {row_count} rows → {s3_key}",
            )
            return "uploaded"

        gz_bytes = self._copy_table_partition_to_gzip(schema, table, time_col, day_start, day_end)

        ok = self.s3.upload_snapshot(s3_key, gz_bytes, storage_class=storage_class)
        if not ok:
            raise RuntimeError("S3 upload_snapshot 回傳 False")

        size_bytes = len(gz_bytes)
        self._audit(
            run_kind="realtime_snapshot",
            schema_name=schema,
            table_name=table,
            severity="ok",
            code="snapshot_uploaded",
            message=f"{row_count} rows, {_human_size(size_bytes)} → s3:{s3_key}",
            details={
                "target_date": date_str,
                "time_col": time_col,
                "row_count": row_count,
                "size_bytes": size_bytes,
                "storage_class": storage_class,
                "s3_key": s3_key,
            },
        )
        return "uploaded"

    def _copy_table_partition_to_gzip(
        self,
        schema: str,
        table: str,
        time_col: str,
        day_start: datetime,
        day_end: datetime,
    ) -> bytes:
        """COPY 指定時間範圍的資料列到 gzip CSV bytes。"""
        sql_tmpl = (
            f'COPY (SELECT * FROM "{schema}"."{table}" '
            f'WHERE "{time_col}" >= %s '
            f'AND "{time_col}" < %s) '
            f'TO STDOUT WITH CSV HEADER'
        )
        raw_buf = io.BytesIO()
        with self._cursor() as cur:
            sql = cur.mogrify(sql_tmpl, (day_start, day_end)).decode()
            cur.copy_expert(sql, raw_buf)
        raw_buf.seek(0)

        gz_buf = io.BytesIO()
        with gzip.GzipFile(fileobj=gz_buf, mode="wb") as gz:
            gz.write(raw_buf.read())
        return gz_buf.getvalue()

    # ──────────────────────────────────────────────
    # ROBOT C — Daily Reconcile
    # ──────────────────────────────────────────────

    # ---- Archive freshness check (detects silent fail of archive.py per collector) ----
    # Default expected lag: archive.py uploads yesterday's data → S3 latest = today - 1
    # Exceptions: some collectors keep N days local before archiving (見 external/*/archive_*.py)
    _ARCHIVE_LAG_BY_COLLECTOR = {
        # 8-day retention (HiCloud VM 設計給 archive 自我修復空間)
        'ship_ais': 8,
        'waste_positions': 8,
        # Weekly collectors
        'cdc_public_health_weekly': 8,         # 週四跑
        'wra_drought_alert': 14,                # 上游不定期 + hash 去重
        # Stopped intentionally
        'flight_fr24': None,                    # 永久 None = 不檢查
    }
    _ARCHIVE_LAG_DEFAULT_DAYS = 2               # 大多 collector 預期 today-1，多給 1 天 grace

    def _check_archive_freshness(self) -> int:
        """檢查每個 collector S3 archive 最新日期是否 stale。

        對應上次 2026-06-15 silent fail 7 天才發現的問題。

        Returns:
            int: 觸發 critical 的 collector 數量
        """
        if not self.s3:
            return 0

        # 從 archive_py_covered 推 collector 名（取 schema.table 的「典型對應」）
        # 簡單方式：直接掃 S3 top-level，看哪些 prefix 有 archives/ 子目錄
        try:
            collectors = self._list_archive_collectors_from_s3()
        except Exception as exc:
            print(f"   ⚠️  archive freshness 掃描失敗: {exc}")
            return 0

        today = datetime.now(TAIPEI_TZ).date()
        critical_count = 0
        checked = 0
        ok_count = 0

        for collector in collectors:
            expected_lag = self._ARCHIVE_LAG_BY_COLLECTOR.get(collector, self._ARCHIVE_LAG_DEFAULT_DAYS)
            if expected_lag is None:
                continue  # 主動跳過（如 flight_fr24 已停跑）
            checked += 1

            try:
                dates = self.s3.list_dates(collector)
            except Exception as exc:
                print(f"   ⚠️  {collector}: list_dates 失敗 {exc}")
                continue

            if not dates:
                msg = f"collector {collector} archives/ 目錄為空"
                self._audit(
                    run_kind="reconcile",
                    severity="warn",
                    code="missing_s3",
                    message=msg,
                    details={"collector": collector},
                )
                continue

            latest_str = max(dates)
            try:
                latest_date = datetime.strptime(latest_str, "%Y-%m-%d").date()
            except ValueError:
                continue

            lag_days = (today - latest_date).days

            if lag_days > expected_lag:
                msg = (
                    f"archive.py silent fail? {collector} 最新 archive {latest_str} "
                    f"= 落後 {lag_days} 天 (預期 ≤ {expected_lag} 天)"
                )
                print(f"   🔴 {msg}")
                self._audit(
                    run_kind="reconcile",
                    severity="critical",
                    code="upload_failed",
                    message=msg,
                    details={
                        "collector": collector,
                        "latest_archive_date": latest_str,
                        "lag_days": lag_days,
                        "expected_lag_days": expected_lag,
                    },
                )
                critical_count += 1
            else:
                ok_count += 1

        print(f"   archive freshness: {checked} collector 檢查 / {ok_count} ✅ / {critical_count} 🔴")
        return critical_count

    def _list_archive_collectors_from_s3(self) -> list[str]:
        """掃 S3 top-level，回傳所有有 archives/ 子目錄的 collector 名稱"""
        if not self.s3 or not self.s3.s3:
            return []
        paginator = self.s3.s3.get_paginator('list_objects_v2')
        collectors: set[str] = set()
        for page in paginator.paginate(Bucket=self.s3.bucket, Delimiter='/'):
            for prefix in page.get('CommonPrefixes', []):
                name = prefix['Prefix'].rstrip('/')
                # 排除非 collector 目錄
                if name.startswith('_') or name in (
                    'supabase-snapshots', 'deploy-assets', 'flight-arc',
                    'mini-taipei', 'pulse-db', 'rail-data',
                ):
                    continue
                collectors.add(name)
        return sorted(collectors)

    def run_reconcile(self) -> dict:
        """Robot C：盤點所有 schema.table vs backup_state vs S3，輸出告警。

        Returns:
            dict: {'ok': int, 'info': int, 'warn': int, 'critical': int}
        """
        severity_counts: dict[str, int] = {"ok": 0, "info": 0, "warn": 0, "critical": 0}

        if not self.db_conn:
            print("⚠️  [Robot C] DB 未連線，跳過")
            return severity_counts

        print("\n🔍 [Robot C] Daily Reconcile 開始")

        yesterday = (datetime.now(TAIPEI_TZ) - timedelta(days=1)).date()
        date_str = yesterday.strftime("%Y-%m-%d")

        # 1. 取得 information_schema 所有表
        all_schemas = self._static_schemas | self._realtime_schemas
        all_tables_in_db: list[tuple[str, str]] = self._list_schema_tables(all_schemas)
        db_table_set = {f"{s}.{t}" for s, t in all_tables_in_db}

        # 2. 取得 backup_state 已知表
        state_table_set = self._get_all_backup_state_tables()

        # 3. 正向檢查：DB 有 → backup_state / S3 應該也有
        for schema, table in all_tables_in_db:
            qualified = f"{schema}.{table}"

            # 跳過不備份的表
            if qualified in self._exclude_tables:
                continue
            if schema in self._exclude_schemas:
                continue
            if qualified in self._archive_covered:
                continue  # archive.py 負責，不由本系統盤點

            is_static = schema in self._static_schemas
            is_realtime = schema in self._realtime_schemas

            if is_static:
                # 靜態表：backup_state 必須有記錄
                if qualified not in state_table_set:
                    msg = f"靜態表 {qualified} 無 backup_state 記錄（尚未備份）"
                    print(f"   ⚠️  {msg}")
                    self._audit(
                        run_kind="reconcile",
                        schema_name=schema,
                        table_name=table,
                        severity="warn",
                        code="missing_s3",
                        message=msg,
                    )
                    severity_counts["warn"] += 1
                else:
                    severity_counts["ok"] += 1

            elif is_realtime:
                # 即時表：S3 昨日 key 必須存在
                s3_key = f"{_S3_REALTIME_PREFIX}/{date_str}/{schema}.{table}.csv.gz"
                exists = self.s3.file_exists(s3_key) if self.s3 else False
                if not exists:
                    msg = f"即時表 {qualified} 昨日 S3 物件不存在: {s3_key}"
                    print(f"   ❌ {msg}")
                    self._audit(
                        run_kind="reconcile",
                        schema_name=schema,
                        table_name=table,
                        severity="critical",
                        code="missing_s3",
                        message=msg,
                        details={"expected_s3_key": s3_key, "date": date_str},
                    )
                    severity_counts["critical"] += 1
                else:
                    severity_counts["ok"] += 1

        # 4. 反向檢查 A：backup_state 有，但 DB 沒有（孤兒記錄）
        for qualified in state_table_set:
            if qualified not in db_table_set:
                schema, table = qualified.split(".", 1)
                msg = f"backup_state 記錄 {qualified} 在 DB 已不存在"
                print(f"   ⚠️  {msg}")
                self._audit(
                    run_kind="reconcile",
                    schema_name=schema,
                    table_name=table,
                    severity="warn",
                    code="orphan_in_manifest",
                    message=msg,
                )
                severity_counts["warn"] += 1

        # 5. 反向檢查 B：DB 有但既無 backup_state 也不在排除清單的新表
        for schema, table in all_tables_in_db:
            qualified = f"{schema}.{table}"
            if (
                qualified not in self._exclude_tables
                and schema not in self._exclude_schemas
                and qualified not in self._archive_covered
                and qualified not in state_table_set
                and schema in self._static_schemas  # 即時表本質上每天重新 dump，不強制記錄 state
            ):
                msg = f"新發現靜態表 {qualified}（尚無 backup_state 記錄）"
                print(f"   ℹ️  {msg}")
                self._audit(
                    run_kind="reconcile",
                    schema_name=schema,
                    table_name=table,
                    severity="info",
                    code="new_table_detected",
                    message=msg,
                )
                severity_counts["info"] += 1

        # 5.5 Archive freshness check（archive.py 是否 silent-fail）
        archive_critical = self._check_archive_freshness()
        severity_counts["critical"] += archive_critical

        # 6. Flush audit
        flushed = self._flush_audit()
        print(f"   audit flush: {flushed} 筆")

        # 7. 人類可讀摘要
        print(f"\n   📊 Reconcile 摘要 (針對 {date_str}):")
        print(f"      ✓ ok:       {severity_counts['ok']}")
        print(f"      ℹ️  info:     {severity_counts['info']}")
        print(f"      ⚠️  warn:     {severity_counts['warn']}")
        print(f"      ❌ critical: {severity_counts['critical']}")

        return severity_counts

    def _get_all_backup_state_tables(self) -> set[str]:
        """讀 metadata.backup_state，回傳所有已知 schema.table 集合。"""
        if not self.db_conn:
            return set()
        try:
            with self._cursor() as cur:
                cur.execute("SELECT schema_name, table_name FROM metadata.backup_state")
                return {f"{row[0]}.{row[1]}" for row in cur.fetchall()}
        except Exception as exc:
            print(f"   ⚠️  無法讀取 backup_state: {exc}")
            return set()

    # ──────────────────────────────────────────────
    # 主入口
    # ──────────────────────────────────────────────

    def run(self) -> dict:
        """執行三機器人備份流程（A → B → C），各自捕捉例外互不影響。

        Returns:
            dict: 各機器人的統計結果
        """
        if self.dry_run:
            print("🔶 DRY_RUN=true：只記錄 log，不實際 COPY / upload")

        print(f"\n{'=' * 60}")
        print(f"🗄️  Supabase Backup 任務啟動")
        print(f"{'=' * 60}")
        print(f"   時間:    {_now_taipei().strftime('%Y-%m-%d %H:%M:%S %Z')}")
        print(f"   S3:      {config.S3_BUCKET or '(未設定)'}")
        print(f"   DB:      {'已連線' if self.db_conn else '(未連線)'}")
        print(f"   DRY_RUN: {self.dry_run}")

        results: dict[str, Any] = {}

        # ── Robot A ──────────────────────────────
        try:
            results["static"] = self.run_static_snapshot()
        except Exception as exc:
            print(f"\n✗ [Robot A] 未預期例外: {exc}")
            results["static"] = {"error": str(exc)}

        # ── Robot B ──────────────────────────────
        try:
            results["realtime"] = self.run_realtime_snapshot()
        except Exception as exc:
            print(f"\n✗ [Robot B] 未預期例外: {exc}")
            results["realtime"] = {"error": str(exc)}

        # ── Robot C ──────────────────────────────
        try:
            results["reconcile"] = self.run_reconcile()
        except Exception as exc:
            print(f"\n✗ [Robot C] 未預期例外: {exc}")
            results["reconcile"] = {"error": str(exc)}

        # ── 最終摘要 ─────────────────────────────
        print(f"\n{'=' * 60}")
        print(f"📊 Supabase Backup 完成")
        print(f"{'=' * 60}")

        s = results.get("static", {})
        r = results.get("realtime", {})
        c = results.get("reconcile", {})

        print(f"   [Robot A] 靜態 snapshot: "
              f"上傳 {s.get('uploaded', '?')} | "
              f"跳過 {s.get('skipped', '?')} | "
              f"失敗 {s.get('failed', '?')}")
        print(f"   [Robot B] 即時 snapshot: "
              f"上傳 {r.get('uploaded', '?')} | "
              f"空表 {r.get('empty', '?')} | "
              f"跳過 {r.get('skipped', '?')} | "
              f"失敗 {r.get('failed', '?')}")
        print(f"   [Robot C] Reconcile: "
              f"ok={c.get('ok', '?')} | "
              f"info={c.get('info', '?')} | "
              f"warn={c.get('warn', '?')} | "
              f"critical={c.get('critical', '?')}")
        print(f"{'=' * 60}")

        # 關閉 DB 連線
        if self.db_conn:
            try:
                self.db_conn.close()
            except Exception:
                pass

        return results


# ────────────────────────────────────────────────────────────────
# CLI entry point
# ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    task = BackupSupabaseTask()
    task.run()
