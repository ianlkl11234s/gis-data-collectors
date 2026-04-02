"""
API Server

提供 HTTP API 下載收集的資料，使用 API Key 認證。
支援本地資料與 S3 歸檔資料的讀取。
"""

import gc
import os
import json
from pathlib import Path
from functools import wraps
from datetime import datetime

from flask import Flask, jsonify, request, send_file, abort, Response

import config


# S3 儲存實例（延遲初始化）
_s3_storage = None


def get_s3_storage():
    """取得 S3 儲存實例（延遲初始化）"""
    global _s3_storage
    if _s3_storage is None and config.S3_BUCKET:
        try:
            from storage.s3 import S3Storage
            _s3_storage = S3Storage()
        except Exception as e:
            print(f"⚠️  S3 初始化失敗: {e}")
    return _s3_storage


def require_api_key(f):
    """API Key 認證裝飾器"""
    @wraps(f)
    def decorated(*args, **kwargs):
        api_key = request.headers.get('X-API-Key') or request.args.get('api_key')

        if not config.API_KEY:
            # 未設定 API Key 時拒絕所有請求
            return jsonify({
                'error': 'API not configured',
                'message': 'API_KEY environment variable is not set'
            }), 503

        if not api_key:
            return jsonify({
                'error': 'Unauthorized',
                'message': 'Missing API key. Use X-API-Key header or api_key parameter'
            }), 401

        if api_key != config.API_KEY:
            return jsonify({
                'error': 'Forbidden',
                'message': 'Invalid API key'
            }), 403

        return f(*args, **kwargs)
    return decorated


def create_app():
    """建立 Flask 應用程式"""
    app = Flask(__name__)

    @app.route('/')
    def index():
        """首頁 - 不需要認證"""
        s3 = get_s3_storage()
        return jsonify({
            'service': 'Data Collectors API',
            'version': '1.1.0',
            'status': 'running',
            'storage': {
                'local': str(config.LOCAL_DATA_DIR),
                's3': config.S3_BUCKET if s3 else None,
                's3_region': config.S3_REGION if s3 else None
            },
            'endpoints': {
                '/health': 'Health check (no auth)',
                '/api/collectors': 'List available collectors',
                '/api/data/<collector>': 'List data files for collector',
                '/api/data/<collector>/latest': 'Get latest data',
                '/api/data/<collector>/<date>': 'Get data by date (YYYY-MM-DD)',
                '/api/data/<collector>/dates': 'List available dates (from S3)',
                '/api/download/<collector>/<path>': 'Download file (e.g., 2025/12/16/weather_1443.json)',
                '/api/archive/status': 'Archive status and statistics'
            },
            'auth': 'Use X-API-Key header or api_key query parameter',
            'note': 'Data automatically retrieved from S3 if not available locally'
        })

    @app.route('/health')
    def health():
        """健康檢查 - 不需要認證"""
        return jsonify({
            'status': 'healthy',
            'timestamp': datetime.now().isoformat(),
            'data_dir': str(config.LOCAL_DATA_DIR),
            'data_dir_exists': config.LOCAL_DATA_DIR.exists()
        })

    @app.route('/api/collectors')
    @require_api_key
    def list_collectors():
        """列出可用的收集器"""
        data_dir = config.LOCAL_DATA_DIR
        collectors = []

        if data_dir.exists():
            for item in data_dir.iterdir():
                if item.is_dir():
                    # 使用遞迴 glob 搜尋所有 JSON 檔案（排除 latest.json）
                    all_files = [f for f in item.glob('**/*.json') if f.name != 'latest.json']
                    collectors.append({
                        'name': item.name,
                        'file_count': len(all_files),
                        'has_latest': (item / 'latest.json').exists()
                    })

        return jsonify({
            'collectors': collectors,
            'data_dir': str(data_dir)
        })

    @app.route('/api/data/<collector>')
    @require_api_key
    def list_data(collector):
        """列出收集器的資料檔案"""
        collector_dir = config.LOCAL_DATA_DIR / collector

        if not collector_dir.exists():
            return jsonify({
                'error': 'Not found',
                'message': f'Collector "{collector}" not found'
            }), 404

        # 使用遞迴 glob 搜尋所有 JSON 檔案（排除 latest.json）
        files = []
        for f in sorted(collector_dir.glob('**/*.json'), key=lambda x: x.stat().st_mtime, reverse=True):
            if f.name == 'latest.json':
                continue
            stat = f.stat()
            # 計算相對路徑以顯示日期結構
            rel_path = f.relative_to(collector_dir)
            files.append({
                'filename': f.name,
                'path': str(rel_path),
                'size': stat.st_size,
                'modified': datetime.fromtimestamp(stat.st_mtime).isoformat()
            })

        return jsonify({
            'collector': collector,
            'files': files,
            'total': len(files)
        })

    @app.route('/api/data/<collector>/latest')
    @require_api_key
    def get_latest(collector):
        """取得最新的資料"""
        collector_dir = config.LOCAL_DATA_DIR / collector

        if not collector_dir.exists():
            return jsonify({
                'error': 'Not found',
                'message': f'Collector "{collector}" not found'
            }), 404

        # 優先使用 latest.json
        latest_file = collector_dir / 'latest.json'
        if not latest_file.exists():
            # 回退到搜尋最新檔案
            files = sorted(
                [f for f in collector_dir.glob('**/*.json') if f.name != 'latest.json'],
                key=lambda x: x.stat().st_mtime,
                reverse=True
            )
            if not files:
                return jsonify({
                    'error': 'No data',
                    'message': f'No data files found for "{collector}"'
                }), 404
            latest_file = files[0]

        # 根據 Accept header 或 format 參數決定回傳格式
        if request.args.get('format') == 'file':
            return send_file(
                latest_file,
                mimetype='application/json',
                as_attachment=True,
                download_name=latest_file.name
            )

        # 預設回傳 JSON 內容
        with open(latest_file, 'r', encoding='utf-8') as f:
            data = json.load(f)

        response = jsonify({
            'filename': latest_file.name,
            'modified': datetime.fromtimestamp(latest_file.stat().st_mtime).isoformat(),
            'data': data
        })

        # 清理載入的資料並觸發 GC
        del data
        gc.collect()

        return response

    @app.route('/api/data/<collector>/<date>')
    @require_api_key
    def get_by_date(collector, date):
        """取得特定日期的資料（自動從本地或 S3 讀取）"""
        # 驗證日期格式
        try:
            parsed_date = datetime.strptime(date, '%Y-%m-%d')
        except ValueError:
            return jsonify({
                'error': 'Invalid date',
                'message': 'Date format should be YYYY-MM-DD'
            }), 400

        result = []
        source = 'local'

        # 先嘗試本地
        collector_dir = config.LOCAL_DATA_DIR / collector
        date_dir = collector_dir / parsed_date.strftime('%Y/%m/%d')

        if date_dir.exists():
            files = list(date_dir.glob('*.json'))
            for f in sorted(files, key=lambda x: x.stat().st_mtime, reverse=True):
                rel_path = f.relative_to(collector_dir)
                result.append({
                    'filename': f.name,
                    'path': str(rel_path),
                    'size': f.stat().st_size,
                    'modified': datetime.fromtimestamp(f.stat().st_mtime).isoformat(),
                    'source': 'local'
                })

        # 如果本地沒有，嘗試從 S3 tar.gz 歸檔讀取
        if not result:
            s3 = get_s3_storage()
            if s3:
                # 新格式：tar.gz 歸檔
                archive_files = s3.list_archive_files(collector, date)
                if archive_files:
                    result = archive_files
                    source = 's3_archive'
                else:
                    # 舊格式：個別檔案（向後相容）
                    s3_files = s3.list_files_by_date(collector, date)
                    for f in s3_files:
                        result.append({
                            'filename': f['key'].split('/')[-1],
                            'path': '/'.join(f['key'].split('/')[1:]),
                            'size': f['size'],
                            'modified': f['modified'],
                            'source': 's3'
                        })
                    if result:
                        source = 's3'

        if not result:
            return jsonify({
                'error': 'No data',
                'message': f'No data found for date {date}'
            }), 404

        return jsonify({
            'collector': collector,
            'date': date,
            'files': result,
            'total': len(result),
            'source': source
        })

    @app.route('/api/download/<collector>/<path:filepath>')
    @require_api_key
    def download_file(collector, filepath):
        """下載特定檔案（支援巢狀路徑如 2025/12/16/weather_1443.json）- 自動從本地或 S3 讀取"""
        # 防止路徑遍歷攻擊
        if '..' in filepath:
            return jsonify({
                'error': 'Invalid path',
                'message': 'Invalid file path'
            }), 400

        file_path = config.LOCAL_DATA_DIR / collector / filepath
        filename = filepath.split('/')[-1]

        # 先嘗試本地
        if file_path.exists():
            def generate():
                """串流讀取檔案，避免一次載入整個檔案到記憶體"""
                try:
                    with open(file_path, 'rb') as f:
                        while True:
                            chunk = f.read(8192)  # 每次讀取 8KB
                            if not chunk:
                                break
                            yield chunk
                finally:
                    gc.collect()  # 串流結束後觸發 GC

            return Response(
                generate(),
                mimetype='application/json',
                headers={
                    'Content-Disposition': f'attachment; filename={filename}',
                    'Content-Length': str(file_path.stat().st_size),
                    'X-Data-Source': 'local'
                }
            )

        # 嘗試從 S3 讀取
        s3 = get_s3_storage()
        if s3:
            # 嘗試從 tar.gz 歸檔提取（新格式）
            # filepath 格式: YYYY/MM/DD/collector_HHMM.json
            parts = filepath.split('/')
            if len(parts) == 4:
                date_str = f"{parts[0]}-{parts[1]}-{parts[2]}"
                archive_filename = parts[3]
                content = s3.extract_file_from_archive(collector, date_str, archive_filename)
                if content:
                    gc.collect()
                    return Response(
                        content,
                        mimetype='application/json',
                        headers={
                            'Content-Disposition': f'attachment; filename={filename}',
                            'Content-Length': str(len(content)),
                            'X-Data-Source': 's3_archive'
                        }
                    )

            # 舊格式：個別檔案（向後相容）
            s3_key = f"{collector}/{filepath}"
            content = s3.get_file(s3_key)
            if content:
                gc.collect()
                return Response(
                    content,
                    mimetype='application/json',
                    headers={
                        'Content-Disposition': f'attachment; filename={filename}',
                        'Content-Length': str(len(content)),
                        'X-Data-Source': 's3'
                    }
                )

        return jsonify({
            'error': 'Not found',
            'message': f'File not found: {filepath}'
        }), 404

    @app.route('/api/data/<collector>/dates')
    @require_api_key
    def list_dates(collector):
        """列出收集器有資料的所有日期（從 S3 讀取）"""
        dates = []

        # 從本地取得日期
        collector_dir = config.LOCAL_DATA_DIR / collector
        if collector_dir.exists():
            for year_dir in collector_dir.iterdir():
                if not year_dir.is_dir() or not year_dir.name.isdigit():
                    continue
                for month_dir in year_dir.iterdir():
                    if not month_dir.is_dir():
                        continue
                    for day_dir in month_dir.iterdir():
                        if not day_dir.is_dir():
                            continue
                        date_str = f"{year_dir.name}-{month_dir.name}-{day_dir.name}"
                        if date_str not in dates:
                            dates.append(date_str)

        # 從 S3 取得日期
        s3 = get_s3_storage()
        if s3:
            s3_dates = s3.list_dates(collector)
            for d in s3_dates:
                if d not in dates:
                    dates.append(d)

        dates.sort(reverse=True)

        return jsonify({
            'collector': collector,
            'dates': dates,
            'total': len(dates)
        })

    # ========== FR24 公開 API（不需要 API Key）==========

    @app.after_request
    def add_cors_for_fr24(response):
        """對 /api/fr24/* 路由加 CORS headers"""
        if request.path.startswith('/api/fr24/'):
            response.headers['Access-Control-Allow-Origin'] = '*'
            response.headers['Access-Control-Allow-Methods'] = 'GET, OPTIONS'
            response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
        return response

    @app.route('/api/fr24/manifest')
    def fr24_manifest():
        """FR24 航班資料 manifest — 可用日期 + 每日 last_modified"""
        collector = 'flight_fr24'
        retention_days = 14
        dates_info = {}

        # 從本地取得
        collector_dir = config.LOCAL_DATA_DIR / collector
        if collector_dir.exists():
            for year_dir in collector_dir.iterdir():
                if not year_dir.is_dir() or not year_dir.name.isdigit():
                    continue
                for month_dir in year_dir.iterdir():
                    if not month_dir.is_dir():
                        continue
                    for day_dir in month_dir.iterdir():
                        if not day_dir.is_dir():
                            continue
                        date_str = f"{year_dir.name}-{month_dir.name}-{day_dir.name}"
                        files = list(day_dir.glob('*.json'))
                        if files:
                            last_mod = max(
                                datetime.fromtimestamp(f.stat().st_mtime).isoformat()
                                for f in files
                            )
                            dates_info[date_str] = {
                                'last_modified': last_mod,
                                'file_count': len(files),
                                'source': 'local',
                            }

        # 從 S3 tar.gz 歸檔補充
        s3 = get_s3_storage()
        if s3:
            s3_dates = s3.list_dates(collector)
            for date_str in s3_dates:
                if date_str in dates_info:
                    continue  # 本地已有，不重複
                archive_files = s3.list_archive_files(collector, date_str)
                if archive_files:
                    last_mod = max(f['modified'] for f in archive_files)
                    dates_info[date_str] = {
                        'last_modified': last_mod,
                        'file_count': len(archive_files),
                        'source': 's3_archive',
                    }

        return jsonify({
            'dates': dates_info,
            'retention_days': retention_days,
        })

    @app.route('/api/fr24/urls/<date>/latest')
    def fr24_url_latest(date):
        """取得某日期最新一筆資料的 presigned URL"""
        collector = 'flight_fr24'

        try:
            datetime.strptime(date, '%Y-%m-%d')
        except ValueError:
            return jsonify({'error': 'Invalid date format, use YYYY-MM-DD'}), 400

        s3 = get_s3_storage()
        if not s3:
            return jsonify({'error': 'S3 not configured'}), 503

        files = s3.list_files_by_date(collector, date)
        if not files:
            return jsonify({'error': f'No data for {date}'}), 404

        # 取最新的檔案（依 key 排序，HHMM 格式保證字典序 = 時間序）
        latest = sorted(files, key=lambda f: f['key'])[-1]
        expires_in = 3600
        url = s3.generate_presigned_url(latest['key'], expires_in)

        return jsonify({
            'date': date,
            'key': latest['key'],
            'url': url,
            'expires_in': expires_in,
        })

    @app.route('/api/fr24/urls/<date>')
    def fr24_urls(date):
        """取得某日期所有檔案的 presigned URLs"""
        collector = 'flight_fr24'

        try:
            datetime.strptime(date, '%Y-%m-%d')
        except ValueError:
            return jsonify({'error': 'Invalid date format, use YYYY-MM-DD'}), 400

        s3 = get_s3_storage()
        if not s3:
            return jsonify({'error': 'S3 not configured'}), 503

        files = s3.list_files_by_date(collector, date)
        if not files:
            return jsonify({'error': f'No data for {date}'}), 404

        expires_in = 3600
        file_urls = []
        for f in sorted(files, key=lambda x: x['key']):
            file_urls.append({
                'key': f['key'],
                'url': s3.generate_presigned_url(f['key'], expires_in),
            })

        return jsonify({
            'date': date,
            'files': file_urls,
            'expires_in': expires_in,
        })

    @app.route('/api/fr24/data/<date>/<filename>')
    def fr24_data_proxy(date, filename):
        """FR24 資料代理 — 從本地或 S3 tar.gz 提供個別檔案資料"""
        collector = 'flight_fr24'

        try:
            parsed_date = datetime.strptime(date, '%Y-%m-%d')
        except ValueError:
            return jsonify({'error': 'Invalid date format, use YYYY-MM-DD'}), 400

        if not filename.endswith('.json'):
            return jsonify({'error': 'Invalid filename'}), 400

        # 先嘗試本地
        date_dir = config.LOCAL_DATA_DIR / collector / parsed_date.strftime('%Y/%m/%d')
        local_file = date_dir / filename
        if local_file.exists():
            with open(local_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return jsonify(data)

        # 從 S3 tar.gz 歸檔提取
        s3 = get_s3_storage()
        if s3:
            content = s3.extract_file_from_archive(collector, date, filename)
            if content:
                return Response(
                    content,
                    mimetype='application/json',
                    headers={'X-Data-Source': 's3_archive'}
                )

        return jsonify({'error': f'File not found: {date}/{filename}'}), 404

    # ========== Mini Taipei 公開 API（不需要 API Key）==========

    @app.after_request
    def add_cors_for_mini_taipei(response):
        """對 /api/mini-taipei/* 路由加 CORS headers"""
        if request.path.startswith('/api/mini-taipei/'):
            response.headers['Access-Control-Allow-Origin'] = '*'
            response.headers['Access-Control-Allow-Methods'] = 'GET, OPTIONS'
            response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
        return response

    @app.route('/api/mini-taipei/<system>/index.json')
    def mini_taipei_index(system):
        """取得每日時刻表可用日期清單

        GET /api/mini-taipei/tra/index.json
        GET /api/mini-taipei/thsr/index.json
        """
        if system not in ('tra', 'thsr'):
            return jsonify({'error': 'Invalid system, use tra or thsr'}), 400

        s3_prefix = getattr(config, 'MINI_TAIPEI_S3_PREFIX', 'mini-taipei')
        s3_key = f"{s3_prefix}/{system}/index.json"

        s3 = get_s3_storage()
        if s3:
            data = s3.get_json(s3_key)
            if data:
                return jsonify(data)

        return jsonify({'dates': [], 'latest': None}), 200

    @app.route('/api/mini-taipei/<system>/daily/<date>.json')
    def mini_taipei_daily(system, date):
        """取得指定日期的轉換後時刻表

        GET /api/mini-taipei/tra/daily/2026-03-21.json
        GET /api/mini-taipei/thsr/daily/2026-03-21.json
        """
        if system not in ('tra', 'thsr'):
            return jsonify({'error': 'Invalid system'}), 400

        # 驗證日期格式
        import re
        if not re.match(r'^\d{4}-\d{2}-\d{2}$', date):
            return jsonify({'error': 'Invalid date format, use YYYY-MM-DD'}), 400

        s3_prefix = getattr(config, 'MINI_TAIPEI_S3_PREFIX', 'mini-taipei')
        s3_key = f"{s3_prefix}/{system}/daily/{date}.json"

        s3 = get_s3_storage()
        if s3:
            data = s3.get_json(s3_key)
            if data:
                return jsonify(data)

        return jsonify({'error': f'Schedule not found for {date}'}), 404

    @app.route('/api/mini-taipei/<system>/coverage/<date>.json')
    def mini_taipei_coverage(system, date):
        """取得指定日期的覆蓋率報告

        GET /api/mini-taipei/tra/coverage/2026-03-21.json
        """
        if system not in ('tra',):
            return jsonify({'error': 'Coverage only available for tra'}), 400

        import re
        if not re.match(r'^\d{4}-\d{2}-\d{2}$', date):
            return jsonify({'error': 'Invalid date format'}), 400

        s3_prefix = getattr(config, 'MINI_TAIPEI_S3_PREFIX', 'mini-taipei')
        s3_key = f"{s3_prefix}/{system}/coverage/{date}.json"

        s3 = get_s3_storage()
        if s3:
            data = s3.get_json(s3_key)
            if data:
                return jsonify(data)

        return jsonify({'error': f'Coverage report not found for {date}'}), 404

    @app.route('/api/archive/status')
    @require_api_key
    def archive_status():
        """取得歸檔狀態"""
        try:
            from tasks.archive import ArchiveTask
            task = ArchiveTask()
            status = task.get_archive_status()
            return jsonify(status)
        except Exception as e:
            return jsonify({
                'error': 'Failed to get archive status',
                'message': str(e)
            }), 500

    return app


def run_api_server(host='0.0.0.0', port=8080):
    """啟動 API Server"""
    app = create_app()

    print(f"\n{'=' * 60}")
    print(f"🌐 API Server")
    print(f"{'=' * 60}")
    print(f"   URL: http://{host}:{port}")
    print(f"   認證: {'已啟用' if config.API_KEY else '未設定 (拒絕所有請求)'}")
    print(f"{'=' * 60}")

    # 使用 Flask 內建 server（生產環境建議使用 gunicorn）
    app.run(host=host, port=port, threaded=True)


if __name__ == '__main__':
    run_api_server()
