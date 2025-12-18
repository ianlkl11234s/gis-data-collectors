"""
API Server

提供 HTTP API 下載收集的資料，使用 API Key 認證。
"""

import gc
import os
import json
from pathlib import Path
from functools import wraps
from datetime import datetime

from flask import Flask, jsonify, request, send_file, abort, Response

import config


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
        return jsonify({
            'service': 'Data Collectors API',
            'version': '1.0.0',
            'status': 'running',
            'endpoints': {
                '/health': 'Health check (no auth)',
                '/api/collectors': 'List available collectors',
                '/api/data/<collector>': 'List data files for collector',
                '/api/data/<collector>/latest': 'Get latest data',
                '/api/data/<collector>/<date>': 'Get data by date (YYYY-MM-DD)',
                '/api/download/<collector>/<path>': 'Download file (e.g., 2025/12/16/weather_1443.json)'
            },
            'auth': 'Use X-API-Key header or api_key query parameter'
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
        """取得特定日期的資料"""
        collector_dir = config.LOCAL_DATA_DIR / collector

        if not collector_dir.exists():
            return jsonify({
                'error': 'Not found',
                'message': f'Collector "{collector}" not found'
            }), 404

        # 驗證日期格式
        try:
            parsed_date = datetime.strptime(date, '%Y-%m-%d')
        except ValueError:
            return jsonify({
                'error': 'Invalid date',
                'message': 'Date format should be YYYY-MM-DD'
            }), 400

        # 根據目錄結構搜尋 (YYYY/MM/DD)
        date_dir = collector_dir / parsed_date.strftime('%Y/%m/%d')

        if date_dir.exists():
            files = list(date_dir.glob('*.json'))
        else:
            # 回退到搜尋所有檔案中包含日期的
            files = []

        if not files:
            return jsonify({
                'error': 'No data',
                'message': f'No data found for date {date}'
            }), 404

        # 回傳檔案列表
        result = []
        for f in sorted(files, key=lambda x: x.stat().st_mtime, reverse=True):
            rel_path = f.relative_to(collector_dir)
            result.append({
                'filename': f.name,
                'path': str(rel_path),
                'size': f.stat().st_size,
                'modified': datetime.fromtimestamp(f.stat().st_mtime).isoformat()
            })

        return jsonify({
            'collector': collector,
            'date': date,
            'files': result,
            'total': len(result)
        })

    @app.route('/api/download/<collector>/<path:filepath>')
    @require_api_key
    def download_file(collector, filepath):
        """下載特定檔案（支援巢狀路徑如 2025/12/16/weather_1443.json）- 使用串流回應"""
        # 防止路徑遍歷攻擊
        if '..' in filepath:
            return jsonify({
                'error': 'Invalid path',
                'message': 'Invalid file path'
            }), 400

        file_path = config.LOCAL_DATA_DIR / collector / filepath

        if not file_path.exists():
            return jsonify({
                'error': 'Not found',
                'message': f'File not found: {filepath}'
            }), 404

        def generate():
            """串流讀取檔案，避免一次載入整個檔案到記憶體"""
            try:
                with open(file_path, 'rb') as f:
                    while chunk := f.read(8192):  # 每次讀取 8KB
                        yield chunk
            finally:
                gc.collect()  # 串流結束後觸發 GC

        return Response(
            generate(),
            mimetype='application/json',
            headers={
                'Content-Disposition': f'attachment; filename={file_path.name}',
                'Content-Length': str(file_path.stat().st_size)
            }
        )

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
