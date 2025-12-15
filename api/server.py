"""
API Server

提供 HTTP API 下載收集的資料，使用 API Key 認證。
"""

import os
import json
from pathlib import Path
from functools import wraps
from datetime import datetime

from flask import Flask, jsonify, request, send_file, abort

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
                '/api/download/<collector>/<filename>': 'Download specific file'
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
                    file_count = len(list(item.glob('*.json')))
                    collectors.append({
                        'name': item.name,
                        'file_count': file_count
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

        files = []
        for f in sorted(collector_dir.glob('*.json'), reverse=True):
            stat = f.stat()
            files.append({
                'filename': f.name,
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

        # 找最新的檔案
        files = sorted(collector_dir.glob('*.json'), reverse=True)
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

        return jsonify({
            'filename': latest_file.name,
            'data': data
        })

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
            datetime.strptime(date, '%Y-%m-%d')
        except ValueError:
            return jsonify({
                'error': 'Invalid date',
                'message': 'Date format should be YYYY-MM-DD'
            }), 400

        # 找該日期的檔案
        pattern = f'*{date}*.json'
        files = list(collector_dir.glob(pattern))

        if not files:
            return jsonify({
                'error': 'No data',
                'message': f'No data found for date {date}'
            }), 404

        # 回傳檔案列表
        result = []
        for f in sorted(files, reverse=True):
            result.append({
                'filename': f.name,
                'size': f.stat().st_size
            })

        return jsonify({
            'collector': collector,
            'date': date,
            'files': result,
            'total': len(result)
        })

    @app.route('/api/download/<collector>/<filename>')
    @require_api_key
    def download_file(collector, filename):
        """下載特定檔案"""
        # 防止路徑遍歷攻擊
        if '..' in filename or filename.startswith('/'):
            return jsonify({
                'error': 'Invalid filename',
                'message': 'Invalid filename'
            }), 400

        file_path = config.LOCAL_DATA_DIR / collector / filename

        if not file_path.exists():
            return jsonify({
                'error': 'Not found',
                'message': f'File not found: {filename}'
            }), 404

        return send_file(
            file_path,
            mimetype='application/json',
            as_attachment=True,
            download_name=filename
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
