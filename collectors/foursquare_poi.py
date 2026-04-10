"""
Foursquare OS Places POI 收集器

從 HuggingFace 取得 Foursquare OS Places 台灣 POI 資料，每月更新一次。
使用 DuckDB 直接查詢遠端 Parquet，篩選 country='TW'。

資料來源：https://huggingface.co/datasets/foursquare/fsq-os-places
授權：Apache 2.0

注意：此 collector 執行時間較長（下載 + 解析約 5-10 分鐘），
      已加入 BACKGROUND_COLLECTORS 避免阻塞高頻 collector。
"""

from datetime import datetime

import config
from .base import BaseCollector


# Foursquare Level 1 → 簡化中文大類
LEVEL1_CATEGORY_MAP = {
    'Arts and Entertainment': '藝文娛樂',
    'Business and Professional Services': '商業服務',
    'Community and Government': '社區政府',
    'Dining and Drinking': '餐飲',
    'Event': '活動',
    'Health and Medicine': '醫療健康',
    'Landmarks and Outdoors': '地標戶外',
    'Retail': '零售購物',
    'Sports and Recreation': '運動休閒',
    'Travel and Transportation': '交通旅遊',
}

# 台灣 bounding box（含外島）
TW_BBOX = {
    'lat_min': 21.5,
    'lat_max': 25.5,
    'lng_min': 119.0,
    'lng_max': 122.5,
}


class FoursquarePOICollector(BaseCollector):
    """Foursquare OS Places POI 收集器（每月一次）"""

    name = "foursquare_poi"
    interval_minutes = config.FOURSQUARE_POI_INTERVAL

    def __init__(self):
        super().__init__()
        self.hf_token = config.HF_TOKEN

        if not self.hf_token:
            raise ValueError("HF_TOKEN 未設定（需 HuggingFace token 存取 gated dataset）")

        # lazy import，僅在需要時載入
        try:
            import duckdb
            self._duckdb = duckdb
        except ImportError:
            raise ImportError("需安裝 duckdb：pip3 install duckdb")

    def _get_latest_release(self) -> str:
        """取得 HuggingFace 上最新的 release dt"""
        con = self._duckdb.connect()
        try:
            con.execute("INSTALL httpfs; LOAD httpfs;")
            con.execute(f"SET hf_token='{self.hf_token}';")

            # 列出所有 release 目錄
            result = con.execute("""
                SELECT DISTINCT regexp_extract(filename, 'dt=([0-9-]+)', 1) as dt
                FROM parquet_scan(
                    'hf://datasets/foursquare/fsq-os-places/release/dt=*/places/parquet/*.parquet',
                    filename=true
                )
                ORDER BY dt DESC
                LIMIT 1
            """).fetchone()

            return result[0] if result else None
        except Exception:
            # fallback：用已知的近期 release
            return None
        finally:
            con.close()

    def _fetch_categories(self, con, dt: str) -> dict:
        """下載 categories 表並建立 ID → (level1, level2) 映射"""
        base = f"hf://datasets/foursquare/fsq-os-places/release/dt={dt}"

        cats = con.execute(f"""
            SELECT *
            FROM read_parquet('{base}/categories/parquet/*.parquet')
        """).fetch_df()

        cat_map = {}
        for _, row in cats.iterrows():
            cat_id = row.get('category_id', '')

            # 找 level1 和 level2 名稱
            level1 = row.get('level1_category_name', '') or ''
            level2 = row.get('level2_category_name', '') or ''

            # 映射到簡化中文
            category = LEVEL1_CATEGORY_MAP.get(level1, level1)
            subcategory = level2 if level2 else None

            cat_map[cat_id] = (category, subcategory)

        return cat_map

    def _fetch_tw_places(self, con, dt: str) -> list:
        """從 HuggingFace 下載台灣 POI 資料"""
        base = f"hf://datasets/foursquare/fsq-os-places/release/dt={dt}"
        bb = TW_BBOX

        df = con.execute(f"""
            SELECT
                fsq_place_id,
                name,
                latitude,
                longitude,
                address,
                locality,
                region,
                postcode,
                tel,
                website,
                fsq_category_ids,
                fsq_category_labels,
                date_refreshed,
                date_closed,
                email,
                facebook_id,
                instagram,
                twitter
            FROM read_parquet('{base}/places/parquet/*.parquet')
            WHERE country = 'TW'
              AND latitude BETWEEN {bb['lat_min']} AND {bb['lat_max']}
              AND longitude BETWEEN {bb['lng_min']} AND {bb['lng_max']}
        """).fetch_df()

        return df

    def collect(self) -> dict:
        """收集 Foursquare OS Places 台灣 POI"""
        fetch_time = datetime.now()

        # 建立 DuckDB 連線
        con = self._duckdb.connect()
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute(f"SET hf_token='{self.hf_token}';")

        try:
            # 1. 嘗試取得最新 release
            print("   正在查詢 HuggingFace 最新 release...")
            dt = config.FOURSQUARE_POI_RELEASE_DT

            if not dt:
                print("   ⚠ 未設定 FOURSQUARE_POI_RELEASE_DT，嘗試自動偵測...")
                dt = self._get_latest_release()

            if not dt:
                raise ValueError("無法取得 release 日期，請設定 FOURSQUARE_POI_RELEASE_DT")

            print(f"   使用 release: dt={dt}")

            # 2. 下載分類表
            print("   正在下載分類表...")
            cat_map = self._fetch_categories(con, dt)
            print(f"   分類表: {len(cat_map)} 個分類")

            # 3. 下載台灣 POI
            print("   正在下載台灣 POI（可能需要數分鐘）...")
            df = self._fetch_tw_places(con, dt)
            total = len(df)
            print(f"   台灣 POI: {total} 筆")

            # 4. 去重（已知 ~19% 重複）
            before_dedup = total
            df = df.drop_duplicates(subset=['fsq_place_id'], keep='last')
            after_dedup = len(df)
            dup_count = before_dedup - after_dedup
            if dup_count > 0:
                print(f"   去重: 移除 {dup_count} 筆重複 ({dup_count/before_dedup*100:.1f}%)")

            # 5. 轉換為輸出格式
            records = []
            for _, row in df.iterrows():
                # 解析分類
                cat_ids = row.get('fsq_category_ids')
                if cat_ids is not None and len(cat_ids) > 0:
                    first_cat = cat_ids[0] if isinstance(cat_ids, list) else str(cat_ids).split(',')[0].strip()
                    category, subcategory = cat_map.get(first_cat, ('其他', None))
                    cat_ids_list = list(cat_ids) if isinstance(cat_ids, list) else [str(cat_ids)]
                else:
                    category, subcategory = '其他', None
                    cat_ids_list = []

                lat = row.get('latitude')
                lng = row.get('longitude')

                # 額外資訊放 properties
                props = {}
                for field in ['email', 'facebook_id', 'instagram', 'twitter', 'postcode']:
                    val = row.get(field)
                    if val is not None and str(val).strip():
                        props[field] = str(val).strip()

                records.append({
                    'fsq_place_id': row['fsq_place_id'],
                    'name': row.get('name'),
                    'category': category,
                    'subcategory': subcategory,
                    'city': row.get('locality'),
                    'district': row.get('region'),
                    'address': row.get('address'),
                    'latitude': float(lat) if lat is not None else None,
                    'longitude': float(lng) if lng is not None else None,
                    'tel': row.get('tel'),
                    'website': row.get('website'),
                    'fsq_category_ids': cat_ids_list,
                    'date_refreshed': str(row.get('date_refreshed')) if row.get('date_refreshed') else None,
                    'date_closed': str(row.get('date_closed')) if row.get('date_closed') else None,
                    'properties': props,
                })

            # 統計
            open_count = sum(1 for r in records if not r['date_closed'] or r['date_closed'] == 'None')
            cat_stats = {}
            for r in records:
                cat_stats[r['category']] = cat_stats.get(r['category'], 0) + 1

            print(f"   營業中: {open_count} / {len(records)}")
            print(f"   分類統計: {dict(sorted(cat_stats.items(), key=lambda x: -x[1]))}")

            return {
                'fetch_time': fetch_time.isoformat(),
                'release_dt': dt,
                'total_raw': before_dedup,
                'duplicates_removed': dup_count,
                'total_poi': len(records),
                'open_poi': open_count,
                'category_stats': cat_stats,
                'data': records,
            }

        finally:
            con.close()
