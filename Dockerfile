# Data Collectors
FROM python:3.11-slim

# 設定時區為台灣
ENV TZ=Asia/Taipei
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# 設定工作目錄
WORKDIR /app

# 系統庫：
#   libeccodes*    cfgrib 讀 GRIB2（global_climate NOAA GFS）必需
#   tesseract-ocr  共機航跡圖左上表格判項次（pla_tracks_vectorize）；只需 eng，
#                  表格是中英雙語但中文行本來就只當雜訊，不裝 chi_tra
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        libeccodes0 libeccodes-data \
        tesseract-ocr tesseract-ocr-eng \
    && rm -rf /var/lib/apt/lists/*

# 先複製依賴檔案（利用 Docker cache）
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 複製程式碼（.dockerignore 會排除 .env 等敏感檔案）
COPY . .

# 建立資料目錄（Zeabur Volume 會掛載到 /data）
RUN mkdir -p /data

# 設定環境變數
ENV ZEABUR=true
ENV PYTHONUNBUFFERED=1

# 開放 API 端口（預設 8080）
EXPOSE 8080

# 健康檢查：探 /health（主迴圈卡死 → 503 → 重啟）。
# 不用 curl（slim 無）、不做 config fallback（會讓卡死的進程仍判 healthy）。
HEALTHCHECK --interval=1m --timeout=10s --start-period=60s --retries=3 \
    CMD python healthcheck.py

# 執行
CMD ["python", "main.py"]
