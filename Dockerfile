# GFW browser assets require Linux-native Tippecanoe and PMTiles. Build pinned
# binaries so production never depends on a developer workstation toolchain.
FROM ubuntu:24.04 AS tippecanoe-builder
ARG TIPPECANOE_VERSION=2.79.0
RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        build-essential ca-certificates git libsqlite3-dev zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*
RUN git clone --depth 1 --branch "${TIPPECANOE_VERSION}" \
        https://github.com/felt/tippecanoe.git /src/tippecanoe \
    && make -C /src/tippecanoe -j"$(nproc)" \
    && install -Dm755 /src/tippecanoe/tippecanoe /out/usr/local/bin/tippecanoe

FROM golang:1.23-bookworm AS pmtiles-builder
ARG PMTILES_VERSION=v1.24.1
RUN GOBIN=/out/usr/local/bin go install \
    "github.com/protomaps/go-pmtiles@${PMTILES_VERSION}"

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

COPY --from=tippecanoe-builder /out/usr/local/bin/tippecanoe /usr/local/bin/tippecanoe
COPY --from=pmtiles-builder /out/usr/local/bin/go-pmtiles /usr/local/bin/pmtiles
# Fail image creation immediately if the fixed production toolchain is broken.
RUN tippecanoe --version && pmtiles version

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
