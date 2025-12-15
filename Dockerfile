# Data Collectors
FROM python:3.11-slim

# 設定工作目錄
WORKDIR /app

# 安裝依賴
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 複製程式碼
COPY . .

# 建立資料目錄
RUN mkdir -p /tmp/data

# 設定環境變數
ENV ZEABUR=true
ENV PYTHONUNBUFFERED=1

# 健康檢查
HEALTHCHECK --interval=5m --timeout=10s --start-period=30s \
    CMD python -c "import config; config.validate_config()" || exit 1

# 執行
CMD ["python", "main.py"]
