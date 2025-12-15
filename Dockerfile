# Data Collectors
FROM python:3.11-slim

# 設定時區為台灣
ENV TZ=Asia/Taipei
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# 設定工作目錄
WORKDIR /app

# 先複製依賴檔案（利用 Docker cache）
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 複製程式碼（.dockerignore 會排除 .env 等敏感檔案）
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
