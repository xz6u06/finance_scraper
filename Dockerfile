# 使用官方 Python 3.11 Slim 版本作為基底
FROM python:3.11-slim-bookworm

# 設定工作目錄
WORKDIR /app

# 1. 安裝系統依賴
RUN apt-get update && apt-get install -u -y --no-install-recommends \
    wget \
    gnupg \
    ca-certificates \
    curl \
    unzip \
    libglib2.0-0 \
    libnss3 \
    libgconf-2-4 \
    libfontconfig1 \
    fonts-liberation \
    libasound2 \
    libgbm1 \
    libvulkan1 \
    xdg-utils \
    tzdata \
    && rm -rf /var/lib/apt/lists/*


# 2. 安裝 Google Chrome (使用較新的 keyring 方式)
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | gpg --dearmor -o /usr/share/keyrings/google-chrome.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-chrome.gpg] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update \
    && apt-get install -y google-chrome-stable \
    && rm -rf /var/lib/apt/lists/*

# 3. 安裝 uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# 4. 複製依賴定義檔
COPY pyproject.toml uv.lock ./

# 5. 安裝 Python 套件
RUN uv sync --frozen

# 6. [修改] 複製程式碼與資料源
# 原本是 COPY src ./src 和 COPY source ./source
COPY src ./src
COPY input ./input

# [修改] 建立輸出目錄 (配合您的新結構)
RUN mkdir -p output/morningstar_ET

# 7. 設定環境變數
ENV PATH="/app/.venv/bin:$PATH"

# 8. [修改] 啟動指令
# 路徑要多一層 morningstar，且檔名若有變更請一併修正
# 原本: src/main/python/earnings_scraper.py (根據你之前的範例)
# 現在圖片顯示: src/morningstar/earnings_scraper.py
CMD ["uv", "run", "src/morningstar/earnings_scraper.py"]