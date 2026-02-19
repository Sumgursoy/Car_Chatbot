FROM python:3.11-slim

WORKDIR /app

# Sistem bağımlılıkları (Crawl4AI/Playwright dahil)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    curl \
    wget \
    gnupg \
    libnss3 \
    libatk-bridge2.0-0 \
    libdrm2 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    libpango-1.0-0 \
    libcairo2 \
    libxshmfence1 \
    && rm -rf /var/lib/apt/lists/*

# Python bağımlılıkları
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Crawl4AI browser kurulumu
RUN crawl4ai-setup

# Uygulama dosyaları
COPY . .

# Streamlit ayarları
RUN mkdir -p /root/.streamlit
RUN echo '[server]\nheadless = true\nport = 8501\nenableCORS = false\nenableXsrfProtection = false\n\n[theme]\nbase = "dark"\nprimaryColor = "#667eea"\nbackgroundColor = "#0f0c29"\nsecondaryBackgroundColor = "#1a1a2e"\ntextColor = "#ffffff"\n' > /root/.streamlit/config.toml

# Loglar dizini
RUN mkdir -p /app/logs

EXPOSE 8501 8000
