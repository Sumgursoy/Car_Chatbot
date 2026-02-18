FROM python:3.11-slim

WORKDIR /app

# Sistem bağımlılıkları
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Python bağımlılıkları
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Uygulama dosyaları
COPY . .

# Streamlit ayarları
RUN mkdir -p /root/.streamlit
RUN echo '[server]\nheadless = true\nport = 8501\nenableCORS = false\nenableXsrfProtection = false\n\n[theme]\nbase = "dark"\nprimaryColor = "#667eea"\nbackgroundColor = "#0f0c29"\nsecondaryBackgroundColor = "#1a1a2e"\ntextColor = "#ffffff"\n' > /root/.streamlit/config.toml

# Loglar dizini
RUN mkdir -p /app/logs

EXPOSE 8501 8000
