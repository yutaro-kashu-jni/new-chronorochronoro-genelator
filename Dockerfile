FROM mcr.microsoft.com/playwright/python:v1.53.0-noble

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libxml2-dev libxslt1-dev zlib1g-dev libjpeg-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# Playwright と Chromium はベースイメージに同梱（追加インストール不要）

COPY . .

ENTRYPOINT ["python", "cloud_main.py"]
