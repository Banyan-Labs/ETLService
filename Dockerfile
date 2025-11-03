FROM python:3.13-slim
WORKDIR /app
RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN playwright install --with-deps
COPY . .
EXPOSE 8000
ENV PYTHONUNBUFFERED=1
ENV SCRAPY_SETTINGS_MODULE=scraper.nashville.settings
CMD ["gunicorn", "--bind", "0.0.0.0:8000", "app:app"]