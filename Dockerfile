FROM python:3.12-slim
WORKDIR /app

# curl for the control-plane healthcheck
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY fetch_secrets.py .
COPY entrypoint.sh .
COPY src/ ./src/
RUN chmod +x entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]
