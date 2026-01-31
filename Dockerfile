FROM python:3.13-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code - add timestamp to bust cache
COPY orderbook.py /app/orderbook.py
RUN echo "Built at $(date)"
COPY config.json .
COPY zmq_subscriber.py .

# Health check: wait for QuestDB to be available
HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=3 \
    CMD curl -f http://questdb:9000 || exit 1

# Run the orderbook ingestion system
CMD ["python", "orderbook.py", "config.json"]
