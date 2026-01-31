FROM python:3.11-slim

WORKDIR /app

# Install dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements (if exists) or install directly
COPY requirements.txt* ./
RUN if [ -f requirements.txt ]; then pip install --no-cache-dir -r requirements.txt; else \
    pip install --no-cache-dir websockets aiohttp pyzmq; fi

# Copy application files
COPY orderbook.py config.json ./

# Set unbuffered output for real-time logs
ENV PYTHONUNBUFFERED=1

# Run the application
CMD ["python", "orderbook.py", "config.json"]
