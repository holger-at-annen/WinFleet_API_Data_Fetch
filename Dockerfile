FROM python:3.11-slim

WORKDIR /usr/local//app

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create logs directory with proper permissions
RUN mkdir -p /app/logs && chmod 755 /app/logs

# Copy the entire app directory
COPY ./app /app

# Install curl for healthcheck
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

CMD ["python", "main.py"]
