FROM python:3.11-slim

WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create logs directory with proper permissions
RUN mkdir -p /app/logs && chmod 755 /app/logs

# Debug: List contents before copy
RUN pwd && ls -la

# Copy all Python files
COPY ./app/*.py ./

# Debug: List contents after copy
RUN pwd && ls -la

# Install curl for healthcheck
RUN apt-get update && \
    apt-get install -y curl && \
    rm -rf /var/lib/apt/lists/*

CMD ["python", "main.py"]
