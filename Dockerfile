FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create directories and set permissions
RUN mkdir -p logs && \
    chown -R 1000:1000 . && \
    chmod -R 755 .

COPY app/main.py .
COPY app/backup.py .
COPY app/logging_config.py .
COPY app/log_cleanup.py .
COPY app/partition_handler.py .

# Set non-root user with UID 1000
USER 1000:1000

CMD ["python", "main.py"]
