FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create logs directory
RUN mkdir -p /app/logs

COPY app/main.py .
COPY app/backup.py .
COPY app/logging_config.py .
COPY app/log_cleanup.py .

# Set proper permissions
RUN chmod 755 /app/logs

CMD ["python", "main.py"]
