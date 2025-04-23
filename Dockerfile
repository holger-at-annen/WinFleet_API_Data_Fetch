FROM python:3.11-slim

WORKDIR /app

# Show current directory contents for debugging
RUN pwd && ls -la

# Copy requirements first
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create logs directory
RUN mkdir -p /app/logs && chmod 755 /app/logs

# Copy Python files from local app directory
COPY app/*.py ./

# Show copied files for debugging
RUN pwd && ls -la

CMD ["python", "main.py"]
