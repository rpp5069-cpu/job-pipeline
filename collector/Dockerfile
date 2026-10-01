FROM python:3.11-slim

WORKDIR /app

# Copy and install only what the collector needs
# Removed 'collector/' prefix because we are building from inside that folder
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy collector logic
# Removed 'collector/' prefix
COPY collector.py .
CMD ["python", "collector.py"]