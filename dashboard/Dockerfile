FROM python:3.11-slim

WORKDIR /app

# Use the pinned versions in requirements.txt rather than an inline install,
# so bumping a version only requires editing one file, not the Dockerfile too.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy bq_proxy.py from the local dashboard folder
COPY bq_proxy.py .

ENV PORT=8080

# Run your proxy server
CMD ["python", "bq_proxy.py"]
