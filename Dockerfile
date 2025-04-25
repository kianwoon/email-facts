# Dockerfile
FROM python:3.11-slim

# 1) Install DuckDB (with Iceberg & HTTPFS bundled), FastAPI, Uvicorn, PyIceberg, S3FS
RUN pip install --no-cache-dir \
      duckdb \
      fastapi \
      uvicorn \
      pyiceberg \
      s3fs

# 2) Copy your app
WORKDIR /app
COPY rag.py .

# 3) Expose and run
EXPOSE 8080
CMD ["uvicorn", "rag:app", "--host", "0.0.0.0", "--port", "8080"]