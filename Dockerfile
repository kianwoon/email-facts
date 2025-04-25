# Dockerfile
# Use Python 3.11 slim base image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy requirements file
COPY requirements.txt .

# Install dependencies from requirements file
RUN pip install --no-cache-dir -r requirements.txt

# Remove the old individual pip install command
# RUN pip install --no-cache-dir \
#      duckdb>=0.9.0 \
#      fastapi \
#      uvicorn \
#      "pyiceberg[pyarrow]" \
#      s3fs

# Copy the application code
COPY rag.py .

# Expose the port the app runs on (matching rag.py)
EXPOSE 8080

# Command to run the application (matching rag.py)
CMD ["uvicorn", "rag:app", "--host", "0.0.0.0", "--port", "8080"]