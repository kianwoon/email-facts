# rag.py
import os
import duckdb
from fastapi import FastAPI

app = FastAPI()

# 1) Connect + install/load extensions
con = duckdb.connect()
con.execute("INSTALL httpfs;")
con.execute("LOAD httpfs;")
con.execute("INSTALL iceberg;")
con.execute("LOAD iceberg;")

# 2) Configure your R2 bucket over S3 API
con.execute(f"SET s3_endpoint='{os.environ['AWS_S3_ENDPOINT']}';")
con.execute("SET s3_region='auto';")
con.execute(f"SET s3_access_key_id='{os.environ['AWS_ACCESS_KEY_ID']}';")
con.execute(f"SET s3_secret_access_key='{os.environ['AWS_SECRET_ACCESS_KEY']}';")
con.execute("SET s3_use_ssl=true;")

# 3) Create the table
# Build the SQL string first
s3_bucket = os.environ["AWS_S3_BUCKET"]
create_table_sql = f"""
CREATE TABLE IF NOT EXISTS email_facts (
  email_id VARCHAR,
  message_id VARCHAR,
  subject VARCHAR,
  sender_name VARCHAR,
  sender_address VARCHAR,
  recipient_addresses VARCHAR[],
  cc_addresses VARCHAR[],
  received_at TIMESTAMP,
  importance VARCHAR,
  sensitivity_level VARCHAR,
  assigned_department VARCHAR,
  content_tags VARCHAR[],
  is_private_content BOOLEAN,
  detected_pii_types VARCHAR[],
  generated_summary VARCHAR,
  extracted_key_points VARCHAR[],
  -- Temporarily commented out for debugging
  -- extracted_entities STRUCT{{
  --   persons: VARCHAR[],
  --   organizations: VARCHAR[],
  --   locations: VARCHAR[]
  -- }},
  intent VARCHAR,
  extraction_method VARCHAR,
  extraction_timestamp TIMESTAMP
)
USING iceberg
LOCATION 's3://{s3_bucket}/email_facts'
"""
# Execute the constructed SQL string
con.execute(create_table_sql)

@app.get("/")
def health():
    cnt = con.execute("SELECT count(*) FROM email_facts").fetchone()[0]
    return {"status": "OK", "rows": cnt}