# rag.py
import os
import duckdb
import pyarrow as pa
from fastapi import FastAPI, HTTPException, Body
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, IntegerType
from dotenv import load_dotenv
from typing import List, Dict, Any

# Load environment variables
load_dotenv()

# Configuration
# Read variables using the names set in Koyeb
R2_ENDPOINT_URL = os.getenv("AWS_S3_ENDPOINT")
R2_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
DUCKDB_S3_REGION = os.getenv("DUCKDB_S3_REGION", "auto") # Default to auto if not set
ICEBERG_CATALOG_NAME = "my_iceberg_data" # Or any logical name
# Use the AWS_S3_BUCKET variable from Koyeb
ICEBERG_WAREHOUSE_PATH = f"s3://{os.getenv('AWS_S3_BUCKET')}/{ICEBERG_CATALOG_NAME}" # Base path for tables

# Define the Iceberg table schema
iceberg_schema = Schema(
    NestedField(field_id=1, name="id", field_type=StringType(), required=True),
    NestedField(field_id=2, name="fact", field_type=StringType(), required=True),
    NestedField(field_id=3, name="source", field_type=StringType(), required=False),
    schema_id=1,
    identifier_field_ids=[1]
)

# Function to initialize DuckDB connection with Iceberg and S3 extensions
def get_duckdb_connection():
    con = duckdb.connect(database=':memory:', read_only=False)
    try:
        con.sql("INSTALL iceberg;")
        con.sql("LOAD iceberg;")
        con.sql("INSTALL httpfs;") # Required for S3 access
        con.sql("LOAD httpfs;")
        # Configure S3 credentials for DuckDB
        con.sql(f"SET s3_endpoint='{R2_ENDPOINT_URL}';")
        con.sql(f"SET s3_access_key_id='{R2_ACCESS_KEY_ID}';")
        con.sql(f"SET s3_secret_access_key='{R2_SECRET_ACCESS_KEY}';")
        con.sql("SET s3_use_ssl=true;")
        con.sql(f"SET s3_region='{DUCKDB_S3_REGION}';") # Use region from env or 'auto'
    except Exception as e:
        print(f"Error initializing DuckDB extensions or S3 config: {e}")
        raise
    return con

# Function to ensure the Iceberg table exists, loading or creating it via pyiceberg
def ensure_iceberg_table(table_name: str, schema: Schema):
    """Loads or creates the Iceberg table using pyiceberg."""
    table_identifier = f"{ICEBERG_CATALOG_NAME}.{table_name}" # Fully qualified identifier for pyiceberg catalog

    properties = {
        "warehouse": ICEBERG_WAREHOUSE_PATH,
        "s3.endpoint": R2_ENDPOINT_URL,
        "s3.access-key-id": R2_ACCESS_KEY_ID,
        "s3.secret-access-key": R2_SECRET_ACCESS_KEY,
        # Ensure no 'type' or explicit catalog 'uri' is set here
        # Let pyiceberg infer S3FileIO from the 'warehouse' path
    }

    try:
        print(f"Attempting to load catalog with properties: {properties}")
        # Catalog name 'default' is arbitrary here, properties drive behavior
        catalog = load_catalog("default", **properties)
        print(f"Catalog loaded. Attempting to load table: {table_identifier}")
        table = catalog.load_table(table_identifier)
        print(f"Table '{table_identifier}' loaded successfully.")
        return table
    except NoSuchTableError:
        print(f"Table '{table_identifier}' not found. Attempting to create...")
        try:
            # Re-ensure catalog is loaded before create attempt within this block
            catalog = load_catalog("default", **properties)
            table = catalog.create_table(identifier=table_identifier, schema=schema)
            print(f"Table '{table_identifier}' created successfully.")
            return table
        except Exception as create_e:
            print(f"Error creating table '{table_identifier}': {create_e}")
            raise HTTPException(status_code=500, detail=f"Failed to create Iceberg table: {create_e}")
    except Exception as load_e:
        print(f"Error loading catalog or table '{table_identifier}': {load_e}")
        raise HTTPException(status_code=500, detail=f"Failed to load Iceberg table/catalog: {load_e}")


# Initialize FastAPI app
app = FastAPI()

# --- Globals ---
# Initialize DuckDB connection and ensure table at startup (or handle errors)
# Store connection and table reference globally if needed, or manage per-request
try:
    print("Initializing DuckDB connection...")
    db_conn = get_duckdb_connection()
    print("DuckDB connection initialized.")
    print("Ensuring Iceberg table 'email_facts' exists...")
    email_facts_table = ensure_iceberg_table("email_facts", iceberg_schema)
    print("Iceberg table 'email_facts' ensured.")
    # Keep connection open if desired, or close and reopen per request
except Exception as startup_e:
    print(f"FATAL: Startup failed: {startup_e}")
    # You might want to exit or prevent the app from fully starting
    # For now, we'll let FastAPI start but endpoints might fail if db_conn/table is None
    db_conn = None
    email_facts_table = None
    # raise # Optionally re-raise to prevent server start on critical failure

# --- API Endpoints ---

@app.post("/add_fact")
async def add_fact(fact_data: Dict[str, Any] = Body(...)):
    """Adds a new fact to the Iceberg table."""
    if not db_conn or not email_facts_table:
        raise HTTPException(status_code=500, detail="Database/Table not initialized")

    required_keys = ["id", "fact"]
    if not all(key in fact_data for key in required_keys):
        raise HTTPException(status_code=400, detail="Missing required keys: id, fact")

    # Prepare data for append (ensure correct types if necessary)
    # PyIceberg append usually works with list of dicts or Arrow table/dataframe
    data_to_append = [fact_data] # Append as a single row

    try:
        print(f"Appending data: {data_to_append}")
        # Use pyiceberg table object to append
        email_facts_table.append(data_to_append)
        print("Data appended successfully via pyiceberg.")

        # Optional: Verify append by querying immediately (might show stale data depending on snapshot timing)
        # result = db_conn.execute(f"SELECT * FROM iceberg_scan('{ICEBERG_WAREHOUSE_PATH}/{ICEBERG_CATALOG_NAME}.email_facts') WHERE id = ?", [fact_data['id']]).fetchall()
        # print(f"Verification query result: {result}")

        return {"message": "Fact added successfully"}
    except Exception as e:
        print(f"Error appending data: {e}")
        # Attempt to get more specific DuckDB error if possible
        try:
            duckdb_error = db_conn.last_error() # Check if DuckDB has a specific error
            if duckdb_error:
                print(f"DuckDB specific error: {duckdb_error}")
        except:
            pass # Ignore if last_error isn't available/applicable
        raise HTTPException(status_code=500, detail=f"Failed to add fact: {e}")


@app.get("/get_fact/{fact_id}")
async def get_fact(fact_id: str):
    """Retrieves a fact by its ID using DuckDB iceberg_scan."""
    if not db_conn:
        raise HTTPException(status_code=500, detail="Database not initialized")

    # Construct the full table path for iceberg_scan
    # Path should be warehouse_path/table_name
    table_name = "email_facts"
    full_table_path = f"{ICEBERG_WAREHOUSE_PATH}/{table_name}"
    query = f"SELECT * FROM iceberg_scan('{full_table_path}') WHERE id = ?"

    try:
        print(f"Executing query: {query} with param: {fact_id}")
        result = db_conn.execute(query, [fact_id]).fetchone()
        print(f"Query result: {result}")
        if result:
            # Assuming result columns match schema: (id, fact, source)
            # Convert result tuple to dict based on schema
            columns = [field.name for field in iceberg_schema.fields]
            return dict(zip(columns, result))
        else:
            raise HTTPException(status_code=404, detail="Fact not found")
    except Exception as e:
        print(f"Error querying fact: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to retrieve fact: {e}")

@app.get("/list_facts")
async def list_facts(limit: int = 10):
    """Lists facts from the Iceberg table using DuckDB iceberg_scan."""
    if not db_conn:
        raise HTTPException(status_code=500, detail="Database not initialized")

    # Construct the full table path for iceberg_scan
    # Path should be warehouse_path/table_name
    table_name = "email_facts"
    full_table_path = f"{ICEBERG_WAREHOUSE_PATH}/{table_name}"
    query = f"SELECT * FROM iceberg_scan('{full_table_path}') LIMIT ?"

    try:
        print(f"Executing query: {query} with limit: {limit}")
        results = db_conn.execute(query, [limit]).fetchall()
        print(f"Query results count: {len(results)}")
        if results:
            columns = [field.name for field in iceberg_schema.fields]
            return [dict(zip(columns, row)) for row in results]
        else:
            return []
    except Exception as e:
        print(f"Error listing facts: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to list facts: {e}")


# Health check endpoint
@app.get("/health")
async def health_check():
    if db_conn and email_facts_table:
        # Basic check: Can we access table metadata?
        try:
            _ = email_facts_table.schema()
            _ = email_facts_table.spec()
            _ = email_facts_table.current_snapshot() # Check if snapshot exists (table not empty/corrupt)
            return {"status": "ok", "message": "Service is healthy, DB connection active, table accessible"}
        except Exception as e:
            print(f"Health check failed during table access: {e}")
            return {"status": "degraded", "message": f"Service might be unhealthy, DB connected but table access error: {e}"}
    elif db_conn:
            return {"status": "degraded", "message": "DB connected, but Iceberg table object is not initialized."}
    else:
            return {"status": "unhealthy", "message": "DB connection failed on startup."}


# --- Main Execution ---
if __name__ == "__main__":
    import uvicorn
    # Check if running in Docker or locally for host binding
    host = os.getenv("DOCKER_HOST", "127.0.0.1") # Default to localhost if not in Docker
    uvicorn.run(app, host=host, port=8080)